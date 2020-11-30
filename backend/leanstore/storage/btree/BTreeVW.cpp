#include "BTree.hpp"
#include "leanstore/concurrency-recovery/CRMG.hpp"
// -------------------------------------------------------------------------------------
#include "gflags/gflags.h"
// -------------------------------------------------------------------------------------
#include <signal.h>
// -------------------------------------------------------------------------------------
using namespace std;
using namespace leanstore::storage;
using OP_RESULT = leanstore::storage::btree::OP_RESULT;
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
namespace btree
{
// -------------------------------------------------------------------------------------
namespace vw
{
struct __attribute__((packed)) Version {
   u8 worker_id : 8;
   u64 tts : 56;
   u64 lsn : 56;
   u8 is_removed : 1;
   u8 is_final : 1;
   Version(u8 worker_id, u64 tts, u64 lsn, bool is_deleted, bool is_final)
       : worker_id(worker_id), tts(tts), lsn(lsn), is_removed(is_deleted), is_final(is_final)
   {
   }
};
static_assert(sizeof(Version) == (2 * sizeof(u64)), "");
// -------------------------------------------------------------------------------------
enum class WAL_LOG_TYPE : u8 { WALInsert, WALUpdate, WALRemove, WALAfterBeforeImage, WALAfterImage, WALLogicalSplit, WALInitPage };
struct WALEntry {
   vw::WAL_LOG_TYPE type;
   vw::Version prev_version;
};
struct WALBeforeAfterImage : WALEntry {
   u16 image_size;
   u8 payload[];
};
struct WALInitPage : WALEntry {
   DTID dt_id;
};
struct WALAfterImage : WALEntry {
   u16 image_size;
   u8 payload[];
};
struct WALLogicalSplit : WALEntry {
   PID parent_pid = -1;
   PID left_pid = -1;
   PID right_pid = -1;
   s32 right_pos = -1;
};
struct WALInsert : WALEntry {
   u16 key_length;
   u16 value_length;
   u8 payload[];
};
struct WALUpdate : WALEntry {
   u16 key_length;
   u16 delta_length;
   u8 payload[];
};
struct WALRemove : WALEntry {
   u16 key_length;
   u16 payload_length;
   u8 payload[];
};
}  // namespace vw
   // -------------------------------------------------------------------------------------
constexpr u64 VW_PAYLOAD_OFFSET = sizeof(vw::Version);
// -------------------------------------------------------------------------------------
// Plan: Value gets an 8-byte version
OP_RESULT BTree::insertVW(u8* key, u16 key_length, u16 value_length_orig, u8* value_orig)
{
   cr::Worker::my().walEnsureEnoughSpace(PAGE_SIZE * 1);
   const u16 value_length = value_length_orig + VW_PAYLOAD_OFFSET;
   while (true) {
      jumpmuTry()
      {
         HybridPageGuard<BTreeNode> leaf_guard;
         findLeafCanJump<OP_TYPE::POINT_INSERT>(leaf_guard, key, key_length);
         // -------------------------------------------------------------------------------------
         auto leaf_ex_guard = ExclusivePageGuard(std::move(leaf_guard));
         s16 pos = leaf_ex_guard->lowerBound<true>(key, key_length);
         if (pos == -1) {
            // Really new
            if (leaf_ex_guard->canInsert(key_length, value_length)) {
               // -------------------------------------------------------------------------------------
               // WAL
               auto wal_entry = leaf_ex_guard.reserveWALEntry<vw::WALInsert>(key_length + value_length_orig);
               wal_entry->type = vw::WAL_LOG_TYPE::WALInsert;
               wal_entry->key_length = key_length;
               wal_entry->value_length = value_length_orig;
               wal_entry->prev_version.lsn = 0;
               std::memcpy(wal_entry->payload, key, key_length);
               std::memcpy(wal_entry->payload + key_length, value_orig, value_length_orig);
               wal_entry.submit();
               // -------------------------------------------------------------------------------------
               u8 value[value_length];
               new (value) vw::Version(myWorkerID(), myTTS(), wal_entry.lsn, false, true);
               std::memcpy(value + VW_PAYLOAD_OFFSET, value_orig, value_length_orig);
               // -------------------------------------------------------------------------------------
               leaf_ex_guard->insert(key, key_length, value, value_length);
               jumpmu_return OP_RESULT::OK;
            }
         } else {
            auto& version = *reinterpret_cast<vw::Version*>(leaf_ex_guard->getPayload(pos));
            if (isVisibleForMe(version.worker_id, version.tts)) {
               if (version.is_removed) {
                  if (leaf_ex_guard->canInsert(key_length, value_length)) {
                     // -------------------------------------------------------------------------------------
                     // WAL
                     auto wal_entry = leaf_ex_guard.reserveWALEntry<vw::WALInsert>(key_length + value_length_orig);
                     wal_entry->type = vw::WAL_LOG_TYPE::WALInsert;
                     wal_entry->key_length = key_length;
                     wal_entry->value_length = value_length_orig;
                     // Link to the previous LSN
                     wal_entry->prev_version = version;
                     // -------------------------------------------------------------------------------------
                     std::memcpy(wal_entry->payload, key, key_length);
                     std::memcpy(wal_entry->payload + key_length, value_orig, value_length_orig);
                     wal_entry.submit();
                     // -------------------------------------------------------------------------------------
                     u8 value[value_length];
                     std::memcpy(value + VW_PAYLOAD_OFFSET, value_orig, value_length_orig);
                     new (value) vw::Version(myWorkerID(), myTTS(), wal_entry.lsn, false, false);
                     // -------------------------------------------------------------------------------------
                     leaf_ex_guard->insert(key, key_length, value, value_length);
                     jumpmu_return OP_RESULT::OK;
                  }
               } else {
                  jumpmu_return OP_RESULT::DUPLICATE;
               }
            } else {
               jumpmu_return OP_RESULT::ABORT_TX;
            }
         }
         // -------------------------------------------------------------------------------------
         // Release lock
         leaf_guard = std::move(leaf_ex_guard);
         leaf_guard.kill();
         // -------------------------------------------------------------------------------------
         trySplit(*leaf_guard.bf);
         // -------------------------------------------------------------------------------------
         jumpmu_continue;
      }
      jumpmuCatch() {}
   }
}
// -------------------------------------------------------------------------------------
OP_RESULT BTree::lookupVW(u8* key, u16 key_length, function<void(const u8*, u16)> payload_callback)
{
   while (true) {
      jumpmuTry()
      {
         HybridPageGuard<BTreeNode> leaf;
         findLeafCanJump<OP_TYPE::POINT_READ>(leaf, key, key_length);
         // -------------------------------------------------------------------------------------
         s16 pos = leaf->lowerBound<true>(key, key_length);
         if (pos != -1) {
            auto version = *reinterpret_cast<vw::Version*>(leaf->getPayload(pos));
            u8* payload = leaf->getPayload(pos) + VW_PAYLOAD_OFFSET;
            u16 payload_length = leaf->getPayloadLength(pos) - VW_PAYLOAD_OFFSET;
            leaf.recheck_done();
            if (isVisibleForMe(version.worker_id, version.tts)) {
               if (version.is_removed) {
                  jumpmu_return OP_RESULT::NOT_FOUND;
               } else {
                  payload_callback(payload, payload_length);
                  leaf.recheck_done();
                  jumpmu_return OP_RESULT::OK;
               }
            } else {
               if (version.is_final) {
                  jumpmu_return OP_RESULT::NOT_FOUND;
               } else {
                  JMUW<std::unique_ptr<u8[]>> reconstructed_payload = std::make_unique<u8[]>(payload_length);
                  std::memcpy(reconstructed_payload->get(), payload, payload_length);
                  reconstructTupleVW(reconstructed_payload.obj, payload_length, version.worker_id, version.lsn);
                  if (payload_length == 0) {
                     jumpmu_return OP_RESULT::NOT_FOUND;
                  } else {
                     payload_callback(reconstructed_payload->get(), payload_length);
                     jumpmu_return OP_RESULT::OK;
                  }
               }
            }
         } else {
            leaf.recheck_done();
            jumpmu_return OP_RESULT::NOT_FOUND;
         }
      }
      jumpmuCatch() {}
   }
}
// -------------------------------------------------------------------------------------
void BTree::reconstructTupleVW(std::unique_ptr<u8[]>& payload, u16& payload_length, u8 next_worker_id, u64 next_lsn)
{
   [[maybe_ununsed]] u64 version_depth = 1;
   bool flag = true;
   while (flag) {
      cr::Worker::my().getWALDTEntry(next_worker_id, next_lsn, [&](u8* entry) {
         auto& wal_entry = *reinterpret_cast<vw::WALEntry*>(entry);
         // raise(SIGTRAP);
         switch (wal_entry.type) {
            case vw::WAL_LOG_TYPE::WALRemove: {
               auto& remove_entry = *reinterpret_cast<vw::WALRemove*>(entry);
               payload_length = remove_entry.payload_length;
               payload.reset(new u8[payload_length]);
               std::memcpy(payload.get(), remove_entry.payload, payload_length);
               break;
            }
            case vw::WAL_LOG_TYPE::WALInsert: {
               payload.reset();
               payload_length = 0;
               break;
            }
            case vw::WAL_LOG_TYPE::WALUpdate: {
               auto& update_entry = *reinterpret_cast<vw::WALUpdate*>(entry);
               applyDeltaVW(payload.get(), update_entry.payload + update_entry.key_length, update_entry.delta_length);
               break;
            }
            default: {
               ensure(false);
            }
         }
         if (isVisibleForMe(wal_entry.prev_version.worker_id, wal_entry.prev_version.tts) || wal_entry.prev_version.lsn == 0) {
            flag = false;
         } else {
            DEBUG_BLOCK() { version_depth++; }
            ensure(next_lsn > wal_entry.prev_version.lsn);
            next_worker_id = wal_entry.prev_version.worker_id;
            next_lsn = wal_entry.prev_version.lsn;
         }
      });
   }
}
// -------------------------------------------------------------------------------------
OP_RESULT BTree::updateVW(u8* key, u16 key_length, function<void(u8* value, u16 value_size)> callback, WALUpdateGenerator wal_update_generator)
{
   cr::Worker::my().walEnsureEnoughSpace(PAGE_SIZE * 1);
   // -------------------------------------------------------------------------------------
   // Four possible scenarios:
   // 1) key not found -> return false
   // 2) key found, version not visible -> abort transaction
   // 3) key found, version visible -> insert delta record
   while (true) {
      jumpmuTry()
      {
         // -------------------------------------------------------------------------------------
         HybridPageGuard<BTreeNode> leaf_guard;
         findLeafCanJump<OP_TYPE::POINT_UPDATE>(leaf_guard, key, key_length);
         auto leaf_ex_guard = ExclusivePageGuard(std::move(leaf_guard));
         s16 pos = leaf_ex_guard->lowerBound<true>(key, key_length);
         if (pos != -1) {
            auto& version = *reinterpret_cast<vw::Version*>(leaf_ex_guard->getPayload(pos));
            u8* payload = leaf_ex_guard->getPayload(pos) + VW_PAYLOAD_OFFSET;
            const u16 payload_length = leaf_ex_guard->getPayloadLength(pos) - VW_PAYLOAD_OFFSET;
            if (isVisibleForMe(version.worker_id, version.tts)) {
               if (version.is_removed) {
                  jumpmu_return OP_RESULT::NOT_FOUND;
               } else {
                  // We can update
                  // -------------------------------------------------------------------------------------
                  // if it is a secondary index, then we can not use updateSameSize
                  assert(wal_update_generator.entry_size > 0);
                  // -------------------------------------------------------------------------------------
                  auto wal_entry = leaf_ex_guard.reserveWALEntry<vw::WALUpdate>(key_length + wal_update_generator.entry_size);
                  wal_entry->type = vw::WAL_LOG_TYPE::WALUpdate;
                  wal_entry->key_length = key_length;
                  wal_entry->delta_length = wal_update_generator.entry_size;
                  wal_entry->prev_version = version;
                  // -------------------------------------------------------------------------------------
                  std::memcpy(wal_entry->payload, key, key_length);
                  wal_update_generator.before(payload, wal_entry->payload + key_length);
                  // The actual update by the client
                  callback(payload, payload_length);
                  wal_update_generator.after(payload, wal_entry->payload + key_length);
                  wal_entry.submit();
                  // -------------------------------------------------------------------------------------
                  version.worker_id = myWorkerID();
                  version.tts = myTTS();
                  version.lsn = wal_entry.lsn;
                  version.is_final = false;
                  version.is_removed = false;
                  // -------------------------------------------------------------------------------------
                  leaf_guard = std::move(leaf_ex_guard);
                  jumpmu_return OP_RESULT::OK;
               }
            } else {
               jumpmu_return OP_RESULT::ABORT_TX;
            }
         } else {
            jumpmu_return OP_RESULT::NOT_FOUND;
         }
      }
      jumpmuCatch() {}
   }
}
// -------------------------------------------------------------------------------------
OP_RESULT BTree::removeVW(u8* key, u16 key_length)
{
   cr::Worker::my().walEnsureEnoughSpace(PAGE_SIZE * 1);
   // -------------------------------------------------------------------------------------
   while (true) {
      jumpmuTry()
      {
         // -------------------------------------------------------------------------------------
         HybridPageGuard<BTreeNode> leaf_guard;
         findLeafCanJump<OP_TYPE::POINT_UPDATE>(leaf_guard, key, key_length);
         auto leaf_ex_guard = ExclusivePageGuard(std::move(leaf_guard));
         s16 pos = leaf_ex_guard->lowerBound<true>(key, key_length);
         if (pos != -1) {
            auto& version = *reinterpret_cast<vw::Version*>(leaf_ex_guard->getPayload(pos));
            u8* payload = leaf_ex_guard->getPayload(pos) + VW_PAYLOAD_OFFSET;
            const u16 payload_length = leaf_ex_guard->getPayloadLength(pos) - VW_PAYLOAD_OFFSET;
            if (isVisibleForMe(version.worker_id, version.tts)) {
               if (version.is_removed) {
                  jumpmu_return OP_RESULT::NOT_FOUND;
               } else {
                  auto wal_entry = leaf_ex_guard.reserveWALEntry<vw::WALRemove>(key_length + payload_length);
                  wal_entry->type = vw::WAL_LOG_TYPE::WALUpdate;
                  wal_entry->key_length = key_length;
                  wal_entry->payload_length = key_length;
                  wal_entry->prev_version = version;
                  // -------------------------------------------------------------------------------------
                  std::memcpy(wal_entry->payload, key, key_length);
                  std::memcpy(wal_entry->payload + key_length, payload, payload_length);
                  wal_entry.submit();
                  // -------------------------------------------------------------------------------------
                  version.worker_id = myWorkerID();
                  version.tts = myTTS();
                  version.lsn = wal_entry.lsn;
                  version.is_final = false;
                  version.is_removed = true;
                  // -------------------------------------------------------------------------------------
                  raise(SIGTRAP);
                  leaf_ex_guard->space_used -= payload_length - VW_PAYLOAD_OFFSET;
                  leaf_ex_guard->setPayloadLength(pos, VW_PAYLOAD_OFFSET);
                  leaf_guard = std::move(leaf_ex_guard);
                  jumpmu_return OP_RESULT::OK;
               }
            } else {
               jumpmu_return OP_RESULT::ABORT_TX;
            }
         } else {
            jumpmu_return OP_RESULT::NOT_FOUND;
         }
      }
      jumpmuCatch() {}
   }
}
// -------------------------------------------------------------------------------------
void BTree::scanAscVW(u8* start_key,
                      u16 key_length,
                      function<bool(u8* key, u16 key_length, u8* value, u16 value_length)> callback,
                      function<void()> undo)
{
   scanAscLL(
       start_key, key_length,
       [&](u8* key, u16 key_length, u8* payload_ll, u16 payload_length_ll) {
          auto& version = *reinterpret_cast<vw::Version*>(payload_ll);
          u8* payload = payload_ll + VW_PAYLOAD_OFFSET;
          u16 payload_length = payload_length_ll - VW_PAYLOAD_OFFSET;
          if (isVisibleForMe(version.worker_id, version.tts)) {
             if (version.is_removed) {
                return true;
             } else {
                return callback(key, key_length, payload, payload_length);
             }
          } else {
             JMUW<std::unique_ptr<u8[]>> reconstructed_payload = std::make_unique<u8[]>(payload_length);
             std::memcpy(reconstructed_payload->get(), payload, payload_length);
             reconstructTupleVW(reconstructed_payload.obj, payload_length, version.worker_id, version.lsn);
             if (payload_length == 0) {
                return true;
             } else {
                return callback(key, key_length, reconstructed_payload->get(), payload_length);
             }
          }
          return true;
       },
       [&]() { undo(); });
}
// -------------------------------------------------------------------------------------
void BTree::scanDescVW(u8* start_key,
                       u16 key_length,
                       function<bool(u8* key, u16 key_length, u8* value, u16 value_length)> callback,
                       function<void()> undo)
{
   scanDescLL(
       start_key, key_length,
       [&](u8* key, u16 key_length, u8* payload_ll, u16 payload_length_ll) {
          auto& version = *reinterpret_cast<vw::Version*>(payload_ll);
          u8* payload = payload_ll + VW_PAYLOAD_OFFSET;
          u16 payload_length = payload_length_ll - VW_PAYLOAD_OFFSET;
          if (isVisibleForMe(version.worker_id, version.tts)) {
             if (version.is_removed) {
                return true;
             } else {
                return callback(key, key_length, payload, payload_length);
             }
          } else {
             JMUW<std::unique_ptr<u8[]>> reconstructed_payload = std::make_unique<u8[]>(payload_length);
             std::memcpy(reconstructed_payload->get(), payload, payload_length);
             reconstructTupleVW(reconstructed_payload.obj, payload_length, version.worker_id, version.lsn);
             if (payload_length == 0) {
                return true;
             } else {
                return callback(key, key_length, reconstructed_payload->get(), payload_length);
             }
          }
       },
       [&]() { undo(); });
}
// -------------------------------------------------------------------------------------
void BTree::applyDeltaVW(u8* dst, const u8* delta_beginning, u16 delta_size)
{
   const u8* delta_ptr = delta_beginning;
   while (delta_ptr - delta_beginning < delta_size) {
      const u16 offset = *reinterpret_cast<const u16*>(delta_ptr);
      delta_ptr += 2;
      const u16 size = *reinterpret_cast<const u16*>(delta_ptr);
      delta_ptr += 2;
      for (u64 b_i = 0; b_i < size; b_i++) {
         *reinterpret_cast<u8*>(dst + offset + b_i) ^= *reinterpret_cast<const u8*>(delta_ptr + b_i);
      }
      delta_ptr += size;
   }
}
// -------------------------------------------------------------------------------------
// For Transaction abort and not for recovery
void BTree::undoVW(void* btree_object, const u8* wal_entry_ptr, const u64)
{
   auto& btree = *reinterpret_cast<BTree*>(btree_object);
   const vw::WALEntry& entry = *reinterpret_cast<const vw::WALEntry*>(wal_entry_ptr);
   switch (entry.type) {
      case vw::WAL_LOG_TYPE::WALInsert: {
         // Outcome:
         // 1- no previous entry -> delete tuple
         // 2- previous entry -> reconstruct in-line tuple
         auto& insert_entry = *reinterpret_cast<const vw::WALInsert*>(&entry);
         const u16 key_length = insert_entry.key_length;
         const u8* key = insert_entry.payload;
         // -------------------------------------------------------------------------------------
         while (true) {
            jumpmuTry()
            {
               HybridPageGuard<BTreeNode> leaf_guard;
               btree.findLeafCanJump<OP_TYPE::POINT_DELETE>(leaf_guard, key, key_length);
               auto leaf_ex_guard = ExclusivePageGuard(std::move(leaf_guard));
               s16 pos = leaf_ex_guard->lowerBound<true>(key, key_length);
               ensure(pos != -1);
               if (insert_entry.prev_version.lsn == 0) {
                  leaf_ex_guard->removeSlot(pos);
                  jumpmu_return;
               } else {
                  // The previous entry was delete
                  auto& version = *reinterpret_cast<vw::Version*>(leaf_ex_guard->getPayload(pos));
                  version.is_removed = true;
                  version.lsn = insert_entry.prev_version.lsn;
                  version.worker_id = insert_entry.prev_version.worker_id;
                  cr::Worker::my().getWALDTEntry(insert_entry.prev_version.worker_id, insert_entry.prev_version.lsn,
                                                 [&](u8* p_entry) {  // Can be optimized away
                                                    const vw::WALEntry& prev_entry = *reinterpret_cast<const vw::WALEntry*>(p_entry);
                                                    version.is_final = (prev_entry.prev_version.lsn == 0);
                                                 });
                  // -------------------------------------------------------------------------------------
                  leaf_ex_guard->space_used -= leaf_ex_guard->getPayloadLength(pos) - VW_PAYLOAD_OFFSET;
                  leaf_ex_guard->setPayloadLength(pos, VW_PAYLOAD_OFFSET);
                  jumpmu_return;
               }
               // -------------------------------------------------------------------------------------
            }
            jumpmuCatch() {}
         }
         break;
      }
      case vw::WAL_LOG_TYPE::WALUpdate: {
         // Prev was insert or update
         const auto& update_entry = *reinterpret_cast<const vw::WALUpdate*>(&entry);
         const u16 key_length = update_entry.key_length;
         const u8* key = update_entry.payload;
         while (true) {
            jumpmuTry()
            {
               HybridPageGuard<BTreeNode> leaf_guard;
               btree.findLeafCanJump<OP_TYPE::POINT_DELETE>(leaf_guard, key, key_length);
               auto leaf_ex_guard = ExclusivePageGuard(std::move(leaf_guard));
               const s16 pos = leaf_ex_guard->lowerBound<true>(key, key_length);
               ensure(pos != -1);
               auto& version = *reinterpret_cast<vw::Version*>(leaf_ex_guard->getPayload(pos));
               // -------------------------------------------------------------------------------------
               // Apply delta
               u8* payload = leaf_ex_guard->getPayload(pos) + VW_PAYLOAD_OFFSET;
               applyDeltaVW(payload, update_entry.payload + update_entry.key_length, update_entry.delta_length);
               // -------------------------------------------------------------------------------------
               version.worker_id = update_entry.prev_version.worker_id;
               version.lsn = update_entry.prev_version.lsn;
               version.is_removed = false;
               version.is_final = false;
               // -------------------------------------------------------------------------------------
               leaf_guard = std::move(leaf_ex_guard);
               jumpmu_return;
            }
            jumpmuCatch() {}
         }
         break;
      }
      case vw::WAL_LOG_TYPE::WALRemove: {
         // Prev was insert or update
         const auto& remove_entry = *reinterpret_cast<const vw::WALRemove*>(&entry);
         while (true) {
            jumpmuTry()
            {
               const u8* key = remove_entry.payload;
               const u16 key_length = remove_entry.key_length;
               const u8* payload = remove_entry.payload + key_length;
               const u16 payload_length = remove_entry.payload_length;
               HybridPageGuard<BTreeNode> leaf_guard;
               btree.findLeafCanJump<OP_TYPE::POINT_DELETE>(leaf_guard, key, key_length);
               auto leaf_ex_guard = ExclusivePageGuard(std::move(leaf_guard));
               const s16 pos = leaf_ex_guard->lowerBound<true>(key, key_length);
               // -------------------------------------------------------------------------------------
               auto& version = *reinterpret_cast<vw::Version*>(leaf_ex_guard->getPayload(pos));
               version.is_final = false;
               version.is_removed = false;
               version.worker_id = remove_entry.prev_version.worker_id;
               version.lsn = remove_entry.prev_version.lsn;
               std::memcpy(leaf_ex_guard->getPayload(pos) + VW_PAYLOAD_OFFSET, payload, payload_length);
               // -------------------------------------------------------------------------------------
               leaf_guard = std::move(leaf_ex_guard);
               jumpmu_return;
            }
            jumpmuCatch() {}
         }
         break;
      }
      default: {
         break;
      }
   }
}
// -------------------------------------------------------------------------------------
}  // namespace btree
}  // namespace storage
}  // namespace leanstore