#include "DTRegistry.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
DTRegistry DTRegistry::global_dt_registry;
// -------------------------------------------------------------------------------------
void DTRegistry::iterateChildrenSwips(DTID dtid, BufferFrame& bf, std::function<bool(Swip<BufferFrame>&)> callback)
{
   auto& dt_meta = dt_instances_ht[dtid];
   dt_types_ht[std::get<0>(dt_meta)].iterate_children(std::get<1>(dt_meta), bf, callback);
}
// -------------------------------------------------------------------------------------
bool DTRegistry::checkSpaceUtilization(DTID dtid, BufferFrame& bf, OptimisticGuard& guard, ParentSwipHandler& parent_handler)
{
   auto dt_meta = dt_instances_ht[dtid];
   return dt_types_ht[std::get<0>(dt_meta)].check_space_utilization(std::get<1>(dt_meta), bf, guard, parent_handler);
}
// -------------------------------------------------------------------------------------
void DTRegistry::checkpoint(DTID dtid, BufferFrame& bf, u8* dest)
{
   auto dt_meta = dt_instances_ht[dtid];
   return dt_types_ht[std::get<0>(dt_meta)].checkpoint(std::get<1>(dt_meta), bf, dest);
}
// -------------------------------------------------------------------------------------
// Datastructures management
// -------------------------------------------------------------------------------------
void DTRegistry::registerDatastructureType(DTType type, DTRegistry::DTMeta dt_meta)
{
   std::unique_lock guard(mutex);
   dt_types_ht[type] = dt_meta;
}
// -------------------------------------------------------------------------------------
DTID DTRegistry::registerDatastructureInstance(DTType type, void* root_object, string name, string short_name)
{
   std::unique_lock guard(mutex);
   DTID new_instance_id = instances_counter++;
   std::cout << "meta: " << type << " name: " << name << std::endl;
   dt_instances_ht.insert({(long unsigned int)new_instance_id, {type, root_object, name, short_name}});
   return new_instance_id;
}
// -------------------------------------------------------------------------------------
void DTRegistry::undo(DTID dt_id, const u8* wal_entry, u64 tts)
{
   auto dt_meta = dt_instances_ht[dt_id];
   return dt_types_ht[std::get<0>(dt_meta)].undo(std::get<1>(dt_meta), wal_entry, tts);
}
// -------------------------------------------------------------------------------------
void DTRegistry::todo(DTID dt_id, const u8* wal_entry, u64 tts)
{
   auto dt_meta = dt_instances_ht[dt_id];
   return dt_types_ht[std::get<0>(dt_meta)].todo(std::get<1>(dt_meta), wal_entry, tts);
}
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
