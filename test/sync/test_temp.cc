#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include "leanstore/env.h"
#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstring>
#include <fstream>
#include <iostream>
#include <random>
#include <thread>
#include <vector>

#include "sync/v0/range_lock.hpp"
#include "sync/v2/range_lock.hpp"
#include "sync/range_lock.h"

#include "gtest/gtest.h"

#include <chrono>
#include <thread>

namespace leanstore {

constexpr int minThreads = 2;
constexpr int maxThreads = 32;
constexpr int numOfRanges = 1000000;
constexpr int size = 64;
constexpr size_t sharedMemorySize = numOfRanges * (size + 1);
constexpr int batchSize = 16;
constexpr int testDurationSeconds = 4;

std::vector<std::pair<int, int>> createNonOverlappingRanges() {
    std::vector<std::pair<int, int>> ranges;
    int k = 1;
    for (int i = 0; i < numOfRanges; i++) {
        ranges.emplace_back(k, k + size);
        k += (size + 1);
    }
    unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
    std::shuffle(ranges.begin(), ranges.end(),
                 std::default_random_engine(seed));
    return ranges;
}

uint64_t runScalabilityV0(int numThreads, std::vector<std::pair<int, int>> ranges) {
    ConcurrentRangeLock<uint64_t, 10> crl{};
    std::vector<std::thread> threads;
    threads.reserve(numThreads);

    auto rangePerThread = ranges.size() / numThreads;

    std::atomic<uint64_t> operationCount{0};
    std::atomic<bool> stopFlag{false};

    for (int i = 0; i < numThreads; i++) {
        threads.emplace_back([&, i]() {
            auto startIdx = i * rangePerThread;
            auto endIdx = (i == numThreads - 1) ? ranges.size() : startIdx + rangePerThread;

            while (!stopFlag.load(std::memory_order_relaxed)) {
                // for (auto j = startIdx; j < endIdx; j += batchSize) {
                //     int batchEnd = std::min(j + batchSize, endIdx);
                //     for (auto k = j; k < batchEnd; ++k) {
                //         crl.tryLock(ranges[k].first, ranges[k].second);
                //     }
                //     for (auto k = j; k < batchEnd; ++k) {
                //         crl.releaseLock(ranges[k].first, ranges[k].second);
                //     }
                // }

                for (auto j = startIdx; j < endIdx; j += batchSize) {
                    crl.tryLock(ranges[j].first, ranges[j].second);
                }

                for (auto j = startIdx; j < endIdx; j += batchSize) {
                    crl.releaseLock(ranges[j].first, ranges[j].second);
                }

                // for (auto j = startIdx; j < endIdx; j += batchSize) {
                //     crl.tryLock(ranges[j].first, ranges[j].second);
                //     crl.releaseLock(ranges[j].first, ranges[j].second);
                // }

                operationCount.fetch_add(2, std::memory_order_relaxed);
            }
        });
    }

    std::this_thread::sleep_for(std::chrono::seconds(testDurationSeconds));
    stopFlag.store(true, std::memory_order_relaxed);

    for (auto& thread : threads) {
        thread.join();
    }

    return operationCount.load();
}

uint64_t runScalabilityV2(int numThreads, std::vector<std::pair<int, int>> ranges) {
    ListRL list;
    std::vector<std::thread> threads;
    threads.reserve(numThreads);

    auto rangePerThread = ranges.size() / numThreads;

    std::atomic<uint64_t> operationCount{0};
    std::atomic<bool> stopFlag{false};

    for (int i = 0; i < numThreads; i++) {
        threads.emplace_back([&, i]() {
            auto startIdx = i * rangePerThread;
            auto endIdx = (i == numThreads - 1) ? ranges.size() : startIdx + rangePerThread;

            while (!stopFlag.load(std::memory_order_relaxed)) {
                std::vector<RangeLock*> rls;
                rls.reserve(batchSize);
                // for (auto j = startIdx; j < endIdx; j += batchSize) {
                //     int batchEnd = std::min(j + batchSize, endIdx);
                //     for (auto k = j; k < batchEnd; ++k) {
                //         auto rl = MutexRangeAcquire(&list, ranges[k].first,
                //                                     ranges[k].second);
                //         rls.emplace_back(rl);
                //     }
                //     for (auto rl : rls) {
                //         MutexRangeRelease(rl);
                //     }
                // }

                for (auto j = startIdx; j < endIdx; ++j) {
                    auto rl = MutexRangeAcquire(&list, ranges[j].first,
                                                ranges[j].second);
                    rls.emplace_back(rl);
                }

                for (auto rl : rls) {
                    MutexRangeRelease(rl);
                }

                // for (auto j = startIdx; j < endIdx; ++j) {
                //     auto rl = MutexRangeAcquire(&list, ranges[j].first,
                //                                 ranges[j].second);
                //     MutexRangeRelease(rl);
                // }

                operationCount.fetch_add(2, std::memory_order_relaxed);
            }
        });
    }

    std::this_thread::sleep_for(std::chrono::seconds(testDurationSeconds));
    stopFlag.store(true, std::memory_order_relaxed);

    for (auto& thread : threads) {
        thread.join();
    }

    return operationCount.load();
}

uint64_t runScalabilityV3(int numThreads, std::vector<std::pair<int, int>> ranges) {
    leanstore::sync::SongRangeLock rl(numThreads);
    std::vector<std::thread> threads;
    threads.reserve(numThreads);

    auto rangePerThread = ranges.size() / numThreads;

    std::atomic<uint64_t> operationCount{0};
    std::atomic<bool> stopFlag{false};

    for (int i = 0; i < numThreads; i++) {
        threads.emplace_back([&, i]() {
            worker_thread_id = i;

            auto startIdx = i * rangePerThread;
            auto endIdx = (i == numThreads - 1) ? ranges.size() : startIdx + rangePerThread;

            while (!stopFlag.load(std::memory_order_relaxed)) {
                // for (auto j = startIdx; j < endIdx; j += batchSize) {
                //     int batchEnd = std::min(j + batchSize, endIdx);
                //     for (auto k = j; k < batchEnd; ++k) {
                //         rl.TryLockRange(ranges[k].first, size);
                //     }
                //     for (auto k = j; k < batchEnd; ++k) {
                //         rl.UnlockRange(ranges[k].first);
                //     }
                // }

                for (auto j = startIdx; j < endIdx; ++j) {
                    rl.TryLockRange(ranges[j].first, size);
                }
                for (auto j = startIdx; j < endIdx; ++j) {
                    rl.UnlockRange(ranges[j].first);
                }

                // for (auto j = startIdx; j < endIdx; ++j) {
                //     rl.TryLockRange(ranges[j].first, size);
                //     rl.UnlockRange(ranges[j].first);
                // }

                operationCount.fetch_add(2, std::memory_order_relaxed);
            }
        });
    }

    std::this_thread::sleep_for(std::chrono::seconds(testDurationSeconds));
    stopFlag.store(true, std::memory_order_relaxed);

    for (auto& thread : threads) {
        thread.join();
    }

    return operationCount.load();
}

TEST(TestThroughput, Throughput) {
    auto ranges = createNonOverlappingRanges();

    for (int i = 4; i <= 32; i += 4) {
        std::cout << "NumThreads: " << i << std::endl;

        uint64_t totalOperationsV0 = runScalabilityV0(i, ranges);
        uint64_t totalOperationsV2 = runScalabilityV2(i, ranges);
        uint64_t totalOperationsV3 = runScalabilityV3(i, ranges);

        double opsPerSecondV0 = static_cast<double>(totalOperationsV0) / testDurationSeconds;
        double opsPerSecondV2 = static_cast<double>(totalOperationsV2) / testDurationSeconds;
        double opsPerSecondV3 = static_cast<double>(totalOperationsV3) / testDurationSeconds;

        std::cout << "Threads: " << i << " V0 Throughput: " << opsPerSecondV0 << " ops/second\n";
        std::cout << "Threads: " << i << " V2 Throughput: " << opsPerSecondV2 << " ops/second\n";
        std::cout << "Threads: " << i << " V3 Throughput: " << opsPerSecondV3 << " ops/second\n";
    }
}

}  // namespace leanstore
