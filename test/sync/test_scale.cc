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

constexpr int numOfRanges = 1000000;
constexpr int size = 256;
constexpr size_t sharedMemorySize = numOfRanges * (size + 1);
constexpr int batchSize = 16;

uint8_t* createSharedMemory() {
    // Allocate memory using mmap
    uint8_t* addr = static_cast<uint8_t*>(
        mmap(nullptr, sharedMemorySize, PROT_READ | PROT_WRITE,
             MAP_ANONYMOUS | MAP_SHARED, -1, 0));
    if (addr == MAP_FAILED) {
        perror("mmap");
        exit(1);
    }
    return addr;
}

void destroySharedMemory(uint8_t* addr) {
    if (munmap(addr, sharedMemorySize) == -1) {
        perror("munmap");
        exit(1);
    }
}

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

double runScalabilityV0(int numThreads) {
    ConcurrentRangeLock<uint64_t, 10> crl{};
    std::vector<std::thread> threads;
    threads.reserve(numThreads);

    uint8_t* sharedMemory = createSharedMemory();
    auto ranges = createNonOverlappingRanges();
    auto rangePerThread = ranges.size() / numThreads;

    auto start = std::chrono::steady_clock::now();
    for (int i = 0; i < numThreads; i++) {
        threads.emplace_back([&, i]() {
            auto startIdx = i * rangePerThread;
            auto endIdx = (i == numThreads - 1) ? ranges.size()
                                                : startIdx + rangePerThread;
            
            // for (auto j = startIdx; j < endIdx; j += batchSize) {
            //     size_t batchEnd = std::min(j + batchSize, endIdx);
            //     for (auto k = j; k < batchEnd; ++k) {
            //         crl.tryLock(ranges[k].first, ranges[k].second);
            //         memset(sharedMemory + ranges[j].first, 1,
            //                ranges[j].second - ranges[j].first);
            //     }
            //     for (auto k = j; k < batchEnd; ++k) {
            //         crl.releaseLock(ranges[k].first, ranges[k].second);
            //     }
            // }

            // for (auto j = startIdx; j < endIdx; j += batchSize) {
            //     crl.tryLock(ranges[j].first, ranges[j].second);
            //     memset(sharedMemory + ranges[j].first, 1,
            //                ranges[j].second - ranges[j].first);
            // }

            // for (auto j = startIdx; j < endIdx; j += batchSize) {
            //     crl.releaseLock(ranges[j].first, ranges[j].second);
            // }

            for (auto j = startIdx; j < endIdx; j += batchSize) {
                crl.tryLock(ranges[j].first, ranges[j].second);
                memset(sharedMemory + ranges[j].first, 1,
                        ranges[j].second - ranges[j].first);
                crl.releaseLock(ranges[j].first, ranges[j].second);
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }
    auto end = std::chrono::steady_clock::now();

    std::chrono::duration<double> duration = end - start;

    destroySharedMemory(sharedMemory);


    return duration.count();
}

double runScalabilityV2(int numThreads) {
    ListRL list;
    std::vector<std::thread> threads;
    threads.reserve(numThreads);

    uint8_t* sharedMemory = createSharedMemory();
    auto ranges = createNonOverlappingRanges();
    auto rangePerThread = ranges.size() / numThreads;

    auto start = std::chrono::steady_clock::now();
    for (int i = 0; i < numThreads; i++) {
        threads.emplace_back([&, i]() {
            auto startIdx = i * rangePerThread;
            auto endIdx = (i == numThreads - 1) ? ranges.size()
                                                : startIdx + rangePerThread;

            std::vector<RangeLock*> rls;
            rls.reserve(batchSize);

            // for (auto j = startIdx; j < endIdx; j += batchSize) {
            //     size_t batchEnd = std::min(j + batchSize, endIdx);
            //     for (auto k = j; k < batchEnd; ++k) {
            //         auto rl = MutexRangeAcquire(&list, ranges[k].first,
            //                                     ranges[k].second);
            //         memset(sharedMemory + ranges[j].first, 1,
            //                ranges[j].second - ranges[j].first);
            //         rls.emplace_back(rl);
            //     }
            //     for (auto rl : rls) {
            //         MutexRangeRelease(rl);
            //     }
            // }

            // for (auto j = startIdx; j < endIdx; ++j) {
            //     auto rl = MutexRangeAcquire(&list, ranges[j].first,
            //                                 ranges[j].second);
            //     rls.emplace_back(rl);
            //     memset(sharedMemory + ranges[j].first, 1,
            //                ranges[j].second - ranges[j].first);
            // }

            // for (auto rl : rls) {
            //     MutexRangeRelease(rl);
            // }

            for (auto j = startIdx; j < endIdx; ++j) {
                auto rl = MutexRangeAcquire(&list, ranges[j].first,
                                            ranges[j].second);
                memset(sharedMemory + ranges[j].first, 1,
                        ranges[j].second - ranges[j].first);                   
                MutexRangeRelease(rl);
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }
    auto end = std::chrono::steady_clock::now();

    std::chrono::duration<double> duration = end - start;

    destroySharedMemory(sharedMemory);


    return duration.count();
}

double runScalabilityV3(int numThreads) {
    leanstore::sync::SongRangeLock rl(numThreads);
    std::vector<std::thread> threads;
    threads.reserve(numThreads);

    uint8_t* sharedMemory = createSharedMemory();
    auto ranges = createNonOverlappingRanges();
    auto rangePerThread = ranges.size() / numThreads;

    auto start = std::chrono::steady_clock::now();
    for (int i = 0; i < numThreads; i++) {
        threads.emplace_back([&, i]() {
            worker_thread_id = i;

            auto startIdx = i * rangePerThread;
            auto endIdx = (i == numThreads - 1) ? ranges.size()
                                                : startIdx + rangePerThread;

            // for (auto j = startIdx; j < endIdx; j += batchSize) {
            //     size_t batchEnd = std::min(j + batchSize, endIdx);
            //     for (auto k = j; k < batchEnd; ++k) {
            //         rl.TryLockRange(ranges[k].first, size);
            //         memset(sharedMemory + ranges[j].first, 1,
            //                ranges[j].second - ranges[j].first);
            //     }
            //     for (auto k = j; k < batchEnd; ++k) {
            //         rl.UnlockRange(ranges[k].first);
            //     }
            // }

            // for (auto j = startIdx; j < endIdx; ++j) {
            //     rl.TryLockRange(ranges[j].first, size);
            //     memset(sharedMemory + ranges[j].first, 1,
            //                ranges[j].second - ranges[j].first);
            // }
            // for (auto j = startIdx; j < endIdx; ++j) {
            //     rl.UnlockRange(ranges[j].first);
            // }

            for (auto j = startIdx; j < endIdx; ++j) {
                rl.TryLockRange(ranges[j].first, size);
                memset(sharedMemory + ranges[j].first, 1,
                        ranges[j].second - ranges[j].first);                
                rl.UnlockRange(ranges[j].first);
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }
    auto end = std::chrono::steady_clock::now();

    std::chrono::duration<double> duration = end - start;

    destroySharedMemory(sharedMemory);

    return duration.count();
}

TEST(TestScale, Scale) {
    constexpr int numRuns = 20;
    for (int i = 4; i <= 32; i += 4) {
        std::cout << "NumThreads: " << i << std::endl;

        double totalDurationV0 = 0.0;
        double totalDurationV2 = 0.0;
        double totalDurationV3 = 0.0;

        for (int run = 0; run < numRuns; ++run) {
            totalDurationV0 += runScalabilityV0(i);
            totalDurationV2 += runScalabilityV2(i);
            totalDurationV3 += runScalabilityV3(i);
        }

        std::cout << "Threads: " << i << " average V0 Duration: " << totalDurationV0 / numRuns << " seconds\n";
        std::cout << "Threads: " << i << " average V2 Duration: " << totalDurationV2 / numRuns << " seconds\n";
        std::cout << "Threads: " << i << " average V3 Duration: " << totalDurationV3 / numRuns << " seconds\n";
    }
}

}  // namespace leanstore
