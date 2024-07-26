/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 */

#ifndef OMNI_RUNTIME_MEMORY_MANAGER_H
#define OMNI_RUNTIME_MEMORY_MANAGER_H

#include <atomic>
#include <memory>
#include <thread>
#include <mutex>
#include <map>
#include <unordered_map>
#include "util/omni_exception.h"
#include "util/error_code.h"
#include "util/compiler_util.h"
#include "memory_manager_allocator.h"
#include "memory_trace.h"

namespace omniruntime {
namespace mem {
using namespace exception;

/**
 * it is responsible for memory usage statistics of each thread and the global memory usage.
 **/
class MemoryManager {
public:
    // unlimited memory usage
    const static int64_t UNLIMIT = -1;
    static ALWAYS_INLINE MemoryManager *GetGlobalMemoryManager()
    {
        static MemoryManager globalMemoryManger;
        return &globalMemoryManger;
    }

    static void SetGlobalMemoryLimit(int64_t limit)
    {
        LogInfo("set global memory manager limit:%ld Byte", limit);
        MemoryManager *globalMemoryManager = GetGlobalMemoryManager();
        globalMemoryManager->SetMemoryLimit(limit);
    }

    static ALWAYS_INLINE int64_t GetGlobalAccountedMemory()
    {
        MemoryManager *globalMemoryManager = GetGlobalMemoryManager();
        return globalMemoryManager->GetMemoryAmount();
    }

    static ALWAYS_INLINE int64_t GetGlobalMemoryLimit()
    {
        MemoryManager *globalMemoryManager = GetGlobalMemoryManager();
        return globalMemoryManager->GetMemoryLimit();
    }

    // constructor for globalMemoryManager
    MemoryManager();

    // constructor for threadMemoryManager
    explicit MemoryManager(MemoryManager *globalMemoryManager);

    ~MemoryManager();

    /**
     * reportedMemory is a positive size, indicate the memory manager need to be added.
     * curAllocateSize indicate the size of memory to be allocated of one object, which triggers the statistical event.
     * AddMemory() interface is called when the reportedMemory exceeds the untracked memory threshold like 1MB
     * */
    void AddMemory(int64_t reportedMemory, int64_t curAllocateSize = 0);

    /**
     * reclaimedMemory is a negative size.
     * SubMemory() interface is called when the reclaimedMemory exceeds the threshold '-THRESHOLD'.
     * */
    void SubMemory(int64_t reclaimedMemory);

    // memoryPeak seems to lack actual application scenario, so memoryPeak is not worth ensuring thread safety.
    void UpdatePeak(int64_t size);

#ifdef DEBUG
    void AddScopeAmount(const std::string &scope, int64_t size);

    void SubScopeAmount(const std::string &scope, int64_t size);
#endif

    void SetParent(MemoryManager *parentMemoryManager);

    MemoryManager *GetParent();

    // for UT
    void SetMemoryAmount(int64_t amount);

    int64_t GetMemoryAmount();

    void SetMemoryLimit(int64_t limit);

    int64_t GetMemoryLimit();

    void SetMemoryPeak(int64_t peak);

    int64_t GetMemoryPeak();

    bool IsMemoryManagerBlocked();

#ifdef DEBUG
    std::unordered_map<std::string, int64_t, std::hash<std::string>, std::equal_to<std::string>,
        MemoryManagerAllocator<std::pair<std::string, int64_t>>>
    GetScopeMap();
#endif

    void Clear();

private:
    std::atomic<MemoryManager *> parent;
    std::atomic<int64_t> memoryAmount;
    std::atomic<int64_t> memoryLimit;
    std::atomic<int64_t> memoryPeak;
    std::atomic<bool> isBlocked;
#ifdef DEBUG
    std::mutex mapLock;
    std::unordered_map<std::string, int64_t, std::hash<std::string>, std::equal_to<std::string>,
        MemoryManagerAllocator<std::pair<std::string, int64_t>>>
        scopeMap; // Scope : Amount
#endif
};
} // vec
} // omniruntime

#endif // OMNI_RUNTIME_MEMORY_MANAGER_H
