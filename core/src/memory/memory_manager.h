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

    /* *
     * A unified interface for updating memory usage.
     * AccountMemory() interface is called when the untrackedMemory exceeds the threshold range [-THRESHOLD, THRESHOLD].
     * Note that untrackedMemory could be less than -THRESHOLD. That is, the memory is continuously allocated,
     * and then be continuously freed.
    *  */
    void AccountMemory(int64_t size)
    {
        int64_t newMemoryAmount = memoryAmount.fetch_add(size, std::memory_order_relaxed) + size;
        int64_t limit = memoryLimit.load(std::memory_order_relaxed);
        if (limit == UNLIMIT || newMemoryAmount < limit) {
            isBlocked.store(false, std::memory_order_relaxed);
        } else {
            /* *
             * A thread cannot throw multiple exceptions in the case of multiple threads.
             * The If statement is used to avoid the following case: When the thread exceeds the limit,
             * the destructor of the thread is called and the AccountMemory interface is executed again,
             * which may cause the OmniException to be thrown again.
            *  */
            if (!isBlocked.load(std::memory_order_relaxed)) {
                isBlocked.store(true, std::memory_order_relaxed);

                auto message =
                        op::GetErrorMessage(op::ErrorCode::MEM_CAP_EXCEEDED) + std::to_string(limit / 1024 / 1024);
                throw OmniException(GetErrorCode(op::ErrorCode::MEM_CAP_EXCEEDED), message);
            }
        }

        if (auto parentMemoryManager = parent.load(std::memory_order_relaxed)) {
            parentMemoryManager->AccountMemory(size);
        }
    }

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
