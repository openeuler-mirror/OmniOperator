/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 */

#ifndef OMNI_RUNTIME_THREAD_MEMORY_MANAGER_H
#define OMNI_RUNTIME_THREAD_MEMORY_MANAGER_H

#include <atomic>
#include <pthread.h>
#include <sys/prctl.h>
#include "memory_manager.h"

namespace omniruntime {
namespace mem {
#ifdef DEBUG
#define THREAD_NAME_SIZE 16
#endif
/**
 * TLS Object, it is responsible for memory aggregation per thread.
 *      */
class ThreadMemoryManager {
public:
    static ALWAYS_INLINE ThreadMemoryManager *GetThreadMemoryManager()
    {
        thread_local ThreadMemoryManager threadMemoryManager;
        return &threadMemoryManager;
    }

    ThreadMemoryManager() noexcept;

    ~ThreadMemoryManager() noexcept;

    static ALWAYS_INLINE void ReportMemory(int64_t size)
    {
        auto threadMemoryManager = ThreadMemoryManager::GetThreadMemoryManager();
        threadMemoryManager->ReportMemoryUsage(size);
    }

    static ALWAYS_INLINE void ReclaimMemory(int64_t size)
    {
        auto threadMemoryManager = ThreadMemoryManager::GetThreadMemoryManager();
        threadMemoryManager->ReclaimMemoryUsage(size);
    }

    ALWAYS_INLINE void ReportMemoryUsage(int64_t size)
    {
        untrackedMemory += size;
        allocMemory += size;
        if (currentMemoryManager && untrackedMemory > untrackedMemoryThreshold) {
            // AddMemory maybe throw an exception, so untrackedMemory needs to be set to 0 in advance.
            int64_t toReportedMemory = untrackedMemory;
            untrackedMemory = 0;
            currentMemoryManager->AddMemory(toReportedMemory, size);
#ifdef DEBUG
            currentMemoryManager->AddScopeAmount(currentScope, untrackedMemory);
#endif
        }
    }

    ALWAYS_INLINE void ReclaimMemoryUsage(int64_t size)
    {
        untrackedMemory -= size;
        freeMemory += size;
        if (currentMemoryManager && labs(untrackedMemory) > untrackedMemoryThreshold) {
            int64_t toReclaimedMemory = untrackedMemory;
            untrackedMemory = 0;
            currentMemoryManager->SubMemory(toReclaimedMemory);
#ifdef DEBUG
            currentMemoryManager->SubScopeAmount(currentScope, untrackedMemory);
#endif
        }
    }

#ifdef DEBUG
    /* *
     * DeleteScope interface is used to end the memory statistics of a certain sql.
     * @param scope: scope is mapped to sql
     *      */
    void DeleteScope(const std::string &scope);
#endif

    // for UT
    void Clear();

    int64_t GetUntrackedMemory() const;

    int64_t GetThreadAccountedMemory();
    int64_t GetAllocMemory() const
    {
        return allocMemory;
    }

    int64_t GetFreeMemory() const
    {
        return freeMemory;
    }

private:
#ifdef DEBUG
    char currentScope[THREAD_NAME_SIZE];
#endif
    MemoryManager *currentMemoryManager;
    /* *
     * Each thread has an untracked memory. the memory usage is not updated when the memory usage of each thread is
     * within the range of [-Threshold, Threshold]. Moreover, a memory usage update request is initiated
     * when the memory usage of each thread exceeds the range.
     * The benefit is to avoid frequent updates of each thread and global memory usage.
     *      */
    int64_t untrackedMemory = 0;
    int64_t untrackedMemoryThreshold = 1 * 1024 * 1024;
    int64_t allocMemory = 0;
    int64_t freeMemory = 0;
};
} // mem
} // omniruntime

#endif // OMNI_RUNTIME_THREAD_MEMORY_MANAGER_H
