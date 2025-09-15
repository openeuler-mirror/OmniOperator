/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef OMNI_RUNTIME_ALLOCATOR_H
#define OMNI_RUNTIME_ALLOCATOR_H

#include <jemalloc/jemalloc.h>
#include "memory_pool.h"
#include "thread_memory_manager.h"
#include "memory_trace.h"

namespace omniruntime::mem {
class Allocator {
public:
    static ALWAYS_INLINE Allocator *GetAllocator()
    {
        static Allocator globalAllocator;
        return &globalAllocator;
    }

    ALWAYS_INLINE void *Alloc(int64_t size, bool zeroFill = false)
    {
        // memory usage statistics. If the memory_cap_exceed exception is thrown, the memory is not allocated.
        ThreadMemoryManager::ReportMemory(size);
        uint8_t *data = nullptr;
        pool->Allocate(size, &data, zeroFill);
        MemoryTrace::AddArenaMemory(reinterpret_cast<uintptr_t>(data), size);
        return data;
    }

    ALWAYS_INLINE void Free(void *data, int64_t size)
    {
        // memory usage statistics
        ThreadMemoryManager::ReclaimMemory(size);
        MemoryTrace::SubArenaMemory(reinterpret_cast<uintptr_t>(reinterpret_cast<uint8_t *>(data)), size);
        pool->Release(reinterpret_cast<uint8_t *>(data));
    }

private:
    Allocator(){};
    ~Allocator(){};
    MemoryPool *pool = GetMemoryPool();
};
} // omniruntime::mem

#endif // OMNI_RUNTIME_ALLOCATOR_H
