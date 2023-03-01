/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef OMNI_RUNTIME_ALLOCATOR_H
#define OMNI_RUNTIME_ALLOCATOR_H

#include <jemalloc/jemalloc.h>
#include "memory/memory_pool.h"
#include "thread_memory_manager.h"

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
        uint8_t *data = nullptr;
        pool->Allocate(size, &data, zeroFill);

        // memory usage statistics
        ThreadMemoryManager::ReportMemory(size);
        return data;
    }

    ALWAYS_INLINE void Free(void *data, int64_t size)
    {
        // memory usage statistics
        ThreadMemoryManager::ReclaimMemory(size);

        pool->Release(reinterpret_cast<uint8_t *>(data));
    }

private:
    Allocator(){};
    ~Allocator(){};
    mem::MemoryPool *pool = mem::GetMemoryPool();
};
} // omniruntime::vector

#endif // OMNI_RUNTIME_ALLOCATOR_H
