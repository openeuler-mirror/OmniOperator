/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

#include "memory_pool.h"

#include <iostream>
#include <jemalloc/jemalloc.h>
#include "memory_statistic.h"

using namespace std;
namespace omniruntime {
namespace mem {

class SimpleAllocator {
public:
    static int Allocate(int64_t size, uint8_t **buffer, bool zeroFill = false)
    {
        if (size < 0) {
            std::cout << "allocate size is negative." << std::endl;
            return -1;
        }

        if (zeroFill) {
            // alloc based on the size
            *buffer = static_cast<uint8_t *>(calloc(static_cast<size_t>(size), static_cast<size_t>(size)));
        } else {
            // alloc based on the size
            *buffer = static_cast<uint8_t *>(malloc(static_cast<size_t>(size)));
        }
        return 0;
    }

    static int Release(uint8_t *buffer)
    {
        // free the memory
        free(static_cast<void *>(buffer));
        return 0;
    }
};

class JemallocAllocator {
public:
    static int Allocate(int64_t size, uint8_t **buffer, bool zeroFill = false)
    {
        if (size < 0) {
            std::cout << "allocate size is negative." << std::endl;
            return -1;
        }
        // jemalloc alloc
        if (zeroFill) {
            *buffer = static_cast<uint8_t *>(mallocx(
                static_cast<size_t>(size),
                MALLOCX_ALIGN(alignment) | MALLOCX_ZERO
            ));
        } else {
            *buffer = static_cast<uint8_t *>(mallocx(static_cast<size_t>(size), MALLOCX_ALIGN(alignment)));
        }
        return 0;
    }

    static int Release(uint8_t *buffer)
    {
        // jemalloc free
        dallocx(static_cast<void *>(buffer), MALLOCX_ALIGN(alignment));
        return 0;
    }
    const static size_t alignment = 64;
};

template <typename Allocator> class BaseMemoryPoolImpl : public MemoryPool {
public:
    int Allocate(int64_t size, uint8_t **buffer, bool zeroFill = false) override
    {
#ifdef DEBUG_VECTOR
        static omniruntime::mem::MemoryStatistic statistic;
        statistic.RecordSize(size);
#endif
        Allocator::Allocate(size, buffer, zeroFill);
        return 0;
    }

    int Release(uint8_t *buffer) override
    {
        Allocator::Release(buffer);
        return 0;
    }

    ~BaseMemoryPoolImpl() override = default;

    uint64_t GetPreferredSize(uint64_t size) override
    {
        return size;
    }
};

class SimpleMemoryPool : public BaseMemoryPoolImpl<SimpleAllocator> {
public:
    uint64_t GetPreferredSize(uint64_t size) override
    {
        if (size == 0) {
            return size;
        }

        const uint64_t smallSize = 8;
        if (size < smallSize) {
            return smallSize;
        }
        uint32_t bits = 63 - __builtin_clzll(size);
        size_t lower = 1U << bits;
        // Size is a power of 2.
        if (lower == size) {
            return size;
        }
        // If size is below 1.5 * previous power of two, return 1.5 *
        // the previous power of two, else the next power of 2.
        uint64_t preferredSize = lower + (lower / 2);
        if (preferredSize >= size) {
            return preferredSize;
        }
        return (lower + lower);
    }
};

class JemallocMemoryPool : public BaseMemoryPoolImpl<JemallocAllocator> {
public:
    uint64_t GetPreferredSize(uint64_t size) override
    {
        if (size == 0) {
            return size;
        }

        const uint64_t smallSize = 8;
        if (size < smallSize) {
            return smallSize;
        }

        uint32_t bits = 63 - __builtin_clzll(size);
        size_t lower = 1ULL << bits;
        // Size is a power of 2.
        if (lower == size) {
            return size;
        }
        // If size is below 1.5 * previous power of two, return 1.5 *
        // the previous power of two, else the next power of 2.
        uint64_t preferredSize = lower + (lower / 2);
        if (preferredSize >= size) {
            return preferredSize;
        }
        return (lower + lower);
    }
};

static omniruntime::mem::JemallocMemoryPool g_memoryPoolInstance;

MemoryPool *GetMemoryPool()
{
    return &g_memoryPoolInstance;
}
}
}
