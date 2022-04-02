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
const size_t ALIGNMENT = 64;

class JemallocAllocator {
public:
    static int Allocate(int64_t size, uint8_t **buffer)
    {
        if (size < 0) {
            std::cout << "allocate size is negative." << std::endl;
            return -1;
        }
        // jemalloc alloc
        *buffer = reinterpret_cast<uint8_t *>(mallocx(static_cast<size_t>(size), MALLOCX_ALIGN(ALIGNMENT)));
        return 0;
    }

    static int Release(uint8_t *buffer)
    {
        // jemalloc free
        dallocx(reinterpret_cast<void *>(buffer), MALLOCX_ALIGN(ALIGNMENT));
        return 0;
    }
};

template <typename Allocator> class BaseMemoryPoolImpl : public MemoryPool {
public:
    int Allocate(int64_t size, uint8_t **buffer) override
    {
        Allocator::Allocate(size, buffer);
        return 0;
    }

    int Release(uint8_t *buffer) override
    {
        Allocator::Release(buffer);
        return 0;
    }

    ~BaseMemoryPoolImpl() override = default;
};


class JemallocMemoryPool : public BaseMemoryPoolImpl<JemallocAllocator> {};
}
}

static omniruntime::mem::JemallocMemoryPool g_jemallocMemoryPool;

static uint64_t GetPreferredSize(uint64_t size)
{
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

static void RecordSize(int size)
{
    static omniruntime::mem::MemoryStatistic statistic;
    statistic.RecordSize(size);
}

void *OmniAllocate(uint64_t size)
{
    uint8_t *buf = nullptr;
    uint64_t preferredSize = GetPreferredSize(size);
#ifdef DEBUG_VECTOR
    RecordSize(preferredSize);
#endif
    g_jemallocMemoryPool.Allocate(preferredSize, &buf);

    return reinterpret_cast<void *>(buf);
}

void OmniRelease(unsigned long address)
{
    uintptr_t ptr = reinterpret_cast<uintptr_t>(address);
    g_jemallocMemoryPool.Release(reinterpret_cast<uint8_t *>(ptr));
}
