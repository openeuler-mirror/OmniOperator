/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

#include <iostream>
#include "memory_pool.h"
#include <jemalloc/jemalloc.h>
#ifdef DEBUG
#include <atomic>
#include <thread>
#include <unistd.h>

std::atomic_long g_allocateCount(0);
std::atomic_long g_releaseCount(0);
#endif
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
    ~BaseMemoryPoolImpl() override {}
};


class JemallocMemoryPool : public BaseMemoryPoolImpl<JemallocAllocator> {};


static JemallocMemoryPool g_jemallocMemoryPool;

MemoryPool *GetMemoryPool()
{
    return &g_jemallocMemoryPool;
}

uint64_t GetPreferredSize(uint64_t size)
{
    if (size < 8) {
        return 8;
    }
    int32_t bits = 63 - __builtin_clzll(size);
    size_t lower = 1U << bits;
    // Size is a power of 2.
    if (lower == size) {
        return size;
    }
    // If size is below 1.5 * previous power of two, return 1.5 *
    // the previous power of two, else the next power of 2.
    if (lower + (lower / 2) >= size) {
        return lower + (lower / 2);
    }
    return lower * 2;
}

void *OmniAllocate(uint64_t size)
{
    uint8_t *buf = nullptr;
    uint64_t preferredSize = GetPreferredSize(size);
    g_jemallocMemoryPool.Allocate(preferredSize, &buf);
#ifdef DEBUG
    g_allocateCount += 1;
#endif
    return reinterpret_cast<void *>(buf);
}

void OmniRelease(unsigned long address)
{
    uintptr_t ptr = reinterpret_cast<uintptr_t>(address);
    g_jemallocMemoryPool.Release(reinterpret_cast<uint8_t *>(ptr));
#ifdef DEBUG
    g_releaseCount += 1;
#endif
}

#ifdef DEBUG
void printStatistics()
{
    while (true) {
        std::cout << "Allocate Count=" << g_allocateCount << ", Release Count=" << g_releaseCount << ", Leak Count=" <<
            (g_allocateCount - g_releaseCount) << std::endl;
        sleep(10); // sleep for 10 ms
    }
}

static std::thread g_backThread = std::thread(printStatistics);
#endif
