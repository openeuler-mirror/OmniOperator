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
const size_t alignment = 64;

class JemallocAllocator {
        public:
    static int allocate(int64_t size, uint8_t** buffer) {
        if (size < 0) {
            std::cout << "allocate size is negative." << std::endl;
            return -1;
        }
        // jemalloc alloc
        //std::cout << "jemalloc allocate" << std::endl;
        *buffer = reinterpret_cast<uint8_t*> (mallocx(static_cast<size_t>(size), MALLOCX_ALIGN(alignment)));
        return 0;
    }
    static int release(uint8_t* buffer) {
        // jemalloc free
        //std::cout << "jemalloc allocate" << std::endl;
        dallocx(reinterpret_cast<void*>(buffer), MALLOCX_ALIGN(alignment));
        return 0;
    }
};

template <typename Allocator>
class BaseMemoryPoolImpl : public MemoryPool {
public:
   int allocate(int64_t size, uint8_t** buffer) override {
       Allocator::allocate(size, buffer);
       return 0;
   }

    int release(uint8_t* buffer) override {
         Allocator::release(buffer);
         return 0;
    }
    ~BaseMemoryPoolImpl() override {}
};


class JemallocMemoryPool: public BaseMemoryPoolImpl<JemallocAllocator> {
};



static JemallocMemoryPool jemallocMemoryPool;

MemoryPool *getMemoryPool()
{
    return &jemallocMemoryPool;
}

void* omni_allocate(uint64_t size) {
    uint8_t* buf;
    jemallocMemoryPool.allocate(size, &buf);
#ifdef DEBUG
    g_allocateCount += 1;
#endif
    return (void *)buf;
}

void omni_release(int64_t address) {
    uint8_t* ptr = (uint8_t*)address;
    jemallocMemoryPool.release(ptr);
#ifdef DEBUG
    g_releaseCount += 1;
#endif
}

#ifdef DEBUG
void printStatistics()
{
    while (true)
    {
        std::cout << "Allocate Count=" << g_allocateCount << ", Release Count=" << g_releaseCount << ", Leak Count=" << (g_allocateCount - g_releaseCount) << std::endl;
        sleep(10);
    }
}

static std::thread g_backThread = std::thread(printStatistics);
#endif
