#define JEMALLOC_NO_DEMANGLE
#include <iostream>
#include "memory_pool.h"
#include <jemalloc/jemalloc.h>
//#include "./jemalloc/jemalloc_defs.h"
class JemallocAllocator {
        public:
    static int allocate(int64_t size, uint8_t** buffer) {
        if (size == 0) {
            std::cout << "allocate size is 0" << std::endl;
            return -1;
        }
        // jemalloc alloc
        //std::cout << "jemalloc allocate" << std::endl;
        *buffer = reinterpret_cast<uint8_t*>(je_malloc(static_cast<size_t>(size)));
        return 0;
    }
    static int release(uint8_t* buffer) {
        // jemalloc free
        //std::cout << "jemalloc allocate" << std::endl;
        je_free(reinterpret_cast<void*>(buffer));
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
    return (void *)buf;
}

void omni_release(int64_t address) {
    uint8_t* ptr = (uint8_t*)address;
    jemallocMemoryPool.release(ptr);
}

