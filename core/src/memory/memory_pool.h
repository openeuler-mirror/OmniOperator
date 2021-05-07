#ifndef __MEMORY_POOL_H__
#define __MEMORY_POOL_H__
#pragma once

#include <iostream>

class MemoryPool {
    public:
    virtual int allocate(int64_t size, uint8_t** buffer) = 0;
    virtual int release(uint8_t* buffer) = 0;
    virtual ~MemoryPool(){}
    protected:
    MemoryPool() = default;
};

MemoryPool *getMemoryPool();
#ifdef __cplusplus
extern "C" {
#endif
void* omni_allocate(uint64_t size);
void omni_release(int64_t address);
#ifdef __cplusplus
}
#endif
#endif
