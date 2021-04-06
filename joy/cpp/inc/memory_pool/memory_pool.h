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

#endif
