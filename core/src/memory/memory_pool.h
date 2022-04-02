/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#ifndef __MEMORY_POOL_H__
#define __MEMORY_POOL_H__
#pragma once

#include <iostream>

namespace omniruntime {
namespace mem {
class MemoryPool {
public:
    virtual int Allocate(int64_t size, uint8_t **buffer) = 0;
    virtual int Release(uint8_t *buffer) = 0;
    virtual ~MemoryPool() {}

protected:
    MemoryPool() = default;
};
}
}

#ifdef __cplusplus
extern "C" {
#endif
void *OmniAllocate(uint64_t size);
void OmniRelease(unsigned long address);
#ifdef __cplusplus
}
#endif
#endif
