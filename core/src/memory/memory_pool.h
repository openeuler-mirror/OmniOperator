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
    virtual void Allocate(int64_t size, uint8_t **buffer, bool zeroFill = false) = 0;

    virtual void Release(uint8_t *buffer) = 0;

    virtual ~MemoryPool() {}

    virtual uint64_t GetPreferredSize(uint64_t size) = 0;

protected:
    MemoryPool() = default;
};

MemoryPool *GetMemoryPool();
} // namespace mem
} // namespace omniruntime
#endif // MEMORY_POOL_H
