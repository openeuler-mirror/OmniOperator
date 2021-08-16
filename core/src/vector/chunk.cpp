/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
//
// Created by root on 6/1/21.
//

#include "chunk.h"
#include "../memory/memory_pool.h"

namespace omniruntime {
namespace vec {
Chunk::Chunk(int64_t sizeInBytes) : sizeInBytes(sizeInBytes), address(OmniAllocate(sizeInBytes))
{}

Chunk::~Chunk()
{
    OmniRelease((int64_t)address);
}

void *Chunk::GetAddress() const
{
    return address;
}

int64_t Chunk::GetSizeInBytes()
{
    return sizeInBytes;
}
} // namespace vec
} // namespace omniruntime