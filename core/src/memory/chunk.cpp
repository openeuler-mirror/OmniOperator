/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */
//
// Created by root on 6/1/21.
//

#include "chunk.h"
#include "../memory/memory_pool.h"

namespace omniruntime {
namespace mem {
Chunk::Chunk(int64_t sizeInBytes)
    :sizeInBytes(sizeInBytes), address(OmniAllocate(sizeInBytes))
{}

Chunk::~Chunk()
{
    OmniRelease(reinterpret_cast<int64_t>(address));
}

void *Chunk::GetAddress() const
{
    return address;
}

int64_t Chunk::GetSizeInBytes()
{
    return sizeInBytes;
}
} // namespace mem
} // namespace omniruntime