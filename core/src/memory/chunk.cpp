/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#include "chunk.h"
#include "memory/memory_pool.h"

namespace omniruntime {
namespace mem {
Chunk::Chunk(int64_t sizeInBytes) : sizeInBytes(sizeInBytes), address(OmniAllocate(sizeInBytes)) {}

Chunk::~Chunk()
{
    if (address == nullptr) {
        std::cerr << "address is null in chunk." << std::endl;
        return;
    }
    OmniRelease(reinterpret_cast<int64_t>(address));
    address = nullptr;
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