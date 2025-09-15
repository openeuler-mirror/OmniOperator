/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2024. All rights reserved.
 */

#include "chunk.h"

namespace omniruntime {
namespace mem {
Chunk::Chunk(Allocator *allocator, void *address, uint64_t sizeInBytes)
    : address(address), sizeInBytes(sizeInBytes), allocator(allocator)
{}

Chunk::~Chunk()
{
    if (address == nullptr) {
        std::cerr << "address is null in chunk." << std::endl;
        return;
    }

    allocator->Free(address, static_cast<int64_t>(sizeInBytes));
    address = nullptr;
}

void *Chunk::GetAddress() const
{
    return address;
}

uint64_t Chunk::GetSizeInBytes()
{
    return sizeInBytes;
}
} // namespace mem
} // namespace omniruntime