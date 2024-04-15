/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2024. All rights reserved.
 */

#ifndef CHUNK_H
#define CHUNK_H

#include <iostream>
#include <memory>
#include "allocator.h"

namespace omniruntime {
namespace mem {
class Chunk {
public:
    ~Chunk();

    void *GetAddress() const;

    uint64_t GetSizeInBytes();

    static Chunk *NewChunk(Allocator *globalAllocator, uint64_t sizeInByte, bool zeroFill = false)
    {
        void *data = globalAllocator->Alloc(static_cast<int64_t>(sizeInByte));
        if (data != nullptr) {
            return new Chunk(globalAllocator, data, sizeInByte);
        }
        throw OmniException("MEMORY_CHUNK_ERROR", "NewChunk return nullptr error");
    }

protected:
    explicit Chunk(Allocator *allocator, void *address, uint64_t sizeInBytes);

private:
    void *address = nullptr;
    uint64_t sizeInBytes;
    Allocator *allocator = nullptr;
};
} // namespace mem
} // namespace omniruntime
#endif // CHUNK_H
