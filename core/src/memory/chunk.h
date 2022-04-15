/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#ifndef CHUNK_H
#define CHUNK_H

#include <iostream>
#include "base_allocator.h"

namespace omniruntime {
namespace mem {
class Chunk {
public:
    ~Chunk();

    void *GetAddress() const;

    int64_t GetSizeInBytes();

    BaseAllocator *GetAllocator()
    {
        return allocator;
    }

    static Chunk* NewChunk(BaseAllocator *allocator, int64_t sizeInBytes)
    {
        void *data = allocator->alloc(sizeInBytes);
        if (data != nullptr) {
            return new Chunk(allocator, data, sizeInBytes);
        }
        return nullptr;
    }

protected:
    explicit Chunk(BaseAllocator *allocator, void *address, int64_t sizeInBytes);

private:
    void *address = nullptr;
    int64_t sizeInBytes;
    BaseAllocator *allocator;
};
} // namespace mem
} // namespace omniruntime
#endif // CHUNK_H
