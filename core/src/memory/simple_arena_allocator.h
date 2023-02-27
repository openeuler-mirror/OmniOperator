/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#ifndef SIMPLE_ARENA_ALLOCATOR_H
#define SIMPLE_ARENA_ALLOCATOR_H

#include <vector>
#include "chunk.h"
#include "base_allocator.h"

namespace omniruntime {
namespace mem {
// this allocator is not thread-safe, and mainly applies for temporary memory usage for operators,
// such as when dealing with types such as varchar/decimal and so on.
class SimpleArenaAllocator {
public:
    explicit SimpleArenaAllocator(int64_t minChunkSize = 4096,
        BaseAllocator *allocator = mem::GetProcessRootAllocator())
        : minChunkSize(minChunkSize), totalBytes(0), availBytes(0), availBuf(NULL), allocator(allocator)
    {}

    ~SimpleArenaAllocator()
    {
        ReleaseChunks(false /* retainFirst */);
    }

    uint8_t *Allocate(int64_t sizeInBytes)
    {
        if (sizeInBytes == 0) {
            static Chunk *ZERO_CHUNK = Chunk::NewChunk(allocator, 0);
            return static_cast<uint8_t *>(ZERO_CHUNK->GetAddress());
        }
        if (availBytes < sizeInBytes) {
            AllocateChunk(std::max(sizeInBytes, minChunkSize));
        }
        continuousUsedMemoryBytes = sizeInBytes;
        uint8_t *ret = availBuf;
        availBuf += sizeInBytes;
        availBytes -= sizeInBytes;
        return ret;
    }

    uint8_t *AllocateContinue(int64_t sizeInBytes, const uint8_t *&start)
    {
        // null means a new begin of allocate
        if (start == nullptr) {
            uint8_t *ret = (Allocate(sizeInBytes));
            start = (ret);
            return ret;
        } else {
            return (AllocateContinueNotNull(sizeInBytes, start));
        }
    }

    void Reset()
    {
        if (chunks.size() == 0) {
            // if there are no chunks, nothing to do.
            return;
        }

        // Release all but the first chunk.
        if (chunks.size() > 1) {
            ReleaseChunks(true);
            chunks.erase(chunks.cbegin() + 1, chunks.cend());
        }

        availBuf = reinterpret_cast<uint8_t *>(chunks.at(0)->GetAddress());
        availBytes = totalBytes = chunks.at(0)->GetSizeInBytes();
    }

    void RollBackContinualMem()
    {
        availBuf -= continuousUsedMemoryBytes;
        availBytes += continuousUsedMemoryBytes;
    }

    int64_t TotalBytes()
    {
        return totalBytes;
    }

    int64_t AvailBytes()
    {
        return availBytes;
    }

    void SetAllocator(BaseAllocator *allocator)
    {
        this->allocator = allocator;
    }

    BaseAllocator *GetAllocator()
    {
        return this->allocator;
    }

private:
    void AllocateChunk(int64_t sizeInBytes)
    {
        Chunk *chunk = Chunk::NewChunk(allocator, sizeInBytes);

        chunks.emplace_back(chunk);
        availBuf = reinterpret_cast<uint8_t *>(chunk->GetAddress());
        availBytes = sizeInBytes; // left-over bytes in the previous chunk cannot be used anymore.
        totalBytes += sizeInBytes;
    }

    void ReleaseChunks(bool retainFirst)
    {
        for (auto &chunk : chunks) {
            if (retainFirst) {
                // skip freeing first chunk.
                retainFirst = false;
                continue;
            }
            delete chunk;
        }
        continuousUsedMemoryBytes = 0;
    }

    uint8_t *AllocateContinueNotNull(int64_t sizeInBytes, const uint8_t *&start)
    {
        auto *p = const_cast<uint8_t *>(start);
        uint8_t *ret = p;
        if (sizeInBytes == 0) {
            return ret;
        }
        auto newSpace = continuousUsedMemoryBytes + sizeInBytes;
        if (availBytes < sizeInBytes) {
            AllocateChunk(std::max(newSpace, minChunkSize));
            memcpy(availBuf, start, continuousUsedMemoryBytes);
            start = availBuf;
            availBuf += continuousUsedMemoryBytes;
            availBytes -= continuousUsedMemoryBytes;
        }
        ret = availBuf;
        availBuf += sizeInBytes;
        continuousUsedMemoryBytes += sizeInBytes;
        availBytes -= sizeInBytes;
        return ret;
    }

    int64_t minChunkSize;
    int64_t totalBytes;
    int64_t availBytes;
    uint8_t *availBuf;
    // Record the size of the memory used continuously.
    uint32_t continuousUsedMemoryBytes;
    std::vector<Chunk *> chunks;
    BaseAllocator *allocator;
};
} // namespace mem
} // namespace omniruntime
#endif // SIMPLE_ARENA_ALLOCATOR_H
