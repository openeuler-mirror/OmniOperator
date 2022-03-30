/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#ifndef SIMPLE_ARENA_ALLOCATOR_H
#define SIMPLE_ARENA_ALLOCATOR_H

#include <vector>
#include "chunk.h"

namespace omniruntime {
namespace mem {
// / \brief Simple arena allocator.
// /
// / Memory is allocated from system in units of chunk-size, and dished out in the
// / requested sizes. If the requested size > chunk-size, allocate directly from the
// / system.
// /
// / The allocated memory gets released only when the arena is destroyed, or on
// / Reset.
// /
// / This code is not multi-thread safe, and avoids all locking for efficiency.
// /
class SimpleArenaAllocator {
public:
    explicit SimpleArenaAllocator(int64_t minChunkSize = 4096);

    ~SimpleArenaAllocator();

    // Allocate buffer of requested size.
    uint8_t *Allocate(int64_t sizeInBytes);

    // Reset arena state.
    void Reset();

    // total bytes allocated from system.
    int64_t TotalBytes()
    {
        return totalBytes;
    }

    // total bytes available for allocations.
    int64_t AvailBytes()
    {
        return availBytes;
    }

private:
    // Allocate new chunk.
    void AllocateChunk(int64_t sizeInBytes);

    // release memory from buffers.
    void ReleaseChunks(bool retainFirst);

    // The chunk-size used for allocations from system.
    int64_t minChunkSize;

    // Total bytes allocated from system.
    int64_t totalBytes;

    // Bytes available from allocated chunk.
    int64_t availBytes;

    // buffer from current chunk.
    uint8_t *availBuf;

    // List of allocated chunks.
    std::vector<Chunk *> chunks;
};

inline SimpleArenaAllocator::SimpleArenaAllocator(int64_t minChunkSize)
    : minChunkSize(minChunkSize), totalBytes(0), availBytes(0), availBuf(nullptr)
{}

inline SimpleArenaAllocator::~SimpleArenaAllocator()
{
    ReleaseChunks(false /* retainFirst */);
}

inline uint8_t *SimpleArenaAllocator::Allocate(int64_t sizeInBytes)
{
    if (availBytes < sizeInBytes) {
        AllocateChunk(std::max(sizeInBytes, minChunkSize));
    }

    uint8_t *ret = availBuf;
    availBuf += sizeInBytes;
    availBytes -= sizeInBytes;
    return ret;
}

inline void SimpleArenaAllocator::AllocateChunk(int64_t sizeInBytes)
{
    // TODO:add new chunk record
    Chunk *chunk = new Chunk(sizeInBytes);

    chunks.emplace_back(chunk);
    availBuf = reinterpret_cast<uint8_t *>(chunk->GetAddress());
    availBytes = sizeInBytes; // left-over bytes in the previous chunk cannot be used anymore.
    totalBytes += sizeInBytes;
}

// In the most common case, a chunk will be allocated when processing the first record.
// And, the same chunk can be used for processing the remaining records in the batch.
// By retaining the first chunk, the number of malloc calls are reduced to one per batch,
// instead of one per record.
inline void SimpleArenaAllocator::Reset()
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

inline void SimpleArenaAllocator::ReleaseChunks(bool retainFirst)
{
    for (auto &chunk : chunks) {
        if (retainFirst) {
            // skip freeing first chunk.
            retainFirst = false;
            continue;
        }
        // TODO:add release chunk record
        delete chunk;
    }
}
} // namespace mem
} // namespace omniruntime
#endif // SIMPLE_ARENA_ALLOCATOR_H
