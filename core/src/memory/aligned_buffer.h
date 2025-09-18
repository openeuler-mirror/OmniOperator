/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 */


#ifndef OMNI_RUNTIME_ALIGNED_BUFFER_H
#define OMNI_RUNTIME_ALIGNED_BUFFER_H
#include "allocator.h"

namespace omniruntime::mem {
template <typename RAW_DATA_TYPE> class AlignedBuffer {
public:
    AlignedBuffer() : buffer(nullptr), capacity(0)
    {
        allocator = Allocator::GetAllocator();
    }

    AlignedBuffer(size_t size, bool zerofill = false)
    {
        capacity = size * sizeof(RAW_DATA_TYPE);
        allocator = Allocator::GetAllocator();
        buffer = reinterpret_cast<RAW_DATA_TYPE *>(allocator->Alloc(capacity, zerofill));
    }

    ~AlignedBuffer()
    {
        Release();
    }

    ALWAYS_INLINE RAW_DATA_TYPE *AllocateReuse(size_t size, bool zerofill)
    {
        if (buffer == nullptr) {
            // no memory allocated, creating new one
            capacity = size * sizeof(RAW_DATA_TYPE);
            buffer = reinterpret_cast<RAW_DATA_TYPE *>(allocator->Alloc(capacity, zerofill));
            return buffer;
        }

        size_t newCapacity = size * sizeof(RAW_DATA_TYPE);
        if (capacity < newCapacity) {
            // memory already allocated, but cannot hold newCapacity
            // releasing previous buffer and allocating new one
            allocator->Free(buffer, capacity);
            capacity = newCapacity;
            buffer = reinterpret_cast<RAW_DATA_TYPE *>(allocator->Alloc(capacity, zerofill));
            return buffer;
        }

        // memory already allocated, and can hold newCapacity
        // just set content to zero (if needed) and return previous buffer
        if (zerofill) {
            // since capacity is usually large (> 1024 bytes), we can use memset_sp.
            // Based on benchmark memset_sp for large buffer sizes is not much worse than std::meset
            // but for smaller buffers (i.e. less than 100 bytes), memset_sp is x2 to x4 times slower than std::meset
            memset(buffer, 0, newCapacity);
        }
        return buffer;
    }

    ALWAYS_INLINE RAW_DATA_TYPE *GetBuffer()
    {
        return buffer;
    }

    ALWAYS_INLINE RAW_DATA_TYPE GetValue(int32_t index)
    {
        return buffer[index];
    }

private:
    ALWAYS_INLINE void Release()
    {
        if (buffer != nullptr) {
            allocator->Free(buffer, capacity);
        }
        buffer = nullptr;
        capacity = 0;
    }

    RAW_DATA_TYPE *buffer;
    size_t capacity;
    Allocator *allocator;
};
}


#endif // OMNI_RUNTIME_ALIGNED_BUFFER_H
