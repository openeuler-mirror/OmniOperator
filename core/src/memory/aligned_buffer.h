/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 */


#ifndef OMNI_RUNTIME_ALIGNED_BUFFER_H
#define OMNI_RUNTIME_ALIGNED_BUFFER_H
#include "allocator.h"

namespace omniruntime::mem {
template <typename RAW_DATA_TYPE> class AlignedBuffer {
public:
    AlignedBuffer(size_t size, bool zerofill = false)
    {
        capacity = size * sizeof(RAW_DATA_TYPE);
        allocator = Allocator::GetAllocator();
        buffer = allocator->Alloc(capacity, zerofill);

        /* *
         * std::string object needs to be initialized, the reason is follow:
         * _M_dataplus variable in std::string is NULL if RAW_DATA_TYPE is std::string, when values variable is
         * allocated memory by mallocx. e.g. std::string object only allocate memory but call constructor. It causes
         * move constructor failed.
         */
        if constexpr (std::is_same_v<std::string, RAW_DATA_TYPE>) {
            for (int index = 0; index < size; ++index) {
                RAW_DATA_TYPE *valuesPtr = reinterpret_cast<RAW_DATA_TYPE *>(buffer);
                new (&valuesPtr[index]) std::string;
            }
        }
    }

    ~AlignedBuffer()
    {
        allocator->Free(buffer, capacity);
    }

    ALWAYS_INLINE RAW_DATA_TYPE *GetBuffer()
    {
        return reinterpret_cast<RAW_DATA_TYPE *>(buffer);
    }

    ALWAYS_INLINE RAW_DATA_TYPE GetValue(int32_t index)
    {
        return reinterpret_cast<RAW_DATA_TYPE *>(buffer)[index];
    }

private:
    void *buffer;
    size_t capacity;
    Allocator *allocator;
};
}


#endif // OMNI_RUNTIME_ALIGNED_BUFFER_H
