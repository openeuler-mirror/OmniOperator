/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef OMNI_RUNTIME_UNSAFE_VECTOR_H
#define OMNI_RUNTIME_UNSAFE_VECTOR_H

#include "vector.h"
#include "unsafe_string_container.h"
#include "unsafe_dictionary_container.h"

namespace omniruntime::vec::unsafe {
/**
 * get raw pointer of nulls in vector.
 * it is unrecommended to call this class due to high risk of manipulating raw pointer.
 */
class UnsafeBaseVector {
public:
    static ALWAYS_INLINE uint8_t *GetNulls(BaseVector *vector)
    {
        return reinterpret_cast<uint8_t *>(vector->nullsBuffer->GetNulls());
    }

    static ALWAYS_INLINE std::shared_ptr<NullsHelper> GetNullsHelper(BaseVector *vector)
    {
        return std::make_shared<NullsHelper>(vector->nullsBuffer);
    }

    static ALWAYS_INLINE void SetSize(BaseVector *vector, int size)
    {
        vector->size = size;
    }
};

/**
 * get raw pointer of values in vector.
 * it is unrecommended to call this class due to high risk of manipulating raw pointer.
 */
class UnsafeVector {
public:
    template <typename DATA_TYPE> static ALWAYS_INLINE DATA_TYPE *GetRawValues(Vector<DATA_TYPE> *vector)
    {
        return vector->values + vector->offset;
    }

    // used by dictionary container
    template <typename DATA_TYPE>
    static ALWAYS_INLINE std::shared_ptr<AlignedBuffer<DATA_TYPE>> GetValues(Vector<DATA_TYPE> *vector)
    {
        return vector->valuesBuffer;
    }
};

/**
 * get raw pointer of member in DictionaryVector.
 * it is unrecommended to call this class due to high risk of manipulating raw pointer.
 */
class UnsafeDictionaryVector {
public:
    template <typename DATA_TYPE>
    static ALWAYS_INLINE DATA_TYPE *GetDictionary(Vector<DictionaryContainer<DATA_TYPE>> *vector)
    {
        return UnsafeDictionaryContainer::GetDictionary(vector->container.get());
    }

    static ALWAYS_INLINE char *GetVarCharDictionary(
        Vector<DictionaryContainer<std::string_view, LargeStringContainer>> *vector)
    {
        return UnsafeDictionaryContainer::GetVarCharDictionary(vector->container.get());
    }

    static ALWAYS_INLINE int32_t *GetDictionaryOffsets(
        Vector<DictionaryContainer<std::string_view, LargeStringContainer>> *vector)
    {
        return UnsafeDictionaryContainer::GetDictionaryOffsets(vector->container.get());
    }

    template <typename DATA_TYPE> static ALWAYS_INLINE int *GetIds(Vector<DictionaryContainer<DATA_TYPE>> *vector)
    {
        return UnsafeDictionaryContainer::GetIds(vector->container.get()) + vector->offset;
    }

    template <typename DATA_TYPE>
    static ALWAYS_INLINE std::shared_ptr<DictionaryContainer<DATA_TYPE>> GetDictionaryOriginal(Vector<DictionaryContainer<DATA_TYPE>> *vector)
    {
        return vector->container;
    }
};

/**
 * get raw pointer of values in StringVector.
 * it is unrecommended to call this class due to high risk of manipulating raw pointer.
 */
class UnsafeStringVector {
public:
    static ALWAYS_INLINE int32_t *GetOffsets(Vector<LargeStringContainer<std::string_view>> *vector)
    {
        return UnsafeStringContainer::GetOffsets(vector->container.get()) + vector->offset;
    }

    static ALWAYS_INLINE char *GetValues(Vector<LargeStringContainer<std::string_view>> *vector)
    {
        return UnsafeStringContainer::GetValues(vector->container.get());
    }

    static ALWAYS_INLINE std::shared_ptr<LargeStringContainer<std::string_view>> GetContainer(
        Vector<LargeStringContainer<std::string_view>> *vector)
    {
        return vector->container;
    }

    static ALWAYS_INLINE char *ExpandStringBuffer(Vector<LargeStringContainer<std::string_view>> *vector,
        size_t toCapacityInBytes)
    {
        auto container = vector->container.get();
        if (container->GetCapacityInBytes() >= toCapacityInBytes) {
            return UnsafeStringContainer::GetValues(container);
        } else {
            return UnsafeStringContainer::ExpandBufferToCapacity(container, toCapacityInBytes);
        }
    }

    static ALWAYS_INLINE size_t GetCapacityInBytes(Vector<LargeStringContainer<std::string_view>> *vector)
    {
        return UnsafeStringContainer::GetCapacityInBytes(vector->container.get());
    }
};
}


#endif // OMNI_RUNTIME_UNSAFE_VECTOR_H
