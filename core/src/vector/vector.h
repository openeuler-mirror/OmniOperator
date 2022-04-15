/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#ifndef __VECTOR_H__
#define __VECTOR_H__

#include "../util/debug.h"
#include "../util/compiler_util.h"
#include "../type/data_type.h"
#include "vector_reference.h"
#include "vector_allocator.h"
#include "vector_encoding.h"

namespace omniruntime {
namespace vec {
using DataTypeId = type::DataTypeId;
class Vector {
public:
    Vector(VectorAllocator *allocator, int capacityInBytes, int size, DataTypeId dataTypeId);

    virtual ~Vector();

    int GetSize() const
    {
        return size;
    }

    void SetSize(int newSize)
    {
        this->size = newSize;
    }

    int GetPositionOffset()
    {
        return positionOffset;
    }

    VectorReference *GetVectorReference()
    {
        return this->reference;
    }

    void SetVectorReference(VectorReference *vectorReference)
    {
        this->reference = vectorReference;
    }

    int64_t GetReference() const
    {
        return reference->GetRef();
    }

    VectorAllocator *GetAllocator() const
    {
        return allocator;
    }

    void *GetValues() const
    {
        return valuesAddress;
    }

    DataTypeId GetTypeId() const
    {
        return dataTypeId;
    }

    int GetCapacityInBytes() const
    {
        return capacityInBytes;
    }

    void *GetValueNulls() const
    {
        return valueNullsAddress;
    }

    void *GetValueOffsets() const
    {
        return valueOffsetsAddress;
    }

    bool IsValueNull(int index)
    {
        return (reinterpret_cast<bool *>(valueNullsAddress))[index + positionOffset];
    }

    void SetValueNull(int index)
    {
        (reinterpret_cast<bool *>(valueNullsAddress))[index + positionOffset] = true;
    }

    virtual void SetValueNull(int index, bool value)
    {
        (reinterpret_cast<bool *>(valueNullsAddress))[index + positionOffset] = value;
    }

    void SetValueNotNull(int index)
    {
        (reinterpret_cast<bool *>(valueNullsAddress))[index + positionOffset] = false;
    }

    int GetValueOffset(int index)
    {
        return static_cast<int32_t *>(valueOffsetsAddress)[index];
    }

    void SetValueOffset(int index, int valueOffset)
    {
        (static_cast<int32_t *>(valueOffsetsAddress))[index] = valueOffset;
    }

    template <class T> T ALWAYS_INLINE *As()
    {
        static_assert(std::is_base_of<Vector, T>::value, "Unsupported type cast.");
        return static_cast<T *>(this);
    }

    template <class T> const T ALWAYS_INLINE *As()
    {
        static_assert(std::is_base_of<Vector, T>::value, "Unsupported type cast.");
        return static_cast<const T *>(this);
    }

    virtual Vector *Slice(int positionOffset, int length) = 0;

    virtual Vector *CopyPositions(const int *positions, int offset, int length) = 0;

    virtual Vector *CopyRegion(int positionOffset, int length) = 0;

    virtual void Append(Vector *other, int positionOffset, int length) = 0;

    virtual int64_t ExpandDataCapacity(int32_t toCapacityInBytes)
    {
        LogInfo("%s", "unsupported expandDataCapacity");
        return 0;
    }

    void RecordStack(std::string &stack, VecOpType opType);

    void SetVectorTracer(VectorTracer *vectorTracer);

    VectorTracer *GetVectorTracer();

    virtual VectorEncoding GetEncoding()
    {
        return OMNI_VEC_ENCODING_FLAT;
    }

protected:
    // this method is mainly used for vector slice
    Vector(Vector *vector, int size, int positionOffset);

    // this method does not apply for memory for chunk,it is mainly used for dictionary vector or other vector
    Vector(VectorAllocator *allocator, int capacityInBytes, int size, DataType type, int32_t positionOffset);

    void SetValueNulls(int startIndex, bool *nulls, int length);

    void *valuesAddress = nullptr;
    void *valueNullsAddress = nullptr;
    void *valueOffsetsAddress = nullptr;
    int positionOffset = 0;
    int capacityInBytes = 0;
    int size = 0;
    DataTypeId dataTypeId;
    VectorReference *reference = nullptr;
    VectorTracer *tracer = nullptr;
    VectorAllocator *allocator = nullptr;
};
} // namespace vec
} // namespace omniruntime
#endif // __VECTOR_H__
