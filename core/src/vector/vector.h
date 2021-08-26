/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#ifndef __VECTOR_H__
#define __VECTOR_H__

#include "../../thirdparty/huawei_secure_c/include/securec.h"
#include "../util/debug.h"
#include "../util/bitmap_util.h"
#include "vector_reference.h"
#include "vector_allocator.h"
#include "vector_type.h"

namespace omniruntime {
namespace vec {
class Vector {
public:
    Vector(VectorAllocator *allocator, int capacityInBytes, int size, VecType type);

    Vector(Vector *vector, int size, int offset);

    Vector() {}

    virtual ~Vector();

    virtual int GetSize()
    {
        return size;
    }

    void SetSize(int size)
    {
        this->size = size;
    }

    int GetPositionOffset()
    {
        return positionOffset;
    }

    int64_t GetReference() const
    {
        return reference->GetRef();
    }

    virtual VectorAllocator *GetAllocator() const
    {
        return allocator;
    }

    void *GetValues() const
    {
        return valuesAddress;
    }

    virtual const VecType &GetType()
    {
        return reference->GetType();
    }

    virtual int GetCapacityInBytes() const
    {
        return reference->GetValueChunk()->GetSizeInBytes();
    }

    void *GetValueNulls() const
    {
        return valueNullsAddress;
    }

    virtual int GetValueNullsSizeInBytes() const
    {
        return reference->GetValueNullChunk()->GetSizeInBytes();
    }

    void *GetValueOffsets() const
    {
        return valueOffsetsAddress;
    }

    int GetValueOffsetsInBytes() const
    {
        return reference->GetValueOffsetChunk()->GetSizeInBytes();
    }

    virtual bool IsValueNull(int index)
    {
        return (reinterpret_cast<bool *>(valueNullsAddress))[index + positionOffset];
    }

    void SetValueNull(int index)
    {
        (reinterpret_cast<bool *>(valueNullsAddress))[index + positionOffset] = true;
    }

    void SetValueNotNull(int index)
    {
        (reinterpret_cast<bool *>(valueNullsAddress))[index + positionOffset] = false;
    }

    void SetValueNulls(int startIndex, bool *nulls, int length);

    void SetValueNullBitMap(int startIndex);

    int GetValueOffset(int index)
    {
        return static_cast<int32_t *>(valueOffsetsAddress)[index];
    }

    void SetValueOffset(int index, int valueOffset)
    {
        (static_cast<int32_t *>(valueOffsetsAddress))[index] = valueOffset;
    }

    virtual Vector *Slice(int positionOffset, int length) = 0;

    virtual Vector *CopyPositions(const int *positions, int offset, int length) = 0;

    virtual Vector *CopyRegion(int positionOffset, int length) = 0;

    virtual void Append(Vector *other, int positionOffset, int length) = 0;

protected:
    void *valuesAddress = nullptr;
    void *valueNullsAddress = nullptr;
    void *valueOffsetsAddress = nullptr;
    int positionOffset = 0;
    int capacityInBytes = 0;
    int size = 0;
    VectorReference *reference = nullptr;
    VectorAllocator *allocator = nullptr;
};
} // namespace vec
} // namespace omniruntime
#endif // __VECTOR_H__
