/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
//
// Created by root on 6/1/21.
//

#ifndef __VECTOR_H__
#define __VECTOR_H__

#include "../../../huawei_secure_c/include/securec.h"
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

    virtual ~Vector();

    virtual int GetSize();

    void SetSize(int size);

    int GetPositionOffset();

    VectorReference *GetReference() const;

    VectorAllocator *GetAllocator() const;

    virtual VecType GetType();

    void *GetValues() const;

    void *GetValueNulls() const;

    void *GetValueOffsets() const;

    bool IsValueNull(int index)
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
    void *valuesAddress;
    void *valueNullsAddress;
    void *valueOffsetsAddress;
    int positionOffset = 0;
    int capacityInBytes = 0;
    int size = 0;

private:
    VectorReference *reference;
    VectorAllocator *allocator;
};
} // namespace vec
} // namespace omniruntime
#endif // __VECTOR_H__
