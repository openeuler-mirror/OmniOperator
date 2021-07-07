//
// Created by root on 6/1/21.
//

#ifndef __VECTOR_H__
#define __VECTOR_H__

#include "../util/debug.h"
#include "../util/bitmap_util.h"
#include "vector_reference.h"
#include "vector_allocator.h"
#include "vector_type.h"

class Vector {
public:
    Vector(VectorAllocator *allocator, int capacityInBytes, int size, VecType type);

    Vector(Vector *vector, int size, int offset);

    virtual ~Vector();

    int getSize();

    void setSize(int size);

    int getPositionOffset();

    VectorReference *getReference();

    VectorAllocator *getAllocator();

    VecType getType();

    void *getValues();

    void *getValueNulls();

    void *getValueOffsets();

    bool isValueNull(int index) {
        ASSERT(index < size);
        ASSERT(valueNullsAddress != nullptr);
        return (reinterpret_cast<bool *>(valueNullsAddress))[index + positionOffset];
    }

    void setValueNull(int index) {
        ASSERT(index < size);
        ASSERT(valueNullsAddress != nullptr);
        (reinterpret_cast<bool *>(valueNullsAddress))[index + positionOffset] = true;
    }

    void setValueNulls(int startIndex, bool *nulls, int length);

    void setValueNullBitMap(int startIndex);

    int getValueOffset(int index) {
        ASSERT(index < size + 1);
        return ((int32_t *) (valueOffsetsAddress))[index];
    }

    void setValueOffset(int index, int valueOffset) {
        ASSERT(index < size + 1);
        ((int32_t *) (valueOffsetsAddress))[index] = valueOffset;
    }

    virtual Vector *slice(int positionOffset, int length) = 0;

    virtual Vector *copyPositions(int *positions, int offset, int length) = 0;

    virtual Vector *copyRegion(int positionOffset, int length) = 0;

    virtual void append(Vector *other, int positionOffset, int length) = 0;

protected:
    void *valuesAddress;
    void *valueNullsAddress;
    void *valueOffsetsAddress;
    int positionOffset = 0;
private:
    int size;
    VectorReference *reference;
    VectorAllocator *allocator;
};

#endif //__VECTOR_H__
