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

    bool isValueNull(int index);

    void setValueNull(int index);

    void setValueNulls(int startIndex, bool *nulls, int length);

    void setValueNullBitMap(int startIndex);

    int getValueOffset(int index);

    void setValueOffset(int index, int valueOffset);

    virtual Vector *slice(int positionOffset, int length) = 0;

    virtual Vector *copyPositions(int *positions, int offset, int length) = 0;

    virtual Vector *copyRegion(int positionOffset, int length) = 0;

protected:
    void *valuesAddress;
    void *valueNullsAddress;
    void *valueOffsetsAddress;
private:
    int size;
    int positionOffset;
    VectorReference *reference;
    VectorAllocator *allocator;
};

#endif //__VECTOR_H__
