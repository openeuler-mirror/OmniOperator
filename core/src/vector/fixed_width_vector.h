#ifndef __FIXED_WIDTH_VECTOR_OPERATOR_H__
#define __FIXED_WIDTH_VECTOR_OPERATOR_H__

#include "vector.h"

template<class T>
class FixedWidthVector : public Vector {
public:
    FixedWidthVector(Vector *vector, int size, int positionOffset) : Vector(vector, size, positionOffset) {};

    FixedWidthVector(VectorAllocator *allocator, int capacityInBytes, int size, VecType type) : Vector(allocator,
                                                                                                       capacityInBytes,
                                                                                                       size,
                                                                                                       type) {}

    T getValue(int index);

    void setValue(int index, T value);

    virtual void setValues(int startIndex, T *values, int length) = 0;
};

#endif //__FIXED_WIDTH_VECTOR_OPERATOR_H__
