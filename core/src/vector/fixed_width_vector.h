/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#ifndef __FIXED_WIDTH_VECTOR_OPERATOR_H__
#define __FIXED_WIDTH_VECTOR_OPERATOR_H__

#include "vector.h"

namespace omniruntime {
namespace vec {
template <class T> class FixedWidthVector : public Vector {
public:
    FixedWidthVector(Vector *vector, int size, int positionOffset) : Vector(vector, size, positionOffset) {};

    FixedWidthVector(VectorAllocator *allocator, int capacityInBytes, int size, VecType type)
        : Vector(allocator, capacityInBytes, size, type)
    {}

    T GetValue(int index) const;

    void SetValue(int index, T value);

    virtual void SetValues(int startIndex, const T *values, int length) = 0;

    ~FixedWidthVector() {}
};
}
}
#endif // __FIXED_WIDTH_VECTOR_OPERATOR_H__
