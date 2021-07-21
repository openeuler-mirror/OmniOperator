/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#ifndef __VARIABLE_WIDTH_VECTOR_OPERATOR_H__
#define __VARIABLE_WIDTH_VECTOR_OPERATOR_H__

#include "vector.h"

template <class T> class VariableWidthVector : public Vector {
public:
    VariableWidthVector(Vector *vector, int size, int positionOffset) : Vector(vector, size, positionOffset) {};

    VariableWidthVector(VectorAllocator *pAllocator, int capacityInBytes, int size, VecType type)
        : Vector(pAllocator, capacityInBytes, size, type)
    {}
    int GetValue(int index, T *dst);

    void SetValue(int index, const T data, int length);

    ~VariableWidthVector() {}
};
#endif // __VARIABLE_WIDTH_VECTOR_OPERATOR_H__
