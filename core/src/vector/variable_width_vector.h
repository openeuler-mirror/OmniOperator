/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#ifndef __VARIABLE_WIDTH_VECTOR_OPERATOR_H__
#define __VARIABLE_WIDTH_VECTOR_OPERATOR_H__

#include "vector.h"

namespace omniruntime {
namespace vec {
template <class T> class VariableWidthVector : public Vector {
public:
    VariableWidthVector(Vector *vector, int size, int positionOffset) : Vector(vector, size, positionOffset) {};

    VariableWidthVector(VectorAllocator *pAllocator, int capacityInBytes, int size, VecType type)
        : Vector(pAllocator, capacityInBytes, size, type)
    {}

    int GetValue(int index, T *dst);

    void SetValue(int index, const T data, int length);

    void SetValueNull(int index)
    {
        FillSlots(index);
        (reinterpret_cast<bool *>(valueNullsAddress))[index + positionOffset] = true;
        SetValueOffset(index + 1, GetValueOffset(index));
        lastOffsetPosition = index;
    }

    void SetValueNull(int index, bool value)
    {
        FillSlots(index);
        (reinterpret_cast<bool *>(valueNullsAddress))[index + positionOffset] = value;
        if (value) {
            SetValueOffset(index + 1, GetValueOffset(index));
        }
        lastOffsetPosition = index;
    }

protected:
    void FillSlots(int index)
    {
        for (int i = lastOffsetPosition + 1; i < index; i++) {
            SetValueOffset(i + 1, GetValueOffset(i));
        }
        lastOffsetPosition = index - 1;
    }

    int32_t  lastOffsetPosition = -1;

    ~VariableWidthVector() {}
};
}
}
#endif // __VARIABLE_WIDTH_VECTOR_OPERATOR_H__
