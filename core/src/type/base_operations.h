/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2023. All rights reserved.
 * Description: base operations implementation
 */

#ifndef OMNI_RUNTIME_BASE_OPERATIONS_H
#define OMNI_RUNTIME_BASE_OPERATIONS_H

#include <iostream>
#include "width_integer.h"

namespace omniruntime::type {
static constexpr int128_t DECIMAL128_MAX_VALUE = (int128_t(5421010862427522170) << 64) + 687399551400673279;
static constexpr int128_t DECIMAL128_MIN_VALUE = -((int128_t(5421010862427522170) << 64) + 687399551400673279);

template<typename T>
static inline bool AddCheckedOverflow(T left, T right, T &result)
{
    bool isOverflow = __builtin_add_overflow(left, right, &result);
    return isOverflow || (result > DECIMAL128_MAX_VALUE) || (result < DECIMAL128_MIN_VALUE);
}

template<typename T>
static inline bool MulCheckedOverflow(T left, T right, T &result)
{
    bool isOverflow = __builtin_mul_overflow(left, right, &result);
    return isOverflow || (result > DECIMAL128_MAX_VALUE) || (result < DECIMAL128_MIN_VALUE);
}

template<typename T>
static inline bool DivideRoundUp(T left, T right, T &result)
{
    if (right == 0) {
        return true;
    }
    T temp = right / 2;
    if ((left > 0 && right < 0) || (left < 0 && right > 0)) {
        temp = -temp;
    }
    result = (left + temp) / right;
    return false;
}
}

#endif //OMNI_RUNTIME_BASE_OPERATIONS_H
