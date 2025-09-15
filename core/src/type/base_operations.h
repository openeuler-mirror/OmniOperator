/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2024. All rights reserved.
 * Description: base operations implementation
 */

#ifndef OMNI_RUNTIME_BASE_OPERATIONS_H
#define OMNI_RUNTIME_BASE_OPERATIONS_H

#include <iostream>
#include <limits>
#include "width_integer.h"

namespace omniruntime::type {
enum Status {
    CONVERT_SUCCESS,
    CONVERT_OVERFLOW,
    IS_NOT_A_NUMBER
};

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

template<typename T>
static inline bool NegateCheckedOverflow(const T a, T &r)
{
    if (UNLIKELY(a == std::numeric_limits<T>::min())) {
        return true;
    }
    r = std::negate<std::remove_cv_t<T>>()(a);
    return false;
}
}

#endif //OMNI_RUNTIME_BASE_OPERATIONS_H
