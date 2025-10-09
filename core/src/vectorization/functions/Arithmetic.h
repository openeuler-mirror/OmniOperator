/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#pragma once
#include "util/compiler_util.h"
#include <cmath>
#include <stdexcept>

namespace omniruntime {
template <typename T>
struct PlusFunction {
    template <typename TInput>
    ALWAYS_INLINE void call(TInput &result, const TInput &a, const TInput &b)
    {
        result = a + b;
    }
};

template <typename T>
struct MinusFunction {
    template <typename TInput>
    ALWAYS_INLINE void call(TInput &result, const TInput &a, const TInput &b)
    {
        result = a - b;
    }
};

template <typename T>
struct MultiplyFunction {
    template <typename TInput>
    ALWAYS_INLINE void call(TInput &result, const TInput &a, const TInput &b)
    {
        result = a * b;
    }
};

template <typename T>
struct DivideFunction {
    template <typename TInput>
    ALWAYS_INLINE void call(TInput &result, const TInput &a, const TInput &b)
    {
        if (b == 0) {
            throw std::invalid_argument("Division by zero is not allowed");
        }
        
        result = a / b;
    }
};

template <typename T>
struct RemainderFunction {
    template <typename TInput>
    ALWAYS_INLINE void call(TInput &result, const TInput &a, const TInput &b)
    {
        if (b == 0) {
            throw std::invalid_argument("Division by zero is not allowed");
        }
        
        if constexpr (std::is_integral_v<TInput>) {
            result = a % b;
        } else {
            result = std::fmod(a, b);
        }
    }
};
}
