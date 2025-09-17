/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#pragma once
#include "util/compiler_util.h"

namespace omniruntime::vectorization {
template <typename T>
struct Greater {
    template <typename TInput>
    ALWAYS_INLINE void call(bool &result, const TInput &a, const TInput &b)
    {
        result = a > b;
    }
};

template <typename T>
struct Not {
    ALWAYS_INLINE void call(bool &result, const bool &a)
    {
        result = not a;
    }
};

template <typename T>
struct And {
    ALWAYS_INLINE void call(bool &result, const bool &a, const bool &b)
    {
        result = a && b;
    }
};
}
