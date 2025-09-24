/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#pragma once
#include "util/compiler_util.h"

namespace omniruntime {
template <typename T>
struct PlusFunction {
    template <typename TInput>
    ALWAYS_INLINE void call(TInput &result, const TInput &a, const TInput &b)
    {
        result = a + b;
    }
};
}
