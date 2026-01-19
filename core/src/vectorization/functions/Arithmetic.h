/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#pragma once
#include "util/compiler_util.h"
#include "vectorization/Status.h"
#include <cmath>

namespace omniruntime::vectorization {
template <typename T>
struct PlusFunction {
    template <typename TInput>
    ALWAYS_INLINE Status call(TInput &result, const TInput &a, const TInput &b)
    {
        result = a + b;
        return Status::OK();
    }
};

template <typename T>
struct MinusFunction {
    template <typename TInput>
    ALWAYS_INLINE Status call(TInput &result, const TInput &a, const TInput &b)
    {
        result = a - b;
        return Status::OK();
    }
};

template <typename T>
struct MultiplyFunction {
    template <typename TInput>
    ALWAYS_INLINE Status call(TInput &result, const TInput &a, const TInput &b)
    {
        result = a * b;
        return Status::OK();
    }
};

template <typename T>
struct DivideFunction {
    template <typename TInput>
    ALWAYS_INLINE Status call(TInput &result, const TInput &a, const TInput &b)
    {
        if (b == 0) {
            return Status::UserError("Arithmetic overflow: {} + {}", a, b);
        }

        result = a / b;
        return Status::OK();
    }
};

template <typename T>
struct RemainderFunction {
    template <typename TInput>
    ALWAYS_INLINE Status call(TInput &result, const TInput &a, const TInput &b)
    {
        if (b == 0) {
            return Status::UserError("Arithmetic overflow: {} + {}", a, b);
        }

        if constexpr (std::is_integral_v<TInput>) {
            result = a % b;
        } else {
            result = std::fmod(a, b);
        }
        return Status::OK();
    }
};
}
