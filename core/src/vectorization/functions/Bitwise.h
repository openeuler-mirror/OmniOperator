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
struct BitwiseAndFunction {
    template <typename TInput>
    ALWAYS_INLINE Status call(TInput &result, const TInput &a, const TInput &b)
    {
        result = a & b;
        return Status::OK();
    }
};

template <typename T>
struct BitwiseOrFunction {
    template <typename TInput>
    ALWAYS_INLINE Status call(TInput &result, const TInput &a, const TInput &b)
    {
        result = a | b;
        return Status::OK();
    }
};

template <typename T>
struct BitwiseXorFunction {
    template <typename TInput>
    ALWAYS_INLINE Status call(TInput &result, const TInput &a, const TInput &b)
    {
        result = a ^ b;
        return Status::OK();
    }
};

template <typename T>
struct ShiftLeftFunction {
    template <typename TInput1, typename TInput2>
    ALWAYS_INLINE Status call(TInput1 &result, const TInput1 &a, const TInput2 &b)
    {
        static_assert(std::is_integral_v<TInput1>, "ShiftLeft only supports integral types");

        TInput2 shift = b;
        if constexpr (std::is_same_v<TInput1, int32_t> || std::is_same_v<TInput1, uint32_t>) {
            if (shift < 0) {
                shift = shift % 32 + 32;
            }
            if (shift >= 32) {
                shift = shift % 32;
            }
        } else if constexpr (std::is_same_v<TInput1, int64_t> || std::is_same_v<TInput1, uint64_t>) {
            if (shift < 0) {
                shift = shift % 64 + 64;
            }
            if (shift >= 64) {
                shift = shift % 64;
            }
        }

        result = a << shift;
        return Status::OK();
    }
};

template <typename T>
struct ShiftRightFunction {
    template <typename TInput1, typename TInput2>
    ALWAYS_INLINE Status call(TInput1 &result, const TInput1 &a, const TInput2 &b)
    {
        static_assert(std::is_integral_v<TInput1>, "ShiftRight only supports integral types");

        TInput2 shift = b;
        if constexpr (std::is_same_v<TInput1, int32_t> || std::is_same_v<TInput1, uint32_t>) {
            if (shift < 0) {
                shift = shift % 32 + 32;
            }
            if (shift >= 32) {
                shift = shift % 32;
            }
        } else if constexpr (std::is_same_v<TInput1, int64_t> || std::is_same_v<TInput1, uint64_t>) {
            if (shift < 0) {
                shift = shift % 64 + 64;
            }
            if (shift >= 64) {
                shift = shift % 64;
            }
        }

        result = a >> shift;
        return Status::OK();
    }
};
}