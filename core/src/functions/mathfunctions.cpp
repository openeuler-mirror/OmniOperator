/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry math function name
 */
#include "common.h"
#include <cmath>

namespace {
const int COMBINE_HASH_VALUE = 31;
}
extern "C" {
// cast template for basic types
#define CAST(IN_TYPE, OUT_TYPE)                                \
    INLINE OUT_TYPE Cast_##IN_TYPE##_to_##OUT_TYPE(IN_TYPE in) \
    {                                                          \
        return static_cast<OUT_TYPE>(in);                      \
    }

CAST(int32, int64)
CAST(int64, int32)
CAST(int32, double)
CAST(int64, double)

#undef CAST

// cast template for double types to int types.
#define CAST_DOUBLE(IN_TYPE, OUT_TYPE)                         \
    INLINE OUT_TYPE Cast_##IN_TYPE##_to_##OUT_TYPE(IN_TYPE in) \
    {                                                          \
        OUT_TYPE out = static_cast<OUT_TYPE>(round(in));       \
        return out;                                            \
    }

CAST_DOUBLE(double, int64)
CAST_DOUBLE(double, int32)

#undef CAST_DOUBLE

INLINE int64_t CombineHash(int64_t prevHashVal, int64_t val)
{
    return COMBINE_HASH_VALUE * prevHashVal + val;
}

INLINE int32_t Pmod(int32_t x, int32_t y)
{
    if (y == 0) {
        return 0;
    }
    int32_t r = x % y;
    if (r < 0) {
        return (r + y) % y;
    } else {
        return r;
    }
}

#define ROUND(TYPE)                                       \
    INLINE TYPE Round_##TYPE(TYPE num, int32_t decimals)  \
    {                                                     \
        if (std::isnan(num) || std::isinf(num)) {         \
            return num;                                   \
        }                                                 \
        int32_t tenthPower = 10;                          \
        double factor = std::pow(tenthPower, decimals);   \
        if (num < 0) {                                    \
            return -(std::round(-num * factor) / factor); \
        }                                                 \
                                                          \
        return std::round(num * factor) / factor;         \
    }

ROUND(int32)
ROUND(int64)
ROUND(double)
#undef ROUND

// Absolute value
#define ABS(TYPE)                  \
    INLINE TYPE Abs_##TYPE(TYPE x) \
    {                              \
        return std::abs(x);        \
    }
ABS(int32)
ABS(int64)
ABS(double)
#undef ABS
}