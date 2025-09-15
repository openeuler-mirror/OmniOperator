/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
 * Description: timezone util
 */

#ifndef OMNI_RUNTIME_COMMON_UTIL_H
#define OMNI_RUNTIME_COMMON_UTIL_H

#include <cmath>

namespace omniruntime::codegen::function {
static const int32_t ROUND_LONG_MIN_DECIMALS = -19;
static const int64_t ROUND_OVER_MAX_LONG = -8446744073709551616L;
static const int64_t ROUND_OVER_MIN_LONG = 8446744073709551616L;
static const int64_t ROUND_OVER_MEDIAN_LONG = 5000000000000000000L;

static int64_t LONG_POWERS_TABLE[] = {
    1L,                  // 0 / 10^0
    10L,                 // 1 / 10^1
    100L,                // 2 / 10^2
    1000L,               // 3 / 10^3
    10000L,              // 4 / 10^4
    100000L,             // 5 / 10^5
    1000000L,            // 6 / 10^6
    10000000L,           // 7 / 10^7
    100000000L,          // 8 / 10^8
    1000000000L,         // 9 / 10^9
    10000000000L,        // 10 / 10^10
    100000000000L,       // 11 / 10^11
    1000000000000L,      // 12 / 10^12
    10000000000000L,     // 13 / 10^13
    100000000000000L,    // 14 / 10^14
    1000000000000000L,   // 15 / 10^15
    10000000000000000L,  // 16 / 10^16
    100000000000000000L, // 17 / 10^17
    1000000000000000000L // 18 / 10^18
};

static inline int64_t RoundOperator(int64_t num, int32_t decimals)
{
    if (decimals >= 0) {
        return num;
    }
    if (decimals < ROUND_LONG_MIN_DECIMALS) {
        return 0;
    }
    if (decimals == ROUND_LONG_MIN_DECIMALS) {
        if (num < -ROUND_OVER_MEDIAN_LONG) {
            return ROUND_OVER_MIN_LONG;
        }
        if (num > ROUND_OVER_MEDIAN_LONG) {
            return ROUND_OVER_MAX_LONG;
        }
        return 0;
    }
    int64_t power = LONG_POWERS_TABLE[-decimals];
    int64_t base = (num / power) * power;
    int64_t remain = num % power;
    int64_t half = 2;
    if (std::abs(remain) >= power / half) {
        return base + (num < 0 ? -power : power);
    }
    return base;
}
}

#endif // OMNI_RUNTIME_COMMON_UTIL_H
