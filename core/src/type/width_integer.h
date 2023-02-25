/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2022-2023. All rights reserved.
 * @Description: width integer implementations
 */

#ifndef OMNI_RUNTIME_WIDTH_INTEGER_H
#define OMNI_RUNTIME_WIDTH_INTEGER_H

#include <boost/multiprecision/cpp_int.hpp>
#include "util/compiler_util.h"

namespace omniruntime::type {
using int128 = __int128_t;
using int256 = boost::multiprecision::int256_t;

static ALWAYS_INLINE std::ostream &operator<<(std::ostream &out, const int128 &x)
{
    out << boost::multiprecision::int128_t(x);
    return out;
}

static ALWAYS_INLINE int128 CreateInt128(const char *s)
{
    return static_cast<int128>(boost::multiprecision::int128_t(s));
}

static ALWAYS_INLINE int128 CreateInt128(const std::string &s)
{
    return static_cast<int128>(boost::multiprecision::int128_t(s));
}

static ALWAYS_INLINE int128 CreateInt128(int64_t high, uint64_t low)
{
    int128 value = high;
    return (value << 64) | low;
}

static constexpr int128 TenOfInt128[39] =
    { int128(1) * 1, int128(1) * 10, int128(1) * 10 * 10, int128(1) * 10 * 10 * 10,
        int128(1) * 10 * 10 * 10 * 10, int128(1) * 10 * 10 * 10 * 10 * 10,
        int128(1) * 10 * 10 * 10 * 10 * 10 * 10, int128(1) * 10 * 10 * 10 * 10 * 10 * 10 * 10,
        int128(1) * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10, int128(1) * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10,
        int128(1) * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10,
        int128(1) * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10,
        int128(1) * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10,
        int128(1) * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10,
        int128(1) * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10,
        int128(1) * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10,
        int128(1) * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10,
        int128(1) * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10,
        int128(1) * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10,
        int128(10000000000000000000L) * 1, int128(10000000000000000000L) * 10,
        int128(10000000000000000000L) * 10 * 10, int128(10000000000000000000L) * 10 * 10 * 10,
        int128(10000000000000000000L) * 10 * 10 * 10 * 10, int128(10000000000000000000L) * 10 * 10 * 10 * 10 * 10,
        int128(10000000000000000000L) * 10 * 10 * 10 * 10 * 10 * 10,
        int128(10000000000000000000L) * 10 * 10 * 10 * 10 * 10 * 10 * 10,
        int128(10000000000000000000L) * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10,
        int128(10000000000000000000L) * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10,
        int128(10000000000000000000L) * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10,
        int128(10000000000000000000L) * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10,
        int128(10000000000000000000L) * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10,
        int128(10000000000000000000L) * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10,
        int128(10000000000000000000L) * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10,
        int128(10000000000000000000L) * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10,
        int128(10000000000000000000L) * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10,
        int128(10000000000000000000L) * int128(10000000000000000L) * 10,
        int128(10000000000000000000L) * int128(10000000000000000L) * 10 * 10,
        int128(10000000000000000000L) * int128(10000000000000000L) * 10 * 10 * 10, };
}

#endif //OMNI_RUNTIME_WIDTH_INTEGER_H
