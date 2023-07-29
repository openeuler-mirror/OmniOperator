/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2022-2023. All rights reserved.
 * @Description: width integer implementations
 */

#ifndef OMNI_RUNTIME_WIDTH_INTEGER_H
#define OMNI_RUNTIME_WIDTH_INTEGER_H

#include <sstream>
#include "util/compiler_util.h"
#include "integer256.h"

namespace omniruntime::type {

constexpr static int OCTAL_BITS = 10;
constexpr static char ASCII_OFFSET = 48;
constexpr static int DOUBLE_MAX_PRECISION = 17;

static inline std::string Uint128ToStr(uint128_t num)
{
    char buffer[40];
    int index = 0;
    do {
        buffer[index++] = static_cast<char>(num % OCTAL_BITS) + ASCII_OFFSET;
        num /= OCTAL_BITS;
    } while (num != 0);

    // Reverse the character array
    for (int i = 0, j = index - 1; i < j; ++i, --j) {
        char temp = buffer[i];
        buffer[i] = buffer[j];
        buffer[j] = temp;
    }
    return std::string(buffer, index);
}

static inline int128_t ToInt128(const std::string &str)
{
    const char *s = str.c_str();
    bool isNeg = false;
    if (*s == '-') {
        isNeg = true;
        ++s;
    }
    int128_t val = 0;
    while (*s) {
        unsigned int temp;
        if (*s >= '0' && *s <= '9') {
            temp = *s - ASCII_OFFSET;
        } else {
            throw std::runtime_error("Can not cast " + str + " to int128_t");
        }
        val *= OCTAL_BITS;
        val += temp;
        ++s;
    }
    if (isNeg) {
        val = -val;
    }
    return val;
}

static inline std::string ToString(int128_t n)
{
    std::string str;
    if (n < 0) {
        str = '-';
    }
    str = str + Uint128ToStr(std::abs(n));
    return str;
}

static inline std::string ToString(double n)
{
    std::stringstream ss;
    ss.precision(DOUBLE_MAX_PRECISION);
    ss << n;
    return ss.str();
}

static ALWAYS_INLINE std::ostream &operator<<(std::ostream &out, const int128_t &x)
{
    out << ToString(x);
    return out;
}

static ALWAYS_INLINE int128_t CreateInt128(const char *s)
{
    return static_cast<int128_t>(ToInt128(std::string(s)));
}

static ALWAYS_INLINE int128_t CreateInt128(const std::string &s)
{
    return static_cast<int128_t>(ToInt128(s));
}

static ALWAYS_INLINE int128_t CreateInt128(int64_t high, uint64_t low)
{
    int128_t value = high;
    return (value << 64) | low;
}

static constexpr int128_t TenOfInt128[39] =
    { int128_t(1) * 1, int128_t(1) * 10, int128_t(1) * 10 * 10, int128_t(1) * 10 * 10 * 10,
      int128_t(1) * 10 * 10 * 10 * 10, int128_t(1) * 10 * 10 * 10 * 10 * 10,
      int128_t(1) * 10 * 10 * 10 * 10 * 10 * 10, int128_t(1) * 10 * 10 * 10 * 10 * 10 * 10 * 10,
      int128_t(1) * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10, int128_t(1) * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10,
      int128_t(1) * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10,
      int128_t(1) * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10,
      int128_t(1) * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10,
      int128_t(1) * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10,
      int128_t(1) * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10,
      int128_t(1) * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10,
      int128_t(1) * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10,
      int128_t(1) * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10,
      int128_t(1) * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10,
      int128_t(10000000000000000000ULL) * 1, int128_t(10000000000000000000ULL) * 10,
      int128_t(10000000000000000000ULL) * 10 * 10, int128_t(10000000000000000000ULL) * 10 * 10 * 10,
      int128_t(10000000000000000000ULL) * 10 * 10 * 10 * 10, int128_t(10000000000000000000ULL) * 10 * 10 * 10 * 10 * 10,
      int128_t(10000000000000000000ULL) * 10 * 10 * 10 * 10 * 10 * 10,
      int128_t(10000000000000000000ULL) * 10 * 10 * 10 * 10 * 10 * 10 * 10,
      int128_t(10000000000000000000ULL) * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10,
      int128_t(10000000000000000000ULL) * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10,
      int128_t(10000000000000000000ULL) * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10,
      int128_t(10000000000000000000ULL) * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10,
      int128_t(10000000000000000000ULL) * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10,
      int128_t(10000000000000000000ULL) * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10,
      int128_t(10000000000000000000ULL) * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10,
      int128_t(10000000000000000000ULL) * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10,
      int128_t(10000000000000000000ULL) * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10 * 10,
      int128_t(10000000000000000000ULL) * int128_t(10000000000000000ULL) * 10,
      int128_t(10000000000000000000ULL) * int128_t(10000000000000000ULL) * 10 * 10,
      int128_t(10000000000000000000ULL) * int128_t(10000000000000000ULL) * 10 * 10 * 10, };
}

#endif //OMNI_RUNTIME_WIDTH_INTEGER_H
