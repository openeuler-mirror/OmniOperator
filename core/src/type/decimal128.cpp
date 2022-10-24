/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Description: decimal 128 type
 */

#include "decimal128.h"

#include <array>
#include <iomanip>
#include "util/debug.h"

namespace omniruntime {
namespace type {
const int32_t PRINT_OUT_HEX_WIDTH = 16;
Decimal128::Decimal128(int64_t highBits, uint64_t lowBits) : lowBits(lowBits), highBits(highBits) {}


Decimal128::Decimal128(int64_t unscaledValue)
{
    if (unscaledValue < 0) {
        highBits = SIGN_LONG_MASK;
        lowBits = -unscaledValue;
    } else {
        highBits = 0;
        lowBits = unscaledValue;
    }
}

Decimal128::Decimal128(__int128_t value)
{
    if (value >= 0) {
        highBits = static_cast<int64_t>(value >> 64);
        lowBits = static_cast<uint64_t>(value);
    } else {
        value = -value;
        highBits = static_cast<int64_t>(value >> 64) ^ (1L << 63);
        lowBits = static_cast<uint64_t>(value);
    }
}

Decimal128::Decimal128(const std::string& s)
{
    bool isNegative = s[0] == '-';
    __int128_t value = 0;
    for (char i : s) {
        if (isdigit(i)) {
            value *= 10;
            value += i - '0';
        }
    }

    value = !isNegative ? value : -value;
    if (value >= 0) {
        highBits = static_cast<int64_t>(value >> 64);
        lowBits = static_cast<uint64_t>(value);
    } else {
        value = -value;
        highBits = static_cast<int64_t>(value >> 64) ^ (1L << 63);
        lowBits = static_cast<uint64_t>(value);
    }
}
// All comparing operator remains due to template function
bool Decimal128::operator == (const Decimal128 &right) const
{
    return lowBits == right.lowBits && highBits == right.highBits;
}

bool Decimal128::operator != (const Decimal128 &right) const
{
    return !operator == (right);
}

bool Decimal128::operator < (const Decimal128 &right) const
{
    return Compare(right) == -1;
}

bool Decimal128::operator <= (const Decimal128 &right) const
{
    return !operator > (right);
}

bool Decimal128::operator > (const Decimal128 &right) const
{
    return Compare(right) == 1;
}

bool Decimal128::operator >= (const Decimal128 &right) const
{
    return !operator < (right);
}

std::ostream &operator << (std::ostream &os, const Decimal128 &decimal128)
{
    os << std::hex << "0x" << std::setfill('0') << std::setw(PRINT_OUT_HEX_WIDTH) << decimal128.HighBits() <<
        std::setfill('0') << std::setw(PRINT_OUT_HEX_WIDTH) << decimal128.LowBits();
    return os;
}
}
}