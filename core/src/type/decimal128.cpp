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