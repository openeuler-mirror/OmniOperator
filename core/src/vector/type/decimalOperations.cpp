/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: DecimalOperations
 */


#include "decimalOperations.h"
#include "decimal128.h"
#include <vector/fixed_width_vector.h>

namespace omniruntime {
int64_t SIGN_LONG_MASK = 1LL << 63;
namespace vec {
void DecimalOperations::DecodeSumDecimal(void *ptr, Decimal128 &val, int64_t &overflow)
{
    overflow = *static_cast<int64_t *>(ptr);
    int64_t highBits = *(static_cast<int64_t *>(ptr) + 1);
    uint64_t lowBits = *(static_cast<uint64_t *>(ptr) + 2);
    val.SetValue(highBits, lowBits);
}

void DecimalOperations::EncodeSumDecimal(void *ptr, const Decimal128 &val, const int64_t &overflow)
{
    int64_t highBits = val.HighBits();
    uint64_t lowBits = val.LowBits();
    auto *p = static_cast<int64_t *>(ptr);
    memcpy_s(p, 8, &overflow, 8);
    memcpy_s(p + 1, 8, &highBits, 8);
    memcpy_s(p + 2, 8, &lowBits, 8);
}

void DecimalOperations::DecodeAvgDecimal(void *ptr, Decimal128 &val, int64_t &overflow, int64_t &count)
{
    count = *static_cast<int64_t *>(ptr);
    overflow = *(static_cast<uint64_t *>(ptr) + 1);
    int64_t highBits = *(static_cast<int64_t *>(ptr) + 2);
    uint64_t lowBits = *(static_cast<uint64_t *>(ptr) + 3);
    val.SetValue(highBits, lowBits);
}

void DecimalOperations::EncodeAvgDecimal(void *ptr, const Decimal128 &val, const int64_t &overflow,
    const int64_t &count)
{
    int64_t highBits = val.HighBits();
    uint64_t lowBits = val.LowBits();
    auto *p = static_cast<int64_t *>(ptr);
    memcpy_s(p, 8, &count, 8);
    memcpy_s(p + 1, 8, &overflow, 8);
    memcpy_s(p + 2, 8, &highBits, 8);
    memcpy_s(p + 3, 8, &lowBits, 8);
}

long DecimalOperations::AddWithOverflow(Decimal128 &left, Decimal128 &right, Decimal128 &result)
{
    bool leftNegative = IsNegative(left);
    bool rightNegative = IsNegative(right);

    long overflow = 0;
    if (leftNegative == rightNegative) {
        overflow = AddUnsignedReturnOverflow(left, right, result, leftNegative);
        if (leftNegative) {
            overflow = -overflow;
        }
    } else {
        int compare = CompareAbsolute(left, right);
        if (compare > 0) {
            SubtractUnsigned(left, right, result, leftNegative);
        } else if (compare < 0) {
            SubtractUnsigned(right, left, result, !leftNegative);
        } else {
            SetToZero(result);
        }
    }
    return overflow;
}

long DecimalOperations::AddUnsignedReturnOverflow(const Decimal128 &left, const Decimal128 &right, Decimal128 &result,
    bool resultNegative)
{
    uint64_t l0 = left.LowBits();
    int64_t l1 = GetLong(left.HighBits());

    uint64_t r0 = right.LowBits();
    int64_t r1 = GetLong(right.HighBits());

    uint64_t z0 = l0 + r0;
    int overflow = UnsignedIsSmaller(z0, l0) ? 1 : 0;
    uint64_t intermediateResult = l1 + r1 + overflow;
    int64_t z1 = intermediateResult & (~SIGN_LONG_MASK);
    Pack(result, z0, z1, resultNegative);

    return ((uint64_t)intermediateResult) >> 63;
}

int DecimalOperations::CompareAbsolute(Decimal128 &left, Decimal128 &right)
{
    int64_t leftHigh = GetLong(left.HighBits());
    int64_t rightHigh = GetLong(right.HighBits());
    if (leftHigh != rightHigh) {
        return CompareUnsigned(leftHigh, rightHigh);
    }
    int64_t leftLow = left.LowBits();
    int64_t rightLow = right.LowBits();
    return CompareUnsigned(leftLow, rightLow);
}

bool DecimalOperations::IsNegative(Decimal128 &left)
{
    return (((uint64_t)left.HighBits()) >> 63) != 0;
}


void DecimalOperations::SubtractUnsigned(Decimal128 &left, Decimal128 &right, Decimal128 &result, bool resultNegative)
{
    // original scheme

    uint64_t l0 = left.LowBits();
    int64_t l1 = GetLong(left.HighBits());

    uint64_t r0 = right.LowBits();
    int64_t r1 = GetLong(right.HighBits());

    uint64_t z0 = l0 - r0;
    int underflow = UnsignedIsSmaller(l0, z0) ? 1 : 0;
    long z1 = l1 - r1 - underflow;
    Pack(result, z0, z1, resultNegative);
}

void DecimalOperations::SetToZero(Decimal128 &decimal128)
{
    decimal128.SetValue(0, 0);
}

bool ExceedsOrEqualTenTOthirtyEight(Decimal128 &decimal128)
{
    int64_t high = decimal128.HighBits();
    if (high >= 0 && high < 0x4b3b4ca85a86c47aL) {
        return false;
    }
    if (high != 0x4b3b4ca85a86c47aL) {
        return true;
    }

    uint64_t low = decimal128.LowBits();
    return low < 0 || low >= 0x098a224000000000L;
}

void DecimalOperations::Pack(Decimal128 &decimal128, uint64_t low, int64_t high, bool negative)
{
    decimal128.SetValue(high | (negative ? SIGN_LONG_MASK : 0), low);
}

bool DecimalOperations::UnsignedIsSmaller(uint64_t first, uint64_t second)
{
    return first + LLONG_MIN < second + LLONG_MIN;
}

int DecimalOperations::CompareUnsigned(int64_t x, int64_t y)
{
    return Compare(x + LLONG_MIN, y + LLONG_MIN);
}

int DecimalOperations::Compare(int64_t x, int64_t y)
{
    return (x < y) ? -1 : ((x == y)) ? 0 : 1;
}

int64_t DecimalOperations::GetLong(int64_t value)
{
    return value & ~SIGN_LONG_MASK;
}

Decimal128 DecimalOperations:: UnscaledDecimal(int64_t unscaledValue)
{
    vec::Decimal128 decimal128;
    if (unscaledValue < 0) {
        decimal128.SetValue(SIGN_LONG_MASK, -unscaledValue);
    } else {
        decimal128.SetValue(0, unscaledValue);
    }
    return decimal128;
}
}
}