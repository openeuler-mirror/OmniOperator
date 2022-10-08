/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: DecimalOperations
 */

#ifndef OMNI_RUNTIME_DECIMAL_OPERATIONS_H
#define OMNI_RUNTIME_DECIMAL_OPERATIONS_H


#include <cstdint>
#include <regex>
#include <iostream>
#include <climits>
#include <huawei_secure_c/include/securec.h>
#include "util/debug.h"
#include "decimal_base.h"
#include "decimal128.h"
#include "operator/aggregation/aggregator/aggregator.h"

namespace omniruntime {
namespace type {
enum OpStatus {
    SUCCESS = 0,
    OP_OVERFLOW = 1,
    DIVIDE_BY_ZERO = 2,
    FAIL = 3
};

using int128_t = __int128_t;

static constexpr int64_t LONG_MIN_VALUE = 0x8000'0000'0000'0000;
static constexpr int64_t LONG_MAX_VALUE = 0x7FFF'FFFF'FFFF'FFFF;
static constexpr int64_t ALL_BITS_SET_64 = 0xFFFF'FFFF'FFFF'FFFFLL;
static constexpr int64_t DECIMAL64_MAX_VALUE = 99'9999'9999'9999'9999;
static const Decimal128 MIN_VALUE = { LLONG_MAX + 1, 0x0000'0000'0000'0000L };
static const Decimal128 MAX_VALUE = { 0x7FFF'FFFF'FFFF'FFFFL, 0xFFFF'FFFF'FFFF'FFFFLL };
static constexpr int64_t INT_BASE = 1L << 32;
static constexpr int MAX_PRECISION = 38;
static constexpr int MAX_SCALE = 38;
static constexpr int32_t MAX_DECIMAL64_DIGITS = 18;
static std::array<int64_t, 14> POWERS_OF_FIVE_INT = { 1,
    5,
    5 * 5,
    5 * 5 * 5,
    5 * 5 * 5 * 5,
    5 * 5 * 5 * 5 * 5,
    5 * 5 * 5 * 5 * 5 * 5,
    5 * 5 * 5 * 5 * 5 * 5 * 5,
    5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
    5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
    5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
    5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
    5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
    5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 };

static std::array<__int128_t, MAX_PRECISION> GetPowersOfFive()
{
    std::array<__int128_t, MAX_PRECISION> powersOfFive;
    powersOfFive[0] = 1;
    for (int i = 1; i < MAX_PRECISION; ++i) {
        powersOfFive[i] = powersOfFive[i - 1] * 5;
    }
    return powersOfFive;
}
static std::array<__int128_t, MAX_PRECISION> POWERS_OF_FIVE_LONG =  GetPowersOfFive();

static std::array<int64_t, 19> INT64_TEN_POWERS_TABLE = {
    1,                     // 0 / 10^0
    10,                    // 1 / 10^1
    100,                   // 2 / 10^2
    1000,                  // 3 / 10^3
    10000,                 // 4 / 10^4
    100000,                // 5 / 10^5
    1000000,               // 6 / 10^6
    10000000,              // 7 / 10^7
    100000000,             // 8 / 10^8
    1000000000,            // 9 / 10^9
    10000000000L,          // 10 / 10^10
    100000000000L,         // 11 / 10^11
    1000000000000L,        // 12 / 10^12
    10000000000000L,       // 13 / 10^13
    100000000000000L,      // 14 / 10^14
    1000000000000000L,     // 15 / 10^15
    10000000000000000L,    // 16 / 10^16
    100000000000000000L,   // 17 / 10^17
    1000000000000000000L   // 18 / 10^18
};

static std::array<__int128_t, MAX_PRECISION> GetPowersOfTen()
{
    std::array<__int128_t, MAX_PRECISION> powersOfTen;
    powersOfTen[0] = 1;
    for (int i = 1; i < MAX_PRECISION; ++i) {
        powersOfTen[i] = powersOfTen[i - 1] * 10;
    }
    return powersOfTen;
}

static std::array<__int128_t, MAX_PRECISION> POWERS_OF_TEN = GetPowersOfTen();

class DecimalOperations {
public:
    DecimalOperations() = delete;

    ~DecimalOperations() = delete;

    // decimal and overflow is encoded and decoded in continuous memory
    static inline void DecodeSumDecimal(op::DecimalSumState *statePtr, Decimal128 &val, int64_t &overflow)
    {
        overflow = statePtr->overflow;
        val.SetValue(statePtr->highBits, statePtr->lowBits);
    }

    static inline void EncodeSumDecimal(op::DecimalSumState *statePtr, const Decimal128 &val, const int64_t &overflow)
    {
        statePtr->overflow = overflow;
        statePtr->highBits = val.HighBits();
        statePtr->lowBits = val.LowBits();
    }

    // decimal and overflow is encoded in continuous memory
    static inline void DecodeAvgDecimal(op::DecimalAverageState *statePtr, Decimal128 &val, int64_t &overflow,
        int64_t &count)
    {
        count = statePtr->count;
        overflow = statePtr->overflow;
        val.SetValue(statePtr->highBits, statePtr->lowBits);
    }

    static inline void EncodeAvgDecimal(op::DecimalAverageState *statePtr, const Decimal128 &val,
        const int64_t &overflow, const int64_t &count)
    {
        statePtr->count = count;
        statePtr->overflow = overflow;
        statePtr->highBits = val.HighBits();
        statePtr->lowBits = val.LowBits();
    }

    static inline void ThrowDivideZero()
    {
        throw OmniException("DIVIDE ZERO", "Decimal divide zero");
    }

    static inline void ThrowIllegalState()
    {
        throw OmniException("ILLEGAL STATE", "Decimal illegal state");
    }

    static inline void ThrowIfOverflows(Decimal128 &decimal)
    {
        if (ExceedsOrEqualTenToThirtyEight(decimal)) {
            throw OmniException("OVERFLOW", "Decimal exceeds maximum value.");
        }
    }

    static inline OpStatus AddWithOverflow(Decimal128 &left, Decimal128 &right, Decimal128 &result)
    {
        __int128_t x = left.ToInt128();
        __int128_t y = right.ToInt128();
        __int128_t r = 0;
        if (__builtin_add_overflow(x, y, &r)) {
            return OP_OVERFLOW;
        } else {
            result = Decimal128(r);
            if (ExceedsOrEqualTenToThirtyEight(result)) {
                return OP_OVERFLOW;
            }
            return SUCCESS;
        }
    }

    static inline int64_t Pow64TenToScale(int64_t x, int32_t reScale)
    {
        if (reScale > 0) {
            while (reScale > 0) {
                reScale--;
                x *= 10;
            }
        } else if (reScale < 0) {
            while (reScale < 0) {
                reScale++;
                x /= 10;
            }
        }
        return x;
    }

    static inline int64_t GetLong(Decimal128 &decimal, int32_t index)
    {
        return index ? UnpackUnsignedLong(decimal.HighBits()) : decimal.LowBits();
    }

    static inline long AddUnsignedReturnOverflow(const Decimal128 &left, const Decimal128 &right, Decimal128 &result,
        bool resultNegative)
    {
        int64_t l0 = left.LowBits();
        int64_t l1 = UnpackUnsignedLong(left.HighBits());

        int64_t r0 = right.LowBits();
        int64_t r1 = UnpackUnsignedLong(right.HighBits());

        int64_t z0 = l0 + r0;
        int32_t overflow = UnsignedIsSmaller(z0, l0) ? 1 : 0;
        int64_t intermediateResult = l1 + r1 + overflow;
        int64_t z1 = intermediateResult & (~Decimal128::SIGN_LONG_MASK);
        if (Pack(result, z0, z1, resultNegative) != SUCCESS) {
            return OP_OVERFLOW;
        }
        return (uint64_t)intermediateResult >> 63;
    }

    static inline OpStatus SubtractUnsigned(Decimal128 &left, Decimal128 &right,
        Decimal128 &result, bool resultNegative)
    {
        int64_t l0 = left.LowBits();
        int64_t l1 = UnpackUnsignedLong(left.HighBits());

        int64_t r0 = right.LowBits();
        int64_t r1 = UnpackUnsignedLong(right.HighBits());

        int64_t z0 = l0 - r0;
        int32_t underflow = UnsignedIsSmaller(l0, z0) ? 1 : 0;
        int64_t z1 = l1 - r1 - underflow;
        return Pack(result, z0, z1, resultNegative);
    }

    static inline OpStatus Subtract(Decimal128 &left, Decimal128 &right, Decimal128 &result)
    {
        if (IsNegative(left) != IsNegative(right)) {
            if (AddUnsignedReturnOverflow(left, right, result, IsNegative(left)) != 0) {
                return OP_OVERFLOW;
            }
        } else {
            int compare = CompareAbsolute(left, right);
            if (compare > 0) {
                SubtractUnsigned(left, right, result, IsNegative(left) && IsNegative(right));
            } else if (compare < 0) {
                SubtractUnsigned(right, left, result, !(IsNegative(left) && IsNegative(right)));
            } else {
                SetToZero(result);
            }
        }
        return SUCCESS;
    }

    static inline OpStatus Multiply(Decimal128 &left, Decimal128 &right, Decimal128 &result)
    {
        __int128_t l0 = left.LowBits();
        __int128_t l1 = UnpackUnsignedLong(left.HighBits());

        __int128_t r0 = right.LowBits();
        __int128_t r1 = UnpackUnsignedLong(right.HighBits());

        __int128_t z0 = 0;
        __int128_t z1 = 0;

        if (l0 != 0) {
            __int128_t accumulator = r0 * l0;
            z0 = accumulator & Decimal128::LOW_64_BITS;
            accumulator = ((__uint128_t)accumulator >> 64) + r1 * l0;

            z1 = accumulator & Decimal128::LOW_64_BITS;

            if (((__uint128_t)accumulator >> 64) != 0) {
                return OP_OVERFLOW;
            }
        }

        if (l1 != 0) {
            __int128_t accumulator = r0 * l1 + z1;
            z1 = accumulator & Decimal128::LOW_64_BITS;

            if (((__uint128_t)accumulator >> 64) != 0) {
                return OP_OVERFLOW;
            }
        }
        return Pack(result, (int64_t)z0, (int64_t)z1, IsNegative(left) != IsNegative(right));
    }

    static inline OpStatus Multiply256(Decimal128 &left, Decimal128 &right, Decimal128 &result,
        int32_t reScale)
    {
        bool leftNegative = left.HighBits() < 0;
        bool rightNegative = right.HighBits() < 0;
        int64_t leftLow = left.LowBits();
        int64_t leftRight = left.HighBits();


        std::vector<int32_t> resultInts = MultiplyUnsignedMultiPrecision(left, right);
        Decimal128 tenToScale = TenToScale(reScale);

        std::vector<int32_t> tenToScaleVector(8);
        tenToScaleVector[0] = LowInt(tenToScale.LowBits());
        tenToScaleVector[1] = HighInt(tenToScale.LowBits());
        tenToScaleVector[2] = LowInt(tenToScale.HighBits());
        tenToScaleVector[3] = HighInt(tenToScale.HighBits());
        DivideUnsignedMultiPrecision(tenToScaleVector, static_cast<int32_t>(tenToScaleVector.size()), 2);
        AddUnsignedMultiPrecision(resultInts, 7, tenToScaleVector, 7);
        Decimal128 remainder(0);
        while (tenToScale.LowBits() != 0) {
            Decimal128 divisor(10000);
            Divide(tenToScale, divisor, 0, 0, tenToScale, remainder);
            if (remainder.LowBits() == 0) {
                DivideUnsignedMultiPrecision(resultInts, static_cast<int32_t>(resultInts.size()), 10000);
            } else {
                DivideUnsignedMultiPrecision(resultInts, static_cast<int32_t>(resultInts.size()), remainder.LowBits());
            }
        }
        OpStatus status = Pack(resultInts, result);
        result.SetValue(leftNegative ^ rightNegative ? result.HighBits() ^ 1L << 63 : result.HighBits(),
            result.LowBits());
        return status;
    }

    /* *
     * value array length should be >= 5 and first 4 int values are multiplied
     */
    static void Multiply256Destructive(std::vector<int32_t> &left, int32_t r0)
    {
        int64_t l0 = left[0] & Decimal128::INT_TO_UNSIGNED_LONG_MASK;
        int64_t l1 = left[1] & Decimal128::INT_TO_UNSIGNED_LONG_MASK;
        int64_t l2 = left[2] & Decimal128::INT_TO_UNSIGNED_LONG_MASK;
        int64_t l3 = (left[3] & ~Decimal128::SIGN_INT_MASK) & Decimal128::INT_TO_UNSIGNED_LONG_MASK;

        int64_t z0;
        int64_t z1;
        int64_t z2;
        int64_t z3;
        int64_t z4;

        int64_t accumulator = r0 * l0;
        z0 = accumulator & Decimal128::LOW_32_BITS;
        z1 = (uint64_t)accumulator >> 32;

        accumulator = r0 * l1 + z1;
        z1 = accumulator & Decimal128::LOW_32_BITS;
        z2 = (uint64_t)accumulator >> 32;

        accumulator = r0 * l2 + z2;
        z2 = accumulator & Decimal128::LOW_32_BITS;
        z3 = (uint64_t)accumulator >> 32;

        accumulator = r0 * l3 + z3;
        z3 = accumulator & Decimal128::LOW_32_BITS;
        z4 = (uint64_t)accumulator >> 32;

        left[0] = (int32_t)z0;
        left[1] = (int32_t)z1;
        left[2] = (int32_t)z2;
        left[3] = (int32_t)z3;
        left[4] = (int32_t)z4;
    }

    static inline std::vector<int32_t> MultiplyUnsignedMultiPrecision(Decimal128 &left, Decimal128 &right)
    {
        int64_t leftLow = left.LowBits();
        int64_t leftRight = left.HighBits();
        int64_t l0 = ToUnsignedLong(leftLow & Decimal128::INT_TO_UNSIGNED_LONG_MASK);
        int64_t l1 = ToUnsignedLong((leftLow >> 32) & Decimal128::INT_TO_UNSIGNED_LONG_MASK);
        int64_t l2 = ToUnsignedLong(leftRight & Decimal128::INT_TO_UNSIGNED_LONG_MASK);
        int64_t l3 =
            ToUnsignedLong(((leftRight >> 32) & Decimal128::INT_TO_UNSIGNED_LONG_MASK) & ~Decimal128::SIGN_INT_MASK);

        int64_t rightLow = right.LowBits();
        int64_t rightHigh = right.HighBits();
        int64_t r0 = ToUnsignedLong(rightLow & Decimal128::INT_TO_UNSIGNED_LONG_MASK);
        int64_t r1 = ToUnsignedLong((rightLow >> 32) & Decimal128::INT_TO_UNSIGNED_LONG_MASK);
        int64_t r2 = ToUnsignedLong(rightHigh & Decimal128::INT_TO_UNSIGNED_LONG_MASK);
        int64_t r3 =
            ToUnsignedLong(((rightHigh >> 32) & Decimal128::INT_TO_UNSIGNED_LONG_MASK) & ~Decimal128::SIGN_INT_MASK);

        int64_t z0 = 0, z1 = 0, z2 = 0, z3 = 0, z4 = 0, z5 = 0, z6 = 0, z7 = 0;

        if (l0 != 0) {
            int64_t accumulator = r0 * l0;
            z0 = accumulator & Decimal128::INT_TO_UNSIGNED_LONG_MASK;
            accumulator = ((uint64_t)accumulator >> 32) + r1 * l0;
            z1 = accumulator & Decimal128::INT_TO_UNSIGNED_LONG_MASK;
            accumulator = ((uint64_t)accumulator >> 32) + r2 * l0;
            z2 = accumulator & Decimal128::INT_TO_UNSIGNED_LONG_MASK;
            accumulator = ((uint64_t)accumulator >> 32) + r3 * l0;

            z3 = accumulator & Decimal128::INT_TO_UNSIGNED_LONG_MASK;
            z4 = ((uint64_t)accumulator >> 32) & Decimal128::INT_TO_UNSIGNED_LONG_MASK;
        }

        if (l1 != 0) {
            int64_t accumulator = r0 * l1 + z1;
            z1 = accumulator & Decimal128::INT_TO_UNSIGNED_LONG_MASK;
            accumulator = ((uint64_t)accumulator >> 32) + r1 * l1 + z2;
            z2 = accumulator & Decimal128::INT_TO_UNSIGNED_LONG_MASK;
            accumulator = ((uint64_t)accumulator >> 32) + r2 * l1 + z3;
            z3 = accumulator & Decimal128::INT_TO_UNSIGNED_LONG_MASK;
            accumulator = ((uint64_t)accumulator >> 32) + r3 * l1 + z4;
            z4 = accumulator & Decimal128::INT_TO_UNSIGNED_LONG_MASK;
            z5 = ((uint64_t)accumulator >> 32) & Decimal128::INT_TO_UNSIGNED_LONG_MASK;
        }

        if (l2 != 0) {
            int64_t accumulator = r0 * l2 + z2;
            z2 = accumulator & Decimal128::INT_TO_UNSIGNED_LONG_MASK;
            accumulator = ((uint64_t)accumulator >> 32) + r1 * l2 + z3;
            z3 = accumulator & Decimal128::INT_TO_UNSIGNED_LONG_MASK;
            accumulator = ((uint64_t)accumulator >> 32) + r2 * l2 + z4;
            z4 = accumulator & Decimal128::INT_TO_UNSIGNED_LONG_MASK;
            accumulator = ((uint64_t)accumulator >> 32) + r3 * l3 + z5;
            z5 = accumulator & Decimal128::INT_TO_UNSIGNED_LONG_MASK;
            z6 = ((uint64_t)accumulator >> 32) & Decimal128::INT_TO_UNSIGNED_LONG_MASK;
        }

        if (l3 != 0) {
            int64_t accumulator = r0 * l3 + z3;
            z3 = accumulator & Decimal128::INT_TO_UNSIGNED_LONG_MASK;
            accumulator = ((uint64_t)accumulator >> 32) + r1 * l3 + z4;
            z4 = accumulator & Decimal128::INT_TO_UNSIGNED_LONG_MASK;
            accumulator = ((uint64_t)accumulator >> 32) + r2 * l3 + z5;
            z5 = accumulator & Decimal128::INT_TO_UNSIGNED_LONG_MASK;
            accumulator = ((uint64_t)accumulator >> 32) + r3 * l3 + z6;
            z6 = accumulator & Decimal128::INT_TO_UNSIGNED_LONG_MASK;
            z7 = ((uint64_t)accumulator >> 32) & Decimal128::INT_TO_UNSIGNED_LONG_MASK;
        }

        std::vector<int32_t> resultInts = { (int32_t)z0, (int32_t)z1, (int32_t)z2, (int32_t)z3,
            (int32_t)z4, (int32_t)z5, (int32_t)z6, (int32_t)z7 };
        return resultInts;
    }

    static inline OpStatus Multiply256(Decimal128 &left, Decimal128 &right)
    {
        std::vector<int32_t> resultInts = MultiplyUnsignedMultiPrecision(left, right);
        return Pack(resultInts, right);
    }

    static inline void SetToZero(Decimal128 &decimal128)
    {
        decimal128.SetValue(0, 0);
    }

    static inline bool ExceedsOrEqualTenToThirtyEight(Decimal128 &decimal128)
    {
        int64_t high = UnpackUnsignedLong(decimal128.HighBits());
        if (high >= 0 && high < 0x4b3b4ca85a86c47aL) {
            return false;
        }
        if (high != 0x4b3b4ca85a86c47aL) {
            return true;
        }

        int64_t low = decimal128.LowBits();
        return low < 0 || low >= 0x098a224000000000L;
    }

    static inline int CompareAbsolute(Decimal128 &left, Decimal128 &right)
    {
        int64_t leftHigh = UnpackUnsignedLong(left.HighBits());
        int64_t rightHigh = UnpackUnsignedLong(right.HighBits());
        if (leftHigh != rightHigh) {
            return CompareUnsigned(leftHigh, rightHigh);
        }
        int64_t leftLow = left.LowBits();
        int64_t rightLow = right.LowBits();
        return CompareUnsigned(leftLow, rightLow);
    }

    static inline void ToIntArray(Decimal128 &decimal, std::vector<int32_t> &array)
    {
        int64_t lowBits = decimal.LowBits();
        int64_t highBits = decimal.HighBits();
        array[0] = LowInt(lowBits);
        array[1] = HighInt(lowBits);
        array[2] = LowInt(highBits);
        array[3] = HighInt(highBits);
    }

    static inline OpStatus Pack(Decimal128 &decimal128, int64_t low, int64_t high, bool negative)
    {
        decimal128.SetValue(high | (negative ? Decimal128::SIGN_LONG_MASK : 0), low);
        if (ExceedsOrEqualTenToThirtyEight(decimal128)) {
            return OP_OVERFLOW;
        }
        return SUCCESS;
    }

    static inline OpStatus Pack(std::vector<int32_t> &parts, Decimal128 &result)
    {
        if (parts[4] != 0 || parts[5] != 0 || parts[6] != 0 || parts[7] != 0) {
            return OP_OVERFLOW;
        }

        if (parts[3] < 0) {
            return OP_OVERFLOW;
        }

        int64_t high = (((int64_t)parts[3]) << 32 | (parts[2] & 0xFFFF'FFFFL));
        int64_t low = (((int64_t)parts[1]) << 32 | (parts[0] & 0xFFFF'FFFFL));

        result.SetValue(high, low);
        return SUCCESS;
    }

    static inline bool UnsignedIsSmaller(int64_t first, int64_t second)
    {
        return (uint64_t)first < (uint64_t)second;
    }

    static inline bool IsNegative(Decimal128 &decimal)
    {
        return decimal.HighBits() < 0;
    }

    static inline int64_t UnpackUnsignedLong(int64_t bits)
    {
        return bits & ~Decimal128::SIGN_LONG_MASK;
    }

    static inline int32_t CompareUnsigned(int64_t x, int64_t y)
    {
        uint64_t xx = (uint64_t)x;
        uint64_t yy = (uint64_t)y;
        return (xx < yy) ? -1 : ((xx == yy)) ? 0 : 1;
    }

    static inline int32_t CompareUnsigned(int64_t leftLow, int64_t leftHigh, int64_t rightLow, int64_t rightHigh)
    {
        int32_t comparison = CompareUnsigned(leftHigh, rightHigh);
        if (comparison == 0) {
            comparison = CompareUnsigned(leftLow, rightLow);
        }
        return comparison;
    }

    static inline Decimal128 UnscaledDecimal(int64_t unscaledValue)
    {
        Decimal128 decimal128;
        if (unscaledValue < 0) {
            decimal128.SetValue(Decimal128::SIGN_LONG_MASK, -unscaledValue);
        } else {
            decimal128.SetValue(0, unscaledValue);
        }
        return decimal128;
    }

    static inline int64_t NegateHigh(int64_t high, int64_t low)
    {
        return high < 0 ? (high & ~Decimal128::SIGN_LONG_MASK) :
            high == 0 ? Decimal128::SIGN_LONG_MASK : high | Decimal128::SIGN_LONG_MASK;
    }

    static inline int64_t NegateLow(int64_t high, int64_t low)
    {
        return -low;
    }

    static inline int64_t NegateHighExact(int64_t high, int64_t low, int64_t &result)
    {
        if (high == MIN_VALUE.HighBits() && low == MIN_VALUE.LowBits()) {
            return OP_OVERFLOW;
        }
        result = NegateHigh(high, low);
        return SUCCESS;
    }

    static inline int64_t NegateLowExact(int64_t high, int64_t low)
    {
        return NegateLow(high, low);
    }

    static inline void Negate(Decimal128 &value, int32_t offset)
    {
        int64_t high = value.HighBits();
        int64_t low = value.LowBits();
        value.SetValue(NegateHigh(high, low), low);
    }

    static inline Decimal128 NegateExact(Decimal128 &value)
    {
        int64_t negateHigh;
        NegateHighExact(value.HighBits(), value.LowBits(), negateHigh);
        return Decimal128(negateHigh, value.LowBits());
    }

    static inline int32_t LowInt(int64_t val)
    {
        return (int32_t)val;
    }

    static inline int64_t High(int64_t val)
    {
        return (uint64_t)val >> 32;
    }

    static inline int32_t HighInt(int64_t val)
    {
        return (int32_t)(High(val));
    }

    static inline void ShiftLeftBy5Destructive(std::vector<int32_t> &value, int32_t shift)
    {
        if (shift <= Decimal128::MAX_POWER_OF_FIVE_INT) {
            Multiply256Destructive(value, POWERS_OF_FIVE_INT[shift]);
        } else if (shift < Decimal128::MAX_POWER_OF_FIVE_LONG) {
            Multiply256Destructive(value, POWERS_OF_FIVE_LONG[shift]);
        } else {
            throw OmniException("Decimal error", "Precision cannot be greater than 27");
        }
    }

    static inline std::vector<int32_t> ShiftLeftMultiPrecision(std::vector<int32_t> &number, int32_t length,
        int32_t shifts)
    {
        if (shifts == 0) {
            return number;
        }

        int32_t wordShifts = (uint32_t)shifts >> 5;
        for (int32_t i = 0; i < wordShifts; ++i) {
            if (number[length - i - 1] != 0) {
                throw OmniException("Decimal", "Leading bits should be zero");
            }
        }
        if (wordShifts > 0) {
            std::copy(number.begin(), number.begin() + length - wordShifts, number.begin() + wordShifts);
            std::fill(number.begin(), number.begin() + wordShifts, 0);
        }
        int32_t bitShifts = shifts & 0b11111;
        if (bitShifts > 0) {
            if (((uint32_t)number[length - 1] >> (32 - bitShifts)) != 0) {
                throw OmniException("Decimal", "Leading bits should be zero");
            }
            for (int32_t position = length - 1; position > 0; position--) {
                number[position] =
                    (number[position] << bitShifts | ((uint32_t)number[position - 1] >> (32 - bitShifts)));
            }
            number[0] = number[0] << bitShifts;
        }
        return number;
    }

    static inline std::vector<int32_t> ShiftRightMultiPrecision(std::vector<int32_t> &number, int32_t length,
        int32_t shifts)
    {
        if (shifts == 0) {
            return number;
        }

        int wordShifts = (uint32_t)shifts >> 5;
        for (int32_t i = 0; i < wordShifts; i++) {
            if (number[i] != 0) {
                ThrowIllegalState();
            }
        }

        if (wordShifts > 0) {
            std::copy(number.begin() + wordShifts, number.begin() + length, number.begin());
            std::fill(number.begin() + length - wordShifts, number.begin() + length, 0);
        }

        int32_t bitShifts = shifts & 0b11111;
        if (bitShifts > 0) {
            if (number[0] << (32 - bitShifts) != 0) {
                ThrowIllegalState();
            }
            for (int32_t position = 0; position < length - 1; position++) {
                number[position] =
                    ((uint32_t)number[position] >> bitShifts) | (number[position + 1] << (32 - bitShifts));
            }
            number[length - 1] = (uint32_t)number[length - 1] >> bitShifts;
        }
        return number;
    }

    static inline int32_t DigitsIntegerBase(std::vector<int32_t> &digits)
    {
        int32_t length = digits.size();
        while (length > 0 && digits[length - 1] == 0) {
            length--;
        }
        return length;
    }

    static inline int64_t DivideUnsignedLong(uint64_t dividend, int32_t divisor)
    {
        int64_t unsignedDivisor = ToUnsignedLong(divisor);

        if (dividend > 0) {
            return dividend / unsignedDivisor;
        }

        int64_t quotient = ((dividend >> 1) / unsignedDivisor) * 2;
        int64_t remainder = dividend - quotient * unsignedDivisor;

        if (CompareUnsigned(remainder, unsignedDivisor) >= 0) {
            quotient++;
        }

        return quotient;
    }

    static inline int64_t ToUnsignedLong(int32_t val)
    {
        return ((int64_t)val) & 0xFFFF'FFFFL;
    }

    static inline int32_t DivideUnsignedMultiPrecision(std::vector<int32_t> &dividend, int32_t dividendLength,
        int32_t divisor)
    {
        if (divisor == 0) {
            ThrowDivideZero();
        }

        if (dividendLength == 1) {
            int64_t dividendUnsigned = ToUnsignedLong(dividend[0]);
            int64_t divisorUnsigned = ToUnsignedLong(divisor);
            int64_t quotient = dividendUnsigned / divisorUnsigned;
            int64_t remainder = dividendUnsigned - (divisorUnsigned * quotient);
            dividend[0] = (int32_t)quotient;
            return (int32_t)remainder;
        }

        int64_t divisorUnsigned = ToUnsignedLong(divisor);
        uint64_t remainder = 0;
        for (int32_t dividendIndex = dividendLength - 1; dividendIndex >= 0; dividendIndex--) {
            remainder = (remainder << 32) + ToUnsignedLong(dividend[dividendIndex]);
            int64_t quotient = DivideUnsignedLong(remainder, divisor);
            dividend[dividendIndex] = (int32_t)quotient;
            remainder = remainder - (quotient * divisorUnsigned);
        }
        return (int32_t)remainder;
    }

    static inline int32_t NumberOfLeadingZeros(int32_t var)
    {
        if (var == 0) {
            return 32;
        } else {
            int32_t count = 1;
            if ((uint32_t)var >> 16 == 0) {
                count += 16;
                var <<= 16;
            }
            if ((uint32_t)var >> 24 == 0) {
                count += 8;
                var <<= 8;
            }
            if ((uint32_t)var >> 28 == 0) {
                count += 4;
                var <<= 4;
            }
            if ((uint32_t)var >> 30 == 0) {
                count += 2;
                var <<= 2;
            }
            count -= (uint32_t)var >> 31;
            return count;
        }
    }

    static inline int64_t CombineInts(int32_t high, int32_t low)
    {
        return (((int64_t)high) << 32) | ToUnsignedLong(low);
    }

    static inline int32_t EstimateQuotient(int32_t u2, int32_t u1, int32_t u0, int32_t v1, int32_t v0)
    {
        int64_t u21 = CombineInts(u2, u1);
        int64_t qHat;
        if (u2 == v1) {
            qHat = INT_BASE - 1;
        } else if (u21 >= 0) {
            qHat = u21 / ToUnsignedLong(v1);
        } else {
            qHat = DivideUnsignedLong(u21, v1);
        }

        if (qHat == 0) {
            return 0;
        }

        int32_t iterations = 0;
        int64_t rHat = u21 - ToUnsignedLong(v1) * qHat;
        while (CompareUnsigned(rHat, INT_BASE) < 0 &&
            CompareUnsigned(ToUnsignedLong(v0) * qHat, CombineInts(LowInt(rHat), u0)) > 0) {
            iterations++;
            qHat--;
            rHat += ToUnsignedLong(v1);
        }

        if (iterations > 2) {
            std::string err("qHat is greater than q by more than 2: " + std::to_string(iterations));
            throw OmniException("Decimal error", err);
        }

        return (int32_t)qHat;
    }

    static inline bool MultiplyAndSubtractUnsignedMultiPrecision(std::vector<int32_t> &left, int32_t leftOffset,
        std::vector<int32_t> &right, int32_t length, int32_t multiplier)
    {
        int64_t unsignedMultiplier = ToUnsignedLong(multiplier);
        int32_t leftIndex = leftOffset - length;
        int64_t multiplyAccumulator = 0;
        int64_t subtractAccumulator = INT_BASE;
        for (int32_t rightIndex = 0; rightIndex < length; rightIndex++, leftIndex++) {
            multiplyAccumulator = ToUnsignedLong(right[rightIndex]) * unsignedMultiplier + multiplyAccumulator;
            subtractAccumulator =
                (subtractAccumulator + ToUnsignedLong(left[leftIndex])) - ToUnsignedLong(LowInt(multiplyAccumulator));
            multiplyAccumulator = High(multiplyAccumulator);
            left[leftIndex] = LowInt(subtractAccumulator);
            subtractAccumulator = High(subtractAccumulator) + INT_BASE - 1;
        }
        subtractAccumulator += ToUnsignedLong(left[leftIndex]) - multiplyAccumulator;
        left[leftIndex] = LowInt(subtractAccumulator);
        return HighInt(subtractAccumulator) == 0;
    }

    static inline void AddUnsignedMultiPrecision(std::vector<int32_t> &left, int32_t leftOffset,
        std::vector<int32_t> &right, int32_t length)
    {
        int32_t leftIndex = leftOffset - length;
        int32_t carry = 0;
        for (int32_t rightIndex = 0; rightIndex < length; rightIndex++, leftIndex++) {
            int64_t accumulator =
                ToUnsignedLong(left[leftIndex]) + ToUnsignedLong(right[rightIndex]) + ToUnsignedLong(carry);
            left[leftIndex] = LowInt(accumulator);
            carry = HighInt(accumulator);
        }
        left[leftIndex] += carry;
    }

    // paper link: https://www.ams.org/journals/mcom/1996-65-213/S0025-5718-96-00688-6/S0025-5718-96-00688-6.pdf
    static inline void DivideKnuthNormalized(std::vector<int32_t> &remainder, int32_t dividendLength,
        std::vector<int32_t> &divisor, int32_t divisorLength, std::vector<int32_t> &quotient)
    {
        int32_t var1 = divisor[divisorLength - 1];
        int32_t var0 = divisor[divisorLength - 2];
        for (int32_t remainderIndex = dividendLength - 1; remainderIndex >= divisorLength; remainderIndex--) {
            int32_t qHat = EstimateQuotient(remainder[remainderIndex], remainder[remainderIndex - 1],
                remainder[remainderIndex - 2], var1, var0);
            if (qHat != 0) {
                bool overflow =
                    MultiplyAndSubtractUnsignedMultiPrecision(remainder, remainderIndex, divisor, divisorLength, qHat);
                if (overflow) {
                    qHat--;
                    AddUnsignedMultiPrecision(remainder, remainderIndex, divisor, divisorLength);
                }
            }
            quotient[remainderIndex - divisorLength] = qHat;
        }
    }

    static inline void DivideUnsignedMultiPrecision(std::vector<int32_t> &dividend, std::vector<int32_t> &divisor,
        std::vector<int32_t> &quotient)
    {
        int32_t divisorLength = DigitsIntegerBase(divisor);
        int32_t dividendLength = DigitsIntegerBase(dividend);
        if (dividendLength < divisorLength) {
            return;
        }

        if (divisorLength == 1) {
            int32_t remainder = DivideUnsignedMultiPrecision(dividend, dividendLength, divisor[0]);
            if (dividend[dividend.size() - 1] != 0) {
                ThrowIllegalState();
            }
            std::copy(dividend.begin(), dividend.begin() + quotient.size(), quotient.begin());
            std::fill(dividend.begin(), dividend.end(), 0);
            dividend[0] = remainder;
            return;
        }

        int32_t nlz = NumberOfLeadingZeros(divisor[divisorLength - 1]);
        ShiftLeftMultiPrecision(divisor, divisorLength, nlz);
        int32_t normalizedDividendLength = std::min((int32_t)dividend.size(), dividendLength + 1);
        ShiftLeftMultiPrecision(dividend, normalizedDividendLength, nlz);

        DivideKnuthNormalized(dividend, normalizedDividendLength, divisor, divisorLength, quotient);

        ShiftRightMultiPrecision(dividend, normalizedDividendLength, nlz);
    }

    static inline OpStatus Divide(Decimal128 dividend, Decimal128 divisor, int32_t dividendScaleFactor,
        int32_t divisorScaleFactor, Decimal128 &quotient, Decimal128 &remainder)
    {
        if (dividendScaleFactor >= Decimal128::MAX_LONG_PRECISION) {
            return OP_OVERFLOW;
        }

        if (divisorScaleFactor >= Decimal128::MAX_LONG_PRECISION) {
            return OP_OVERFLOW;
        }

        int64_t dividendHigh = dividend.HighBits();
        int64_t dividendLow = dividend.LowBits();
        int64_t divisorHigh = divisor.HighBits();
        int64_t divisorLow = divisor.LowBits();

        bool dividendIsNegative = dividendHigh < 0;
        bool divisorIsNegative = divisorHigh < 0;
        bool quotientIsNegative = (dividendIsNegative != divisorIsNegative);

        if (dividendIsNegative) {
            int64_t tmpHigh = NegateHigh(dividendHigh, dividendLow);
            dividendHigh = tmpHigh;
        }

        if (divisorIsNegative) {
            int64_t tmpHigh = NegateHigh(divisorHigh, divisorLow);
            divisorHigh = tmpHigh;
        }

        OpStatus status = DividePositives(dividendLow, dividendHigh, dividendScaleFactor, divisorLow, divisorHigh,
            divisorScaleFactor, quotient, remainder);
        if (status != SUCCESS) {
            return status;
        }
        if (dividendIsNegative) {
            Negate(remainder, 0);
        }
        if (quotientIsNegative) {
            Negate(quotient, 0);
        }
        return SUCCESS;
    }

    // not support dividend's precision is greater than 27. All input long should be positive.
    static inline OpStatus DividePositives(int64_t dividendLow, int64_t dividendHigh, int32_t dividendScaleFactor,
        int64_t divisorLow, int64_t divisorHigh, int32_t divisorScaleFactor, Decimal128 &quotient,
        Decimal128 &remainder)
    {
        if (divisorHigh == 0 && divisorLow == 0) {
            return DIVIDE_BY_ZERO;
        }

        // to fit 128b * 128b * 32b unsigned multiplication
        std::vector<int32_t> dividend(9);
        dividend[0] = LowInt(dividendLow);
        dividend[1] = HighInt(dividendLow);
        dividend[2] = LowInt(dividendHigh);
        dividend[3] = HighInt(dividendHigh);

        if (dividendScaleFactor > 0) {
            Decimal128 left = Decimal128(POWERS_OF_FIVE_LONG[dividendScaleFactor]);
            Decimal128 right;
            if (Pack(dividend, right) != SUCCESS) {
                return OP_OVERFLOW;
            }
            dividend = MultiplyUnsignedMultiPrecision(left, right);
            dividend.push_back(0);

            ShiftLeftMultiPrecision(dividend, 8, dividendScaleFactor);
        }

        std::vector<int32_t> divisor(8);
        divisor[0] = LowInt(divisorLow);
        divisor[1] = HighInt(divisorLow);
        divisor[2] = LowInt(divisorHigh);
        divisor[3] = HighInt(divisorHigh);

        if (divisorScaleFactor > 0) {
            Decimal128 left = Decimal128(POWERS_OF_FIVE_LONG[divisorScaleFactor]);
            Decimal128 right;
            if (Pack(divisor, right) != SUCCESS) {
                return OP_OVERFLOW;
            }
            if (Multiply256(left, right) != SUCCESS) {
                return OP_OVERFLOW;
            }
            ToIntArray(right, divisor);
            ShiftLeftMultiPrecision(divisor, 8, divisorScaleFactor);
        }

        std::vector<int32_t> multiPrecisionQuotient(8);
        DivideUnsignedMultiPrecision(dividend, divisor, multiPrecisionQuotient);
        // two positive do division if quotient is negative throw exception
        if (Pack(multiPrecisionQuotient, quotient) != SUCCESS) {
            return OP_OVERFLOW;
        }
        if (Pack(dividend, remainder) != SUCCESS) {
            return OP_OVERFLOW;
        }
        return SUCCESS;
    }

    static inline OpStatus DivideRoundUp(Decimal128 &dividend, Decimal128 &divisor, int32_t dividendScaleFactor,
        int32_t divisorScaleFactor, Decimal128 &quotient)
    {
        if (dividendScaleFactor >= Decimal128::MAX_LONG_PRECISION) {
            return OP_OVERFLOW;
        }

        if (divisorScaleFactor >= Decimal128::MAX_LONG_PRECISION) {
            return OP_OVERFLOW;
        }

        int64_t dividendHigh = dividend.HighBits();
        int64_t dividendLow = dividend.LowBits();
        int64_t divisorHigh = divisor.HighBits();
        int64_t divisorLow = divisor.LowBits();

        bool dividendIsNegative = dividendHigh < 0;
        bool divisorIsNegative = divisorHigh < 0;
        bool quotientIsNegative = (dividendIsNegative != divisorIsNegative);

        if (dividendIsNegative) {
            int64_t tmpHigh = NegateHigh(dividendHigh, dividendLow);
            dividendHigh = tmpHigh;
        }

        if (divisorIsNegative) {
            int64_t tmpHigh = NegateHigh(divisorHigh, divisorLow);
            divisorHigh = tmpHigh;
        }

        Decimal128 remainder;
        OpStatus status = DividePositives(dividendLow, dividendHigh, dividendScaleFactor, divisorLow, divisorHigh,
            divisorScaleFactor, quotient, remainder);
        if (status != SUCCESS) {
            return status;
        }

        // Round up. If (2 * remainder >= divisor) increment quotient by one
        status = ShiftLeftDestructive(remainder, 1);
        if (status != SUCCESS) {
            return status;
        }
        int64_t remainderLow = remainder.LowBits();
        int64_t remainderHigh = remainder.HighBits();
        if (CompareUnsigned(remainderLow, remainderHigh, divisorLow, divisorHigh) >= 0) {
            IncrementUnsafe(quotient, 0);
        }
        if (ExceedsOrEqualTenToThirtyEight(quotient)) {
            return OP_OVERFLOW;
        }

        if (quotientIsNegative) {
            Negate(quotient, 0);
        }

        return SUCCESS;
    }

    static inline OpStatus Remainder(Decimal128 &dividend, int32_t dividendScaleFactor, Decimal128 &divisor,
        int32_t divisorScaleFactor, Decimal128 &remainder)
    {
        Decimal128 quotient;
        OpStatus status = Divide(dividend, divisor, dividendScaleFactor, divisorScaleFactor, quotient, remainder);
        return status;
    }

    static inline OpStatus ShiftLeftDestructive(Decimal128 &decimal, int32_t shift)
    {
        if (shift == 0) {
            return SUCCESS;
        }

        int32_t wordShifts = shift / 64;
        int32_t bitShiftsInWord = shift % 64;
        int32_t shiftRestore = 64 - bitShiftsInWord;

        if (bitShiftsInWord != 0) {
            if ((GetLong(decimal, 1 - wordShifts) & (-1LL << shiftRestore)) != 0) {
                return OP_OVERFLOW;
            }
        }
        if (wordShifts == 1) {
            if (GetLong(decimal, 1) != 0) {
                return OP_OVERFLOW;
            }
        }

        bool negative = IsNegative(decimal);
        int64_t low;
        int64_t high;

        switch (wordShifts) {
            case 0: {
                low = GetLong(decimal, 0);
                high = GetLong(decimal, 1);
                break;
            }
            case 1: {
                low = 0;
                high = GetLong(decimal, 0);
                break;
            }
            default:
                ThrowIllegalState();
        }

        if (bitShiftsInWord > 0) {
            high = (high << bitShiftsInWord) | ((uint64_t)low >> shiftRestore);
            low = low << bitShiftsInWord;
        }
        return Pack(decimal, low, high, negative);
    }

    static inline void IncrementUnsafe(Decimal128 &value, int32_t offset)
    {
        int64_t low = value.LowBits();
        if (low != ALL_BITS_SET_64) {
            value.SetValue(value.HighBits(), IncrementLow(low));
            return;
        }
        int64_t highUnsigned = UnpackUnsignedLong(value.HighBits());
        int64_t high = IncrementHigh(highUnsigned);
        value.SetValue(IsNegative(value) ? high | Decimal128::SIGN_LONG_MASK : high, low);
    }

    static inline int64_t IncrementHigh(int64_t high)
    {
        return high + 1;
    }

    static inline int64_t IncrementLow(int64_t low)
    {
        return low + 1;
    }

    static inline Decimal128 AbsExact(Decimal128 &value)
    {
        if (value.HighBits() < 0) {
            return NegateExact(value);
        }
        return value;
    }

    static inline int32_t RescaleFactor(int32_t fromScale, int32_t toScale)
    {
        return std::max(0, toScale - fromScale);
    }

    static inline int32_t DivideRescaleFactor(int32_t dividendScale, int32_t divisorScale, int32_t resultScale)
    {
        return resultScale - dividendScale + divisorScale;
    }

    static inline OpStatus IsOverflows(Decimal128 value, int precision)
    {
        if (precision == MAX_PRECISION) {
            if (ExceedsOrEqualTenToThirtyEight(value)) {
                return OP_OVERFLOW;
            }
            return SUCCESS;
        }
        int64_t high = value.HighBits();
        __int128_t t = high < 0 ? high ^ (1L << 63) : high;
        t = t << 64;
        t = t + value.LowBits();
        if (precision > MAX_PRECISION || t >= POWERS_OF_TEN[precision]) {
            return OP_OVERFLOW;
        }
        return SUCCESS;
    }

    static inline OpStatus IsOverflows(int64_t value, int precision)
    {
        if (precision > 19 || value == INT64_MIN || abs(value) >= POWERS_OF_TEN[precision]) {
            return OP_OVERFLOW;
        }
        return SUCCESS;
    }

    static inline OpStatus Rescale128(Decimal128 &decimal, int32_t rescaleFactor, Decimal128 &result)
    {
        if (rescaleFactor == 0) {
            result = decimal;
            return SUCCESS;
        }
        Decimal128 scaleValue = TenToScale(abs(rescaleFactor));
        OpStatus status;
        if (rescaleFactor >= 0) {
            status = DecimalOperations::Multiply(decimal, scaleValue, result);
        } else {
            status = DecimalOperations::DivideRoundUp(decimal, scaleValue, 0, 0, result);
        }

        if (status != SUCCESS) {
            return OP_OVERFLOW;
        }
        return SUCCESS;
    }

    static inline OpStatus Rescale128RoundToZero(Decimal128 &decimal, int32_t rescaleFactor, Decimal128 &result)
    {
        if (rescaleFactor == 0) {
            result = decimal;
            return SUCCESS;
        }
        Decimal128 scaleValue = TenToScale(abs(rescaleFactor));
        OpStatus status;
        if (rescaleFactor > 0) {
            status = DecimalOperations::Multiply(decimal, scaleValue, result);
        } else {
            Decimal128 remainder;
            status = DecimalOperations::Divide(decimal, scaleValue, 0, 0, result, remainder);
        }
        if (status != SUCCESS) {
            return OP_OVERFLOW;
        }
        return SUCCESS;
    }

    static inline OpStatus Rescale64(int64_t value, int32_t rescaleFactor, int64_t &result)
    {
        if (rescaleFactor == 0) {
            result = value;
            return SUCCESS;
        }
        int64_t tenOfScale;
        int128_t tmpValue = value;
        if (rescaleFactor < 0) {
            tenOfScale = INT64_TEN_POWERS_TABLE[-rescaleFactor];
            if (value < 0) {
                tmpValue -= tenOfScale / 2;
            } else {
                tmpValue += tenOfScale / 2;
            }
            tmpValue /= tenOfScale;
        } else {
            tenOfScale = INT64_TEN_POWERS_TABLE[rescaleFactor];
            tmpValue *= tenOfScale;
        }
        if (abs(tmpValue) > DECIMAL64_MAX_VALUE) {
            return OP_OVERFLOW;
        }
        result = tmpValue;
        return SUCCESS;
    }

    static inline OpStatus Rescale64RoundToZero(int64_t value, int32_t rescaleFactor, int64_t &result)
    {
        if (rescaleFactor == 0 || value == 0) {
            result = value;
            return SUCCESS;
        }

        if (rescaleFactor > MAX_DECIMAL64_DIGITS || rescaleFactor < -MAX_DECIMAL64_DIGITS) {
            return OP_OVERFLOW;
        }

        if (rescaleFactor < 0) {
            int64_t p = INT64_TEN_POWERS_TABLE[-rescaleFactor];
            result = value / p;
            return SUCCESS;
        } else {
            int64_t p = INT64_TEN_POWERS_TABLE[rescaleFactor];
            if (LONG_MAX_VALUE / value < p) {
                return OP_OVERFLOW;
            }
            result = value * p;
            return SUCCESS;
        }
    }

    // TODO this function is not able to handle scale factor >= 18
    static inline OpStatus Rescale64To128(int64_t value, int32_t rescaleFactor, Decimal128 &result)
    {
        Decimal128 input(value);
        return Rescale128(input, rescaleFactor, result);
    }

    static inline OpStatus Rescale128To64(Decimal128 input, int32_t rescaleFactor, int64_t &result)
    {
        if (Rescale128(input, rescaleFactor, input) != SUCCESS) {
            return OP_OVERFLOW;
        }
        if ((input.HighBits() != 0 && input.HighBits() != 1L << 63) || input.LowBits() > INT64_MAX) {
            return OP_OVERFLOW;
        }
        result = input.HighBits() < 0 ? -input.LowBits() : input.LowBits();
        return SUCCESS;
    }

    static inline OpStatus UnscaledDecimal128ToLong(Decimal128 decimal, int64_t &result)
    {
        uint64_t low = decimal.LowBits();
        int64_t high = decimal.HighBits();
        bool negative = false;
        if (high < 0) {
            negative = true;
        }
        if ((high != 0 && high != 1L << 63) ||
            (low > INT64_MAX && !negative) ||
            (low - 1 > INT64_MAX && negative)) {
            return OP_OVERFLOW;
        }
        result = negative ? -low : low;
        return SUCCESS;
    }

    static inline OpStatus ToIntExact(long value, int &result)
    {
        if ((int)value != value) {
            return OP_OVERFLOW;
        }
        result = (int)value;
        return SUCCESS;
    }

    static inline OpStatus StringToDecimal64(const std::string &s, int64_t &rs, int32_t &scale, int32_t &precision)
    {

        bool isDot = false;
        bool isNeg = false;
        bool isExp = false;
        bool isSpace = false;
        int32_t exponent = 0;
        uint64_t len = s.size();
        int offset = 0;
        while (s[offset] == ' ') {
            offset += 1;
        }
        if (s[0] == '-') {
            isNeg = true;
            offset++;
        } else if (s[0] == '+') {
            offset++;
        }
        if (!isdigit(s[offset])) {
            return FAIL;
        }
        for (; offset < len; offset++) {
            if (isdigit(s[offset])) {
                long tmpValue = 0;
                bool status = __builtin_smull_overflow(rs, 10, &tmpValue);
                if (status) {
                    return OP_OVERFLOW;
                }
                status = __builtin_saddl_overflow(tmpValue, int(s[offset]) - 48, &rs);
                if (status) {
                    return OP_OVERFLOW;
                }
                precision++;
                if (precision > 18) {
                    return OP_OVERFLOW;
                }
                if (isDot) {
                    scale++;
                }
            } else if (s[offset] == '.' && !isDot) {
                isDot = true;
            } else if (s[offset] == 'e' || s[offset] == 'E') {
                offset++;
                isExp = true;
                break;
            } else if (s[offset] == ' ') {
                offset++;
                isSpace = true;
                break;
            } else {
                return FAIL;
            }
        }

        if (isExp) {
            for (; offset < len; offset++) {
                if (isdigit(s[offset])) {
                    exponent *= 10;
                    exponent += int(s[offset]) - 48;
                    if (exponent + precision - scale > 18) {
                        return OP_OVERFLOW;
                    }
                } else if (s[offset] == ' ') {
                    isSpace = true;
                    offset++;
                    break;
                } else {
                    return FAIL;
                }
            }
            if (exponent == 0) {
                return FAIL;
            }
        }

        if (isSpace) {
            for (; offset < len; offset++) {
                if (s[offset] != ' ') {
                    return FAIL;
                }
            }
        }

        scale -= exponent;
        while (scale < 0) {
            rs *= 10;
            scale++;
            precision++;
        }

        if (isNeg) {
            rs = -rs;
        }
        return SUCCESS;
    }

    static inline OpStatus StringToDecimal128(const std::string &s, Decimal128 &rs, int32_t &scale, int32_t &precision)
    {
        __int128_t result = 0;
        bool isDot = false;
        bool isNeg = false;
        bool isExp = false;
        bool isSpace = false;
        int32_t exponent = 0;
        uint64_t len = s.size();
        int32_t offset = 0;
        while (s[offset] == ' ') {
            offset += 1;
        }
        if (s[0] == '-') {
            isNeg = true;
            offset++;
        } else if (s[0] == '+') {
            offset++;
        }
        if (!isdigit(s[offset])) {
            return FAIL;
        }
        for (; offset < len; offset++) {
            if (isdigit(s[offset])) {
                precision++;
                if (precision > 38) {
                    return OP_OVERFLOW;
                }
                result *= 10;
                result += int(s[offset]) - 48;
                if (isDot) {
                    scale++;
                }
            } else if (s[offset] == '.' && !isDot) {
                isDot = true;
            } else if (s[offset] == 'e' || s[offset] == 'E') {
                offset++;
                isExp = true;
                break;
            } else if (s[offset] == ' ') {
                offset++;
                isSpace = true;
                break;
            } else {
                return FAIL;
            }
        }

        if (isExp) {
            for (; offset < len; offset++) {
                if (isdigit(s[offset])) {
                    exponent *= 10;
                    exponent += int(s[offset]) - 48;
                    if (exponent + precision - scale > 38) {
                        return OP_OVERFLOW;
                    }
                } else if (s[offset] == ' ') {
                    isSpace = true;
                    offset++;
                    break;
                } else {
                    return FAIL;
                }
            }
            if (exponent == 0) {
                return FAIL;
            }
        }

        if (isSpace) {
            for (; offset < len; offset++) {
                if (s[offset] != ' ') {
                    return FAIL;
                }
            }
        }

        scale -= exponent;
        while (scale < 0) {
            result *= 10;
            scale++;
            precision++;
        }

        if (isNeg) {
            result = -result;
        }
        rs = Decimal128(result);
        return SUCCESS;
    }

    static inline std::string ScaleOfDecimal(std::string inputString, int scale)
    {
        std::string unscaledValueString = std::move(inputString);
        std::string resultBuilder;
        if (unscaledValueString[0] == '-') {
            resultBuilder.append("-");
            unscaledValueString = unscaledValueString.substr(1);
        }

        if (unscaledValueString.length() <= scale) {
            resultBuilder.append("0");
        } else {
            resultBuilder.append(unscaledValueString.substr(0, unscaledValueString.length() - scale));
        }

        if (scale > 0) {
            resultBuilder.append(".");
            if (unscaledValueString.length() < scale) {
                for (int i = 0; i < scale - unscaledValueString.length(); ++i) {
                    resultBuilder.append("0");
                }
                resultBuilder.append(unscaledValueString);
            } else {
                resultBuilder.append(unscaledValueString.substr(unscaledValueString.length() - scale));
            }
        }
        return resultBuilder;
    }

    static inline Decimal128 TenToScale(int32_t scale)
    {
        Decimal128 x = DecimalOperations::UnscaledDecimal(10);
        Decimal128 r = DecimalOperations::UnscaledDecimal(1);
        while (scale > 0) {
            scale--;
            DecimalOperations::Multiply(x, r, r);
        }
        return r;
    }

    // Decimal Internal Operation
    static inline OpStatus InternalAddDec128(Decimal128 x, int32_t xScale, Decimal128 y, int32_t yScale,
        int32_t &resultScale, Decimal128 &r)
    {
        int32_t xRescaleFactor = RescaleFactor(xScale, yScale);
        int32_t yRescaleFactor = RescaleFactor(yScale, xScale);

        Decimal128 left;
        Decimal128 right;
        if (xRescaleFactor > 0) {
            if (Rescale128(x, xRescaleFactor, left) != SUCCESS) {
                return OP_OVERFLOW;
            }
            right = y;
            resultScale = yScale;
        } else {
            if (Rescale128(y, yRescaleFactor, left) != SUCCESS) {
                return OP_OVERFLOW;
            }
            right = x;
            resultScale = xScale;
        }
        if (AddWithOverflow(left, right, left) != 0) {
            return OP_OVERFLOW;
        }
        r = left;
        return SUCCESS;
    }

    static inline OpStatus InternalSubDec128(Decimal128 x, int32_t xScale, Decimal128 y, int32_t yScale,
        int32_t &resultScale, Decimal128 &r)
    {
        r.SetValue(0, 0);
        int32_t xRescaleFactor = RescaleFactor(xScale, yScale);
        int32_t yRescaleFactor = RescaleFactor(yScale, xScale);
        if (xRescaleFactor > 0) {
            if (Rescale128(x, xRescaleFactor, r) != SUCCESS) {
                return OP_OVERFLOW;
            }
            if (Subtract(r, y, r) != SUCCESS) {
                return OP_OVERFLOW;
            }
            resultScale = yScale;
        } else {
            if (Rescale128(y, yRescaleFactor, r) != SUCCESS) {
                return OP_OVERFLOW;
            }
            if (Subtract(x, r, r) != SUCCESS) {
                return OP_OVERFLOW;
            }
            resultScale = xScale;
        }
        return SUCCESS;
    }

    static inline OpStatus InternalDivDec128(Decimal128 x, int32_t xScale, Decimal128 y, int32_t yScale, Decimal128 &r,
        int32_t outScale)
    {
        int32_t scaleFactor = DivideRescaleFactor(xScale, yScale, outScale);
        OpStatus status = DivideRoundUp(x, y, scaleFactor, 0, r);
        return status;
    }

    static inline OpStatus InternalModDec128(const Decimal128 &x, int32_t xScale, const Decimal128 &y, int32_t yScale,
        int32_t &resultScale, Decimal128 &r)
    {
        int32_t xRescaleFactor = RescaleFactor(xScale, yScale);
        int32_t yRescaleFactor = RescaleFactor(yScale, xScale);
        Decimal128 dividend(x.HighBits(), x.LowBits());
        Decimal128 divisor(y.HighBits(), y.LowBits());
        if (xRescaleFactor > 0) {
            if (Remainder(dividend, xRescaleFactor, divisor, 0, r) != SUCCESS) {
                return OP_OVERFLOW;
            }
            resultScale = yScale;
        } else {
            if (Remainder(dividend, 0, divisor, yRescaleFactor, r) != SUCCESS) {
                return OP_OVERFLOW;
            }
            resultScale = xScale;
        }
        return SUCCESS;
    }

    // check if unscaled value is overflow under the representation of Decimal(precision, scale)
    static inline bool IsUnscaledLongOverflow(int64_t unscaled, int32_t precision, int32_t scale)
    {
        int64_t maxDecimal64 = INT64_TEN_POWERS_TABLE[MAX_DECIMAL64_DIGITS];
        if (unscaled <= -maxDecimal64 || unscaled >= maxDecimal64) {
            if (precision <= MAX_DECIMAL64_DIGITS) {
                return true;
            }
            return false;
        }

        int64_t p = precision <= MAX_DECIMAL64_DIGITS ? INT64_TEN_POWERS_TABLE[precision] : pow(10, precision);
        if (unscaled <= -p || unscaled >= p) {
            return true;
        }
        return false;
    }

};
}
}

#endif // OMNI_RUNTIME_DECIMAL_OPERATIONS_H
