/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: DecimalOperations
 */

#ifndef OMNI_RUNTIME_DECIMAL_OPERATIONS_H
#define OMNI_RUNTIME_DECIMAL_OPERATIONS_H


#include <cstdint>
#include <iostream>
#include <climits>
#include <huawei_secure_c/include/securec.h>
#include "util/debug.h"
#include "decimal_base.h"
#include "decimal128.h"
#include "operator/aggregation/aggregator/aggregator.h"

namespace omniruntime {
namespace type {
static constexpr int64_t LONG_MIN_VALUE = 0x8000'0000'0000'0000;
static constexpr int64_t LONG_MAX_VALUE = 0x7FFF'FFFF'FFFF'FFFF;
static constexpr int64_t ALL_BITS_SET_64 = 0xFFFF'FFFF'FFFF'FFFFLL;
static const Decimal128 MIN_VALUE = { LLONG_MAX + 1, 0x0000'0000'0000'0000L };
static const Decimal128 MAX_VALUE = { 0x7FFF'FFFF'FFFF'FFFFL, 0xFFFF'FFFF'FFFF'FFFFLL };
static constexpr int64_t INT_BASE = 1L << 32;
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
static std::array<int64_t, 28> POWERS_OF_FIVE_LONG = { 1,
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
                                                       5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5,
                                                       5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5L,
                                                       5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5L * 5L,
                                                       5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5L * 5L * 5L,
                                                       5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5L * 5L * 5L * 5L,
                                                       5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5L * 5L * 5L * 5L * 5L,
                                                       5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5L * 5L * 5L * 5L * 5L * 5L,
                                                       5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5L * 5L * 5L * 5L * 5L * 5L * 5L,
                                                       5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5L * 5L * 5L * 5L * 5L * 5L * 5L * 5L,
                                                       5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5L * 5L * 5L * 5L * 5L * 5L * 5L * 5L * 5L,
                                                       5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5L * 5L * 5L * 5L * 5L * 5L * 5L * 5L * 5L * 5L,
                                                       5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5L * 5L * 5L * 5L * 5L * 5L * 5L * 5L * 5L * 5L * 5L,
                                                       5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5L * 5L * 5L * 5L * 5L * 5L * 5L * 5L * 5L * 5L * 5L * 5L,
                                                       5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5L * 5L * 5L * 5L * 5L * 5L * 5L * 5L * 5L * 5L * 5L * 5L * 5L,
                                                       5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5 * 5L * 5L * 5L * 5L * 5L * 5L * 5L * 5L * 5L * 5L * 5L * 5L * 5L *
    5L };

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

    static inline void ThrowOverflow()
    {
        throw OmniException("OVERFLOW", "Decimal overflow");
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

    static inline long AddWithOverflow(Decimal128 &left, Decimal128 &right, Decimal128 &result)
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
        Pack(result, z0, z1, resultNegative);

        return intermediateResult >> 63;
    }

    static inline void SubtractUnsigned(Decimal128 &left, Decimal128 &right, Decimal128 &result, bool resultNegative)
    {
        int64_t l0 = left.LowBits();
        int64_t l1 = UnpackUnsignedLong(left.HighBits());

        int64_t r0 = right.LowBits();
        int64_t r1 = UnpackUnsignedLong(right.HighBits());

        int64_t z0 = l0 - r0;
        int32_t underflow = UnsignedIsSmaller(l0, z0) ? 1 : 0;
        int64_t z1 = l1 - r1 - underflow;
        Pack(result, z0, z1, resultNegative);
    }

    static inline void Subtract(Decimal128 &left, Decimal128 &right, Decimal128 &result)
    {
        if (IsNegative(left) != IsNegative(right)) {
            if (AddUnsignedReturnOverflow(left, right, result, IsNegative(left)) != 0) {
                ThrowOverflow();
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
    }

    static inline void Multiply(Decimal128 &left, Decimal128 &right, Decimal128 &result)
    {
        int64_t l0 = left.LowBits();
        int64_t l1 = UnpackUnsignedLong(left.HighBits());

        int64_t r0 = right.LowBits();
        int64_t r1 = UnpackUnsignedLong(right.HighBits());

        int64_t z0 = 0;
        int64_t z1 = 0;

        if (l0 != 0) {
            __int128_t accumulator = r0 * l0;
            z0 = accumulator & Decimal128::LOW_64_BITS;
            accumulator = (accumulator >> 64) + r1 * l0;

            z1 = accumulator & Decimal128::LOW_64_BITS;

            if ((accumulator >> 64) != 0) {
                ThrowOverflow();
            }
        }

        if (l1 != 0) {
            __int128_t accumulator = r0 * l1 + z1;
            z1 = accumulator & Decimal128::LOW_64_BITS;

            if ((accumulator >> 64) != 0) {
                ThrowOverflow();
            }
        }
        Pack(result, z0, z1, IsNegative(left) != IsNegative(right));
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
        z1 = accumulator >> 32;

        accumulator = r0 * l1 + z1;
        z1 = accumulator & Decimal128::LOW_32_BITS;
        z2 = accumulator >> 32;

        accumulator = r0 * l2 + z2;
        z2 = accumulator & Decimal128::LOW_32_BITS;
        z3 = accumulator >> 32;

        accumulator = r0 * l3 + z3;
        z3 = accumulator & Decimal128::LOW_32_BITS;
        z4 = accumulator >> 32;

        left[0] = (int32_t)z0;
        left[1] = (int32_t)z1;
        left[2] = (int32_t)z2;
        left[3] = (int32_t)z3;
        left[4] = (int32_t)z4;
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

    static inline void Pack(Decimal128 &decimal128, int64_t low, int64_t high, bool negative)
    {
        decimal128.SetValue(high | (negative ? Decimal128::SIGN_LONG_MASK : 0), low);
    }

    static inline void Pack(std::vector<int32_t> &parts, Decimal128 &result)
    {
        if (parts[4] != 0 || parts[5] != 0 || parts[6] != 0 || parts[7] != 0) {
            ThrowOverflow();
        }

        if (parts[3] < 0) {
            ThrowOverflow();
        }

        int64_t high = (((int64_t)parts[3]) << 32 | (parts[2] & 0xFFFF'FFFFL));
        int64_t low = (((int64_t)parts[1]) << 32 | (parts[0] & 0xFFFF'FFFFL));

        result.SetValue(high, low);
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
                          high == 0 ? Decimal128::SIGN_LONG_MASK : high & Decimal128::SIGN_LONG_MASK;
    }

    static inline int64_t NegateLow(int64_t high, int64_t low)
    {
        return -low;
    }

    static inline int64_t NegateHighExact(int64_t high, int64_t low)
    {
        if (high == MIN_VALUE.HighBits() && low == MIN_VALUE.LowBits()) {
            ThrowOverflow();
        }
        return NegateHigh(high, low);
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
        Decimal128 result(NegateHighExact(value.HighBits(), value.LowBits()), value.LowBits());
        return result;
    }

    static inline int32_t LowInt(int64_t val)
    {
        return (int32_t)val;
    }

    static inline int64_t High(int64_t val)
    {
        return val >> 32;
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

        int32_t wordShifts = shifts >> 5;
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
            if ((number[length - 1] >> (32 - bitShifts)) != 0) {
                throw OmniException("Decimal", "Leading bits should be zero");
            }
            for (int32_t position = length - 1; position > 0; position--) {
                number[position] = (number[position] << bitShifts | (number[position - 1] >> (32 - bitShifts)));
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

        int wordShifts = shifts >> 5;
        for (int32_t i = 0; i < wordShifts; i++) {
            if (number[i] != 0) {
                ThrowIllegalState();
            }
        }
        if (wordShifts > 0) {
            std::copy(number.begin() + wordShifts, number.begin() + length, number.begin());
            std::fill(number.begin() + length - wordShifts, number.begin() + 2 * length - wordShifts, 0);
        }
        int32_t bitShifts = shifts & 0b11111;
        if (bitShifts > 0) {
            if (number[0] << (32 - bitShifts) == 0) {
                ThrowIllegalState();
            }
            for (int32_t position = 0; position < length - 1; position++) {
                number[position] = (number[position] >> bitShifts) | (number[position + 1] << (32 - bitShifts));
            }
            number[length - 1] = number[length - 1] >> bitShifts;
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
        int64_t unsignedDivisor = (uint64_t)divisor;

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
            if (var >> 16 == 0) {
                count += 16;
                var <<= 16;
            }
            if (var >> 25 == 0) {
                count += 8;
                var <<= 8;
            }
            if (var >> 28 == 0) {
                count += 4;
                var <<= 4;
            }
            if (var >> 30 == 0) {
                count += 2;
                var <<= 2;
            }
            count -= var >> 31;
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

        std::string err("qHat is greater than q by more than 2: " + std::to_string(iterations));
        if (iterations > 0) {
            throw OmniException("Decimal error", err);
        }

        return (int32_t)qHat;
    }

    static inline bool MultiplyAndSubtractUnsignedMultiPrecision(std::vector<int32_t> &left, int32_t leftOffset,
        std::vector<int32_t> &right, int32_t length, int32_t multiplier)
    {
        int64_t unsignedMultiplier = (uint64_t)multiplier;
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
        int32_t var0 = divisor[divisorLength - 1];
        int32_t var1 = divisor[divisorLength - 2];
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

    // not support dividend's precision is greater than 27. All input long should be positive.
    static inline void DividePositives(int64_t dividendLow, int64_t dividendHigh, int32_t dividendScaleFactor,
        int64_t divisorLow, int64_t divisorHigh, int32_t divisorScaleFactor, Decimal128 &quotient,
        Decimal128 &remainder)
    {
        if (divisorHigh == 0 && divisorLow == 0) {
            ThrowDivideZero();
        }

        // to fit 128b * 128b * 32b unsigned multiplication
        std::vector<int32_t> dividend(9);
        dividend[0] = LowInt(dividendLow);
        dividend[1] = HighInt(dividendLow);
        dividend[2] = LowInt(dividendHigh);
        dividend[3] = HighInt(dividendHigh);

        if (dividendScaleFactor > 0) {
            ShiftLeftBy5Destructive(dividend, dividendScaleFactor);
            ShiftLeftMultiPrecision(dividend, 8, dividendScaleFactor);
        }

        std::vector<int32_t> divisor(8);
        divisor[0] = LowInt(divisorLow);
        divisor[1] = HighInt(divisorLow);
        divisor[2] = LowInt(divisorHigh);
        divisor[3] = HighInt(divisorHigh);

        if (divisorScaleFactor > 0) {
            ShiftLeftBy5Destructive(divisor, dividendScaleFactor);
            ShiftLeftMultiPrecision(divisor, 8, dividendScaleFactor);
        }

        std::vector<int32_t> multiPrecisionQuotient(8);
        DivideUnsignedMultiPrecision(dividend, divisor, multiPrecisionQuotient);
        // two positive do division if quotient is negative throw exception
        Pack(multiPrecisionQuotient, quotient);
        Pack(dividend, remainder);
    }

    static inline Decimal128 DivideRoundUp(Decimal128 &dividend, Decimal128 &divisor, int32_t dividendScaleFactor,
        int32_t divisorScaleFactor)
    {
        if (dividendScaleFactor >= Decimal128::MAX_PRECISION) {
            ThrowOverflow();
        }

        if (divisorScaleFactor >= Decimal128::MAX_PRECISION) {
            ThrowOverflow();
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

        Decimal128 quotient;
        Decimal128 remainder;
        DividePositives(dividendLow, dividendHigh, dividendScaleFactor, divisorLow, divisorHigh, divisorScaleFactor,
            quotient, remainder);

        // Round up. If (2 * remainder >= divisor) increment quotient by one
        ShiftLeftDestructive(remainder, 1);
        int64_t remainderLow = remainder.LowBits();
        int64_t remainderHigh = remainder.HighBits();
        if (CompareUnsigned(remainderLow, remainderHigh, divisorLow, divisorHigh) >= 0) {
            IncrementUnsafe(quotient, 0);
        }

        if (quotientIsNegative) {
            Negate(quotient, 0);
        }

        ThrowIfOverflows(quotient);
        return quotient;
    }

    static inline void ShiftLeftDestructive(Decimal128 &decimal, int32_t shift)
    {
        if (shift == 0) {
            return;
        }

        int32_t wordShifts = shift / 64;
        int32_t bitShiftsInWord = shift % 64;
        int32_t shiftRestore = 64 - bitShiftsInWord;

        if (bitShiftsInWord != 0) {
            if ((GetLong(decimal, 1 - wordShifts) & (-1 << shiftRestore)) != 0) {
                ThrowOverflow();
            }
        }
        if (wordShifts == 1) {
            if (GetLong(decimal, 1) != 0) {
                ThrowOverflow();
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
            high = (high << bitShiftsInWord) | (low >> shiftRestore);
            low = low << bitShiftsInWord;
        }
        Pack(decimal, low, high, negative);
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
};
}
}

#endif // OMNI_RUNTIME_DECIMAL_OPERATIONS_H
