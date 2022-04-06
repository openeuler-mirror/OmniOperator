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

namespace omniruntime {
namespace type {
static constexpr int64_t SIGN_LONG_MASK = 1LL << 63;
static constexpr int32_t BYTES_OF_LONG = 8;
static constexpr uint32_t RIGHT_SHIFT = 63;

class DecimalOperations {
public:
    DecimalOperations(int64_t high_bits, uint64_t low_bits) = delete;
    ~DecimalOperations() = delete;

    // decimal and overflow is encoded and decoded in continuous memory
    static inline void DecodeSumDecimal(void *ptr, Decimal128 &val, int64_t &overflow)
    {
        overflow = *static_cast<int64_t *>(ptr);
        int64_t highBits = *(static_cast<int64_t *>(ptr) + 1);
        uint64_t lowBits = *(static_cast<uint64_t *>(ptr) + 2);
        val.SetValue(highBits, lowBits);
    }
    static inline void EncodeSumDecimal(void *ptr, const Decimal128 &val, const int64_t &overflow)
    {
        int64_t highBits = val.HighBits();
        uint64_t lowBits = val.LowBits();
        auto *p = static_cast<int64_t *>(ptr);
        auto err1 = memcpy_s(p, BYTES_OF_LONG, &overflow, BYTES_OF_LONG);
        auto err2 = memcpy_s(p + 1, BYTES_OF_LONG, &highBits, BYTES_OF_LONG);
        auto err3 = memcpy_s(p + 2, BYTES_OF_LONG, &lowBits, BYTES_OF_LONG);
        if (err1 != EOK || err2 != EOK || err3 != EOK) {
            LogError("Encode sum decimal failed!");
        }
    }

    // decimal and overflow is encoded in continuous memory
    static inline void DecodeAvgDecimal(void *ptr, Decimal128 &val, int64_t &overflow, int64_t &count)
    {
        count = *static_cast<int64_t *>(ptr);
        overflow = *(static_cast<uint64_t *>(ptr) + 1);
        int64_t highBits = *(static_cast<int64_t *>(ptr) + 2);
        uint64_t lowBits = *(static_cast<uint64_t *>(ptr) + 3);
        val.SetValue(highBits, lowBits);
    }

    static inline void EncodeAvgDecimal(void *ptr, const Decimal128 &val, const int64_t &overflow, const int64_t &count)
    {
        int64_t highBits = val.HighBits();
        uint64_t lowBits = val.LowBits();
        auto *p = static_cast<int64_t *>(ptr);
        auto err1 = memcpy_s(p, BYTES_OF_LONG, &count, BYTES_OF_LONG);
        auto err2 = memcpy_s(p + 1, BYTES_OF_LONG, &overflow, BYTES_OF_LONG);
        auto err3 = memcpy_s(p + 2, BYTES_OF_LONG, &highBits, BYTES_OF_LONG);
        auto err4 = memcpy_s(p + 3, BYTES_OF_LONG, &lowBits, BYTES_OF_LONG);
        if (err1 != EOK || err2 != EOK || err3 != EOK || err4 != EOK) {
            LogError("Encode avg decimal failed!");
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

    static inline long AddUnsignedReturnOverflow(const Decimal128 &left, const Decimal128 &right, Decimal128 &result,
        bool resultNegative)
    {
        uint64_t l0 = left.LowBits();
        int64_t l1 = GetLong(left.HighBits());

        uint64_t r0 = right.LowBits();
        int64_t r1 = GetLong(right.HighBits());

        uint64_t z0 = l0 + r0;
        int overflow = UnsignedIsSmaller(z0, l0) ? 1 : 0;
        uint64_t intermediateResult = l1 + r1 + overflow;
        int64_t z1 = intermediateResult & (~omniruntime::type::SIGN_LONG_MASK);
        Pack(result, z0, z1, resultNegative);

        return ((uint64_t)intermediateResult) >> RIGHT_SHIFT;
    }

    static inline void SetToZero(Decimal128 &decimal128)
    {
        decimal128.SetValue(0, 0);
    }

    static inline bool ExceedsOrEqualTenToThirtyEight(Decimal128 &decimal128)
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

    static inline int CompareAbsolute(Decimal128 &left, Decimal128 &right)
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

    static inline void SubtractUnsigned(Decimal128 &left, Decimal128 &right, Decimal128 &result, bool resultNegative)
    {
        uint64_t l0 = left.LowBits();
        int64_t l1 = GetLong(left.HighBits());

        uint64_t r0 = right.LowBits();
        int64_t r1 = GetLong(right.HighBits());

        uint64_t z0 = l0 - r0;
        int underflow = UnsignedIsSmaller(l0, z0) ? 1 : 0;
        long z1 = l1 - r1 - underflow;
        Pack(result, z0, z1, resultNegative);
    }

    static inline void Pack(Decimal128 &decimal128, uint64_t low, int64_t high, bool negative)
    {
        decimal128.SetValue(high | (negative ? omniruntime::type::SIGN_LONG_MASK : 0), low);
    }

    static inline bool UnsignedIsSmaller(uint64_t first, uint64_t second)
    {
        return first + LLONG_MIN < second + LLONG_MIN;
    }

    static inline bool IsNegative(Decimal128 &left)
    {
        return (((uint64_t)left.HighBits()) >> RIGHT_SHIFT) != 0;
    }

    static inline int64_t GetLong(int64_t bits)
    {
        return bits & ~omniruntime::type::SIGN_LONG_MASK;
    }

    static inline int CompareUnsigned(int64_t x, int64_t y)
    {
        return Compare(x + LLONG_MIN, y + LLONG_MIN);
    }

    static inline int Compare(int64_t x, int64_t y)
    {
        return (x < y) ? -1 : ((x == y)) ? 0 : 1;
    }

    static inline Decimal128 UnscaledDecimal(int64_t unscaledValue)
    {
        Decimal128 decimal128;
        if (unscaledValue < 0) {
            decimal128.SetValue(omniruntime::type::SIGN_LONG_MASK, -unscaledValue);
        } else {
            decimal128.SetValue(0, unscaledValue);
        }
        return decimal128;
    }

    static inline void RoundUp(Decimal128 &dividend, Decimal128 &divisor, Decimal128 &result, Decimal128 &remainder)
    {
        if (remainder.Abs() * 2 >= divisor.Abs()) {
            result += (dividend.Sign() ^ divisor.Sign()) + 1;
            return;
        }
    }
};
}
}

#endif // OMNI_RUNTIME_DECIMAL_OPERATIONS_H
