/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#ifndef OMNI_RUNTIME_DECIMAL128_H
#define OMNI_RUNTIME_DECIMAL128_H

#include <cstdint>
#include <iostream>
#include <array>
#include <cstring>

#include "decimal_base.h"

namespace omniruntime {
namespace type {
// The highest bit of Decimal128 is sign flag.
class Decimal128 : public BasicDecimal {
public:
    Decimal128(int64_t highBits, uint64_t lowBits);

    Decimal128(__int128_t value);

    Decimal128(const std::string& s);

    Decimal128() : Decimal128(0, 0) {}

    Decimal128(const Decimal128 &rhs) = default;

    explicit Decimal128(int64_t unscaledValue);

    Decimal128 &operator = (const Decimal128 &rhs) = default;

    // / \brief Convert any integer value into a Decimal128.
    template <typename T,
        typename = typename std::enable_if<std::is_integral<T>::value && (sizeof(T) <= sizeof(uint64_t)), T>::type>
    constexpr Decimal128(T value) noexcept : Decimal128((value >= T { 0 }) ? 0 : -1, static_cast<uint64_t>(value))
    { // NOLINT
    }

    ~Decimal128() {}

    bool operator == (const Decimal128 &right) const;

    bool operator != (const Decimal128 &right) const;

    bool operator < (const Decimal128 &right) const;

    bool operator > (const Decimal128 &right) const;

    bool operator <= (const Decimal128 &right) const;

    bool operator >= (const Decimal128 &right) const;

    int64_t HighBits() const
    {
        return highBits;
    }

    uint64_t LowBits() const
    {
        return lowBits;
    }

    void SetValue(int64_t highBitsField, uint64_t lowBitsField)
    {
        this->highBits = highBitsField;
        this->lowBits = lowBitsField;
    }

    int64_t Sign() const
    {
        return 1 | (highBits >> 63);
    }

    static int64_t Absolute(int64_t bits)
    {
        return bits & ~SIGN_LONG_MASK;
    }

    // this function is for template
    int32_t Compare(const Decimal128 &right) const
    {
        bool isLeftNegative = highBits < 0;
        bool isRightNegative = right.HighBits() < 0;
        if (isLeftNegative != isRightNegative) {
            return isLeftNegative ? -1 : 1;
        } else {
            int32_t absoluteComparison;
            int64_t leftHigh = Absolute(highBits);
            uint64_t leftLow = lowBits;
            int64_t rightHigh = Absolute(right.HighBits());
            uint64_t rightLow = right.LowBits();
            if (leftHigh != rightHigh) {
                absoluteComparison = leftHigh > rightHigh ? 1 : -1;
            } else {
                absoluteComparison = leftLow > rightLow ? 1 : leftLow == rightLow ? 0 : -1;
            }
            return isLeftNegative ? -absoluteComparison : absoluteComparison;
        }
    }

    std::string ToString()
    {
        std::string s;
        bool negative = false;
        if (highBits < 0) {
            negative = true;
        }
        __int128_t decimal = highBits < 0 ? highBits ^ (1L << 63) : highBits;
        decimal = decimal << 64;
        decimal = decimal + lowBits;
        while (decimal > 9) {
            s.insert(0, std::to_string(static_cast<int>(decimal % 10)));
            decimal = decimal / 10;
        }
        s.insert(0, std::to_string(static_cast<int>(decimal)));
        if (negative) {
            s.insert(0, "-");
        }
        return s;
    }

    __int128_t ToInt128() const
    {
        __int128_t decimal = highBits < 0 ? highBits ^ (1L << 63) : highBits;
        decimal = decimal << 64;
        decimal = decimal + lowBits;
        return highBits < 0 ? -decimal : decimal;
    }

    static constexpr int64_t SIGN_LONG_MASK = 1LL << 63;
    static constexpr int64_t SIGN_INT_MASK = 1 << 31;
    static constexpr uint32_t INT_TO_UNSIGNED_LONG_MASK = 0xFFFF'FFFF;
    static constexpr int32_t MAX_LONG_PRECISION = 38;
    static constexpr int32_t MAX_SHORT_PRECISION = 18;
    static constexpr int32_t BYTES_OF_LONG = 8;
    static constexpr uint64_t LOW_64_BITS = 0xFFFF'FFFF'FFFF'FFFF;
    static constexpr uint32_t LOW_32_BITS = 0xFFFF'FFFF;
    static constexpr int32_t MAX_POWER_OF_FIVE_INT = 13;
    static constexpr int32_t MAX_POWER_OF_FIVE_LONG = 27;

private:
    uint64_t lowBits;
    int64_t highBits;
};

std::ostream &operator << (std::ostream &os, const Decimal128 &decimal128);
}
}

#endif // OMNI_RUNTIME_DECIMAL128_H
