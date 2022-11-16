/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#ifndef OMNI_RUNTIME_DECIMAL128_H
#define OMNI_RUNTIME_DECIMAL128_H

#include <cstdint>
#include <iostream>
#include <array>
#include <cstring>
#include <boost/multiprecision/cpp_int.hpp>
#include "decimal_base.h"

namespace omniruntime {
namespace type {
using namespace boost::multiprecision;

// The highest bit of Decimal128 is sign flag.
class Decimal128 : public BasicDecimal {
public:
    Decimal128(int64_t highBits, uint64_t lowBits);

    explicit Decimal128(__int128_t value);

    explicit Decimal128(const std::string &s);

    explicit Decimal128(const char *s);

    Decimal128() : Decimal128(0, 0)
    {}

    Decimal128(const Decimal128 &rhs) = default;

    Decimal128 &operator=(const Decimal128 &rhs) = default;

    explicit Decimal128(int64_t unscaledValue);

    template<typename T,
        typename = typename std::enable_if<std::is_integral<T>::value && (sizeof(T) <= sizeof(uint64_t)), T>::type>
    Decimal128(T value):highBits(value < 0 ? SIGN_LONG_MASK : 0), lowBits(value < 0 ? -value : value)
    {}

    ~Decimal128()
    {}

    bool operator==(const Decimal128 &right) const;

    bool operator!=(const Decimal128 &right) const;

    bool operator<(const Decimal128 &right) const;

    bool operator>(const Decimal128 &right) const;

    bool operator<=(const Decimal128 &right) const;

    bool operator>=(const Decimal128 &right) const;

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

    __int128_t ToInt128() const
    {
        bool isNegative = (highBits < 0);
        __int128_t value = isNegative ? highBits ^ (1L << 63) : highBits;
        value = (value << 64) + lowBits;
        if (isNegative) {
            value = -value;
        }
        return value;
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

    static constexpr int64_t SIGN_LONG_MASK = 1LL << 63;
    static constexpr int32_t MAX_LONG_PRECISION = 38;

private:
    uint64_t lowBits;
    int64_t highBits;
};

std::ostream &operator<<(std::ostream &os, const Decimal128 &decimal128);
}
}

#endif // OMNI_RUNTIME_DECIMAL128_H
