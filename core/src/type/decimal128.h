/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#ifndef OMNI_RUNTIME_DECIMAL128_H
#define OMNI_RUNTIME_DECIMAL128_H

#include <climits>
#include <cstdint>
#include <iostream>
#include <array>
#include <string_view>
#include "decimal_base.h"
#include "width_integer.h"


namespace omniruntime {
namespace type {

// The highest bit of Decimal128 is sign flag.
class Decimal128 : public BasicDecimal {
public:
    constexpr Decimal128() : Decimal128(0) {};

    constexpr Decimal128(const Decimal128 &rhs) = default;

    constexpr explicit Decimal128(const std::string_view &s): lowBits(0), highBits(0)
    {
        bool isNegative = s[0] == '-';
        int128_t value = 0;
        for (char i : s) {
            if (isdigit(i)) {
                value *= 10;
                value += i - '0';
            }
        }
        if (isNegative) {
            value = -value;
        }
        highBits = static_cast<int64_t>(value >> (CHAR_BIT * sizeof(int64_t)));
        lowBits = static_cast<uint64_t>(value);
    }

    constexpr explicit Decimal128(const char *input): Decimal128(std::string_view(input))
    {}

    constexpr explicit Decimal128(int128_t value): Decimal128(
        static_cast<int64_t>(value >> (CHAR_BIT * sizeof(int64_t))), static_cast<uint64_t>(value))
    {}

    constexpr Decimal128(int64_t highBits, uint64_t lowBits): lowBits(lowBits), highBits(highBits)
    {}

    constexpr explicit Decimal128(int64_t unscaledValue) : Decimal128(static_cast<int128_t>(unscaledValue))
    {}

    template<typename T,
        typename = typename std::enable_if<std::is_integral<T>::value && (sizeof(T) <= sizeof(uint64_t)), T>::type>
    constexpr Decimal128(T value): lowBits(static_cast<int64_t>(value)), highBits(value < 0 ? -1 : 0)
    {}

    bool operator==(const Decimal128 &right) const;

    bool operator!=(const Decimal128 &right) const;

    bool operator<(const Decimal128 &right) const;

    bool operator>(const Decimal128 &right) const;

    bool operator<=(const Decimal128 &right) const;

    bool operator>=(const Decimal128 &right) const;

    Decimal128 &operator=(const Decimal128 &rhs) = default;

    constexpr int128_t ToInt128() const
    {
        return static_cast<int128_t>(highBits) << SIGN_LONG_BYTE | lowBits;
    }

    constexpr int64_t HighBits() const
    {
        return highBits;
    }

    constexpr uint64_t LowBits() const
    {
        return lowBits;
    }

    void SetValue(int64_t highBitsField, uint64_t lowBitsField)
    {
        this->highBits = highBitsField;
        this->lowBits = lowBitsField;
    }

    void SetValue(int128_t value)
    {
        *reinterpret_cast<int128_t*>(this) = value;
    }

    bool IsZero() const
    {
        return lowBits == 0 && highBits == 0;
    }

    // this function is for template
    int32_t Compare(const Decimal128 &right) const
    {
        int128_t vLeft = ToInt128();
        int128_t vRight = right.ToInt128();
        if (vLeft == vRight) {
            return 0;
        }
        if (vLeft < vRight) {
            return -1;
        }
        return 1;
    }

    std::string ToString() const
    {
        std::string s;
        int128_t decimal =  this->ToInt128();
        bool negative = false;
        if (decimal < 0) {
            negative = true;
            decimal = -decimal;
        }
        
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
    static constexpr int32_t SIGN_LONG_BYTE = 64;

private:
    uint64_t lowBits;
    int64_t highBits;
};

std::ostream &operator<<(std::ostream &os, const Decimal128 &decimal128);
}
}

#endif // OMNI_RUNTIME_DECIMAL128_H
