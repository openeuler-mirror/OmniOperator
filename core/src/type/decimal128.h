/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#ifndef OMNI_RUNTIME_DECIMAL128_H
#define OMNI_RUNTIME_DECIMAL128_H

#include <cstdint>
#include <iostream>

#include "decimal_base.h"

namespace omniruntime {
namespace type {
enum class OpStatus {
    SUCCESS = 0,
    OP_OVERFLOW = 1,
    DIVIDE_BY_ZERO = 2,
    FAIL = 3
};

class Decimal128 : public BasicDecimal {
public:
    Decimal128(int64_t highBits, uint64_t lowBits);

    Decimal128() : Decimal128(0, 0) {}

    Decimal128(const Decimal128 &rhs) = default;

    Decimal128 &operator = (const Decimal128 &rhs) = default;

    // / \brief Convert any integer value into a Decimal128.
    template <typename T,
        typename = typename std::enable_if<std::is_integral<T>::value && (sizeof(T) <= sizeof(uint64_t)), T>::type>
    constexpr Decimal128(T value) noexcept : Decimal128((value >= T { 0 }) ? 0 : -1, static_cast<uint64_t>(value))
    { // NOLINT
    }

    ~Decimal128() {}

    Decimal128 &operator += (const Decimal128 &right);

    Decimal128 &operator -= (const Decimal128 &right);

    Decimal128 &operator *= (const Decimal128 &right);

    Decimal128 &operator /= (const Decimal128 &right);

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

    int64_t IsNegative() const
    {
        return highBits < 0;
    }

    Decimal128 &Negate();

    Decimal128 &Abs();

    static Decimal128 &Abs(const Decimal128 &decimal);

    OpStatus Divide(const Decimal128 &divisor, Decimal128 &result, Decimal128 &remainder) const;

    Decimal128 &Rescale(int32_t delta);

    static constexpr int32_t BYTE_WIDTH = 16;
    static constexpr int32_t BIT_WIDTH = 128;
    static constexpr int32_t LOW_BITS_WIDTH = 64;
    static constexpr int64_t DIVISION_ARRAY_LENGTH_FOUR = 4;
    static constexpr int64_t DIVISION_ARRAY_LENGTH_THREE = 3;
    static constexpr int64_t DIVISION_ARRAY_LENGTH_TWO = 2;

private:
    uint64_t lowBits;
    int64_t highBits;
    static const Decimal128 SCALE_MULTIPLIERS[];
};

Decimal128 operator + (const Decimal128 &left, const Decimal128 &right);

Decimal128 operator - (const Decimal128 &left, const Decimal128 &right);

Decimal128 operator*(const Decimal128 &left, const Decimal128 &right);

Decimal128 operator / (const Decimal128 &left, const Decimal128 &right);

Decimal128 operator % (const Decimal128 &left, const Decimal128 &right);

std::ostream &operator << (std::ostream &os, const Decimal128 &decimal128);
}
}

#endif // OMNI_RUNTIME_DECIMAL128_H
