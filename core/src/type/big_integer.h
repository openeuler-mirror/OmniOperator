/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 * @Description: big integer implementations
 */

#ifndef OMNI_RUNTIME_BIG_INTEGER_H
#define OMNI_RUNTIME_BIG_INTEGER_H

#include <cstdlib>
#include <cstring>
#include <iostream>
#include "util/omni_exception.h"

#define DOUBLE_CONVERSION_UINT64_2PART_C(a, b) (((static_cast<uint64_t>(a) << 32) + 0x##b##u))

namespace omniruntime::type {
class BigInteger {
public:
    using LimbType = uint32_t;
    using DoubleLimbType = uint64_t;
    // 3584 = 128 * 28. We can represent 2^3584 > 10^1000 accurately.
    // This BigInteger can encode much bigger numbers, since it contains an
    // exponent.
    static constexpr int MAX_SIGNIFICANT_BITS = 3584;

    BigInteger() : usedData_(0), exponent_(0)
    {}

    explicit BigInteger(uint16_t value) : usedData_(0), exponent_(0)
    {
        if (value > 0) {
            RawBigit(0) = value;
            usedData_ = 1;
        }
    }

    explicit BigInteger(uint64_t value) : usedData_(0), exponent_(0)
    {
        for (int i = 0; value > 0; ++i) {
            RawBigit(i) = value & DATA_MASK;
            value >>= DATA_SIZE;
            ++usedData_;
        }
    }

    BigInteger(const BigInteger &other)
    {
        exponent_ = other.exponent_;
        for (int i = 0; i < other.usedData_; ++i) {
            RawBigit(i) = other.RawBigit(i);
        }
        usedData_ = other.usedData_;
    }

    BigInteger(const std::string_view value);

    BigInteger(uint16_t base, const int powerExponent);

    void AssignUInt16(const uint16_t value);

    void AssignUInt64(uint64_t value);

    void AddUInt64(const uint64_t operand);

    void AddBigInteger(const BigInteger &other);

    // Precondition: this >= other.
    void SubtractBigInteger(const BigInteger &other);

    void Square();

    void ShiftLeft(const int32_t shiftAmount);

    void MultiplyByUInt32(const uint32_t factor);

    void MultiplyByUInt64(const uint64_t factor);

    void MultiplyByPowerOfTen(const int exponent);

    void Times10()
    {
        return MultiplyByUInt32(10);
    }

    // In the worst case this function is in O(this/other).
    uint16_t DivideModuloIntBigInteger(const BigInteger &other);

    std::string ToHexString() const;

    // Returns
    //  -1 if a < b,
    //   0 if a == b, and
    //  +1 if a > b.
    static int Compare(const BigInteger &a, const BigInteger &b);

    static bool Equal(const BigInteger &a, const BigInteger &b)
    {
        return Compare(a, b) == 0;
    }

    static bool LessEqual(const BigInteger &a, const BigInteger &b)
    {
        return Compare(a, b) <= 0;
    }

    static bool Less(const BigInteger &a, const BigInteger &b)
    {
        return Compare(a, b) < 0;
    }

    // Returns Compare(a + b, c).
    static int PlusCompare(const BigInteger &a, const BigInteger &b, const BigInteger &c);

private:
    static constexpr int32_t TYPE_SIZE = sizeof(LimbType) * 8;
    static constexpr int32_t DOUBLE_TYPE_SIZE = sizeof(DoubleLimbType) * 8;
    // With bigit size of 28 we loose some bits, but a double still fits easily
    // into two limb_types, and more importantly we can use the Comba multiplication.
    static constexpr int32_t DATA_SIZE = 28;
    static constexpr int32_t SQUARE_DATA_SIZE = 1 << (2 * (TYPE_SIZE - DATA_SIZE));
    static constexpr LimbType DATA_MASK = (1 << DATA_SIZE) - 1;
    // Every instance allocates kBigitLength limb_types on the stack. BigIntegers cannot
    // grow. There are no checks if the stack-allocated space is sufficient.
    static constexpr int32_t DATA_CAPACITY = MAX_SIGNIFICANT_BITS / DATA_SIZE;

    static void EnsureCapacity(const int32_t size)
    {
        if (size > DATA_CAPACITY) {
            throw omniruntime::exception::OmniException("Runtime Error", "BigInteger is out of range");
        }
    }

    void Align(const BigInteger &other);

    void Clamp();

    bool IsClamped() const
    {
        return usedData_ == 0 || RawBigit(usedData_ - 1) != 0;
    }

    void Zero()
    {
        usedData_ = 0;
        exponent_ = 0;
    }

    // Requires this to have enough capacity (no tests done).
    // Updates u_ if necessary.
    // shiftAmount must be < DATA_SIZE.
    void BigitsShiftLeft(const int shiftAmount);

    // BigitLength includes the "hidden" bigits encoded in the exponent.
    int BigIntegerLength() const
    {
        return usedData_ + exponent_;
    }

    LimbType &RawBigit(const int index);

    const LimbType &RawBigit(const int index) const;

    LimbType BigitOrZero(const int index) const;

    void SubtractTimes(const BigInteger &other, const int factor);

    // The BigInteger's value is value(dataBuffer_) * 2^(exponent_ * DATA_SIZE),
    // where the value of the buffer consists of the lower DATA_SIZE bits of
    // the first usedData_ limb_types in dataBuffer_, first limb_type has lowest
    // significant bits.
    int16_t usedData_ = 0;
    int16_t exponent_ = 0;
    LimbType dataBuffer_[DATA_CAPACITY] = {0};
};
}

#endif // OMNI_RUNTIME_BIG_INTEGER_H
