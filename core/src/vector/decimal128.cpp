/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#include "decimal128.h"

#include <limits>
#include <array>
#include "op_util_internal.h"
#include "../util/compiler_util.h"

namespace omniruntime {
namespace vec {
static constexpr int32_t INT_BIT_WIDTH = 32;
static constexpr int32_t MAX_PRECISION = 38;
const Decimal128 Decimal128::SCALE_MULTIPLIERS[] = {
    Decimal128(1LL),
    Decimal128(10LL),
    Decimal128(100LL),
    Decimal128(1000LL),
    Decimal128(10000LL),
    Decimal128(100000LL),
    Decimal128(1000000LL),
    Decimal128(10000000LL),
    Decimal128(100000000LL),
    Decimal128(1000000000LL),
    Decimal128(10000000000LL),
    Decimal128(100000000000LL),
    Decimal128(1000000000000LL),
    Decimal128(10000000000000LL),
    Decimal128(100000000000000LL),
    Decimal128(1000000000000000LL),
    Decimal128(10000000000000000LL),
    Decimal128(100000000000000000LL),
    Decimal128(1000000000000000000LL),
    Decimal128(0LL, 10000000000000000000ULL),
    Decimal128(5LL, 7766279631452241920ULL),
    Decimal128(54LL, 3875820019684212736ULL),
    Decimal128(542LL, 1864712049423024128ULL),
    Decimal128(5421LL, 200376420520689664ULL),
    Decimal128(54210LL, 2003764205206896640ULL),
    Decimal128(542101LL, 1590897978359414784ULL),
    Decimal128(5421010LL, 15908979783594147840ULL),
    Decimal128(54210108LL, 11515845246265065472ULL),
    Decimal128(542101086LL, 4477988020393345024ULL),
    Decimal128(5421010862LL, 7886392056514347008ULL),
    Decimal128(54210108624LL, 5076944270305263616ULL),
    Decimal128(542101086242LL, 13875954555633532928ULL),
    Decimal128(5421010862427LL, 9632337040368467968ULL),
    Decimal128(54210108624275LL, 4089650035136921600ULL),
    Decimal128(542101086242752LL, 4003012203950112768ULL),
    Decimal128(5421010862427522LL, 3136633892082024448ULL),
    Decimal128(54210108624275221LL, 12919594847110692864ULL),
    Decimal128(542101086242752217LL, 68739955140067328ULL),
    Decimal128(5421010862427522170LL, 687399551400673280ULL)
};

Decimal128::Decimal128(int64_t highBits, uint64_t lowBits) : highBits(highBits), lowBits(lowBits) {}

Decimal128 &Decimal128::operator += (const Decimal128 &right)
{
    const uint64_t sum = lowBits + right.lowBits;
    highBits = SafeSignedAdd<int64_t>(highBits, right.highBits);
    if (sum < lowBits) {
        highBits = SafeSignedAdd<int64_t>(highBits, 1);
    }
    lowBits = sum;
    return *this;
}

Decimal128 &Decimal128::operator -= (const Decimal128 &right)
{
    const uint64_t diff = lowBits - right.lowBits;
    highBits -= right.highBits;
    if (diff > lowBits) {
        --highBits;
    }
    lowBits = diff;
    return *this;
}

Decimal128 &Decimal128::operator *= (const Decimal128 &right)
{
    const bool negate = Sign() != right.Sign();
    Decimal128 x = Abs(*this);
    Decimal128 y = Abs(right);
    __uint128_t r = (static_cast<__uint128_t>(x.highBits) << LOW_BITS_WIDTH) | x.lowBits;
    r *= (static_cast<__uint128_t>(y.highBits) << LOW_BITS_WIDTH) | y.lowBits;

    highBits = r >> LOW_BITS_WIDTH;
    lowBits = r & 0xFFFFFFFFFFFFFFFF;
    if (negate) {
        Negate();
    }
    return *this;
}

Decimal128 &Decimal128::operator /= (const Decimal128 &right)
{
    Decimal128 remainder;
    Divide(right, *this, remainder);
    return *this;
}

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
    return highBits < right.highBits || (highBits == right.highBits && lowBits < right.lowBits);
}

bool Decimal128::operator <= (const Decimal128 &right) const
{
    return !operator > (right);
}

bool Decimal128::operator > (const Decimal128 &right) const
{
    return !operator < (right) && !operator == (right);
}

bool Decimal128::operator >= (const Decimal128 &right) const
{
    return !operator < (right);
}

Decimal128 &Decimal128::Negate()
{
    lowBits = ~lowBits + 1;
    highBits = ~highBits;
    if (lowBits == 0) {
        highBits = SafeSignedAdd<int64_t>(highBits, 1);
    }
    return *this;
}

Decimal128 &Decimal128::Abs()
{
    return (*this < Decimal128(0, 0)) ? Negate() : *this;
}

Decimal128 &Decimal128::Abs(const Decimal128 &decimal)
{
    return Decimal128(decimal).Abs();
}

// / Expands the given value into a big endian array of ints so that we can work on
// / it. The array will be converted to an absolute value and the wasNegative
// / flag will be set appropriately. The array will remove leading zeros from
// / the value.
// / \param array a big endian array of length 4 to set with the value
// / \param wasNegative a flag for whether the value was original negative
// / \result the output length of the array
static int64_t FillInArray(const Decimal128 &value, uint32_t *array, uint32_t arraySize, bool &wasNegative)
{
    Decimal128 absValue = Decimal128::Abs(value);
    wasNegative = value.HighBits() < 0;
    auto high = static_cast<uint64_t>(absValue.HighBits());
    auto low = absValue.LowBits();

    // FillInArray(std::array<uint64_t, N>& value_array, uint32_t* result_array) is not
    // called here as the following code has better performance, to avoid regression on
    // Decimal128 Division.
    if (high != 0) {
        if (high > std::numeric_limits<uint32_t>::max()) {
            array[0] = static_cast<uint32_t>(high >> INT_BIT_WIDTH);
            array[1] = static_cast<uint32_t>(high);
            array[2] = static_cast<uint32_t>(low >> INT_BIT_WIDTH);
            array[3] = static_cast<uint32_t>(low);
            return 4;
        }

        array[0] = static_cast<uint32_t>(high);
        array[1] = static_cast<uint32_t>(low >> INT_BIT_WIDTH);
        array[2] = static_cast<uint32_t>(low);
        return 3;
    }

    if (low > std::numeric_limits<uint32_t>::max()) {
        array[0] = static_cast<uint32_t>(low >> INT_BIT_WIDTH);
        array[1] = static_cast<uint32_t>(low);
        return 2;
    }

    if (low == 0) {
        return 0;
    }

    array[0] = static_cast<uint32_t>(low);
    return 1;
}

// / Shift the number in the array left by bits positions.
// / \param array the number to shift, must have length elements
// / \param length the number of entries in the array
// / \param bits the number of bits to shift (0 <= bits < INT_BIT_WIDTH)
static void ShiftArrayLeft(uint32_t *array, int64_t length, int64_t bits)
{
    if (length > 0 && bits != 0) {
        for (int64_t i = 0; i < length - 1; ++i) {
            array[i] = (array[i] << bits) | (array[i + 1] >> (INT_BIT_WIDTH - bits));
        }
        array[length - 1] <<= bits;
    }
}

// / Shift the number in the array right by bits positions.
// / \param array the number to shift, must have length elements
// / \param length the number of entries in the array
// / \param bits the number of bits to shift (0 <= bits < INT_BIT_WIDTH)
static inline void ShiftArrayRight(uint32_t *array, int64_t length, int64_t bits)
{
    if (length > 0 && bits != 0) {
        for (int64_t i = length - 1; i > 0; --i) {
            array[i] = (array[i] >> bits) | (array[i - 1] << (INT_BIT_WIDTH - bits));
        }
        array[0] >>= bits;
    }
}

// Fix the signs of the result and remainder at the end of the division based on
// the signs of the dividend and divisor.
template <class DecimalClass>
static inline void FixDivisionSigns(DecimalClass &result, DecimalClass &remainder, bool dividendWasNegative,
    bool divisorWasNegative)
{
    if (dividendWasNegative != divisorWasNegative) {
        result.Negate();
    }

    if (dividendWasNegative) {
        remainder.Negate();
    }
}

// Build a little endian array of uint64_t from a big endian array of uint32_t.
template <size_t N>
static OpStatus BuildFromArray(std::array<uint64_t, N> &resultArray, const uint32_t *array, int64_t length)
{
    for (int64_t i = length - 2 * N - 1; i >= 0; i--) {
        if (array[i] != 0) {
            return OpStatus::OP_OVERFLOW;
        }
    }
    int64_t nextIndex = length - 1;
    size_t i = 0;
    for (; i < N && nextIndex >= 0; i++) {
        uint64_t lowerBits = array[nextIndex--];
        if (nextIndex < 0) {
            resultArray[i] = lowerBits;
        } else {
            resultArray[i] = ((static_cast<uint64_t>(array[nextIndex]) << INT_BIT_WIDTH) + lowerBits);
            nextIndex--;
        }
    }
    for (; i < N; i++) {
        resultArray[i] = 0;
    }
    return OpStatus::SUCCESS;
}

// Build a Decimal128 from a big endian array of uint32_t.
static OpStatus BuildFromArray(Decimal128 &value, const uint32_t *array, int64_t length)
{
    std::array<uint64_t, 2> resultArray;
    auto status = BuildFromArray(resultArray, array, length);
    if (status != OpStatus::SUCCESS) {
        return status;
    }
    value = { static_cast<int64_t>(resultArray[1]), resultArray[0] };
    return OpStatus::SUCCESS;
}

// Do a division where the divisor fits into a single 32 bit value.
template <class DecimalClass>
static OpStatus SingleDivide(const uint32_t *dividend, int64_t dividendLength, uint32_t divisor,
    DecimalClass &remainder, DecimalClass &result)
{
    if (divisor == 0) {
        return OpStatus::DIVIDE_BY_ZERO;
    }
    uint64_t r = 0;
    // The length of result will be not greater than dividendLength,
    // so here we just need a dividendLength length of result array.
    uint32_t resultArray[dividendLength];
    for (int64_t i = 0; i < dividendLength; i++) {
        r <<= INT_BIT_WIDTH;
        r += dividend[i];
        resultArray[i] = static_cast<uint32_t>(r / divisor);
        r %= divisor;
    }
    auto status = BuildFromArray(result, resultArray, dividendLength);
    if (status != OpStatus::SUCCESS) {
        return status;
    }

    remainder = static_cast<int64_t>(r);
    return OpStatus::SUCCESS;
}

static uint32_t GuessResult(uint32_t *dividendArray, uint32_t *divisorArray, uint32_t divisorLength, int64_t pos)
{
    // Guess the next digit. At worst it is two too large
    uint32_t guess = std::numeric_limits<uint32_t>::max();
    const auto highDividend = ((static_cast<uint64_t>(dividendArray[pos])) << INT_BIT_WIDTH) | dividendArray[pos + 1];
    if (dividendArray[pos] != divisorArray[0]) {
        guess = static_cast<uint32_t>(highDividend / divisorArray[0]);
    }

    // catch all of the cases where guess is two too large and most of the
    // cases where it is one too large
    auto rhat = static_cast<uint32_t>(highDividend - guess * static_cast<uint64_t>(divisorArray[0]));
    while (static_cast<uint64_t>(divisorArray[1]) * guess >
        (static_cast<uint64_t>(rhat) << INT_BIT_WIDTH) + dividendArray[pos + 2]) {
        --guess;
        rhat += divisorArray[0];
        if (static_cast<uint64_t>(rhat) < divisorArray[0]) {
            break;
        }
    }

    // subtract off the guess * divisor from the dividend
    uint64_t mult = 0;
    for (int64_t i = divisorLength - 1; i >= 0; --i) {
        mult += static_cast<uint64_t>(guess) * divisorArray[i];
        uint32_t prev = dividendArray[pos + i + 1];
        dividendArray[pos + i + 1] -= static_cast<uint32_t>(mult);
        mult >>= INT_BIT_WIDTH;
        if (dividendArray[pos + i + 1] > prev) {
            ++mult;
        }
    }
    uint32_t prev = dividendArray[pos];
    dividendArray[pos] -= static_cast<uint32_t>(mult);

    // if guess was too big, we add back divisor
    if (dividendArray[pos] > prev) {
        --guess;
        uint32_t carry = 0;
        for (int64_t i = divisorLength - 1; i >= 0; --i) {
            const auto sum = static_cast<uint64_t>(divisorArray[i]) + dividendArray[pos + i + 1] + carry;
            dividendArray[pos + i + 1] = static_cast<uint32_t>(sum);
            carry = static_cast<uint32_t>(sum >> INT_BIT_WIDTH);
        }
        dividendArray[pos] += carry;
    }
    return guess;
}

template <class DecimalClass>
static OpStatus ComplexDivide(uint32_t *dividendArray, int64_t dividendLength, uint32_t *divisorArray,
    uint32_t divisorLength, DecimalClass &remainder, DecimalClass &result)
{
    int64_t resultLength = dividendLength - divisorLength;
    uint32_t resultArray[resultLength];

    // Normalize by shifting both by a multiple of 2 so that
    // the digit guessing is better. The requirement is that
    // divisorArray[0] is greater than 2**31.
    int64_t normalizeBits = 64;
    if (divisorArray[0] != 0) {
        normalizeBits = static_cast<int32_t>(__builtin_clzll(divisorArray[0]));
    }
    ShiftArrayLeft(divisorArray, divisorLength, normalizeBits);
    ShiftArrayLeft(dividendArray, dividendLength, normalizeBits);

    // compute each digit in the result
    for (int64_t i = 0; i < resultLength; ++i) {
        resultArray[i] = GuessResult(dividendArray, divisorArray, divisorLength, i);
    }

    // denormalize the remainder
    ShiftArrayRight(dividendArray, dividendLength, normalizeBits);

    // return result and remainder
    auto status = BuildFromArray(result, resultArray, resultLength);
    if (status != OpStatus::SUCCESS) {
        return status;
    }
    status = BuildFromArray(remainder, dividendArray, dividendLength);
    if (status != OpStatus::SUCCESS) {
        return status;
    }

    return OpStatus::SUCCESS;
}
// / \brief Do a decimal division with remainder.
template <class DecimalClass>
static OpStatus DecimalDivide(const DecimalClass &dividend, const DecimalClass &divisor, DecimalClass &result,
    DecimalClass &remainder)
{
    // Leave an extra zero before the dividend
    if (UNLIKELY(divisor == 0)) {
        return OpStatus::DIVIDE_BY_ZERO;
    }

    // Split the dividend and divisor into integer pieces so that we can work on them.
    constexpr int64_t decimalArrayLength = DecimalClass::BYTE_WIDTH / sizeof(uint32_t);
    uint32_t dividendArray[decimalArrayLength + 1];
    uint32_t divisorArray[decimalArrayLength];
    bool dividendWasNegative = 0;
    bool divisorWasNegative = 0;

    // Fill divisor array.
    int64_t divisorLength = FillInArray(divisor, divisorArray, decimalArrayLength, divisorWasNegative);
    // Leave an extra zero before the dividend
    if (UNLIKELY(divisorLength == 0)) {
        return OpStatus::DIVIDE_BY_ZERO;
    }

    // Fill dividend array.
    dividendArray[0] = 0;
    int64_t dividendLength = FillInArray(dividend, dividendArray + 1, decimalArrayLength, dividendWasNegative) + 1;
    // Case1: if divisor is greater than dividend, then result is ZERO and remainder is the dividend.
    if (dividendLength <= divisorLength) {
        remainder = dividend;
        result = 0;
        return OpStatus::SUCCESS;
    }

    OpStatus status;
    if (divisorLength == 1) {
        // Case2: if divisor is only 1 length, then call SingleDivide to handle.
        status = SingleDivide(dividendArray, dividendLength, divisorArray[0], remainder, result);
    } else {
        // Case3: handle complex cases.
        status = ComplexDivide(dividendArray, dividendLength, divisorArray, divisorLength, remainder, result);
    }
    FixDivisionSigns(result, remainder, dividendWasNegative, divisorWasNegative);
    return status;
}

OpStatus Decimal128::Divide(const Decimal128 &divisor, Decimal128 &result, Decimal128 &remainder) const
{
    return DecimalDivide(*this, divisor, result, remainder);
}

Decimal128 &Decimal128::Rescale(int32_t delta)
{
    if (delta == 0) {
        return *this;
    }

    int32_t multiplierIndex = std::abs(delta);
    if (multiplierIndex <= MAX_PRECISION) {
        Decimal128 reminder;
        if (delta < 0) {
            Divide(SCALE_MULTIPLIERS[multiplierIndex], *this, reminder);
            if (reminder != 0) {
                // TODO: data loss
            }
            return *this;
        }
        // TODO: may data overflow
        *this *= SCALE_MULTIPLIERS[multiplierIndex];
        return *this;
    }
    std::cerr << "multiplier index is more than 38, it is: " << multiplierIndex << std::endl;
    return *this;
}

Decimal128 operator + (const Decimal128 &left, const Decimal128 &right)
{
    Decimal128 result(left.HighBits(), left.LowBits());
    result += right;
    return result;
}

Decimal128 operator - (const Decimal128 &left, const Decimal128 &right)
{
    Decimal128 result(left.HighBits(), left.LowBits());
    result -= right;
    return result;
}

Decimal128 operator*(const Decimal128 &left, const Decimal128 &right)
{
    Decimal128 result(left.HighBits(), left.LowBits());
    result *= right;
    return result;
}

Decimal128 operator / (const Decimal128 &left, const Decimal128 &right)
{
    if (right == 0) {
        std::cerr << "right is zero" << std::endl;
        return left;
    }
    Decimal128 result(left.HighBits(), left.LowBits());
    result /= right;
    return result;
}

Decimal128 operator % (const Decimal128 &left, const Decimal128 &right)
{
    Decimal128 result, remainder;
    left.Divide(right, result, remainder);
    return remainder;
}

std::ostream& operator << (std::ostream &os, const Decimal128 &decimal128)
{
    os << std::hex << decimal128.HighBits() << decimal128.LowBits();
    return os;
}
}
}