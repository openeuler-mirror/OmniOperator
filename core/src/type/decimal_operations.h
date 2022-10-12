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
#include <cmath>
#include <huawei_secure_c/include/securec.h>
#include <boost/multiprecision/cpp_int.hpp>
#include "util/debug.h"
#include "util/omni_exception.h"
#include "decimal_base.h"
#include "decimal128.h"

namespace omniruntime {
namespace type {
using namespace exception;
using namespace boost::multiprecision;

enum class Op {
    ADD,
    SUBTRACT,
    MULTIPLY,
    DIVIDE,
    MOD,
};

enum class OpStatus {
    SUCCESS = 0,
    OP_OVERFLOW = 1,
    DIVIDE_BY_ZERO = 2,
    FAIL = 3
};

using DecimalAverageState = struct DecimalAverageState {
    int64_t count = 0;
    int64_t overflow = 0;
    uint64_t lowBits = 0;
    int64_t highBits = 0;
};

using DecimalSumState = struct DecimalSumState {
    int64_t overflow = 0;
    uint64_t lowBits = 0;
    int64_t highBits = 0;
};


using int128_t = __int128_t;
enum class RoundingMode {
    ROUND_UP,
    ROUND_FLOOR
};

static constexpr int MAX_PRECISION = 38;
static constexpr int MAX_SCALE = 38;
static constexpr int32_t MAX_DECIMAL64_DIGITS = 18;
static constexpr uint128_t TenOfScaleMultipliers[39] = {
    uint128_t(1LL),
    uint128_t(10LL),
    uint128_t(100LL),
    uint128_t(1000LL),
    uint128_t(10000LL),
    uint128_t(100000LL),
    uint128_t(1000000LL),
    uint128_t(10000000LL),
    uint128_t(100000000LL),
    uint128_t(1000000000LL),
    uint128_t(10000000000LL),
    uint128_t(100000000000LL),
    uint128_t(1000000000000LL),
    uint128_t(10000000000000LL),
    uint128_t(100000000000000LL),
    uint128_t(1000000000000000LL),
    uint128_t(10000000000000000LL),
    uint128_t(100000000000000000LL),
    uint128_t(1000000000000000000LL),
    uint128_t(__uint128_t(1000000000000000000LL) * 10LL),
    uint128_t(__uint128_t(1000000000000000000LL) * 100LL),
    uint128_t(__uint128_t(1000000000000000000LL) * 1000LL),
    uint128_t(__uint128_t(1000000000000000000LL) * 10000LL),
    uint128_t(__uint128_t(1000000000000000000LL) * 100000LL),
    uint128_t(__uint128_t(1000000000000000000LL) * 1000000LL),
    uint128_t(__uint128_t(1000000000000000000LL) * 10000000LL),
    uint128_t(__uint128_t(1000000000000000000LL) * 100000000LL),
    uint128_t(__uint128_t(1000000000000000000LL) * 1000000000LL),
    uint128_t(__uint128_t(1000000000000000000LL) * 10000000000LL),
    uint128_t(__uint128_t(1000000000000000000LL) * 100000000000LL),
    uint128_t(__uint128_t(1000000000000000000LL) * 1000000000000LL),
    uint128_t(__uint128_t(1000000000000000000LL) * 10000000000000LL),
    uint128_t(__uint128_t(1000000000000000000LL) * 100000000000000LL),
    uint128_t(__uint128_t(1000000000000000000LL) * 1000000000000000LL),
    uint128_t(__uint128_t(1000000000000000000LL) * 10000000000000000LL),
    uint128_t(__uint128_t(1000000000000000000LL) * 100000000000000000LL),
    uint128_t(__uint128_t(1000000000000000000LL) * 1000000000000000000LL),
    uint128_t(__uint128_t(1000000000000000000LL) * 1000000000000000000LL * 10),
    uint128_t(__uint128_t(1000000000000000000LL) * 1000000000000000000LL * 100) };

static constexpr double DOUBLE_10_POW[] = {
    1.0e0, 1.0e1, 1.0e2, 1.0e3, 1.0e4, 1.0e5,
    1.0e6, 1.0e7, 1.0e8, 1.0e9, 1.0e10, 1.0e11,
    1.0e12, 1.0e13, 1.0e14, 1.0e15, 1.0e16, 1.0e17,
    1.0e18, 1.0e19, 1.0e20, 1.0e21, 1.0e22
};

inline OpStatus DecimalFromString(const std::string &s, int128_t &result, int32_t &scale, int32_t &precision)
{
    result = 0;
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
    if (s[offset] == '-') {
        isNeg = true;
        offset++;
    } else if (s[0] == '+') {
        offset++;
    }
    if (s[offset] == '0' && s[offset + 1] == '.') {
        offset += 2;
        isDot = true;
    }
    if (!isdigit(s[offset])) {
        return OpStatus::FAIL;
    }
    for (; offset < len; offset++) {
        if (isdigit(s[offset])) {
            precision++;
            if (precision > 38) {
                return OpStatus::OP_OVERFLOW;
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
            return OpStatus::FAIL;
        }
    }

    if (isExp) {
        for (; offset < len; offset++) {
            if (isdigit(s[offset])) {
                exponent *= 10;
                exponent += int(s[offset]) - 48;
                if (exponent + precision - scale > 38) {
                    return OpStatus::OP_OVERFLOW;
                }
            } else if (s[offset] == ' ') {
                isSpace = true;
                offset++;
                break;
            } else {
                return OpStatus::FAIL;
            }
        }
        if (exponent == 0) {
            return OpStatus::FAIL;
        }
    }

    if (isSpace) {
        for (; offset < len; offset++) {
            if (s[offset] != ' ') {
                return OpStatus::FAIL;
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
    return OpStatus::SUCCESS;
}

inline std::string ToStringWithScale(std::string inputString, int scale)
{
    std::string unscaledValueString = std::move(inputString);
    std::string resultBuilder;
    if (unscaledValueString[0] == '-') {
        resultBuilder.append("-");
        unscaledValueString = unscaledValueString.substr(1);
    }
    auto unscaledValueLength = unscaledValueString.length();
    if (unscaledValueLength <= scale) {
        resultBuilder.append("0");
    } else {
        resultBuilder.append(unscaledValueString.substr(0, unscaledValueLength - scale));
    }

    if (scale > 0) {
        resultBuilder.append(".");
        if (unscaledValueLength < scale) {
            auto subScaleLength = scale - unscaledValueLength;
            for (int i = 0; i < subScaleLength; ++i) {
                resultBuilder.append("0");
            }
            resultBuilder.append(unscaledValueString);
        } else {
            resultBuilder.append(unscaledValueString.substr(unscaledValueLength - scale));
        }
    }
    return resultBuilder;
}

inline int32_t GetResultScale(int32_t x, int32_t y, Op op)
{
    int32_t r;
    switch (op) {
        case Op::ADD:
        case Op::SUBTRACT:
            r = std::max(x, y);
            break;
        case Op::MULTIPLY:
            r = x + y;
            break;
        case Op::DIVIDE:
            r = std::max(6, x + y + 1);
            break;
        case Op::MOD:
            r = std::max(x, y);
            break;
    }
    return r;
}

// Decimal128Wrapper
class Decimal128Wrapper {
public:
    Decimal128Wrapper() : val(0), signum(0)
    {}

    Decimal128Wrapper(int64_t highBits, uint64_t lowBits)
    {
        if (highBits > 0) {
            signum = 1;
        } else if (highBits < 0) {
            signum = -1;
            highBits = highBits ^ SIGN_LONG_MASK;
        } else {
            if (lowBits == 0) {
                signum = 0;
            } else {
                signum = 1;
            }
        }
        val = static_cast<uint128_t>(highBits) << 64 | lowBits;
    }

    Decimal128Wrapper(Decimal128 value) : Decimal128Wrapper(value.HighBits(), value.LowBits())
    {}

    Decimal128Wrapper(const uint128_t &value)
    {
        if (value == 0) {
            signum = 0;
            val = 0;
            return;
        }
        if (value < (uint128_t(1) << 127)) {
            signum = 1;
            val = value;
        } else {
            signum = -1;
            val = (value ^ (uint128_t(1) << 127));
        }
    }

    Decimal128Wrapper(const __uint128_t &value) : Decimal128Wrapper(uint128_t(value))
    {}

    Decimal128Wrapper(const int128_t &value)
    {
        if (value == 0) {
            signum = 0;
            val = 0;
            return;
        }
        if (value > 0) {
            signum = 1;
            val = value.convert_to<uint128_t>();
        } else {
            signum = -1;
            val = (-value).convert_to<uint128_t>();
        }
    }

    Decimal128Wrapper(const char *s)
    {
        int32_t inputScale = 0;
        int32_t precision = 0;
        int128_t result = 0;
        overflow = DecimalFromString(s, result, inputScale, precision);
        if (result == 0) {
            signum = 0;
            val = 0;
            scale = inputScale;
            return;
        }
        if (result > 0) {
            signum = 1;
            val = result.convert_to<uint128_t>();
        } else {
            signum = -1;
            val = (-result).convert_to<uint128_t>();
        }
        scale = inputScale;
    }

    template<typename T>
    Decimal128Wrapper(T value)
    {
        if (value == 0) {
            val = 0;
            signum = 0;
            return;
        }
        if (value > 0) {
            val = value;
            signum = 1;
        } else {
            int128_t tmp = value;
            val = (-tmp).convert_to<uint128_t>();
            signum = -1;
        }
    }

    explicit Decimal128Wrapper(double value) : Decimal128Wrapper(boost::lexical_cast<std::string>(value).c_str())
    {}

    Decimal128Wrapper &operator=(const Decimal128Wrapper &rhs) = default;

    ~Decimal128Wrapper()
    {}

    Decimal128Wrapper &SetScale(int32_t inputScale)
    {
        scale = inputScale;
        return *this;
    }

    Decimal128Wrapper &operator+=(const Decimal128Wrapper &right)
    {
        if (signum == right.signum) {
            val = right.val + val;
            signum = right.signum;
            return *this;
        }
        if (right.val == val) {
            val = 0;
            signum = 0;
            return *this;
        }
        if (right.val > val) {
            val = right.val - val;
            signum = right.signum;
        } else {
            val = val - right.val;
        }
        return *this;
    }

    Decimal128Wrapper &operator-=(const Decimal128Wrapper &right)
    {
        Decimal128Wrapper copy(right);
        *this += copy.Negate();
        return *this;
    }

    Decimal128Wrapper &operator*=(const Decimal128Wrapper &right)
    {
        if (signum == 0 || right.signum == 0) {
            val = 0;
            signum = 0;
            return *this;
        }
        val = val * right.val;
        if (signum == right.signum) {
            signum = 1;
        } else {
            signum = -1;
        }
        return *this;
    }

    Decimal128Wrapper &operator/=(const Decimal128Wrapper &right)
    {
        if (right.signum == 0) {
            overflow = OpStatus::DIVIDE_BY_ZERO;
            return *this;
        }
        if (signum == 0) {
            val = 0;
            signum = 0;
            return *this;
        }
        val = val / right.val;
        if (signum == right.signum) {
            signum = 1;
        } else {
            signum = -1;
        }
        return *this;
    }

    Decimal128Wrapper &operator%=(const Decimal128Wrapper &right)
    {
        if (right.signum == 0) {
            overflow = OpStatus::DIVIDE_BY_ZERO;
            return *this;
        }
        if (signum == 0) {
            val = 0;
            signum = 0;
            return *this;
        }
        val = val % right.val;
        return *this;
    }

    Decimal128Wrapper Add(const Decimal128Wrapper &right)
    {
        if (overflow == OpStatus::OP_OVERFLOW) {
            return *this;
        }
        Decimal128Wrapper result;
        if (signum == right.signum) {
            checked_uint128_t x = val;
            checked_uint128_t y = right.val;
            checked_uint128_t r;
            try {
                add(r, x, y);
                if (r > DECIMAL128_MAX_VALUE) {
                    throw std::overflow_error("Add Decimal128 overflow");
                }
            } catch (std::overflow_error &e) {
                result.overflow = OpStatus::OP_OVERFLOW;
                return result;
            }

            result.val = r;
            result.signum = right.signum;
            return result;
        }
        if (right.val == val) {
            result.val = 0;
            result.signum = 0;
            return result;
        }
        if (right.val > val) {
            result.val = right.val - val;
            result.signum = right.signum;
        } else {
            result.val = val - right.val;
            result.signum = signum;
        }
        return result;
    }

    Decimal128Wrapper Subtract(const Decimal128Wrapper &right) const
    {
        if (overflow == OpStatus::OP_OVERFLOW) {
            return *this;
        }
        Decimal128Wrapper result = *this;
        Decimal128Wrapper copy = right;
        result = result.Add(copy.Negate());
        return result;
    }

    Decimal128Wrapper Multiply(const Decimal128Wrapper &right)
    {
        if (overflow == OpStatus::OP_OVERFLOW) {
            return *this;
        }
        Decimal128Wrapper result;
        if (signum == 0 || right.signum == 0) {
            return result;
        }
        checked_uint128_t x = val;
        checked_uint128_t y = right.val;
        checked_uint128_t r;
        try {
            multiply(r, x, y);
        } catch (std::overflow_error &e) {
            result.overflow = OpStatus::OP_OVERFLOW;
            return result;
        }
        result.val = r;
        if (signum == right.signum) {
            result.signum = 1;
        } else {
            result.signum = -1;
        }
        return result;
    }

    Decimal128Wrapper MultiplyRoundUp(const Decimal128Wrapper &right, int32_t rescaleFactor)
    {
        if (overflow == OpStatus::OP_OVERFLOW) {
            return *this;
        }
        Decimal128Wrapper result;
        if (signum == 0 || right.signum == 0) {
            return result;
        }
        uint128_t tenOfScale = TenOfScaleMultipliers[rescaleFactor];
        uint256_t x = val;
        uint256_t y = right.val;
        uint256_t r = x * y;
        uint256_t q = r % tenOfScale;
        r /= tenOfScale;
        if (r > TenOfScaleMultipliers[38] - 1) {
            result.overflow = OpStatus::OP_OVERFLOW;
            return result;
        }
        if (q >= (tenOfScale / 2) && tenOfScale != 1) {
            r += 1;
        }
        result.val = r.convert_to<uint128_t>();
        if (signum == right.signum) {
            result.signum = 1;
        } else {
            result.signum = -1;
        }
        return result;
    }

    Decimal128Wrapper Divide(const Decimal128Wrapper &right, int32_t rescaleFactor) const
    {
        if (overflow == OpStatus::OP_OVERFLOW) {
            return *this;
        }
        Decimal128Wrapper result;
        uint256_t dividend256 = ReScaleTo256Bits(rescaleFactor + scale);
        uint256_t divisor256 = right.val;
        uint256_t quotient256;
        uint256_t remainder256;
        if (divisor256 == 0) {
            result.overflow = OpStatus::DIVIDE_BY_ZERO;
            return result;
        }
        if (dividend256 == 0) {
            result.signum = 0;
            result.val = 0;
            return result;
        }
        divide_qr(dividend256, divisor256, quotient256, remainder256);
        if (remainder256 * 2 >= divisor256) {
            quotient256 += 1;
        }
        if (quotient256 > DECIMAL128_MAX_VALUE) {
            result.overflow = OpStatus::OP_OVERFLOW;
            return result;
        }
        result.val = quotient256.convert_to<uint128_t>();
        result.signum = (signum != right.signum) ? -1 : 1;
        return result;
    }

    Decimal128Wrapper Mod(const Decimal128Wrapper &right)
    {
        Decimal128Wrapper result;
        int32_t ScaleFactor = GetResultScale(scale, right.scale, Op::MOD);
        uint256_t dividend256 = ReScaleTo256Bits(ScaleFactor);
        uint256_t divisor256 = right.ReScaleTo256Bits(ScaleFactor);
        uint256_t remainder256;
        if (divisor256 == 0) {
            result.overflow = OpStatus::DIVIDE_BY_ZERO;
            return result;
        }
        if (dividend256 == 0) {
            result.val = 0;
            result.signum = 0;
            result.SetScale(ScaleFactor);
            return result;
        }
        remainder256 = dividend256 % divisor256;
        result.val = remainder256.convert_to<uint128_t>();
        result.signum = signum;
        result.SetScale(ScaleFactor);
        return result;
    }

    bool operator==(const Decimal128Wrapper &right) const
    {
        return signum == right.signum && val == right.val;
    }

    bool operator!=(const Decimal128Wrapper &right) const
    {
        return !operator==(right);
    }

    bool operator<(const Decimal128Wrapper &right) const
    {
        return Compare(right) == -1;
    }

    bool operator>(const Decimal128Wrapper &right) const
    {
        return Compare(right) == 1;
    }

    bool operator<=(const Decimal128Wrapper &right) const
    {
        return !operator>(right);
    }

    bool operator>=(const Decimal128Wrapper &right) const
    {
        return !operator<(right);
    }

    OpStatus ToInt(int32_t &res) const
    {
        Decimal128Wrapper result = this->Divide(Decimal128Wrapper(TenOfScaleMultipliers[scale]), 0);
        if (signum == 0) {
            res = 0;
            return OpStatus::SUCCESS;
        }
        if (signum > 0) {
            if (result > INT32_MAX) {
                return OpStatus::OP_OVERFLOW;
            }
            res = result.val.convert_to<int32_t>();
        } else {
            if (result > 1L + INT32_MAX) {
                return OpStatus::OP_OVERFLOW;
            }
            res = -result.val.convert_to<int32_t>();
        }
        return OpStatus::SUCCESS;
    }

    OpStatus ToLong(int64_t &res) const
    {
        Decimal128Wrapper result = this->Divide(Decimal128Wrapper(TenOfScaleMultipliers[scale]), 0);
        if (signum == 0) {
            res = 0;
            return OpStatus::SUCCESS;
        }
        if (signum > 0) {
            if (result > INT64_MAX) {
                return OpStatus::OP_OVERFLOW;
            }
            res = result.val.convert_to<int64_t>();
        } else {
            if (result > UNSIGNED_INT64_MIN) {
                return OpStatus::OP_OVERFLOW;
            }
            res = static_cast<int64_t>((-result.val.convert_to<int128_t>()));
        }
        return OpStatus::SUCCESS;
    }

    explicit operator int32_t() const
    {
        int32_t result;
        if (ToInt(result) != OpStatus::SUCCESS) {
            throw std::overflow_error("Overflow when Decimal128 cast to int");
        } else {
            return result;
        }
    }

    explicit operator int64_t() const
    {
        int64_t result;
        if (ToLong(result) != OpStatus::SUCCESS) {
            throw std::overflow_error("Overflow when Decimal128 cast to long");
        } else {
            return result;
        }
    }

    explicit operator double() const
    {
        return std::stod(ToString());
    }

    explicit operator uint64_t() const
    {
        if (signum == 0) {
            return 0;
        }
        if (signum > 0) {
            if (val > INT64_MAX) {
                throw std::overflow_error("Overflow when decimal128 cast to unsigned long");
            }
            return val.convert_to<uint64_t>();
        } else {
            if (val > UNSIGNED_INT64_MIN) {
                throw std::overflow_error("Overflow when decimal128 cast to unsigned long");
            }
            return -val.convert_to<uint64_t>();
        }
    }

    Decimal128Wrapper &ReScale(int32_t newScale, RoundingMode mode = RoundingMode::ROUND_UP)
    {
        switch (mode) {
            case RoundingMode::ROUND_UP:
                *this = ReScaleRoundUp(newScale);
                return *this;
            case RoundingMode::ROUND_FLOOR:
                *this = ReScaleRoundFloor(newScale);
                return *this;
        }
    }

    OpStatus IsOverflow(int32_t precision = 38) const
    {
        if (val < TenOfScaleMultipliers[precision]) {
            return overflow;
        }
        return OpStatus::OP_OVERFLOW;
    }

    int32_t GetSignum() const
    {
        return signum;
    }

    int32_t GetScale() const
    {
        return scale;
    }

    uint128_t GetValue() const
    {
        return val;
    }

    int64_t HighBits() const
    {
        __uint128_t t = val.convert_to<__uint128_t>();
        if (signum == -1) {
            return static_cast<int64_t>(t >> 64) ^ SIGN_LONG_MASK;
        }
        return static_cast<int64_t>(t >> 64);
    }

    uint64_t LowBits() const
    {
        return val.convert_to<uint64_t>();
    }

    void SetValue(int64_t highBitsField, uint64_t lowBitsField)
    {
        if (highBitsField > 0) {
            signum = 1;
        } else if (highBitsField < 0) {
            signum = -1;
            highBitsField = highBitsField ^ SIGN_LONG_MASK;
        } else {
            if (lowBitsField == 0) {
                signum = 0;
            } else {
                signum = 1;
            }
        }
        val = static_cast<uint128_t>(highBitsField) << 64 | lowBitsField;
    }

    void SetValue(uint128_t value)
    {
        val = value;
    }

    Decimal128Wrapper &Negate()
    {
        if (signum == 0) {
            return *this;
        }
        if (signum == 1) {
            signum = -1;
        } else {
            signum = 1;
        }
        return *this;
    }

    bool IsNegative()
    {
        if (signum == -1) {
            return true;
        }
        return false;
    }

    Decimal128Wrapper &Abs()
    {
        if (signum == -1) {
            signum = 1;
        }
        return *this;
    }

    static Decimal128Wrapper Negate(const Decimal128Wrapper &input)
    {
        Decimal128Wrapper result = input;
        if (result.signum == 0) {
            return result;
        }
        if (result.signum == 1) {
            result.signum = -1;
        } else {
            result.signum = 1;
        }
        return result;
    }

    // this function is for template
    int32_t Compare(const Decimal128Wrapper &right) const
    {
        if (signum > right.signum) {
            return 1;
        }
        if (signum < right.signum) {
            return -1;
        }
        int32_t newScale = GetResultScale(scale, right.scale, Op::SUBTRACT);
        Decimal128Wrapper x = *this;
        Decimal128Wrapper y = right;
        Decimal128Wrapper r = x.ReScale(newScale).Subtract(y.ReScale(newScale));
        return r.signum;
    }

    int128_t ToInt128() const
    {
        if (signum == 0) {
            return 0;
        }
        if (signum > 0) {
            return val.convert_to<int128_t>();
        } else {
            return -val.convert_to<int128_t>();
        }
    }

    std::string ToString() const
    {
        std::string s = ToStringUnscale();
        return ToStringWithScale(s, scale);
    }

    std::string ToStringUnscale() const
    {
        std::string s = val.str();
        if (signum == -1) {
            s.insert(0, "-");
        }
        return s;
    }

    Decimal128 ToDecimal128() const
    {
        Decimal128 result(HighBits(), LowBits());
        return result;
    }

    Decimal128Wrapper operator+(const Decimal128Wrapper &right) const
    {
        Decimal128Wrapper result = right;
        result += *this;
        return result;
    }

    Decimal128Wrapper operator-(const Decimal128Wrapper &right) const
    {
        Decimal128Wrapper result = right;
        result -= *this;
        return result;
    }

    Decimal128Wrapper operator*(const Decimal128Wrapper &right) const
    {
        Decimal128Wrapper result = right;
        result *= *this;
        return result;
    }

    Decimal128Wrapper operator/(const Decimal128Wrapper &right) const
    {
        Decimal128Wrapper result = right;
        result /= *this;
        return result;
    }

    Decimal128Wrapper operator%(const Decimal128Wrapper &right) const
    {
        Decimal128Wrapper result = right;
        result %= *this;
        return result;
    }

    static constexpr int64_t SIGN_LONG_MASK = 1LL << 63;
    static constexpr int DOUBLE_MAX_PRECISION = std::numeric_limits<double>::max_digits10;
    static constexpr uint128_t UNSIGNED_INT64_MIN = __uint128_t(INT64_MAX) + 1;
    static constexpr uint128_t DECIMAL128_MAX_VALUE = (__int128_t(0X4b3b4ca85a86c47a) << 64) + 0x098a223fffffffff;

private:
    uint256_t ReScaleTo256Bits(int32_t newScale) const
    {
        uint256_t result = val;
        if (scale == newScale) {
            return result;
        }
        if (scale > newScale) {
            result = (val + TenOfScaleMultipliers[newScale - scale] / 2) /
                TenOfScaleMultipliers[newScale - scale];
        } else {
            result *= TenOfScaleMultipliers[newScale - scale];
        }
        return result;
    }

    Decimal128Wrapper &ReScaleRoundUp(int32_t newScale)
    {
        if (scale == newScale) {
            return *this;
        }
        if (scale > newScale) {
            val = (val + TenOfScaleMultipliers[scale - newScale] / 2) / TenOfScaleMultipliers[scale - newScale];
        } else {
            *this = Multiply(Decimal128Wrapper(TenOfScaleMultipliers[newScale - scale]));
        }
        scale = newScale;
        return *this;
    }

    Decimal128Wrapper &ReScaleRoundFloor(int32_t newScale)
    {
        if (scale == newScale) {
            return *this;
        }
        if (scale > newScale) {
            val = val / TenOfScaleMultipliers[scale - newScale];
        } else {
            *this = Multiply(TenOfScaleMultipliers[newScale - scale]);
        }
        return *this;
    }

    int32_t scale = 0;
    int8_t signum = 1;
    uint128_t val = 0;
    OpStatus overflow = OpStatus::SUCCESS;
};

class Decimal64 : public BasicDecimal {
public:
    Decimal64()
    {
        val = 0;
    }

    Decimal64(int32_t inputVal)
    {
        val = inputVal;
    }

    Decimal64(int64_t inputVal)
    {
        val = inputVal;
    }

    Decimal64(double inputVal)
    {
        std::stringstream os;
        os << std::setprecision(DOUBLE_MAX_PRECISION) << inputVal;
        std::string s = os.str();
        new(this)Decimal64(s);
    }

    Decimal64(const std::string &s)
    {
        int32_t inputScale = 0;
        int32_t precision = 0;
        int128_t result = 0;
        if (DecimalFromString(s, result, inputScale, precision) != OpStatus::SUCCESS) {
            overflow = OpStatus::OP_OVERFLOW;
        }
        if (result > INT64_MAX || result < INT64_MIN) {
            overflow = OpStatus::OP_OVERFLOW;
        }
        scale = inputScale;
        val = result.convert_to<int64_t>();
    }

    Decimal64(const uint128_t &input)
    {
        val = static_cast<int64_t>(input);
    }

    Decimal64(const Decimal128Wrapper &decimal128)
    {
        if (decimal128.IsOverflow() != OpStatus::SUCCESS) {
            overflow = OpStatus::OP_OVERFLOW;
            return;
        }
        if (decimal128.GetSignum() == 0) {
            val = 0;
        } else if (decimal128.GetSignum() > 0) {
            if (decimal128 > INT64_MAX) {
                overflow = OpStatus::OP_OVERFLOW;
                return;
            }
            val = static_cast<int64_t>(decimal128.GetValue());
        } else {
            if (decimal128 < INT64_MIN) {
                overflow = OpStatus::OP_OVERFLOW;
                return;
            }
            val = -static_cast<int64_t>(decimal128.GetValue());
        }
        scale = decimal128.GetScale();
    }

    Decimal64 &ReScale(int32_t newScale, RoundingMode mode = RoundingMode::ROUND_UP)
    {
        switch (mode) {
            case RoundingMode::ROUND_UP:
                return ReScaleRoundUp(newScale);
            case RoundingMode::ROUND_FLOOR:
                return ReScaleRoundFloor(newScale);
        }
    }

    Decimal64 &operator+=(const Decimal64 &right)
    {
        val = val + right.val;
        return *this;
    }

    Decimal64 &operator-=(const Decimal64 &right)
    {
        val = val - right.val;
        return *this;
    }

    Decimal64 &operator*=(const Decimal64 &right)
    {
        val = val * right.val;
        return *this;
    }

    Decimal64 &operator/=(const Decimal64 &right)
    {
        val = val / right.val;
        return *this;
    }


    Decimal64 &operator%=(const Decimal64 &right)
    {
        val = val % right.val;
        return *this;
    }

    Decimal64 Add(const Decimal64 &right) const
    {
        Decimal64 result;
        if (overflow == OpStatus::OP_OVERFLOW || __builtin_saddl_overflow(val, right.val, &result.val)) {
            result.overflow = OpStatus::OP_OVERFLOW;
        }
        return result;
    }

    Decimal64 Subtract(const Decimal64 &right) const
    {
        Decimal64 result;
        if (overflow == OpStatus::OP_OVERFLOW || __builtin_ssubl_overflow(val, right.val, &result.val)) {
            result.overflow = OpStatus::OP_OVERFLOW;
        }
        return result;
    }

    Decimal64 Multiply(const Decimal64 &right) const
    {
        if (overflow == OpStatus::OP_OVERFLOW) {
            return *this;
        }
        Decimal64 result;
        if (overflow == OpStatus::OP_OVERFLOW || __builtin_smull_overflow(val, right.val, &result.val)) {
            result.overflow = OpStatus::OP_OVERFLOW;
        }
        return result;
    }

    Decimal64 Divide(const Decimal64 &right, int32_t rescaleFactor) const
    {
        if (overflow == OpStatus::OP_OVERFLOW) {
            return *this;
        }
        Decimal64 result;
        bool isNeg = (right.val > 0 ^ val > 0) ? 1 : 0;
        int128_t dividend128 = abs(int128_t(ReScaleTo128Bits(rescaleFactor + scale)));
        int128_t divisor128 = abs(int128_t(right.val));
        int128_t quotient128;
        int128_t remainder128;
        if (divisor128 == 0) {
            result.overflow = OpStatus::DIVIDE_BY_ZERO;
            return result;
        }
        divide_qr(dividend128, divisor128, quotient128, remainder128);
        if (remainder128 * 2 >= divisor128) {
            quotient128 += 1;
        }
        if (quotient128 > DECIMAL128_MAX_VALUE) {
            result.overflow = OpStatus::OP_OVERFLOW;
            return result;
        }
        result.val = isNeg ? (-quotient128).convert_to<int64_t>() : quotient128.convert_to<int64_t>();
        return result;
    }

    Decimal64 Mod(const Decimal64 &right) const
    {
        Decimal64 result;
        int32_t ScaleFactor = GetResultScale(scale, right.scale, Op::MOD);
        int128_t dividend128 = ReScaleTo128Bits(ScaleFactor);
        int128_t divisor128 = right.ReScaleTo128Bits(ScaleFactor);
        int128_t remainder256;
        if (divisor128 == 0) {
            result.overflow = OpStatus::DIVIDE_BY_ZERO;
            return result;
        }
        if (dividend128 == 0) {
            result.val = 0;
            result.SetScale(ScaleFactor);
            return result;
        }
        remainder256 = dividend128 % divisor128;
        result.val = remainder256.convert_to<int64_t>();
        result.SetScale(ScaleFactor);
        return result;
    }

    int32_t Compare(const Decimal64 &right) const
    {
        int32_t newScale = GetResultScale(scale, right.scale, Op::SUBTRACT);
        Decimal64 x = *this;
        Decimal64 y = right;
        Decimal64 r = x.ReScale(newScale).Subtract(y.ReScale(newScale));
        if (r.val > 0) {
            return 1;
        } else if (r.val == 0) {
            return 0;
        } else {
            return -1;
        }
    }

    std::string ToString() const
    {
        return ToStringWithScale(ToStringUnscale(), scale);
    }

    std::string ToStringUnscale() const
    {
        return std::to_string(val);
    }

    OpStatus IsOverflow(int32_t precision = 18)
    {
        if (abs(val) < TenOfScaleMultipliers[precision]) {
            return overflow;
        }
        return OpStatus::OP_OVERFLOW;
    }

    int128_t ReScaleTo128Bits(int32_t newScale) const
    {
        int128_t result = val;
        if (scale == newScale) {
            return result;
        }
        if (scale > newScale) {
            result = (val + TenOfScaleMultipliers[newScale - scale] / 2) /
                TenOfScaleMultipliers[newScale - scale];
        } else {
            result *= TenOfScaleMultipliers[newScale - scale];
        }
        return result;
    }

    int64_t GetValue() const
    {
        return val;
    }

    int32_t GetScale() const
    {
        return scale;
    }

    Decimal64 &SetScale(int32_t inputScale)
    {
        scale = inputScale;
        return *this;
    }

    OpStatus ToInt(int32_t &res) const
    {
        auto tenOfScale = TenOfScaleMultipliers[scale].convert_to<int64_t>();
        Decimal64 result = this->Divide(Decimal64(tenOfScale), 0);
        if (result.val > INT32_MAX || result.val < INT32_MIN) {
            return OpStatus::OP_OVERFLOW;
        } else {
            res = static_cast<int32_t>(result.val);
            return OpStatus::SUCCESS;
        }
    }

    OpStatus ToLong(int64_t &res) const
    {
        auto tenOfScale = TenOfScaleMultipliers[scale].convert_to<int64_t>();
        Decimal64 result = this->Divide(Decimal64(tenOfScale), 0);
        res = static_cast<int64_t>(result.val);
        return OpStatus::SUCCESS;
    }

    explicit operator int32_t() const
    {
        int32_t result;
        if (ToInt(result) != OpStatus::SUCCESS) {
            throw std::overflow_error("Overflow when Decimal64 cast to int");
        } else {
            return result;
        }
    }

    explicit operator int64_t() const
    {
        int64_t result;
        if (ToLong(result) != OpStatus::SUCCESS) {
            throw std::overflow_error("Overflow when Decimal64 cast to long");
        } else {
            return result;
        }
    }

    explicit operator double() const
    {
        return std::stod(ToString());
    }

    static constexpr int DOUBLE_MAX_PRECISION = std::numeric_limits<double>::max_digits10;
    static constexpr uint128_t UNSIGNED_INT64_MIN = __uint128_t(INT64_MAX) + 1;
    static constexpr uint128_t DECIMAL128_MAX_VALUE = (__int128_t(0X4b3b4ca85a86c47a) << 64) + 0x098a223fffffffff;
private:
    Decimal64 &ReScaleRoundUp(int32_t newScale)
    {
        if (scale == newScale) {
            return *this;
        }
        if (scale > newScale) {
            *this = Divide(TenOfScaleMultipliers[scale - newScale], 0);
        } else {
            *this = Multiply(TenOfScaleMultipliers[newScale - scale]);
        }
        return *this;
    }

    Decimal64 &ReScaleRoundFloor(int32_t newScale)
    {
        if (scale == newScale) {
            return *this;
        }
        if (scale > newScale) {
            val = val / static_cast<int64_t>(TenOfScaleMultipliers[scale - newScale]);
        } else {
            *this = Multiply(TenOfScaleMultipliers[newScale - scale]);
        }
        return *this;
    }

    int32_t scale = 0;
    int64_t val;
    OpStatus overflow = OpStatus::SUCCESS;
};

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

class DecimalOperations {
public:
    DecimalOperations() = delete;

    ~DecimalOperations() = delete;

    // decimal and overflow is encoded and decoded in continuous memory
    static inline void DecodeSumDecimal(op::DecimalSumState *statePtr, int128 &val, int64_t &overflow)
    {
        overflow = statePtr->overflow;
        val = statePtr->val;
    }

    static inline void EncodeSumDecimal(op::DecimalSumState *statePtr, const int128 &val,
        const int64_t &overflow)
    {
        statePtr->overflow = overflow;
        statePtr->val = val;
    }

    // decimal and overflow is encoded in continuous memory
    static inline void DecodeAvgDecimal(op::DecimalAverageState *statePtr, int128 &val, int64_t &overflow,
        int64_t &count)
    {
        count = statePtr->count;
        overflow = statePtr->overflow;
        val = statePtr->val;
    }

    static inline void EncodeAvgDecimal(op::DecimalAverageState *statePtr, const int128 &val,
        const int64_t &overflow, const int64_t &count)
    {
        statePtr->count = count;
        statePtr->overflow = overflow;
        statePtr->val = val;
    }

    // todo:
    template<typename Decimal>
    static inline void Round(Decimal &input, int32_t outScale, int32_t round)
    {
        int32_t inScale = input.GetScale();
        int32_t realRound = inScale - round;
        if (realRound <= 0) {
            return;
        } else {
            if (realRound > 37) {
                input.IsOverflow();
                return;
            }
            uint128_t tenOfScale = TenOfScaleMultipliers[realRound];
            input = input.Add(tenOfScale / 2);
            input /= tenOfScale;
            if (round < 0) {
                input *= TenOfScaleMultipliers[-round - 1];
            }
        }
    }

    // Decimal Internal Operation
    template<typename Decimal>
    static inline void InternalDecimalAdd(Decimal x, int32_t xScale, int32_t xPrecision, Decimal y,
        int32_t yScale, int32_t yPrecision, Decimal &result)
    {
        int32_t resultScale = GetResultScale(xScale, yScale, Op::ADD);
        result = x.ReScale(resultScale).Add(y.ReScale(resultScale)).SetScale(resultScale);
    }

    template<typename Decimal>
    static inline void InternalDecimalSubtract(Decimal x, int32_t xScale, int32_t xPrecision, Decimal y,
        int32_t yScale, int32_t yPrecision, Decimal &result)
    {
        int32_t resultScale = GetResultScale(xScale, yScale, Op::SUBTRACT);
        result = x.ReScale(resultScale).Subtract(y.ReScale(resultScale)).SetScale(resultScale);
    }

    template<typename Decimal>
    static inline void InternalDecimalMultiply(Decimal x, int32_t xScale, int32_t xPrecision, Decimal y,
        int32_t yScale, int32_t yPrecision, Decimal &result)
    {
        int32_t resultScale = GetResultScale(xScale, yScale, Op::MULTIPLY);
        result = x.Multiply(y).SetScale(resultScale);
    }

    template<typename Decimal, typename ResultDecimal>
    static inline void InternalDecimalDivide(Decimal x, int32_t xScale, int32_t xPrecision, Decimal y,
        int32_t yScale, int32_t yPrecision, ResultDecimal &result, int32_t &rScale)
    {
        result = x.Divide(y, rScale - xScale + yScale).SetScale(rScale);
    }

    template<typename Decimal, typename ResultDecimal>
    static inline void InternalDecimalMod(Decimal x, int32_t xScale, int32_t xPrecision, Decimal y,
        int32_t yScale, int32_t yPrecision, ResultDecimal &result)
    {
        int32_t resultScale = GetResultScale(xScale, yScale, Op::MOD);
        result = x.Mod(y).SetScale(resultScale);
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
