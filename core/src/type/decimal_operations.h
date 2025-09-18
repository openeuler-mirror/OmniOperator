/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2024. All rights reserved.
 * Description: DecimalOperations
 */

#ifndef OMNI_RUNTIME_DECIMAL_OPERATIONS_H
#define OMNI_RUNTIME_DECIMAL_OPERATIONS_H


#include <cstdint>
#include <regex>
#include <iostream>
#include <climits>
#include <cmath>
#include <cstring>
#include "integer256.h"
#include "util/debug.h"
#include "util/omni_exception.h"
#include "decimal_base.h"
#include "decimal128.h"
#include "base_operations.h"
#include "data_operations.h"
#include "codegen/functions/dtoa.h"

namespace omniruntime {
namespace type {
using namespace exception;
using uint128_t = __uint128_t;
using int128_t = __int128_t;
using int256_t = Integer256;

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

enum class RoundingMode {
    ROUND_UP,
    ROUND_FLOOR
};

static constexpr int MAX_PRECISION = 38;
static constexpr int MAX_SCALE = 38;
static constexpr int32_t MAX_DECIMAL64_DIGITS = 18;
static constexpr int I64_BIT = 64;
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
    uint128_t(__uint128_t(1000000000000000000LL) * 1000000000000000000LL * 100)};

static constexpr uint128_t HalfTenOfScaleMultipliers[39] = {
    uint128_t(0LL),
    uint128_t(5LL),
    uint128_t(50LL),
    uint128_t(500LL),
    uint128_t(5000LL),
    uint128_t(50000LL),
    uint128_t(500000LL),
    uint128_t(5000000LL),
    uint128_t(50000000LL),
    uint128_t(500000000LL),
    uint128_t(5000000000LL),
    uint128_t(50000000000LL),
    uint128_t(500000000000LL),
    uint128_t(5000000000000LL),
    uint128_t(50000000000000LL),
    uint128_t(500000000000000LL),
    uint128_t(5000000000000000LL),
    uint128_t(50000000000000000LL),
    uint128_t(500000000000000000LL),
    uint128_t(__uint128_t(1000000000000000000LL) * 5LL),
    uint128_t(__uint128_t(1000000000000000000LL) * 50LL),
    uint128_t(__uint128_t(1000000000000000000LL) * 500LL),
    uint128_t(__uint128_t(1000000000000000000LL) * 5000LL),
    uint128_t(__uint128_t(1000000000000000000LL) * 50000LL),
    uint128_t(__uint128_t(1000000000000000000LL) * 500000LL),
    uint128_t(__uint128_t(1000000000000000000LL) * 5000000LL),
    uint128_t(__uint128_t(1000000000000000000LL) * 50000000LL),
    uint128_t(__uint128_t(1000000000000000000LL) * 500000000LL),
    uint128_t(__uint128_t(1000000000000000000LL) * 5000000000LL),
    uint128_t(__uint128_t(1000000000000000000LL) * 50000000000LL),
    uint128_t(__uint128_t(1000000000000000000LL) * 500000000000LL),
    uint128_t(__uint128_t(1000000000000000000LL) * 5000000000000LL),
    uint128_t(__uint128_t(1000000000000000000LL) * 50000000000000LL),
    uint128_t(__uint128_t(1000000000000000000LL) * 500000000000000LL),
    uint128_t(__uint128_t(1000000000000000000LL) * 5000000000000000LL),
    uint128_t(__uint128_t(1000000000000000000LL) * 50000000000000000LL),
    uint128_t(__uint128_t(1000000000000000000LL) * 500000000000000000LL),
    uint128_t(__uint128_t(1000000000000000000LL) * 1000000000000000000LL * 5),
    uint128_t(__uint128_t(1000000000000000000LL) * 1000000000000000000LL * 50)};

static constexpr double DOUBLE_10_POW[] = {
    1.0e0, 1.0e1, 1.0e2, 1.0e3, 1.0e4, 1.0e5,
    1.0e6, 1.0e7, 1.0e8, 1.0e9, 1.0e10, 1.0e11,
    1.0e12, 1.0e13, 1.0e14, 1.0e15, 1.0e16, 1.0e17,
    1.0e18, 1.0e19, 1.0e20, 1.0e21, 1.0e22
};

template<bool allowDecimalRoundUp = false>
inline OpStatus DecimalFromString(const char *s, std::size_t len, int128_t &result, int32_t &scale,
    int32_t &precision)
{
    result = 0;
    bool isDot = false;
    bool isNeg = false;
    bool isExp = false;
    bool isSpace = false;
    const int roundChecked = 5;
    int32_t exponent = 0;
    int32_t offset = 0;
    while (offset < len && s[offset] == ' ') offset++;
    if (offset == len) return OpStatus::FAIL;
    int end = len - 1;
    while (end > offset && s[end] == ' ') end--;

    if (s[offset] == '-') {
        isNeg = true;
        offset++;
    } else if (s[offset] == '+') {
        offset++;
    }
    while (offset < len && s[offset] == '0') offset++;
    if (offset > end) {
        precision++;
        return OpStatus::SUCCESS;
    }
    if (s[offset] == '.') {
        offset += 1;
        isDot = true;
    }
    if (!isdigit(s[offset])) {
        return OpStatus::FAIL;
    }
    bool isOverflow = false;
    for (; offset <= end; offset++) {
        if (isdigit(s[offset])) {
            if (precision >= 38) {
                if constexpr (allowDecimalRoundUp) {
                    isOverflow = true;
                    break;
                } else {
                    return OpStatus::OP_OVERFLOW;
                }
            }
            precision++;
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
        } else {
            return OpStatus::FAIL;
        }
    }

    if constexpr (allowDecimalRoundUp) {
        if (isOverflow) {
            if (!isDot) {
                return OpStatus::OP_OVERFLOW;
            }
            int roundValue = int(s[offset]) - 48;
            // the result following rounding rules
            if (roundValue >= roundChecked) {
                result++;
            }
            offset++;
            for (; offset <= end; offset++) {
                if (!isdigit(s[offset])) {
                    return OpStatus::FAIL;
                } else if (s[offset] == 'e' || s[offset] == 'E') {
                    isExp = true;
                    break;
                }
            }
        }
    }

    bool isExpNeg = false;
    if (isExp) {
        for (; offset <= end; offset++) {
            if (isdigit(s[offset])) {
                exponent *= 10;
                exponent += int(s[offset]) - 48;
            } else if (s[offset] == '-') {
                isExpNeg = true;
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

    if (isExpNeg) {
        exponent = -exponent;
    }
    if (exponent + precision - scale > 38) {
        return OpStatus::OP_OVERFLOW;
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

template<bool allowDecimalRoundUp = false>
inline OpStatus DecimalFromString(const std::string &s, int128_t &result, int32_t &scale, int32_t &precision)
{
    return DecimalFromString<allowDecimalRoundUp>(s.data(), s.size(), result, scale, precision);
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

    int adjusted = unscaledValueLength - 1 - scale;
    if (adjusted >= -6) {
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
    } else {
        resultBuilder.append(unscaledValueString.substr(0, 1));
        if (unscaledValueLength > 1) {
            resultBuilder.append(".");
            resultBuilder.append(unscaledValueString.substr(1));
        }
        if (adjusted != 0) {
            resultBuilder.append("E");
            resultBuilder.append(std::to_string(adjusted));
        }
        return resultBuilder;
    }
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
template<bool allowDecimalRoundUp = false>
class Decimal128Wrapper {
public:
    Decimal128Wrapper() : val(0), signum(0)
    {}

    Decimal128Wrapper(int64_t highBits, uint64_t lowBits) : Decimal128Wrapper(Decimal128(highBits, lowBits))
    {}

    Decimal128Wrapper(Decimal128 value) : Decimal128Wrapper(value.ToInt128())
    {}

    Decimal128Wrapper(const uint128_t &value) : Decimal128Wrapper(static_cast<const int128_t &>(value))
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
            val = static_cast<uint128_t>(value);
        } else {
            signum = -1;
            val = static_cast<uint128_t>(-value);
        }
    }

    Decimal128Wrapper(const char *s, std::size_t length)
    {
        int32_t inputScale = 0;
        int32_t precision = 0;
        int128_t result = 0;
        overflow = DecimalFromString<allowDecimalRoundUp>(s, length, result, inputScale, precision);
        if (result == 0) {
            signum = 0;
            val = 0;
            scale = inputScale;
            return;
        }
        if (result > 0) {
            signum = 1;
            val = static_cast<uint128_t>(result);
        } else {
            signum = -1;
            val = static_cast<uint128_t>(-result);
        }
        scale = inputScale;
    }

    Decimal128Wrapper(const std::string &s) : Decimal128Wrapper(s.c_str(), s.size())
    {}

    Decimal128Wrapper(const char* s)
    {
        int32_t inputScale = 0;
        int32_t precision = 0;
        int128_t result = 0;
        overflow = DecimalFromString<allowDecimalRoundUp>(s, result, inputScale, precision);
        if (result == 0) {
            signum = 0;
            val = 0;
            scale = inputScale;
            return;
        }
        if (result > 0) {
            signum = 1;
            val = static_cast<uint128_t>(result);
        } else {
            signum = -1;
            val = static_cast<uint128_t>(-result);
        }
        scale = inputScale;
    }

    template<typename T, typename = std::enable_if_t<!std::is_same_v<T, const char*>>>
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
            val = static_cast<uint128_t>(-tmp);
            signum = -1;
        }
    }

    explicit Decimal128Wrapper(double value)
    {
        char s[codegen::function::MAX_DATA_LENGTH] = {0};
        auto length = codegen::function::DoubleToString::DoubleToStringConverter(value, s);
        new(this)Decimal128Wrapper(s, length);
    }

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
            uint128_t x = val;
            uint128_t y = right.val;
            uint128_t r;
            bool isOverflow = __builtin_add_overflow(x, y, &r);
            if (isOverflow) {
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
        uint128_t x = val;
        uint128_t y = right.val;
        uint128_t r;
        bool isOverflow = __builtin_mul_overflow(x, y, &r);
        if (isOverflow || r > DECIMAL128_MAX_VALUE) {
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
        int256_t x = val;
        int256_t y = right.val;
        int256_t r = x * y;
        int256_t q = r % tenOfScale;
        r = r / tenOfScale;
        if (r > TenOfScaleMultipliers[38] - 1) {
            result.overflow = OpStatus::OP_OVERFLOW;
            return result;
        }
        if (q >= (tenOfScale / 2) && tenOfScale != 1) {
            r = r + 1;
        }
        result.val = r.ConvertTo<uint128_t>();
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
        int256_t dividend256 = ReScaleTo256Bits(rescaleFactor + scale);
        int256_t divisor256 = right.val;
        int256_t quotient256;
        int256_t remainder256;
        if (divisor256 == 0) {
            result.overflow = OpStatus::DIVIDE_BY_ZERO;
            return result;
        }
        if (dividend256 == 0) {
            result.signum = 0;
            result.val = 0;
            return result;
        }
        Integer256::Divide(dividend256, divisor256, quotient256, remainder256);
        if (remainder256 * 2 >= divisor256) {
            quotient256 = quotient256 + 1;
        }
        if (quotient256 > DECIMAL128_MAX_VALUE) {
            result.overflow = OpStatus::OP_OVERFLOW;
            return result;
        }
        result.val = quotient256.ConvertTo<uint128_t>();
        result.signum = (signum != right.signum) ? -1 : 1;
        return result;
    }

    Decimal128Wrapper Mod(const Decimal128Wrapper &right)
    {
        Decimal128Wrapper result;
        int32_t ScaleFactor = GetResultScale(scale, right.scale, Op::MOD);
        int256_t dividend256 = ReScaleTo256Bits(ScaleFactor);
        int256_t divisor256 = right.ReScaleTo256Bits(ScaleFactor);
        int256_t remainder256;
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
        result.val = remainder256.ConvertTo<uint128_t>();
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
            res = static_cast<int32_t>(result.val);
        } else {
            // '1L + INT32_MAX', which is positive, is implicitly converted to Decimal128Wrapper with signum_=1,
            // comparing it with result, which is negative, can never detect overflow
            // that is why here, we should compare '1L + INT32_MAX' with result.val_ not result itself
            if (result.val > 1L + INT32_MAX) {
                return OpStatus::OP_OVERFLOW;
            }
            res = static_cast<int32_t>(-result.val);
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
            res = static_cast<int64_t>(result.val);
        } else {
            // 'UNSIGNED_INT64_MIN', which is positive, is implicitly converted to Decimal128Wrapper with signum_=1,
            // comparing it with result, which is negative, can never detect overflow
            // that is why here, we should compare 'UNSIGNED_INT64_MIN' with result.val_ not result itself
            if (result.val > UNSIGNED_INT64_MIN) {
                return OpStatus::OP_OVERFLOW;
            }
            res = static_cast<int64_t>(-result.val);
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
        double result;
        ConvertStringToDouble(result, ToString());
        return result;
    }

    Decimal128Wrapper &ReScale(int32_t newScale, RoundingMode mode = RoundingMode::ROUND_UP)
    {
        if (overflow != OpStatus::SUCCESS) {
            return *this;
        }
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
        int128_t t = static_cast<int128_t>(val);
        if (signum == -1) {
            t = -t;
        }
        return static_cast<int64_t>(t >> 64);
    }

    uint64_t LowBits() const
    {
        int128_t t = static_cast<int128_t>(val);
        if (signum == -1) {
            t = -t;
        }
        return static_cast<uint64_t>(t);
    }

    void SetValue(int64_t highBitsField, uint64_t lowBitsField)
    {
        int128_t v = Decimal128(highBitsField, lowBitsField).ToInt128();
        if (v < 0) {
            v = -v;
            signum = -1;
        } else if (v == 0) {
            signum = 0;
        } else {
            signum = 1;
        }
        val = static_cast<uint128_t>(v);
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

    void Unset()
    {
        val = 0;
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
            return static_cast<int128_t>(val);
        } else {
            return static_cast<int128_t>(-val);
        }
    }

    std::string ToString() const
    {
        std::string s = ToStringUnscale();
        return ToStringWithScale(s, scale);
    }

    std::string ToStringUnscale() const
    {
        std::string s = Uint128ToStr(val);
        if (signum == -1) {
            s.insert(0, "-");
        }
        return s;
    }

    Decimal128 ToDecimal128() const
    {
        int128_t result = val;
        if (signum < 0) {
            result = -val;
        }
        return Decimal128(result);
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
    int256_t ReScaleTo256Bits(int32_t newScale) const
    {
        int256_t result = val;
        if (scale == newScale) {
            return result;
        }
        if (scale > newScale) {
            result = (val + HalfTenOfScaleMultipliers[scale - newScale]) /
                TenOfScaleMultipliers[scale - newScale];
        } else {
            result = result * TenOfScaleMultipliers[newScale - scale];
        }
        return result;
    }

    Decimal128Wrapper &ReScaleRoundUp(int32_t newScale)
    {
        if (scale == newScale) {
            return *this;
        }
        if (scale > newScale) {
            int32_t refactorScale = scale - newScale;
            if (refactorScale > MAX_SCALE) {
                val = 0;
                return *this;
            }
            val = (val + HalfTenOfScaleMultipliers[refactorScale]) / TenOfScaleMultipliers[refactorScale];
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
            int32_t refactorScale = scale - newScale;
            if (refactorScale > MAX_SCALE) {
                val = 0;
                return *this;
            }
            val = val / TenOfScaleMultipliers[refactorScale];
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

static inline int CountLeadingZeros(uint64_t value)
{
    int bitpos = 0;
    while (value != 0) {
        value >>= 1;
        ++bitpos;
    }
    return I64_BIT - bitpos;
}

// Suppose we have a number that requires x bits to be represented and we scale it up by
// 10^scale_by. Let's say now y bits are required to represent it. This function returns
// the maximum possible y - x for a given 'scale_by'.
static inline int32_t MaxBitsRequiredIncreaseAfterScaling(int32_t scale_by)
{
    // We rely on the following formula:
    // bits_required(x * 10^y) <= bits_required(x) + floor(log2(10^y)) + 1
    // We precompute floor(log2(10^x)) + 1 for x = 0, 1, 2...75, 76
    static const int32_t floor_log2_plus_one[] = {
        0, 4, 7, 10, 14, 17, 20, 24, 27, 30, 34, 37, 40, 44, 47, 50,
        54, 57, 60, 64, 67, 70, 74, 77, 80, 84, 87, 90, 94, 97, 100, 103,
        107, 110, 113, 117, 120, 123, 127, 130, 133, 137, 140, 143, 147, 150, 153, 157,
        160, 163, 167, 170, 173, 177, 180, 183, 187, 190, 193, 196, 200, 203, 206, 210,
        213, 216, 220, 223, 226, 230, 233, 236, 240, 243, 246, 250, 253};
    return floor_log2_plus_one[scale_by];
}

// Returns the maximum possible number of bits required to represent num * 10^scale_by.
static inline int32_t MaxBitsRequiredAfterScaling(int64_t value, int32_t scale_by)
{
    auto value_abs = std::abs(value);

    int32_t num_occupied = 64 - CountLeadingZeros(value_abs);
    return num_occupied + MaxBitsRequiredIncreaseAfterScaling(scale_by);
}

template<bool allowDecimalRoundUp = false>
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
        char s[codegen::function::MAX_DATA_LENGTH];
        auto length = codegen::function::DoubleToString::DoubleToStringConverter(inputVal, s);
        new(this)Decimal64(s, length);
    }

    Decimal64(const char *s, std::size_t length)
    {
        int32_t inputScale = 0;
        int32_t precision = 0;
        int128_t result = 0;
        if (DecimalFromString<allowDecimalRoundUp>(s, length, result, inputScale, precision) != OpStatus::SUCCESS) {
            overflow = OpStatus::OP_OVERFLOW;
        }
        if (precision - inputScale > 18) {
            overflow = OpStatus::OP_OVERFLOW;
        }
        int32_t newPrecision = precision - 18;
        if (newPrecision > 0) {
            DivideRoundUp(result, static_cast<int128_t>(TenOfScaleMultipliers[newPrecision]), result);
            inputScale -= newPrecision;
        }

        scale = inputScale;
        val = static_cast<int64_t>(result);
    }

    Decimal64(const std::string &s) : Decimal64(s.data(), s.size())
    {}

    Decimal64(const uint128_t &input)
    {
        val = static_cast<int64_t>(input);
    }

    Decimal64(const Decimal128Wrapper<allowDecimalRoundUp> &decimal128)
    {
        val = 0;
        if (decimal128.IsOverflow() != OpStatus::SUCCESS) {
            overflow = OpStatus::OP_OVERFLOW;
            return;
        }
        if (decimal128.GetSignum() == 0) {
            val = 0;
        } else if (decimal128.GetSignum() > 0) {
            if (decimal128.GetValue() > DECIMAL64_MAX_VALUE) {
                overflow = OpStatus::OP_OVERFLOW;
                return;
            }
            val = static_cast<int64_t>(decimal128.GetValue());
        } else {
            if (decimal128.GetValue() > DECIMAL64_MAX_VALUE) {
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
        if (right.val == 0) {
            result.overflow = OpStatus::DIVIDE_BY_ZERO;
            return result;
        }

        bool isNeg = (right.val > 0 ^ val > 0) ? 1 : 0;
        auto numBitsRequiredAfterScaling = MaxBitsRequiredAfterScaling(val, rescaleFactor);
        if (numBitsRequiredAfterScaling < I64_BIT) {
            // consider to use fast-path
            auto dividend = (rescaleFactor <= 0) ? val : val * INT64_TEN_POWERS_TABLE[rescaleFactor];
            result.val = dividend / right.val;
            auto reminder = dividend % right.val;

            // round-up
            if (std::abs(2 * reminder) >= abs(right.val)) {
                result.val += (isNeg ? (-1) : 1);
            }
        } else {
            int128_t dividend128 = abs(int128_t(ReScaleTo128Bits(rescaleFactor + scale)));
            int128_t divisor128 = abs(int128_t(right.val));
            int128_t quotient128 = dividend128 / divisor128;
            int128_t remainder128 = dividend128 % divisor128;
            if (remainder128 * 2 >= divisor128) {
                quotient128 += 1;
            }
            if (quotient128 > DECIMAL128_MAX_VALUE) {
                result.overflow = OpStatus::OP_OVERFLOW;
                return result;
            }
            result.val = isNeg ? static_cast<int64_t>(-quotient128) : static_cast<int64_t>(quotient128);
        }
        return result;
    }

    Decimal64 Mod(const Decimal64 &right) const
    {
        if (overflow == OpStatus::OP_OVERFLOW) {
            return *this;
        }
        Decimal64 result;
        if (right.val == 0) {
            result.overflow = OpStatus::DIVIDE_BY_ZERO;
            return result;
        }

        int32_t scaleFactor = GetResultScale(scale, right.scale, Op::MOD);
        if (val == 0) {
            result.val = 0;
            result.SetScale(scaleFactor);
            return result;
        }

        if (scale == right.scale) {
            result.val = val % right.val;
        } else {
            int128_t dividend128 = ReScaleTo128Bits(scaleFactor);
            int128_t divisor128 = right.ReScaleTo128Bits(scaleFactor);
            int128_t remainder128 = dividend128 % divisor128;
            result.val = static_cast<int64_t>(remainder128);
        }
        result.SetScale(scaleFactor);

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
            result = (val + HalfTenOfScaleMultipliers[scale - newScale]) /
                TenOfScaleMultipliers[scale - newScale];
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

    bool IsNegative()
    {
        return val < 0;
    }

    void Unset()
    {
        val = 0;
    }

    Decimal64 &SetScale(int32_t inputScale)
    {
        scale = inputScale;
        return *this;
    }

    OpStatus ToInt(int32_t &res) const
    {
        auto tenOfScale = static_cast<int64_t>(TenOfScaleMultipliers[scale]);
        int64_t res64;
        if (scale == 0) {
            res64 = val;
        } else {
            res64 = val / tenOfScale;
            auto reminder = val % tenOfScale;
            if (std::abs(2 * reminder) >= tenOfScale) {
                res64 += ((val < 0) ? -1 : 1);
            }
        }
        if (res64 > INT32_MAX || res64 < INT32_MIN) {
            return OpStatus::OP_OVERFLOW;
        } else {
            res = static_cast<int32_t>(res64);
            return OpStatus::SUCCESS;
        }
    }

    OpStatus ToLong(int64_t &res) const
    {
        auto tenOfScale = static_cast<int64_t>(TenOfScaleMultipliers[scale]);
        if (scale == 0) {
            res = val;
        } else {
            res = val / tenOfScale;
            auto reminder = val % tenOfScale;
            if (std::abs(2 * reminder) >= tenOfScale) {
                res += ((val < 0) ? -1 : 1);
            }
        }
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
        double result;
        ConvertStringToDouble(result, ToString());
        return result;
    }

    static constexpr int DOUBLE_MAX_PRECISION = std::numeric_limits<double>::max_digits10;
    static constexpr uint128_t UNSIGNED_INT64_MIN = __uint128_t(INT64_MAX) + 1;
    static constexpr uint128_t DECIMAL128_MAX_VALUE = (__int128_t(0X4b3b4ca85a86c47a) << 64) + 0x098a223fffffffff;
    static constexpr uint128_t DECIMAL64_MAX_VALUE = 999999999999999999LL;
private:
    Decimal64 &ReScaleRoundUp(int32_t newScale)
    {
        if (scale == newScale) {
            return *this;
        }
        if (scale > newScale) {
            int32_t refactorScale = scale - newScale;
            if (refactorScale > MAX_SCALE) {
                val = 0;
                return *this;
            }
            int64_t scaleMultiplier = static_cast<int64_t>(TenOfScaleMultipliers[refactorScale]);
            auto result = val / scaleMultiplier;
            auto remainder = val % scaleMultiplier;
            if (abs(remainder) >= (scaleMultiplier >> 1)) {
                result += (val > 0 ? 1 : -1);
            }
            val = result;
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
            int32_t refactorScale = scale - newScale;
            if (refactorScale > MAX_SCALE) {
                val = 0;
                return *this;
            }
            val = val / static_cast<int64_t>(TenOfScaleMultipliers[refactorScale]);
        } else {
            *this = Multiply(TenOfScaleMultipliers[newScale - scale]);
        }
        return *this;
    }

    int32_t scale = 0;
    int64_t val = 0;
    OpStatus overflow = OpStatus::SUCCESS;
};

class DecimalOperations {
public:
    DecimalOperations() = delete;

    ~DecimalOperations() = delete;

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
                input.Unset();
                return;
            }
            uint128_t tenOfScale = TenOfScaleMultipliers[realRound];
            input = input.IsNegative() ? input.Subtract(tenOfScale / 2) : input.Add(tenOfScale / 2);
            input /= tenOfScale;
            if (round < 0) {
                input *= TenOfScaleMultipliers[-round];
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
