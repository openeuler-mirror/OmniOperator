/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <folly/Conv.h>
#include <folly/Expected.h>
#include <cctype>
#include <string>
#include <type_traits>

#include "data_type.h"
#include "vectorization/Status.h"
#include "type/Timestamp.h"
#include "type/TimestampConversion.h"
#include "type/base_operations.h"

namespace omniruntime::type::util {
using namespace vectorization;

struct SparkCastPolicy {
    static constexpr bool truncate = true;
    static constexpr bool legacyCast = false;
    static constexpr bool throwOnUnicode = false;
};

struct LegacyCastPolicy {
    static constexpr bool truncate = false;
    static constexpr bool legacyCast = true;
    static constexpr bool throwOnUnicode = false;
};

template <DataTypeId KIND, typename = void, typename TPolicy = SparkCastPolicy>
struct Converter {
    using TTo = typename NativeType<KIND>::type;

    template <typename TFrom>
    static Expected<TTo> tryCast(TFrom)
    {
        static const std::string kErrorMessage = fmt::format("Conversion to {} is not supported",
            NativeType<KIND>::name);
        return folly::makeUnexpected(vectorization::Status::UserError(kErrorMessage));
    }
};

/// Casts a string to a boolean. Allows a fixed set of strings. Casting from
/// other strings throws.
/// @tparam TPolicy PrestoCastPolicy and LegacyCastPolicy allow `t, f, 1, 0,
/// true, false` and their upper case equivalents. SparkCastPolicy additionally
/// allows 'y, n, yes, no' and their upper case equivalents.
template <typename TPolicy>
Expected<bool> castToBoolean(const char *data, size_t len)
{
    const auto &TU = static_cast<int (*)(int)>(std::toupper);

    if (len == 1) {
        auto character = TU(data[0]);
        if (character == 'T' || character == '1') {
            return true;
        }
        if (character == 'F' || character == '0') {
            return false;
        }
        if (character == 'Y') {
            return true;
        }
        if (character == 'N') {
            return false;
        }
    }

    // Case-insensitive 'true'.
    if ((len == 4) && (TU(data[0]) == 'T') && (TU(data[1]) == 'R') && (TU(data[2]) == 'U') && (TU(data[3]) == 'E')) {
        return true;
    }

    // Case-insensitive 'false'.
    if ((len == 5) && (TU(data[0]) == 'F') && (TU(data[1]) == 'A') && (TU(data[2]) == 'L') && (TU(data[3]) == 'S') && (
        TU(data[4]) == 'E')) {
        return false;
    }

    // Case-insensitive 'yes'.
    if ((len == 3) && (TU(data[0]) == 'Y') && (TU(data[1]) == 'E') && (TU(data[2]) == 'S')) {
        return true;
    }

    // Case-insensitive 'no'.
    if ((len == 2) && (TU(data[0]) == 'N') && (TU(data[1]) == 'O')) {
        return false;
    }

    return folly::makeUnexpected(vectorization::Status::UserError("Cannot cast {} to BOOLEAN",
        std::string_view(data, len)));
}

namespace detail {
template <typename T, typename F>
Expected<T> callFollyTo(const F &v)
{
    const auto result = folly::tryTo<T>(v);
    if (result.hasError()) {
        return folly::makeUnexpected(
            vectorization::Status::UserError("{}", folly::makeConversionError(result.error(), "").what()));
    }

    return result.value();
}
} // namespace detail

/// To BOOLEAN converter.
template <typename TPolicy>
struct Converter<OMNI_BOOLEAN, void, TPolicy> {
    using T = bool;

    template <typename TFrom>
    static Expected<T> tryCast(const TFrom &v)
    {
        return folly::makeUnexpected(vectorization::Status::UserError("Conversion to BOOLEAN is not supported"));
    }

    static Expected<T> tryCast(folly::StringPiece v)
    {
        return castToBoolean<TPolicy>(v.data(), v.size());
    }

    static Expected<T> tryCast(std::string_view v)
    {
        return castToBoolean<TPolicy>(v.data(), v.size());
    }

    static Expected<T> tryCast(const std::string &v)
    {
        return castToBoolean<TPolicy>(v.data(), v.length());
    }

    static Expected<T> tryCast(const bool &v)
    {
        return detail::callFollyTo<T>(v);
    }

    static Expected<T> tryCast(const float &v)
    {
        if (std::isnan(v)) {
            return false;
        }
        return v != 0;
    }

    static Expected<T> tryCast(const double &v)
    {
        if (std::isnan(v)) {
            return false;
        }
        return v != 0;
    }

    static Expected<T> tryCast(const int8_t &v)
    {
        return static_cast<T>(v);
    }

    static Expected<T> tryCast(const int16_t &v)
    {
        return static_cast<T>(v);
    }

    static Expected<T> tryCast(const int32_t &v)
    {
        return static_cast<T>(v);
    }

    static Expected<T> tryCast(const int64_t &v)
    {
        return static_cast<T>(v);
    }

    static Expected<T> tryCast(const Timestamp &)
    {
        return folly::makeUnexpected(
            vectorization::Status::UserError("Conversion of Timestamp to Boolean is not supported"));
    }
};

/// Presto compatible trim of whitespace. This also trims
/// control characters from both front and back and returns
/// a StringView of the trimmed string.
std::string_view trimWhiteSpace(const char *data, size_t length);

/// To TINYINT, SMALLINT, INTEGER, BIGINT, and HUGEINT converter.
template <DataTypeId KIND, typename TPolicy>
struct Converter<KIND, std::enable_if_t<KIND == OMNI_BYTE || KIND == OMNI_SHORT || KIND == OMNI_INT || KIND == OMNI_LONG
        , void>, TPolicy> {
    using T = typename NativeType<KIND>::type;

    template <typename From>
    static Expected<T> tryCast(const From &)
    {
        static const std::string kErrorMessage = fmt::format("Conversion to {} is not supported",
            NativeType<KIND>::name);

        return folly::makeUnexpected(vectorization::Status::UserError(kErrorMessage));
    }

    static Expected<T> convertStringToInt(const folly::StringPiece v)
    {
        // Handling integer target cases
        T result = 0;
        int index = 0;
        int len = v.size();
        if (len == 0) {
            return folly::makeUnexpected(
                vectorization::Status::UserError("Cannot cast an empty string to an integral value."));
        }

        // Setting negative flag
        bool negative = false;
        // Setting decimalPoint flag
        bool decimalPoint = false;
        if (v[0] == '-' || v[0] == '+') {
            if (len == 1) {
                return folly::makeUnexpected(
                    vectorization::Status::UserError("Cannot cast an '{}' string to an integral value.", v[0]));
            }
            negative = v[0] == '-';
            index = 1;
        }
        if (negative) {
            for (; index < len; index++) {
                // Truncate the decimal
                if (!decimalPoint && v[index] == '.') {
                    decimalPoint = true;
                    if (++index == len) {
                        break;
                    }
                }
                if (!std::isdigit(v[index])) {
                    return folly::makeUnexpected(vectorization::Status::UserError("Encountered a non-digit character"));
                }
                if (!decimalPoint) {
                    result = checkedMultiply<T>(result, 10);
                    result = checkedMinus<T>(result, v[index] - '0');
                }
            }
        } else {
            for (; index < len; index++) {
                // Truncate the decimal
                if (!decimalPoint && v[index] == '.') {
                    decimalPoint = true;
                    if (++index == len) {
                        break;
                    }
                }
                if (!std::isdigit(v[index])) {
                    return folly::makeUnexpected(vectorization::Status::UserError("Encountered a non-digit character"));
                }
                if (!decimalPoint) {
                    result = checkedMultiply<T>(result, 10);
                    result = checkedPlus<T>(result, v[index] - '0');
                }
            }
        }
        // Final result
        return result;
    }

    static Expected<T> tryCast(std::string_view v)
    {
        return convertStringToInt(v);
    }

    static Expected<T> tryCast(folly::StringPiece v)
    {
        return convertStringToInt(v);
    }

    static Expected<T> tryCast(const std::string &v)
    {
        return convertStringToInt(v);
    }

    static Expected<T> tryCast(const bool &v)
    {
        return folly::to<T>(v);
    }

    struct LimitType {
        static constexpr bool kByteOrSmallInt = std::is_same_v<T, int8_t> || std::is_same_v<T, int16_t>;

        static int64_t minLimit()
        {
            if (kByteOrSmallInt) {
                return std::numeric_limits<int32_t>::min();
            }
            return std::numeric_limits<T>::min();
        }

        static int64_t maxLimit()
        {
            if (kByteOrSmallInt) {
                return std::numeric_limits<int32_t>::max();
            }
            return std::numeric_limits<T>::max();
        }

        static T min()
        {
            if (kByteOrSmallInt) {
                return 0;
            }
            return std::numeric_limits<T>::min();
        }

        static T max()
        {
            if (kByteOrSmallInt) {
                return -1;
            }
            return std::numeric_limits<T>::max();
        }

        template <typename FP>
        static Expected<T> tryCast(const FP &v)
        {
            if (kByteOrSmallInt) {
                return T(static_cast<int32_t>(v));
            }
            return T(v);
        }
    };

    static Expected<T> tryCast(const float &v)
    {
        if (std::isnan(v)) {
            return 0;
        }

        if constexpr (std::is_same_v<T, int128_t>) {
            return std::numeric_limits<int128_t>::max();
        } else if (v > LimitType::maxLimit()) {
            return LimitType::max();
        } else if (v < LimitType::minLimit()) {
            return LimitType::min();
        }

        return LimitType::tryCast(v);
    }

    static Expected<T> tryCast(const double &v)
    {
        if (std::isnan(v)) {
            return 0;
        }

        if constexpr (std::is_same_v<T, int128_t>) {
            return std::numeric_limits<int128_t>::max();
        } else if (v > LimitType::maxLimit()) {
            return LimitType::max();
        } else if (v < LimitType::minLimit()) {
            return LimitType::min();
        }

        return LimitType::tryCast(v);
    }

    static Expected<T> tryCast(const int8_t &v)
    {
        return T(v);
    }

    static Expected<T> tryCast(const int16_t &v)
    {
        return T(v);
    }

    static Expected<T> tryCast(const int32_t &v)
    {
        return T(v);
    }

    static Expected<T> tryCast(const int64_t &v)
    {
        return T(v);
    }
};

/// To REAL and DOUBLE converter.
template <DataTypeId KIND, typename TPolicy>
struct Converter<KIND, std::enable_if_t<KIND == OMNI_FLOAT || KIND == OMNI_DOUBLE, void>, TPolicy> {
    using T = typename NativeType<KIND>::type;

    template <typename From>
    static Expected<T> tryCast(const From &v)
    {
        return detail::callFollyTo<T>(v);
    }

    static Expected<T> tryCast(folly::StringPiece v)
    {
        return tryCast<folly::StringPiece>(v);
    }

    static Expected<T> tryCast(std::string_view v)
    {
        return tryCast<folly::StringPiece>(v);
    }

    static Expected<T> tryCast(const std::string &v)
    {
        return tryCast<std::string>(v);
    }

    static Expected<T> tryCast(const bool &v)
    {
        return tryCast<bool>(v);
    }

    static Expected<T> tryCast(const float &v)
    {
        return tryCast<float>(v);
    }

    static Expected<T> tryCast(const double &v)
    {
        return T(v);
    }

    static Expected<T> tryCast(const int8_t &v)
    {
        return tryCast<int8_t>(v);
    }

    static Expected<T> tryCast(const int16_t &v)
    {
        return tryCast<int16_t>(v);
    }

    // Convert integer to double or float directly, not using folly, as it
    // might throw 'loss of precision' error.
    static Expected<T> tryCast(const int32_t &v)
    {
        return static_cast<T>(v);
    }

    // Convert large integer to double or float directly, not using folly, as it
    // might throw 'loss of precision' error.
    static Expected<T> tryCast(const int64_t &v)
    {
        return static_cast<T>(v);
    }

    // Convert large integer to double or float directly, not using folly, as it
    // might throw 'loss of precision' error.
    static Expected<T> tryCast(const int128_t &v)
    {
        return static_cast<T>(v);
    }

    static Expected<T> tryCast(const Timestamp &)
    {
        return folly::makeUnexpected(
            vectorization::Status::UserError("Conversion of Timestamp to Real or Double is not supported"));
    }
};

/// To VARBINARY converter.
template <typename TPolicy>
struct Converter<OMNI_VARBINARY, void, TPolicy> {
    // Same semantics of TypeKind::VARCHAR converter.
    template <typename T>
    static Expected<std::string> tryCast(const T &val)
    {
        return Converter<OMNI_VARCHAR, void, TPolicy>::tryCast(val);
    }
};

/// To VARCHAR converter.
template <typename TPolicy>
struct Converter<OMNI_VARCHAR, void, TPolicy> {
    template <typename T>
    static Expected<std::string> tryCast(const T &val)
    {
        if constexpr (std::is_same_v<T, double> || std::is_same_v<T, float>) {
            if constexpr (TPolicy::legacyCast) {
                auto str = folly::to<std::string>(val);
                normalizeStandardNotation(str);
                return str;
            }

            // Implementation below is close to String.of(double) of Java. For
            // example, for some rare cases the result differs in precision by
            // the least significant bit.
            if (FOLLY_UNLIKELY(std::isinf(val) || std::isnan(val))) {
                return folly::to<std::string>(val);
            }
            if ((val > -10'000'000 && val <= -0.001) || (val >= 0.001 && val < 10'000'000) || val == 0.0) {
                auto str = fmt::format("{}", val);
                normalizeStandardNotation(str);
                return str;
            }
            // Precision of float is at most 8 significant decimal digits. Precision
            // of double is at most 17 significant decimal digits.
            auto str = fmt::format(std::is_same_v<T, float> ? "{:.7E}" : "{:.16E}", val);
            normalizeScientificNotation(str);
            return str;
        }

        return folly::to<std::string>(val);
    }

    static Expected<std::string> tryCast(const Timestamp &val)
    {
        TimestampToStringOptions options;
        options.precision = TimestampToStringOptions::Precision::kMilliseconds;
        return val.toString(options);
    }

    static Expected<std::string> tryCast(const bool &val)
    {
        return val ? "true" : "false";
    }

    /// Normalize the given floating-point standard notation string in place, by
    /// appending '.0' if it has only the integer part but no fractional part. For
    /// example, for the given string '12345', replace it with '12345.0'.
    static void normalizeStandardNotation(std::string &str)
    {
        if (str.find(".") == std::string::npos && isdigit(str[str.length() - 1])) {
            str += ".0";
        }
    }

    /// Normalize the given floating-point scientific notation string in place, by
    /// removing the trailing 0s of the coefficient as well as the leading '+' and
    /// 0s of the exponent. For example, for the given string '3.0000000E+005',
    /// replace it with '3.0E5'. For '-1.2340000E-010', replace it with
    /// '-1.234E-10'.
    static void normalizeScientificNotation(std::string &str)
    {
        size_t idxE = str.find('E');
        OMNI_CHECK(idxE != std::string::npos, "Expect a character 'E' in scientific notation.");

        int endCoef = idxE - 1;
        while (endCoef >= 0 && str[endCoef] == '0') {
            --endCoef;
        }
        OMNI_CHECK(endCoef > 0, "Coefficient should not be all zeros.");

        int pos = endCoef + 1;
        if (str[endCoef] == '.') {
            pos++;
        }
        str[pos++] = 'E';

        int startExp = idxE + 1;
        if (str[startExp] == '-') {
            str[pos++] = '-';
            startExp++;
        }
        while (startExp < str.length() && (str[startExp] == '0' || str[startExp] == '+')) {
            startExp++;
        }
        OMNI_CHECK(startExp < str.length(), "Exponent should not be all zeros.");
        str.replace(pos, str.length() - startExp, str, startExp);
        pos += str.length() - startExp;

        str.resize(pos);
    }
};

/// To TIMESTAMP converter.
template <typename TPolicy>
struct Converter<OMNI_TIMESTAMP, void, TPolicy> {
    template <typename From>
    static Expected<int64_t> tryCast(const From & /* v */)
    {
        return folly::makeUnexpected(vectorization::Status::UserError("Conversion to Timestamp is not supported"));
    }

    static Expected<int64_t> tryCast(folly::StringPiece v)
    {
        return FromTimestampString(v.data(), v.size());
    }

    static Expected<int64_t> tryCast(std::string_view v)
    {
        return FromTimestampString(v.data(), v.size());
    }

    static Expected<int64_t> tryCast(const std::string &v)
    {
        return FromTimestampString(v.data(), v.size());
    }
};
}
