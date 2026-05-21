/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: DateFormat function implementation
 * Formats a timestamp value into a string according to the format specifier.
 * Follows Velox Spark SQL behavior: date_format(timestamp, format) -> varchar
 * Aligned with Spark/Java DateTimeFormatter pattern semantics. Shares pattern
 * parsing approach with FromUnixTime.cpp. The key differences vs. from_unixtime:
 *   1) input is microsecond-precision TIMESTAMP, so 'S' token outputs real
 *      sub-second digits (FormatFractionOfSecond).
 *   2) setenv("TZ") + tzset() are non-thread-safe and modify global process
 *      state; they are called exactly once per Apply (per batch), hoisted out
 *      of the per-row loop, never per-row.
 *   3) timeParserPolicy is hardcoded to "CORRECTED" (Spark's default after
 *      Spark 3.0); legacy SimpleDateFormat semantics are not supported, so
 *      all isLegacy branches are removed.
 */

#include "DateFormat.h"
#include "vector/vector.h"
#include "../VectorFunction.h"
#include "vectorization/SelectivityVector.h"
#include "vector/vector_helper.h"
#include "util/bit_util.h"
#include "util/config/QueryConfig.h"
#include <algorithm>
#include <array>
#include <cctype>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <string>
#include <string_view>

namespace omniruntime::vectorization {
using namespace omniruntime::vec;
using namespace omniruntime::type;

namespace {

static constexpr std::array<const char *, 12> SHORT_MONTH_NAMES = {
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
static constexpr std::array<const char *, 12> FULL_MONTH_NAMES = {
    "January", "February", "March", "April", "May", "June", "July", "August",
    "September", "October", "November", "December"};
static constexpr std::array<const char *, 7> SHORT_WEEKDAY_NAMES = {
    "Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"};
static constexpr std::array<const char *, 7> FULL_WEEKDAY_NAMES = {
    "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"};

std::string PadNumber(int64_t value, size_t width)
{
    std::ostringstream oss;
    oss << std::setw(static_cast<int>(width)) << std::setfill('0') << value;
    return oss.str();
}

std::string FormatNumber(int64_t value, size_t count)
{
    return count >= 2 ? PadNumber(value, count) : std::to_string(value);
}

void ThrowUnsupportedPattern(char token)
{
    OMNI_THROW("DateFormat function Error", "Illegal pattern character '" + std::string(1, token) + "'");
}

int GetGmtOffsetSeconds(const struct tm &timeInfo)
{
#if defined(__linux__) || defined(__APPLE__)
    return static_cast<int>(timeInfo.tm_gmtoff);
#else
    return 0;
#endif
}

std::string FormatRfcOffset(int offsetSeconds, bool withColon)
{
    char sign = offsetSeconds >= 0 ? '+' : '-';
    int absOffset = std::abs(offsetSeconds);
    int hours = absOffset / 3600;
    int minutes = (absOffset % 3600) / 60;
    std::ostringstream oss;
    oss << sign << PadNumber(hours, 2);
    if (withColon) {
        oss << ':';
    }
    oss << PadNumber(minutes, 2);
    return oss.str();
}

std::string FormatIsoOffset(int offsetSeconds, size_t count, bool zeroAsZ)
{
    if (offsetSeconds == 0 && zeroAsZ) {
        return "Z";
    }
    if (count == 1 && offsetSeconds % 3600 == 0) {
        char sign = offsetSeconds >= 0 ? '+' : '-';
        return std::string(1, sign) + PadNumber(std::abs(offsetSeconds) / 3600, 2);
    }
    return FormatRfcOffset(offsetSeconds, count >= 3);
}

std::string FormatLocalizedOffset(int offsetSeconds, size_t count)
{
    if (offsetSeconds == 0) {
        return "GMT";
    }
    char sign = offsetSeconds >= 0 ? '+' : '-';
    int absOffset = std::abs(offsetSeconds);
    int hours = absOffset / 3600;
    int minutes = (absOffset % 3600) / 60;
    std::ostringstream oss;
    oss << "GMT" << sign;
    if (count >= 4) {
        oss << PadNumber(hours, 2) << ':' << PadNumber(minutes, 2);
    } else {
        oss << hours;
        if (minutes != 0) {
            oss << ':' << PadNumber(minutes, 2);
        }
    }
    return oss.str();
}

std::string FormatZoneName(const struct tm &timeInfo, const std::string &timeZoneStr, int offsetSeconds)
{
    if (timeZoneStr.rfind("GMT", 0) == 0) {
        return timeZoneStr;
    }
#if defined(__linux__) || defined(__APPLE__)
    return timeInfo.tm_zone != nullptr ? timeInfo.tm_zone : "";
#else
    return FormatRfcOffset(offsetSeconds, false);
#endif
}

std::string GetDisplayTimeZoneId(const std::string &timeZoneStr)
{
    if (timeZoneStr == "Asia/Beijing" || timeZoneStr == "Asia/Shanghai") {
        return "Asia/Shanghai";
    }
    if (timeZoneStr == "GMT+08:00") {
        return "GMT+08:00";
    }
    return timeZoneStr.empty() ? "UTC" : timeZoneStr;
}

/// Convert "GMT+08:00" to POSIX-compatible "Etc/GMT-8".
/// POSIX/IANA use inverted sign convention: GMT+8 means UTC-8 in POSIX.
std::string NormalizeTimeZone(const std::string &tzStr)
{
    if (tzStr == "GMT+08:00") {
        return "Etc/GMT-8";
    }
    if (tzStr == "Asia/Beijing") {
        return "Asia/Shanghai";
    }
    return tzStr;
}

/// Fraction-of-second token 'S' with the given digit count.
/// micros: microseconds within the current second (0..999999).
/// Per Spark/Java DateTimeFormatter:
///   S      -> first digit of fraction (tenths of second)
///   SS     -> first two digits (hundredths)
///   SSS    -> milliseconds (3 digits)
///   SSSSSS -> microseconds (6 digits)
///   SSSSSSSSS -> nanoseconds (9 digits, low 3 always 0 for us-precision input)
/// If count > 9, extra digits are zeros.
std::string FormatFractionOfSecond(int64_t micros, size_t count)
{
    int64_t nanos = micros * 1000;
    char digits[9];
    int64_t value = nanos;
    for (int i = 8; i >= 0; --i) {
        digits[i] = static_cast<char>('0' + (value % 10));
        value /= 10;
    }
    std::string result;
    result.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        result.push_back(i < 9 ? digits[i] : '0');
    }
    return result;
}

/// Nano-of-second token 'n'.
/// count <= 1: print as plain number; count > 1: left-pad to count digits.
std::string FormatNanoOfSecond(int64_t micros, size_t count)
{
    int64_t nanos = micros * 1000;
    return count <= 1 ? std::to_string(nanos) : PadNumber(nanos, count);
}

std::string_view GetStringValueFromVector(BaseVector *vec, int32_t row)
{
    Encoding encoding = vec->GetEncoding();
    if (encoding == OMNI_ENCODING_CONST) {
        auto *constVec = static_cast<ConstVector<std::string_view> *>(vec);
        return constVec->GetConstValue();
    } else if (encoding == OMNI_FLAT) {
        auto *flatVec = static_cast<Vector<LargeStringContainer<std::string_view>> *>(vec);
        return flatVec->GetValue(row);
    } else if (encoding == OMNI_DICTIONARY) {
        auto *dictVec = static_cast<Vector<DictionaryContainer<std::string_view, LargeStringContainer>> *>(vec);
        return dictVec->GetValue(row);
    }
    return std::string_view();
}

/// Resolve a microsecond timestamp into a broken-down struct tm in the given
/// timezone. Uses localtime_r when useLocalTime is true so glibc populates
/// tm_gmtoff / tm_zone (required by timezone tokens). Otherwise uses gmtime_r.
///
/// IMPORTANT: setenv("TZ") + tzset() are NOT thread-safe and MUST be called
/// by the caller exactly once per batch (outside per-row loops), not here.
/// This function only reads the thread-local time conversion functions.
void ResolveLocalTime(int64_t timestampMicros, bool useLocalTime,
    struct tm &outLtm, int64_t &outMicroInSecond)
{
    int64_t seconds = timestampMicros / 1000000;
    int64_t microInSecond = timestampMicros % 1000000;
    if (microInSecond < 0) {
        microInSecond += 1000000;
        seconds -= 1;
    }
    outMicroInSecond = microInSecond;

    time_t timeStampVal = static_cast<time_t>(seconds);
    outLtm = {};
    if (useLocalTime) {
        localtime_r(&timeStampVal, &outLtm);
    } else {
        gmtime_r(&timeStampVal, &outLtm);
    }
}

/// Format a broken-down time using Spark/Java DateTimeFormatter pattern tokens.
/// ltm must already be populated for the desired timezone (tm_gmtoff / tm_zone
/// filled in for timezone tokens to work).
/// timeZoneStr is the display timezone id (e.g. "Asia/Shanghai"), only used by
/// 'v'/'z'/'V' tokens for canonicalization.
std::string FormatTimestamp(const struct tm &ltm, int64_t microInSecond,
    const std::string &sparkFormat, const std::string &timeZoneStr)
{
    std::string result;
    result.reserve(sparkFormat.size() * 2);
    const int offsetSeconds = GetGmtOffsetSeconds(ltm);
    const std::string zoneId = GetDisplayTimeZoneId(timeZoneStr);
    const int hour = ltm.tm_hour;
    const int month = ltm.tm_mon + 1;
    const int dayOfMonth = ltm.tm_mday;

    size_t i = 0;
    while (i < sparkFormat.size()) {
        char token = sparkFormat[i];

        // Java DateTimeFormatter literal-quoting: '...' and '' for a single quote
        if (token == '\'') {
            ++i;
            if (i < sparkFormat.size() && sparkFormat[i] == '\'') {
                result += '\'';
                ++i;
                continue;
            }
            while (i < sparkFormat.size()) {
                if (sparkFormat[i] == '\'') {
                    ++i;
                    break;
                }
                result += sparkFormat[i++];
            }
            continue;
        }

        // Count consecutive identical pattern letters.
        size_t count = 1;
        while (i + count < sparkFormat.size() && sparkFormat[i + count] == token) {
            ++count;
        }

        switch (token) {
            case 'y':
            case 'Y':
                result += count == 2 ? PadNumber((ltm.tm_year + 1900) % 100, 2)
                                     : PadNumber(ltm.tm_year + 1900, std::max<size_t>(4, count));
                break;
            case 'M':
                if (count == 3) {
                    result += SHORT_MONTH_NAMES[ltm.tm_mon];
                } else if (count >= 4) {
                    result += FULL_MONTH_NAMES[ltm.tm_mon];
                } else {
                    result += FormatNumber(month, count);
                }
                break;
            case 'd':
                result += FormatNumber(dayOfMonth, count);
                break;
            case 'H':
                result += FormatNumber(hour, count);
                break;
            case 'h': {
                int clockHour = hour % 12;
                result += FormatNumber(clockHour == 0 ? 12 : clockHour, count);
                break;
            }
            case 'K':
                result += FormatNumber(hour % 12, count);
                break;
            case 'k':
                result += FormatNumber(hour == 0 ? 24 : hour, count);
                break;
            case 'm':
                result += FormatNumber(ltm.tm_min, count);
                break;
            case 's':
                result += FormatNumber(ltm.tm_sec, count);
                break;
            case 'S':
                result += FormatFractionOfSecond(microInSecond, count);
                break;
            case 'n':
                result += FormatNanoOfSecond(microInSecond, count);
                break;
            case 'a':
                result += hour < 12 ? "AM" : "PM";
                break;
            case 'D':
                result += FormatNumber(ltm.tm_yday + 1, count);
                break;
            case 'E':
                result += count >= 4 ? FULL_WEEKDAY_NAMES[ltm.tm_wday] : SHORT_WEEKDAY_NAMES[ltm.tm_wday];
                break;
            case 'F':
                result += std::to_string(((dayOfMonth - 1) / 7) + 1);
                break;
            case 'q':
            case 'Q':
                result += FormatNumber(((month - 1) / 3) + 1, count);
                break;
            case 'w': {
                char buffer[8] = {};
                size_t len = strftime(buffer, sizeof(buffer), "%V", &ltm);
                std::string week = len == 0 ? "" : std::string(buffer, len);
                if (count == 1 && week.size() == 2 && week[0] == '0') {
                    week.erase(0, 1);
                }
                result += week;
                break;
            }
            case 'V':
                result += zoneId;
                break;
            case 'v':
            case 'z':
                result += FormatZoneName(ltm, timeZoneStr, offsetSeconds);
                break;
            case 'O':
                result += FormatLocalizedOffset(offsetSeconds, count);
                break;
            case 'X':
                result += FormatIsoOffset(offsetSeconds, count, true);
                break;
            case 'x':
                result += FormatIsoOffset(offsetSeconds, count, false);
                break;
            case 'Z':
                result += count >= 4 ? FormatLocalizedOffset(offsetSeconds, count)
                                     : FormatRfcOffset(offsetSeconds, false);
                break;
            default:
                if (std::isalpha(static_cast<unsigned char>(token))) {
                    ThrowUnsupportedPattern(token);
                }
                result.append(count, token);
                break;
        }
        i += count;
    }
    return result;
}

/// DateFormat function
/// date_format(timestamp, format) -> varchar
/// Converts timestamp to string using the given Spark/Java DateTimeFormatter
/// pattern. Aligned with Spark's DateFormatClass and Velox sparksql DateFormatFunction.
class DateFormatFunction : public VectorFunction {
public:
    void Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, BaseVector *&result,
        op::ExecutionContext *context) const override
    {
        if (args.size() < 2) {
            OMNI_THROW("DateFormat function Error", "date_format requires exactly 2 arguments");
        }

        const auto formatArg = args.top();
        args.pop();
        const auto timestampArg = args.top();
        args.pop();

        int32_t size = 0;
        for (const auto *arg : {timestampArg, formatArg}) {
            if (arg->GetEncoding() != OMNI_ENCODING_CONST) {
                size = arg->GetSize();
                break;
            }
        }
        if (size == 0) {
            size = timestampArg->GetSize();
        }

        if (result == nullptr) {
            result = VectorHelper::CreateFlatVector(outputType->GetId(), size);
        }

        auto *resultFlatVector = static_cast<Vector<LargeStringContainer<std::string_view>> *>(result);

        // Resolve timezone from session config once per batch.
        // setenv("TZ") + tzset() are NOT thread-safe and modify global process
        // state, so they are called exactly once here, outside the per-row
        // loop. Once set, localtime_r reads tzname/timezone/daylight via the
        // process-wide state, which is sufficient for tm_gmtoff / tm_zone
        // population required by timezone tokens (v/z/O/X/x/Z).
        std::string timeZoneStr;
        if (context->queryConfig().AdjustTimestampToTimezone()) {
            timeZoneStr = context->queryConfig().SessionTimezone();
        }
        const bool useLocalTime = !timeZoneStr.empty();
        if (useLocalTime) {
            std::string normalizedTz = NormalizeTimeZone(timeZoneStr);
            setenv("TZ", normalizedTz.c_str(), 1);
            tzset();
        }

        bool tsIsConst = (timestampArg->GetEncoding() == OMNI_ENCODING_CONST);
        int64_t constTsValue = 0;
        const int64_t *tsRaw = nullptr;
        const uint64_t *tsNulls = nullptr;

        if (tsIsConst) {
            if (timestampArg->IsNull(0)) {
                auto *resultNulls = unsafe::UnsafeBaseVector::GetNulls(result);
                auto nullsSize = BitUtil::Nbytes(size);
                memset(resultNulls, 0xFF, nullsSize);
                delete timestampArg;
                delete formatArg;
                return;
            }
            constTsValue = static_cast<ConstVector<int64_t> *>(timestampArg)->GetConstValue();
        } else {
            auto *tsVector = reinterpret_cast<Vector<int64_t> *>(timestampArg);
            tsRaw = unsafe::UnsafeVector::GetRawValues(tsVector);
            tsNulls = reinterpret_cast<uint64_t *>(unsafe::UnsafeBaseVector::GetNulls(timestampArg));
        }

        bool fmtIsConst = (formatArg->GetEncoding() == OMNI_ENCODING_CONST);
        std::string constFormat;
        const uint64_t *fmtNulls = nullptr;

        if (fmtIsConst) {
            if (formatArg->IsNull(0)) {
                auto *resultNulls = unsafe::UnsafeBaseVector::GetNulls(result);
                auto nullsSize = BitUtil::Nbytes(size);
                memset(resultNulls, 0xFF, nullsSize);
                delete timestampArg;
                delete formatArg;
                return;
            }
            constFormat = std::string(GetStringValueFromVector(formatArg, 0));
        } else {
            fmtNulls = reinterpret_cast<uint64_t *>(unsafe::UnsafeBaseVector::GetNulls(formatArg));
        }

        auto *resultNulls = reinterpret_cast<uint64_t *>(unsafe::UnsafeBaseVector::GetNulls(result));
        auto nullsSize = BitUtil::Nbytes(size);
        if (tsIsConst) {
            memset(resultNulls, 0x00, nullsSize);
        } else {
            memcpy(resultNulls, tsNulls, nullsSize);
        }

        SelectivityVector rows(size);
        if (tsIsConst) {
            rows.setAll();
        } else {
            rows.setFromBitsNegate(tsNulls, size);
        }

        rows.applyToSelected([&](vector_size_t i) {
            if (!fmtIsConst && fmtNulls && BitUtil::IsBitSet(fmtNulls, i)) {
                result->SetNull(i);
                return;
            }

            int64_t micros = tsIsConst ? constTsValue : tsRaw[i];

            std::string sparkFormat;
            if (fmtIsConst) {
                sparkFormat = constFormat;
            } else {
                sparkFormat = std::string(GetStringValueFromVector(formatArg, i));
            }

            struct tm ltm = {};
            int64_t microInSecond = 0;
            ResolveLocalTime(micros, useLocalTime, ltm, microInSecond);

            std::string formatted;
            try {
                formatted = FormatTimestamp(ltm, microInSecond, sparkFormat, timeZoneStr);
            } catch (...) {
                result->SetNull(i);
                return;
            }

            resultFlatVector->SetValue(i, std::string_view(formatted));
            result->SetNotNull(i);
        });

        delete timestampArg;
        delete formatArg;
    }
};
} // namespace

void RegisterDateFormatFunction(const std::string &name)
{
    auto func = std::make_shared<DateFormatFunction>();

    // (TIMESTAMP, VARCHAR) -> VARCHAR
    VectorFunction::RegisterVectorFunction(name, {OMNI_TIMESTAMP, OMNI_VARCHAR}, OMNI_VARCHAR, func);
    // (LONG, VARCHAR) -> VARCHAR (OMNI_TIMESTAMP and OMNI_LONG are equivalent at runtime)
    VectorFunction::RegisterVectorFunction(name, {OMNI_LONG, OMNI_VARCHAR}, OMNI_VARCHAR, func);
}
}
