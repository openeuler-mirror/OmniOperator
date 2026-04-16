/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: FromUnixTime function implementation for vectorized execution
 */

#include "FromUnixTime.h"
#include "vector/vector.h"
#include "../VectorFunction.h"
#include "vectorization/SelectivityVector.h"
#include "vector/vector_helper.h"
#include "util/bit_util.h"
#include "util/config/QueryConfig.h"
#include <ctime>
#include <cstring>
#include <string>
#include <string_view>
#include <cmath>

namespace omniruntime::vectorization {
using namespace omniruntime::vec;
using namespace omniruntime::type;

namespace {

/// Convert Joda date/time format string to strftime format string.
/// Joda: yyyy-MM-dd HH:mm:ss  ->  strftime: %Y-%m-%d %H:%M:%S
std::string ConvertJodaToStrftime(const std::string &jodaFormat)
{
    std::string result;
    result.reserve(jodaFormat.size() * 2);
    size_t i = 0;
    size_t len = jodaFormat.size();
    while (i < len) {
        char c = jodaFormat[i];
        if (c == '\'') {
            // Joda: literal text enclosed in single quotes
            ++i;
            while (i < len && jodaFormat[i] != '\'') {
                result += jodaFormat[i];
                ++i;
            }
            if (i < len) {
                ++i;
            }
        } else if (c == 'y' || c == 'Y') {
            while (i < len && (jodaFormat[i] == 'y' || jodaFormat[i] == 'Y')) {
                ++i;
            }
            result += "%Y";
        } else if (c == 'M') {
            size_t count = 0;
            while (i < len && jodaFormat[i] == 'M') {
                ++count;
                ++i;
            }
            if (count == 3) {
                result += "%b"; // abbreviated month name
            } else if (count >= 4) {
                result += "%B"; // full month name
            } else {
                result += "%m"; // numeric month
            }
        } else if (c == 'd') {
            while (i < len && jodaFormat[i] == 'd') {
                ++i;
            }
            result += "%d";
        } else if (c == 'H') {
            while (i < len && jodaFormat[i] == 'H') {
                ++i;
            }
            result += "%H";
        } else if (c == 'h') {
            while (i < len && jodaFormat[i] == 'h') {
                ++i;
            }
            result += "%I"; // 12-hour clock
        } else if (c == 'm') {
            while (i < len && jodaFormat[i] == 'm') {
                ++i;
            }
            result += "%M";
        } else if (c == 's') {
            while (i < len && jodaFormat[i] == 's') {
                ++i;
            }
            result += "%S";
        } else if (c == 'S') {
            // Fractional seconds - skip for second-level precision
            while (i < len && jodaFormat[i] == 'S') {
                ++i;
            }
        } else if (c == 'a') {
            while (i < len && jodaFormat[i] == 'a') {
                ++i;
            }
            result += "%p"; // AM/PM
        } else if (c == 'D') {
            while (i < len && jodaFormat[i] == 'D') {
                ++i;
            }
            result += "%m/%d/%y"; // MM/dd/yy
        } else if (c == 'E') {
            // Day of week
            size_t count = 0;
            while (i < len && jodaFormat[i] == 'E') {
                ++count;
                ++i;
            }
            if (count >= 4) {
                result += "%A"; // full weekday name
            } else {
                result += "%a"; // abbreviated weekday name
            }
        } else if (c == 'w') {
            while (i < len && jodaFormat[i] == 'w') {
                ++i;
            }
            result += "%V"; // week of year (ISO)
        } else if (c == 'Z' || c == 'z') {
            // Time zone - skip
            while (i < len && (jodaFormat[i] == 'Z' || jodaFormat[i] == 'z')) {
                ++i;
            }
        } else {
            result += c;
            ++i;
        }
    }
    return result;
}

/// Convert "GMT+08:00" to POSIX-compatible "Etc/GMT-8".
/// POSIX/IANA use inverted sign convention: GMT+8 means UTC-8 in POSIX.
static std::string NormalizeTimeZone(const std::string &tzStr)
{
    if (tzStr == "GMT+08:00") {
        return "Etc/GMT-8";
    }
    return tzStr;
}

/// Format a timestamp (seconds since epoch) using the given strftime format.
/// If timeZoneStr is non-empty, sets TZ env and uses localtime_r (local time).
/// If timeZoneStr is empty, uses gmtime_r (UTC time).
/// This matches the codegen layer's approach for consistency with Spark.
std::string FormatTimestamp(int64_t secondsSinceEpoch, const std::string &strftimeFormat,
    const std::string &timeZoneStr)
{
    time_t timeStampVal = static_cast<time_t>(secondsSinceEpoch);
    struct tm ltm = {};

    if (!timeZoneStr.empty()) {
        // Set timezone environment variable and use localtime_r
        std::string normalizedTz = NormalizeTimeZone(timeZoneStr);
        setenv("TZ", normalizedTz.c_str(), 1);
        tzset();
        localtime_r(&timeStampVal, &ltm);
    } else {
        // No timezone specified: use UTC
        gmtime_r(&timeStampVal, &ltm);
    }

    // Format using strftime
    char buffer[256] = {};
    size_t resultLen = strftime(buffer, sizeof(buffer), strftimeFormat.c_str(), &ltm);
    if (resultLen == 0) {
        return "";
    }
    return std::string(buffer, resultLen);
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
    } else {
        return std::string_view();
    }
}

/// from_unixtime(unixtime, format) -> string
/// from_unixtime(unixtime, format, timezone) -> string
/// Converts a unix timestamp (seconds since epoch) to a formatted string.
/// unixtime: int64_t (seconds since epoch)
/// format: Joda date/time format string (e.g., "yyyy-MM-dd HH:mm:ss")
/// timezone: optional timezone string (e.g., "GMT+08:00", "Asia/Shanghai")
/// Returns NULL if input is NULL or format is invalid.
class FromUnixTimeFunction : public VectorFunction {
public:
    void Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, BaseVector *&result,
        op::ExecutionContext *context) const override
    {
        size_t argCount = args.size();
        if (argCount < 2) {
            OMNI_THROW("FromUnixTime function Error", "Expected at least 2 arguments (unixtime, format)");
        }

        // Parse arguments: stack top is last arg
        // 3-arg form: from_unixtime(unixtime, format, timezone) - Gluten path
        // 2-arg form: from_unixtime(unixtime, format) - direct path
        BaseVector *tzArg = nullptr;
        BaseVector *formatArg = nullptr;
        BaseVector *inputArg = nullptr;

        if (argCount == 3) {
            tzArg = args.top();
            args.pop();
            formatArg = args.top();
            args.pop();
            inputArg = args.top();
            args.pop();
        } else {
            formatArg = args.top();
            args.pop();
            inputArg = args.top();
            args.pop();
        }

        const auto size = inputArg->GetSize();

        if (result == nullptr) {
            result = VectorHelper::CreateFlatVector(OMNI_VARCHAR, size);
        }

        bool formatIsConst = (formatArg->GetEncoding() == OMNI_ENCODING_CONST);
        std::string constStrftimeFormat;
        bool constFormatValid = true;

        if (formatIsConst) {
            if (formatArg->IsNull(0)) {
                constFormatValid = false;
            } else {
                auto *constFormatVec = static_cast<ConstVector<std::string_view> *>(formatArg);
                std::string_view formatView = constFormatVec->GetConstValue();
                std::string jodaFormat(formatView);
                constStrftimeFormat = ConvertJodaToStrftime(jodaFormat);
            }
        }

        // Resolve timezone string: prefer explicit tzArg, fallback to session config
        std::string timeZoneStr;
        if (tzArg != nullptr) {
            bool tzIsConst = (tzArg->GetEncoding() == OMNI_ENCODING_CONST);
            if (tzIsConst && !tzArg->IsNull(0)) {
                timeZoneStr = std::string(GetStringValueFromVector(tzArg, 0));
            }
        }
        if (timeZoneStr.empty()) {
            // Fallback to session timezone from config
            if (context->queryConfig().AdjustTimestampToTimezone()) {
                timeZoneStr = context->queryConfig().SessionTimezone();
            }
        }

        auto *resultVec = static_cast<Vector<LargeStringContainer<std::string_view>> *>(result);

        for (int32_t row = 0; row < size; ++row) {
            if (inputArg->IsNull(row)) {
                result->SetNull(row);
                continue;
            }

            if (!formatIsConst && formatArg->IsNull(row)) {
                result->SetNull(row);
                continue;
            }

            // Get unix seconds value
            int64_t unixSeconds = 0;
            const auto inputTypeId = inputArg->GetTypeId();
            if (inputTypeId == OMNI_LONG || inputTypeId == OMNI_TIMESTAMP) {
                auto *inputVec = static_cast<Vector<int64_t> *>(inputArg);
                unixSeconds = inputVec->GetValue(row);
            } else {
                result->SetNull(row);
                continue;
            }

            // Get format string
            std::string strftimeFormat;
            if (formatIsConst) {
                if (!constFormatValid) {
                    result->SetNull(row);
                    continue;
                }
                strftimeFormat = constStrftimeFormat;
            } else {
                std::string_view formatView = GetStringValueFromVector(formatArg, row);
                std::string jodaFormat(formatView);
                strftimeFormat = ConvertJodaToStrftime(jodaFormat);
            }

            // Resolve per-row timezone if tzArg is non-const
            std::string rowTzStr = timeZoneStr;
            if (tzArg != nullptr && tzArg->GetEncoding() != OMNI_ENCODING_CONST) {
                if (!tzArg->IsNull(row)) {
                    rowTzStr = std::string(GetStringValueFromVector(tzArg, row));
                } else {
                    rowTzStr = "";
                }
                if (rowTzStr.empty()) {
                    if (context->queryConfig().AdjustTimestampToTimezone()) {
                        rowTzStr = context->queryConfig().SessionTimezone();
                    }
                }
            }

            // Format the timestamp
            std::string formatted = FormatTimestamp(unixSeconds, strftimeFormat, rowTzStr);
            if (formatted.empty()) {
                result->SetNull(row);
                continue;
            }

            resultVec->SetValue(row, std::string_view(formatted));
            result->SetNotNull(row);
        }

        if (inputArg != nullptr) {
            delete inputArg;
        }
        if (formatArg != nullptr) {
            delete formatArg;
        }
        if (tzArg != nullptr) {
            delete tzArg;
        }
    }
};

} // namespace

void RegisterFromUnixTimeFunction(const std::string &name)
{
    auto fromUnixTimeFunction = std::make_shared<FromUnixTimeFunction>();
    // 2-arg signatures: from_unixtime(bigint, varchar) -> varchar
    VectorFunction::RegisterVectorFunction(name, {OMNI_LONG, OMNI_VARCHAR}, OMNI_VARCHAR,
        fromUnixTimeFunction);
    VectorFunction::RegisterVectorFunction(name, {OMNI_TIMESTAMP, OMNI_VARCHAR}, OMNI_VARCHAR,
        fromUnixTimeFunction);
    // 3-arg signatures: from_unixtime(bigint, varchar, varchar) -> varchar
    // The 3rd argument is timezone string, matching Gluten's calling convention
    VectorFunction::RegisterVectorFunction(name, {OMNI_LONG, OMNI_VARCHAR, OMNI_VARCHAR}, OMNI_VARCHAR,
        fromUnixTimeFunction);
    VectorFunction::RegisterVectorFunction(name, {OMNI_TIMESTAMP, OMNI_VARCHAR, OMNI_VARCHAR}, OMNI_VARCHAR,
        fromUnixTimeFunction);
}

}
