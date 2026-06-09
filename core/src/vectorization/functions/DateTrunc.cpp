/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: DateTrunc function implementation
 * Truncates a timestamp or date value to the specified precision.
 * Follows Velox Spark SQL date_trunc semantics, supporting all precision levels:
 *   MICROSECOND, MILLISECOND, SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER, YEAR
 *
 * date_trunc(precision, timestamp) -> timestamp (microseconds since epoch)
 * date_trunc(precision, date)     -> date     (days since epoch)
 *
 * For TIMESTAMP input:
 *   - Sub-day truncations (MICROSECOND to HOUR) use direct arithmetic on
 *     the microsecond value for optimal performance.
 *   - Day-level and above (DAY to YEAR) use calendar time manipulation
 *     via Timestamp::epochToCalendarUtc / calendarUtcToEpoch.
 *
 * For DATE32 input:
 *   - Only DAY, WEEK, MONTH, QUARTER, YEAR are valid.
 *   - Uses the existing Date32::TruncDate infrastructure.
 */

#include "DateTrunc.h"
#include "vector/vector.h"
#include "../VectorFunction.h"
#include "vectorization/SelectivityVector.h"
#include "type/date32.h"
#include "type/Timestamp.h"
#include "type/tz/TimeZoneMap.h"
#include "vector/vector_helper.h"
#include "util/bit_util.h"
#include <ctime>
#include <chrono>
#include <cstring>
#include <string>
#include <string_view>

namespace omniruntime::vectorization {
using namespace omniruntime::vec;
using namespace omniruntime::type;

namespace {

/// Floor division for int64_t (rounds toward negative infinity).
/// C++ integer division truncates toward zero, which is incorrect for
/// negative timestamps.  E.g. -1'500'000 μs / 1'000'000 = -1 in C++,
/// but the correct second for -1.5 s is -2 (floor toward −∞).
static inline int64_t FloorDiv(int64_t a, int64_t b) {
    int64_t q = a / b;
    if (a < 0 && a % b != 0) {
        q -= 1;
    }
    return q;
}

/// Truncates the calendar time fields according to the specified level.
/// Modifies the tm struct in-place, setting finer-grained fields to their
/// minimum values (0 for hour/minute/second, 1 for day/month).
static void TruncateCalendarFields(std::tm &tmValue, DateTruncMode level)
{
    switch (level) {
        case DateTruncMode::TRUNC_TO_MICROSECOND:
        case DateTruncMode::TRUNC_TO_MILLISECOND:
        case DateTruncMode::TRUNC_TO_SECOND: {
            // Already handled at the microsecond-arithmetic level.
            // No calendar field changes needed.
            break;
        }
        case DateTruncMode::TRUNC_TO_MINUTE: {
            tmValue.tm_sec = 0;
            break;
        }
        case DateTruncMode::TRUNC_TO_HOUR: {
            tmValue.tm_min = 0;
            tmValue.tm_sec = 0;
            break;
        }
        case DateTruncMode::TRUNC_TO_DAY: {
            tmValue.tm_hour = 0;
            tmValue.tm_min = 0;
            tmValue.tm_sec = 0;
            break;
        }
        case DateTruncMode::TRUNC_TO_WEEK: {
            // Go back to the Monday of the current week.
            // tm_wday: 0=Sunday, 1=Monday, ..., 6=Saturday
            // Convert to ISO weekday: 1=Monday, ..., 7=Sunday
            int32_t isoWeekday = (tmValue.tm_wday == 0) ? 7 : tmValue.tm_wday;
            // Subtract (isoWeekday - 1) days to get to Monday
            tmValue.tm_mday -= (isoWeekday - 1);
            tmValue.tm_hour = 0;
            tmValue.tm_min = 0;
            tmValue.tm_sec = 0;
            break;
        }
        case DateTruncMode::TRUNC_TO_MONTH: {
            tmValue.tm_mday = 1;
            tmValue.tm_hour = 0;
            tmValue.tm_min = 0;
            tmValue.tm_sec = 0;
            break;
        }
        case DateTruncMode::TRUNC_TO_QUARTER: {
            // Set to first month of the quarter
            tmValue.tm_mon = (tmValue.tm_mon / 3) * 3;
            tmValue.tm_mday = 1;
            tmValue.tm_hour = 0;
            tmValue.tm_min = 0;
            tmValue.tm_sec = 0;
            break;
        }
        case DateTruncMode::TRUNC_TO_YEAR: {
            tmValue.tm_mon = 0;  // January
            tmValue.tm_mday = 1;
            tmValue.tm_hour = 0;
            tmValue.tm_min = 0;
            tmValue.tm_sec = 0;
            break;
        }
        default: {
            // Invalid level - throw or handle gracefully
            break;
        }
    }
}

/// Truncates a TIMESTAMP value (microseconds since epoch) to the specified
/// precision level. For sub-DAY levels, uses direct arithmetic. For DAY and
/// above, uses calendar-time manipulation.
/// @param timestampMicros Input timestamp in microseconds since epoch
/// @param level Truncation precision level
/// @param timeZone Optional timezone for local-time truncation (DAY+ levels)
/// @return Truncated timestamp in microseconds since epoch
static int64_t TruncateTimestamp(int64_t timestampMicros, DateTruncMode level,
    const tz::TimeZone *timeZone = nullptr)
{
    switch (level) {
        case DateTruncMode::TRUNC_TO_MICROSECOND: {
            return timestampMicros;
        }
        case DateTruncMode::TRUNC_TO_MILLISECOND: {
            return FloorDiv(timestampMicros, 1000LL) * 1000LL;
        }
        case DateTruncMode::TRUNC_TO_SECOND: {
            return FloorDiv(timestampMicros, 1000000LL) * 1000000LL;
        }
        case DateTruncMode::TRUNC_TO_MINUTE:
        case DateTruncMode::TRUNC_TO_HOUR:
        case DateTruncMode::TRUNC_TO_DAY:
        case DateTruncMode::TRUNC_TO_WEEK:
        case DateTruncMode::TRUNC_TO_MONTH:
        case DateTruncMode::TRUNC_TO_QUARTER:
        case DateTruncMode::TRUNC_TO_YEAR: {
            int64_t utcSeconds = FloorDiv(timestampMicros, 1000000LL);

            if (timeZone != nullptr) {
                // Timezone-aware path for MINUTE and above (Spark applies
                // timezone to all levels ≥ MINUTE; non-whole-hour offsets
                // like +05:30 would otherwise cause discrepancies).
                auto localSeconds = timeZone->to_local(
                    std::chrono::seconds(utcSeconds));
                int64_t localEpoch = localSeconds.count();

                std::tm tmValue;
                if (!Timestamp::epochToCalendarUtc(localEpoch, tmValue)) {
                    return timestampMicros;
                }

                TruncateCalendarFields(tmValue, level);
                int64_t truncatedLocalEpoch =
                    Timestamp::calendarUtcToEpoch(tmValue);

                auto utcResult = timeZone->to_sys(
                    std::chrono::seconds(truncatedLocalEpoch));
                return static_cast<int64_t>(utcResult.count()) * 1000000LL;
            }

            // Direct arithmetic path (UTC / no timezone).
            // For MINUTE and HOUR this is trivially correct for whole-hour
            // timezones; for DAY+ we fall through to calendar conversion.
            if (level == DateTruncMode::TRUNC_TO_MINUTE) {
                return FloorDiv(timestampMicros, 60000000LL) * 60000000LL;
            }
            if (level == DateTruncMode::TRUNC_TO_HOUR) {
                return FloorDiv(timestampMicros, 3600000000LL) * 3600000000LL;
            }

            // DAY and above: calendar-based UTC truncation.
            std::tm tmValue;
            if (!Timestamp::epochToCalendarUtc(utcSeconds, tmValue)) {
                return timestampMicros;
            }

            TruncateCalendarFields(tmValue, level);
            int64_t truncatedSeconds =
                Timestamp::calendarUtcToEpoch(tmValue);

            return truncatedSeconds * 1000000LL;
        }
        default: {
            return timestampMicros;
        }
    }
}

/// DateTruncFunction: date_trunc(precision VARCHAR, input TIMESTAMP) -> TIMESTAMP
/// Also: date_trunc(precision VARCHAR, input DATE32) -> DATE32
///
/// This function uses the existing DateTruncMode/ParseTruncLevel infrastructure
/// from type/date32.h for parsing precision format strings, ensuring consistency
/// with the existing Trunc (trunc_date) function.
class DateTruncFunction : public VectorFunction {
public:
    void Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, BaseVector *&result,
        op::ExecutionContext *context) const override
    {
        if (args.size() < 2) {
            return;
        }

        // Extract arguments from stack: the evaluator visits children
        // in order (format, then timestamp), pushing each result onto the stack.
        // So timestamp is on top (pushed last), format is below (pushed first).
        const auto inputArg = args.top();
        args.pop();
        const auto formatArg = args.top();
        args.pop();

        const auto size = inputArg->GetSize();
        const auto inputTypeId = inputArg->GetTypeId();

        // Create result vector if it doesn't exist
        if (result == nullptr) {
            result = VectorHelper::CreateFlatVector(outputType->GetId(), size);
        }

        // Route to the appropriate handler based on input type
        // Retrieve session timezone from query context (e.g. "Asia/Shanghai").
        const tz::TimeZone *timeZone = nullptr;
        std::string tzName = context->queryConfigRef().SessionTimezone();
        if (!tzName.empty()) {
            timeZone = tz::locateZone(tzName, /*failOnError=*/false);
        }

        if (inputTypeId == OMNI_TIMESTAMP || inputTypeId == OMNI_LONG) {
            ProcessTimestamp(formatArg, inputArg, result, size, timeZone);
        } else if (inputTypeId == OMNI_DATE32 || inputTypeId == OMNI_INT) {
            ProcessDate(formatArg, inputArg, result, size);
        }

        // Clean up temporary arguments
        delete formatArg;
        delete inputArg;
    }

private:
    /// Handles TIMESTAMP input (microseconds since epoch)
    void ProcessTimestamp(BaseVector *formatArg, BaseVector *inputArg, BaseVector *result,
        vector_size_t size, const tz::TimeZone *timeZone = nullptr) const
    {
        auto *inputVector = reinterpret_cast<Vector<int64_t> *>(inputArg);
        const auto *inputRaw = unsafe::UnsafeVector::GetRawValues(inputVector);
        const auto *inputNulls = reinterpret_cast<uint64_t *>(unsafe::UnsafeBaseVector::GetNulls(inputArg));

        auto *resultVector = reinterpret_cast<Vector<int64_t> *>(result);
        auto *resultRaw = unsafe::UnsafeVector::GetRawValues(resultVector);
        auto *resultNulls = reinterpret_cast<uint64_t *>(unsafe::UnsafeBaseVector::GetNulls(result));

        // Copy NULL bits from input to result
        auto nullsSize = BitUtil::Nbytes(size);
        memcpy(resultNulls, inputNulls, nullsSize);

        // Process only non-NULL rows
        SelectivityVector rows(size);
        rows.setFromBitsNegate(inputNulls, size);

        // CONST and DICTIONARY encodings represent a literal format
        // string — pre-parse it once rather than in the per-row loop.
        auto formatEnc = formatArg->GetEncoding();
        bool formatConstOrDict = (formatEnc == OMNI_ENCODING_CONST ||
                                  formatEnc == OMNI_DICTIONARY);
        DateTruncMode constLevel = DateTruncMode::TRUNC_INVALID;
        std::string constFormat;

        // Pre-parse format for efficiency
        if (formatConstOrDict) {
            std::string_view formatView = GetStringValueFromVector(formatArg, 0);
            constFormat = std::string(formatView);
            constLevel = Date32::ParseTruncLevel(constFormat);
            // If format is invalid, mark all as null
            if (constLevel == DateTruncMode::TRUNC_INVALID) {
                for (int32_t i = 0; i < size; ++i) {
                    result->SetNull(i);
                }
                return;
            }
        }

        rows.applyToSelected([&](vector_size_t i) {
            // Check if format is NULL (for non-const / non-dict format)
            if (!formatConstOrDict) {
                const auto *formatNulls = reinterpret_cast<uint64_t *>(unsafe::UnsafeBaseVector::GetNulls(formatArg));
                if (formatNulls && BitUtil::IsBitSet(formatNulls, i)) {
                    result->SetNull(i);
                    return;
                }
            }

            // Get format string and parse truncation level
            DateTruncMode level;
            if (formatConstOrDict) {
                level = constLevel;
            } else {
                std::string_view formatView = GetStringValueFromVector(formatArg, i);
                std::string formatStr(formatView);
                level = Date32::ParseTruncLevel(formatStr);
            }

            // Check if format is invalid
            if (level == DateTruncMode::TRUNC_INVALID) {
                result->SetNull(i);
                return;
            }

            // Perform truncation
            int64_t timestampMicros = inputRaw[i];
            int64_t truncatedValue =
                TruncateTimestamp(timestampMicros, level, timeZone);
            resultRaw[i] = truncatedValue;
            result->SetNotNull(i);
        });
    }

    /// Handles DATE32 input (days since epoch)
    void ProcessDate(BaseVector *formatArg, BaseVector *inputArg, BaseVector *result,
        vector_size_t size) const
    {
        auto *inputVector = reinterpret_cast<Vector<int32_t> *>(inputArg);
        const auto *inputRaw = unsafe::UnsafeVector::GetRawValues(inputVector);
        const auto *inputNulls = reinterpret_cast<uint64_t *>(unsafe::UnsafeBaseVector::GetNulls(inputArg));

        auto *resultVector = reinterpret_cast<Vector<int32_t> *>(result);
        auto *resultRaw = unsafe::UnsafeVector::GetRawValues(resultVector);
        auto *resultNulls = reinterpret_cast<uint64_t *>(unsafe::UnsafeBaseVector::GetNulls(result));

        // Copy NULL bits from input to result
        auto nullsSize = BitUtil::Nbytes(size);
        memcpy(resultNulls, inputNulls, nullsSize);

        // Process only non-NULL rows
        SelectivityVector rows(size);
        rows.setFromBitsNegate(inputNulls, size);

        // CONST and DICTIONARY encodings represent a literal format
        // string — pre-parse it once rather than in the per-row loop.
        auto formatEnc = formatArg->GetEncoding();
        bool formatConstOrDict = (formatEnc == OMNI_ENCODING_CONST ||
                                  formatEnc == OMNI_DICTIONARY);
        DateTruncMode constLevel = DateTruncMode::TRUNC_INVALID;
        std::string constFormat;

        // Pre-parse format for efficiency
        if (formatConstOrDict) {
            std::string_view formatView = GetStringValueFromVector(formatArg, 0);
            constFormat = std::string(formatView);
            constLevel = Date32::ParseTruncLevel(constFormat);
            // If format is invalid, mark all as null
            if (constLevel == DateTruncMode::TRUNC_INVALID) {
                for (int32_t i = 0; i < size; ++i) {
                    result->SetNull(i);
                }
                return;
            }
        }

        rows.applyToSelected([&](vector_size_t i) {
            // Check if format is NULL (for non-const / non-dict format)
            if (!formatConstOrDict) {
                const auto *formatNulls = reinterpret_cast<uint64_t *>(unsafe::UnsafeBaseVector::GetNulls(formatArg));
                if (formatNulls && BitUtil::IsBitSet(formatNulls, i)) {
                    result->SetNull(i);
                    return;
                }
            }

            // Get format string and parse truncation level
            DateTruncMode level;
            if (formatConstOrDict) {
                level = constLevel;
            } else {
                std::string_view formatView = GetStringValueFromVector(formatArg, i);
                std::string formatStr(formatView);
                level = Date32::ParseTruncLevel(formatStr);
            }

            // Check if format is invalid
            if (level == DateTruncMode::TRUNC_INVALID) {
                result->SetNull(i);
                return;
            }

            int32_t daysSinceEpoch = inputRaw[i];

            // For DATE32:
            // - MICROSECOND/MILLISECOND/SECOND/MINUTE/HOUR: these are time-level
            //   truncations and don't apply to dates. Treat as DAY (identity).
            // - DAY: return as-is (date is already at day granularity)
            // - WEEK/MONTH/QUARTER/YEAR: use existing TruncDate infrastructure
            if (level <= DateTruncMode::TRUNC_TO_DAY) {
                // Time-level truncations on a date: return the date as-is.
                resultRaw[i] = daysSinceEpoch;
                result->SetNotNull(i);
            } else {
                // Week/month/quarter/year truncation using existing infrastructure
                int32_t truncatedDays = 0;
                if (Date32::TruncDate(daysSinceEpoch, level, truncatedDays) == CONVERT_SUCCESS) {
                    resultRaw[i] = truncatedDays;
                    result->SetNotNull(i);
                } else {
                    result->SetNull(i);
                }
            }
        });
    }

    /// Helper: extract string value from vector with different encodings
    static std::string_view GetStringValueFromVector(BaseVector *vec, int32_t row)
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
};

} // namespace

void RegisterDateTruncFunction(const std::string &name)
{
    // date_trunc(format VARCHAR, timestamp TIMESTAMP) -> TIMESTAMP
    // Supports: MICROSECOND, MILLISECOND, SECOND, MINUTE, HOUR, DAY, WEEK,
    //           MONTH, QUARTER, YEAR
    VectorFunction::RegisterVectorFunction(name, {OMNI_VARCHAR, OMNI_TIMESTAMP}, OMNI_TIMESTAMP,
        std::make_shared<DateTruncFunction>());

    // date_trunc(format VARCHAR, date DATE32) -> DATE32
    // Supports: DAY, WEEK, MONTH, QUARTER, YEAR
    // (Time-level truncations like HOUR/MINUTE/SECOND are treated as identity
    //  for date inputs, consistent with Spark SQL behavior)
    VectorFunction::RegisterVectorFunction(name, {OMNI_VARCHAR, OMNI_DATE32}, OMNI_DATE32,
        std::make_shared<DateTruncFunction>());
}
}