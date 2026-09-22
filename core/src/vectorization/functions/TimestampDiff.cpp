/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: TimestampDiff function implementation
 *
 * TIMESTAMPDIFF(timeunit VARCHAR, timestamp1 TIMESTAMP, timestamp2 TIMESTAMP) -> BIGINT
 *
 * Returns the (signed) number of units between timepoint1 and timepoint2, computed as
 * timestamp1 - timestamp2, matching Flink's TimestampDiffCallGen:
 *   SECOND/MINUTE/HOUR/DAY -> (timepoint1 - timepoint2) / unitInMillis
 *   MONTH/YEAR             -> DateTimeUtils.subtractMonths(timepoint1, timepoint2) / multiplier
 * Supported time units: SECOND, MINUTE, HOUR, DAY, MONTH, YEAR
 *
 * Flink TIMESTAMP is represented natively as OMNI_LONG milliseconds since epoch,
 * so the time-based units are computed by integer division of the millisecond difference.
 */

#include "TimestampDiff.h"
#include "vector/vector.h"
#include "../VectorFunction.h"
#include "vectorization/SelectivityVector.h"
#include "type/date32.h"
#include "type/Timestamp.h"
#include "vector/vector_helper.h"
#include "util/bit_util.h"
#include <ctime>
#include <cstring>
#include <string>
#include <string_view>
#include <algorithm>
#include <cctype>

namespace omniruntime::vectorization {
using namespace omniruntime::vec;
using namespace omniruntime::type;

namespace {

/// Time unit enumeration for TIMESTAMPDIFF
enum TimeUnitKind {
    UNIT_SECOND,
    UNIT_MINUTE,
    UNIT_HOUR,
    UNIT_DAY,
    UNIT_MONTH,
    UNIT_YEAR,
    UNIT_INVALID
};

/// Case-insensitive string comparison
static bool EqualsIgnoreCase(std::string_view a, const char* b)
{
    size_t bLen = std::strlen(b);
    if (a.size() != bLen) {
        return false;
    }
    for (size_t i = 0; i < a.size(); ++i) {
        if (std::tolower(static_cast<unsigned char>(a[i])) !=
            std::tolower(static_cast<unsigned char>(b[i]))) {
            return false;
        }
    }
    return true;
}

/// Parse time unit string to enum
static TimeUnitKind ParseTimeUnit(std::string_view unit)
{
    if (EqualsIgnoreCase(unit, "SECOND") || EqualsIgnoreCase(unit, "SQL_TSI_SECOND")) {
        return UNIT_SECOND;
    }
    if (EqualsIgnoreCase(unit, "MINUTE") || EqualsIgnoreCase(unit, "SQL_TSI_MINUTE")) {
        return UNIT_MINUTE;
    }
    if (EqualsIgnoreCase(unit, "HOUR") || EqualsIgnoreCase(unit, "SQL_TSI_HOUR")) {
        return UNIT_HOUR;
    }
    if (EqualsIgnoreCase(unit, "DAY") || EqualsIgnoreCase(unit, "SQL_TSI_DAY")) {
        return UNIT_DAY;
    }
    if (EqualsIgnoreCase(unit, "MONTH") || EqualsIgnoreCase(unit, "SQL_TSI_MONTH")) {
        return UNIT_MONTH;
    }
    if (EqualsIgnoreCase(unit, "YEAR") || EqualsIgnoreCase(unit, "SQL_TSI_YEAR")) {
        return UNIT_YEAR;
    }
    return UNIT_INVALID;
}

/// Millisecond constants (Flink TIMESTAMP native representation is epoch millis)
static constexpr int64_t kMillisPerSecond = 1000LL;
static constexpr int64_t kMillisPerMinute = 60LL * kMillisPerSecond;
static constexpr int64_t kMillisPerHour = 60LL * kMillisPerMinute;
static constexpr int64_t kMillisPerDay = 24LL * kMillisPerHour;

/// Calcite/Flink floor division and modulo (round toward negative infinity).
/// Call sites pass positive compile-time constants (12, 31, kMillisPerDay); the
/// guard keeps the division and remainder well-defined for any divisor.
static int64_t FloorDiv(int64_t x, int64_t y)
{
    OMNI_CHECK(y != 0, "FloorDiv divisor must not be zero");
    int64_t q = x / y;
    if ((x % y != 0) && ((x < 0) != (y < 0))) {
        --q;
    }
    return q;
}

static int64_t FloorMod(int64_t x, int64_t y)
{
    OMNI_CHECK(y != 0, "FloorMod divisor must not be zero");
    return x - FloorDiv(x, y) * y;
}

/// Proleptic Gregorian decomposition of an epoch day count (days since 1970-01-01).
static void EpochDayToYmd(int32_t epochDay, int32_t &year, int32_t &month, int32_t &day)
{
    int64_t z = static_cast<int64_t>(epochDay) + 719468;
    int64_t era = (z >= 0 ? z : z - 146096) / 146097;
    int64_t doe = z - era * 146097;                                       // [0, 146096]
    int64_t yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;  // [0, 399]
    int64_t y = yoe + era * 400;
    int64_t doy = doe - (365 * yoe + yoe / 4 - yoe / 100);                // [0, 365]
    int64_t mp = (5 * doy + 2) / 153;                                     // [0, 11]
    int64_t d = doy - (153 * mp + 2) / 5 + 1;                             // [1, 31]
    int64_t m = mp < 10 ? mp + 3 : mp - 9;                                // [1, 12]
    year = static_cast<int32_t>(y + (m <= 2 ? 1 : 0));
    month = static_cast<int32_t>(m);
    day = static_cast<int32_t>(d);
}

/// Inverse of EpochDayToYmd.
static int32_t YmdToEpochDay(int32_t year, int32_t month, int32_t day)
{
    int64_t y = year;
    int64_t m = month;
    y -= (m <= 2 ? 1 : 0);
    int64_t era = (y >= 0 ? y : y - 399) / 400;
    int64_t yoe = y - era * 400;                                       // [0, 399]
    int64_t doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + day - 1;    // [0, 365]
    int64_t doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;               // [0, 146096]
    return static_cast<int32_t>(era * 146097 + doe - 719468);
}

/// Last day of the given month (Calcite DateTimeUtils.lastDay).
static int32_t LastDayOfMonth(int32_t y, int32_t m)
{
    switch (m) {
        case 2:
            return (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) ? 29 : 28;
        case 4:
        case 6:
        case 9:
        case 11:
            return 30;
        default:
            return 31;
    }
}

/// Calcite DateTimeUtils.addMonths(int date, int m): shift an epoch day count by m
/// months, clamping the day-of-month to the last valid day of the target month.
static int32_t AddMonthsToDate(int32_t date, int32_t m)
{
    int32_t y0, m0, d0;
    EpochDayToYmd(date, y0, m0, d0);
    m0 += m;
    int32_t deltaYear = static_cast<int32_t>(FloorDiv(m0, 12));
    y0 += deltaYear;
    m0 = static_cast<int32_t>(FloorMod(m0, 12));
    if (m0 == 0) {
        y0 -= 1;
        m0 += 12;
    }
    int32_t last = LastDayOfMonth(y0, m0);
    if (d0 > last) {
        d0 = last;
    }
    return YmdToEpochDay(y0, m0, d0);
}

/// Calcite DateTimeUtils.subtractMonths(int date0, int date1): whole months from
/// date1 to date0 at day granularity.
static int32_t SubtractMonthsOfDate(int32_t date0, int32_t date1)
{
    if (date0 < date1) {
        return -SubtractMonthsOfDate(date1, date0);
    }
    // (date0 - date1) / 31 under-estimates the month count because no month is
    // longer than 31 days, so advance while the shifted date1 still does not
    // pass date0. Stops at the largest m with date1 + m months <= date0.
    int32_t m = (date0 - date1) / 31;
    while (AddMonthsToDate(date1, m + 1) <= date0) {
        ++m;
    }
    return m;
}

/// Calcite DateTimeUtils.subtractMonths(long t0, long t1): month difference between
/// two epoch-millis timestamps; this is Flink's basis for MONTH/YEAR/QUARTER.
static int64_t SubtractMonthsOfMillis(int64_t t0, int64_t t1)
{
    int64_t millis0 = FloorMod(t0, kMillisPerDay);
    int32_t d0 = static_cast<int32_t>(FloorDiv(t0 - millis0, kMillisPerDay));
    int64_t millis1 = FloorMod(t1, kMillisPerDay);
    int32_t d1 = static_cast<int32_t>(FloorDiv(t1 - millis1, kMillisPerDay));
    int32_t x = SubtractMonthsOfDate(d0, d1);
    int32_t d2 = AddMonthsToDate(d1, x);
    if (d2 == d0 && millis0 < millis1) {
        --x;
    }
    return static_cast<int64_t>(x);
}

/// Compute difference for time-based units (SECOND/MINUTE/HOUR/DAY).
/// Flink codegen: (timepoint1 - timepoint2) / unitInMillis
static int64_t ComputeTimeDiff(int64_t ts1Millis, int64_t ts2Millis, TimeUnitKind unit)
{
    int64_t diffMillis = ts1Millis - ts2Millis;
    int64_t millisPerUnit;
    switch (unit) {
        case UNIT_SECOND: millisPerUnit = kMillisPerSecond; break;
        case UNIT_MINUTE: millisPerUnit = kMillisPerMinute; break;
        case UNIT_HOUR:   millisPerUnit = kMillisPerHour; break;
        case UNIT_DAY:    millisPerUnit = kMillisPerDay; break;
        default: return 0; // Should not reach here
    }
    return diffMillis / millisPerUnit;
}

/// Compute difference for calendar-based units (MONTH/YEAR).
/// Flink codegen: subtractMonths(timepoint1, timepoint2) / unit.multiplier
static bool ComputeCalendarDiff(int64_t ts1Millis, int64_t ts2Millis, TimeUnitKind unit,
                                int64_t &result)
{
    int64_t months = SubtractMonthsOfMillis(ts1Millis, ts2Millis);
    if (unit == UNIT_MONTH) {
        result = months;
    } else { // UNIT_YEAR (multiplier 12, integer division truncates toward zero)
        result = months / 12;
    }
    return true;
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

class TimestampDiffFunction : public VectorFunction {
public:
    void Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, BaseVector *&result,
        op::ExecutionContext *context) const override
    {
        if (args.size() < 3) {
            return;
        }

        // Extract arguments from stack (LIFO order). The eval stack pushes the
        // arguments in declaration order, so the LAST argument is on top:
        // Stack: ts2 (top), ts1, unit (bottom)
        const auto ts2Arg = args.top();
        args.pop();
        const auto ts1Arg = args.top();
        args.pop();
        const auto unitArg = args.top();
        args.pop();

        const auto size = ts1Arg->GetSize();

        // Create result vector if it doesn't exist
        if (result == nullptr) {
            result = VectorHelper::CreateFlatVector(outputType->GetId(), size);
        }

        auto *resultVector = reinterpret_cast<Vector<int64_t> *>(result);
        auto *resultRaw = unsafe::UnsafeVector::GetRawValues(resultVector);
        auto *resultNulls = reinterpret_cast<uint64_t *>(unsafe::UnsafeBaseVector::GetNulls(result));

        // Get timestamp1 values
        auto *ts1Vector = reinterpret_cast<Vector<int64_t> *>(ts1Arg);
        const auto *ts1Raw = unsafe::UnsafeVector::GetRawValues(ts1Vector);
        const auto *ts1Nulls = reinterpret_cast<uint64_t *>(unsafe::UnsafeBaseVector::GetNulls(ts1Arg));

        // Get timestamp2 values
        auto *ts2Vector = reinterpret_cast<Vector<int64_t> *>(ts2Arg);
        const auto *ts2Raw = unsafe::UnsafeVector::GetRawValues(ts2Vector);
        const auto *ts2Nulls = reinterpret_cast<uint64_t *>(unsafe::UnsafeBaseVector::GetNulls(ts2Arg));

        // Check if unit is constant
        bool unitIsConst = (unitArg->GetEncoding() == OMNI_ENCODING_CONST ||
                            unitArg->GetEncoding() == OMNI_DICTIONARY);
        TimeUnitKind constUnit = UNIT_INVALID;

        if (unitIsConst) {
            std::string_view unitView = GetStringValueFromVector(unitArg, 0);
            constUnit = ParseTimeUnit(unitView);
        }

        // Copy NULL bits from ts1 input to result
        auto nullsSize = BitUtil::Nbytes(size);
        if (ts1Nulls != nullptr) {
            memcpy(resultNulls, ts1Nulls, nullsSize);
        } else {
            memset(resultNulls, 0, nullsSize);
        }

        // Merge NULL bits from ts2
        if (ts2Nulls != nullptr) {
            for (int32_t i = 0; i < size; ++i) {
                if (BitUtil::IsBitSet(ts2Nulls, i)) {
                    BitUtil::SetBit(resultNulls, i);
                }
            }
        }

        // If constant unit is invalid, mark all as null
        if (unitIsConst && constUnit == UNIT_INVALID) {
            // Check if unit itself is NULL
            if (unitArg->IsNull(0)) {
                memset(resultNulls, 0xFF, nullsSize);
            } else {
                memset(resultNulls, 0xFF, nullsSize);
            }
            delete unitArg;
            delete ts1Arg;
            delete ts2Arg;
            return;
        }

        // Process only non-NULL rows
        SelectivityVector rows(size);
        rows.setFromBitsNegate(resultNulls, size);

        rows.applyToSelected([&](vector_size_t i) {
            // Check if unit is NULL (for non-const case)
            if (!unitIsConst) {
                const auto *unitNullsLocal = reinterpret_cast<uint64_t *>(unsafe::UnsafeBaseVector::GetNulls(unitArg));
                if (unitNullsLocal && BitUtil::IsBitSet(unitNullsLocal, i)) {
                    result->SetNull(i);
                    return;
                }
            }

            // Get time unit
            TimeUnitKind unit;
            if (unitIsConst) {
                unit = constUnit;
            } else {
                std::string_view unitView = GetStringValueFromVector(unitArg, i);
                std::string unitStr(unitView);
                unit = ParseTimeUnit(unitStr);
            }

            if (unit == UNIT_INVALID) {
                result->SetNull(i);
                return;
            }

            // Get timestamps
            int64_t ts1Millis = ts1Raw[i];
            int64_t ts2Millis = ts2Raw[i];

            // Perform the difference calculation
            int64_t resultValue;
            if (unit == UNIT_SECOND || unit == UNIT_MINUTE || unit == UNIT_HOUR || unit == UNIT_DAY) {
                resultValue = ComputeTimeDiff(ts1Millis, ts2Millis, unit);
                resultRaw[i] = resultValue;
                result->SetNotNull(i);
            } else {
                // MONTH or YEAR - calendar-based
                if (ComputeCalendarDiff(ts1Millis, ts2Millis, unit, resultValue)) {
                    resultRaw[i] = resultValue;
                    result->SetNotNull(i);
                } else {
                    result->SetNull(i);
                }
            }
        });

        // Clean up
        delete unitArg;
        delete ts1Arg;
        delete ts2Arg;
    }
};

} // namespace

void RegisterTimestampDiffFunction(const std::string &name)
{
    // timestampdiff(unit VARCHAR, ts1 BIGINT(epoch millis), ts2 BIGINT(epoch millis)) -> BIGINT
    VectorFunction::RegisterVectorFunction(name,
        {OMNI_VARCHAR, OMNI_LONG, OMNI_LONG}, OMNI_LONG,
        std::make_shared<TimestampDiffFunction>());
}

} // namespace omniruntime::vectorization
