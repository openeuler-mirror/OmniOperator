/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: Flink-compatible timestamp and day-time interval arithmetic
 */

#pragma once

#include <algorithm>
#include <cstdint>
#include <ctime>
#include <limits>
#include <string>

#include "type/Timestamp.h"
#include "type/date32.h"
#include "vectorization/Status.h"

namespace omniruntime::vectorization {
namespace detail {
constexpr int64_t MILLIS_PER_DAY = 86400000LL;

inline int64_t Int64FromBits(uint64_t bits)
{
    if (bits <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
        return static_cast<int64_t>(bits);
    }
    return std::numeric_limits<int64_t>::min()
        + static_cast<int64_t>(bits - (uint64_t{1} << 63));
}

inline int32_t Int32FromBits(uint32_t bits)
{
    if (bits <= static_cast<uint32_t>(std::numeric_limits<int32_t>::max())) {
        return static_cast<int32_t>(bits);
    }
    return std::numeric_limits<int32_t>::min()
        + static_cast<int32_t>(bits - (uint32_t{1} << 31));
}

inline int32_t LastDayOfMonth(int32_t year, int32_t month)
{
    return type::Date32::IsLeapYear(year)
        ? type::LEAP_YEAR_OF_DAYS[month]
        : type::NORMAL_YEAR_OF_DAYS[month];
}

// 将 months（YEAR-MONTH interval，总月数）按日历语义加到 DATE 天数上，
// 日分量钳制到目标月最后一天；超出可表示 LocalDate 范围时返回 false。
inline bool AddMonthsToDate(int32_t daysSinceEpoch, int32_t months, int32_t &result)
{
    std::tm calendar{};
    const int64_t seconds = static_cast<int64_t>(daysSinceEpoch) * type::SECOND_OF_DAY;
    if (!Timestamp::epochToCalendarUtc(seconds, calendar)) {
        return false;
    }

    const int32_t year = calendar.tm_year + type::TM_YEAR_BASE;
    const int32_t month = calendar.tm_mon + 1;
    const int32_t day = calendar.tm_mday;
    const int64_t monthAdded = static_cast<int64_t>(month) - 1 + months;
    const int64_t yearOffset = (monthAdded >= 0 ? monthAdded : monthAdded - 11) / 12;
    const int64_t resultYear = static_cast<int64_t>(year) + yearOffset;
    if (resultYear < type::MIN_YEAR || resultYear > type::MAX_YEAR) {
        return false;
    }

    const int32_t resultMonth = static_cast<int32_t>(monthAdded - yearOffset * 12 + 1);
    const int32_t resultDay = std::min(day, LastDayOfMonth(static_cast<int32_t>(resultYear), resultMonth));
    int64_t resultDays = 0;
    if (!type::Date32::DaysSinceEpochFromDate(
        static_cast<int32_t>(resultYear), resultMonth, resultDay, resultDays)) {
        return false;
    }
    result = Int32FromBits(static_cast<uint32_t>(resultDays));
    return true;
}

// 将 months 加到 epoch 毫秒的 TIMESTAMP 上：仅日期部分参与月算术，日内毫秒保留后重拼。
inline bool AddMonthsToTimestamp(int64_t timestampMillis, int32_t months, int64_t &result)
{
    int64_t millisOfDay = timestampMillis % MILLIS_PER_DAY;
    int64_t daysSinceEpoch = timestampMillis / MILLIS_PER_DAY;
    if (millisOfDay < 0) {
        millisOfDay += MILLIS_PER_DAY;
        --daysSinceEpoch;
    }
    if (daysSinceEpoch < std::numeric_limits<int32_t>::min()
        || daysSinceEpoch > std::numeric_limits<int32_t>::max()) {
        return false;
    }

    int32_t resultDate = 0;
    if (!AddMonthsToDate(static_cast<int32_t>(daysSinceEpoch), months, resultDate)) {
        return false;
    }
    const int64_t resultDayMillis = static_cast<int64_t>(resultDate) * MILLIS_PER_DAY;
    result = Int64FromBits(
        static_cast<uint64_t>(resultDayMillis) + static_cast<uint64_t>(millisOfDay));
    return true;
}
} // namespace detail

template <typename T>
struct TimestampPlusDayTimeIntervalFunction {
    Status call(int64_t &result, const int64_t &timestampMillis, const int64_t &intervalMillis)
    {
        const uint64_t bits = static_cast<uint64_t>(timestampMillis) + static_cast<uint64_t>(intervalMillis);
        result = detail::Int64FromBits(bits);
        return Status::OK();
    }
};

template <typename T>
struct TimestampMinusDayTimeIntervalFunction {
    Status call(int64_t &result, const int64_t &timestampMillis, const int64_t &intervalMillis)
    {
        const uint64_t bits = static_cast<uint64_t>(timestampMillis) - static_cast<uint64_t>(intervalMillis);
        result = detail::Int64FromBits(bits);
        return Status::OK();
    }
};

template <typename T>
struct TimestampPlusYearMonthIntervalFunction {
    Status call(int32_t &result, const int32_t &date, const int32_t &months)
    {
        if (!detail::AddMonthsToDate(date, months, result)) {
            return Status::UserError("Datetime plus year-month interval is out of range");
        }
        return Status::OK();
    }

    Status call(int64_t &result, const int64_t &timestampMillis, const int32_t &months)
    {
        if (!detail::AddMonthsToTimestamp(timestampMillis, months, result)) {
            return Status::UserError("Datetime plus year-month interval is out of range");
        }
        return Status::OK();
    }
};

void RegisterTimestampIntervalArithmeticFunctions(const std::string &prefix);
} // namespace omniruntime::vectorization
