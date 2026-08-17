/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: Flink-compatible datetime plus interval functions
 */

#pragma once

#include <algorithm>
#include <cstdint>
#include <ctime>
#include <limits>
#include <stdexcept>
#include <string>

#include "type/Timestamp.h"
#include "type/date32.h"
#include "vectorization/Status.h"

namespace omniruntime::vectorization {
namespace detail {
constexpr int64_t MILLIS_PER_DAY = 86400000LL;

// Flink codegen 用 Java long 算术实现 datetime + day-time interval。
// Java long 溢出时按二补码静默回绕；C++ 有符号溢出是 UB，
// 因此先按 uint64_t 相加，再把比特位重新解释回 int64_t，
// 保证与 Flink 在极端值上的结果逐位一致。
inline int64_t AddInt64Wrapping(int64_t left, int64_t right)
{
    const uint64_t bits = static_cast<uint64_t>(left) + static_cast<uint64_t>(right);
    if (bits <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
        return static_cast<int64_t>(bits);
    }
    // bits > INT64_MAX：减去 2^63 后从 INT64_MIN 起算，映射到 int64_t 的负半区，
    // 复现二补码回绕（例如 INT64_MAX + 1 == INT64_MIN）。
    return std::numeric_limits<int64_t>::min()
        + static_cast<int64_t>(bits - (uint64_t{1} << 63));
}

// 与 AddInt64Wrapping 同理，但作用于 32 位：把 uint32_t 比特模式转换成
// Java int 算术会得到的 int32_t。用于 DATE32 结果——其天数在 Flink 中以
// Java int 存储，回绕方式与 Java int 一致。
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

// 将 `months`（SQL YEAR-MONTH interval，以总月数表示）按日历语义加到
// DATE32（自 epoch 起的天数）上：日分量被钳制到目标月的最后一天
// （如 Jan 31 + 1 个月 = Feb 28/29），与 Flink 的 temporal-plus 月算术一致。
// 当平移后的年份超出可表示的 LocalDate 范围时返回 false（由调用方抛 UserError）；
// 成功时 result 为新的天数，经 Int32FromBits 按 Java int 回绕。
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
    // 用 0 起始的月份下标运算，使年/月拆分就是普通的按 12 取基。
    const int64_t monthAdded = static_cast<int64_t>(month) - 1 + months;
    // 按 12 做向下取整除法：C++ 的 `/` 向零截断，因此对负的 monthAdded
    // 先偏移 -11，使截断行为等同于向下取整
    // （如 monthAdded=-1 -> yearOffset=-1，落在上一年 12 月）。
    const int64_t yearOffset = (monthAdded >= 0 ? monthAdded : monthAdded - 11) / 12;
    const int64_t resultYear = static_cast<int64_t>(year) + yearOffset;
    if (resultYear < type::MIN_YEAR || resultYear > type::MAX_YEAR) {
        return false;
    }

    const int32_t resultMonth = static_cast<int32_t>(monthAdded - yearOffset * 12 + 1);
    // 钳制日：目标月天数少于源月时，回退到目标月最后一天，而不是溢出到下个月。
    const int32_t resultDay = std::min(day, LastDayOfMonth(static_cast<int32_t>(resultYear), resultMonth));
    int64_t resultDays = 0;
    if (!type::Date32::DaysSinceEpochFromDate(
        static_cast<int32_t>(resultYear), resultMonth, resultDay, resultDays)) {
        return false;
    }
    // 此处不做 int32 范围检查（与兄弟 add_months 不同）：当年份远离 epoch 时
    // 天数可能超出 int32，而 Flink 的 DATE 以 Java int 存储会回绕，故此处
    // 用回绕匹配而非报错。
    result = Int32FromBits(static_cast<uint32_t>(resultDays));
    return true;
}

// 将 `months`（YEAR-MONTH interval）加到以 epoch 毫秒存储的 TIMESTAMP 上。
// 仅日期部分参与月算术；一天内的时间毫秒保留并随后重新拼接。
// 最终的毫秒求和使用 AddInt64Wrapping，以匹配 Java long 的溢出行为。
inline bool AddMonthsToTimestamp(int64_t timestampMillis, int32_t months, int64_t &result)
{
    // 拆分为天与天内两部分。对负时间戳，C++ 的 `/` 和 `%` 向零截断，
    // 因此负余数需借一天修正（如 -1ms -> day=-1, millisOfDay=MILLIS_PER_DAY-1）。
    int64_t millisOfDay = timestampMillis % MILLIS_PER_DAY;
    int64_t daysSinceEpoch = timestampMillis / MILLIS_PER_DAY;
    if (millisOfDay < 0) {
        millisOfDay += MILLIS_PER_DAY;
        --daysSinceEpoch;
    }
    // AddMonthsToDate 按 int32 天数运算；收窄前先做范围保护。
    if (daysSinceEpoch < std::numeric_limits<int32_t>::min()
        || daysSinceEpoch > std::numeric_limits<int32_t>::max()) {
        return false;
    }

    int32_t resultDate = 0;
    if (!AddMonthsToDate(static_cast<int32_t>(daysSinceEpoch), months, resultDate)) {
        return false;
    }
    const int64_t resultDayMillis = static_cast<int64_t>(resultDate) * MILLIS_PER_DAY;
    result = AddInt64Wrapping(resultDayMillis, millisOfDay);
    return true;
}
} // namespace detail

// DateTimePlusYearMonthFunction：实现 `<datetime> + <year-month interval>`。
// interval 以 int32 总月数传入。两个重载覆盖两种 datetime 存储类型
// （DATE32 天数 / TIMESTAMP 毫秒）；框架按已注册的返回类型签名选择对应重载。
//
// 模板参数 T 为返回类型，是 RegisterFunction 的 template-template-parameter
// 签名所要求的；body 中未使用它，因为重载派发由具体的 call(...) 参数与结果类型决定。
template <typename T>
struct DateTimePlusYearMonthFunction {
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

// TIME 与 TIMESTAMP 在 Flink adaptor 中都使用 OMNI_LONG，但 YEAR-MONTH
// interval 对两者的语义不同：TIMESTAMP 需要移动日历月份，TIME 则保持日内值不变。
// 因此 TIME 使用独立函数名，避免在相同的 (LONG, INT) 物理签名上误派发。
template <typename T>
struct TimePlusYearMonthFunction {
    Status call(int64_t &result, const int64_t &timeMillis, const int32_t &months)
    {
        (void)months;
        result = timeMillis;
        return Status::OK();
    }
};

// DateTimePlusDayTimeFunction：实现 `<datetime> + <day-time interval>`。
// interval 以 int64 总毫秒数传入。三个重载覆盖返回类型矩阵：
//   - DATE + interval -> DATE32        ：天内毫秒被截断（除以 MILLIS_PER_DAY 取整），
//                                        且天数须能装入 int32（Flink 用
//                                        Math.toIntExact，否则抛异常）。
//   - DATE + interval -> TIMESTAMP(LONG)：date 提升为毫秒，保留 interval 的时间分量；
//                                        结果按 Java long 回绕。
//   - TIMESTAMP + interval -> TIMESTAMP ：纯 Java long 毫秒加法。
// 哪个重载被触发由注册的返回类型决定：DATE 加一个带时间分量的 day-time
// interval 时，通过注册 LONG 返回重载路由到 TIMESTAMP 结果。
template <typename T>
struct DateTimePlusDayTimeFunction {
    Status call(int32_t &result, const int32_t &date, const int64_t &intervalMillis)
    {
        const int64_t intervalDays = intervalMillis / detail::MILLIS_PER_DAY;
        if (intervalDays < std::numeric_limits<int32_t>::min()
            || intervalDays > std::numeric_limits<int32_t>::max()) {
            // 对应 Flink 的 Math.toIntExact：天数溢出 int 的 day-time interval
            // 没有合法的 DATE 结果，因此让表达式失败而非静默回绕。
            throw std::overflow_error("Day-time interval is out of DATE range");
        }
        result = detail::Int32FromBits(
            static_cast<uint32_t>(date) + static_cast<uint32_t>(intervalDays));
        return Status::OK();
    }

    Status call(int64_t &result, const int32_t &date, const int64_t &intervalMillis)
    {
        // 将 DATE（天数）提升为毫秒，使 interval 的天内部分保留在 TIMESTAMP 结果中。
        // 按 Java long 回绕。
        const int64_t dateMillis = static_cast<int64_t>(date) * detail::MILLIS_PER_DAY;
        result = detail::AddInt64Wrapping(dateMillis, intervalMillis);
        return Status::OK();
    }

    Status call(int64_t &result, const int64_t &timestampMillis, const int64_t &intervalMillis)
    {
        result = detail::AddInt64Wrapping(timestampMillis, intervalMillis);
        return Status::OK();
    }
};

void RegisterDateTimePlusFunctions(const std::string &prefix);
} // namespace omniruntime::vectorization
