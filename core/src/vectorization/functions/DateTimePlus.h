/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: Flink-compatible datetime plus interval functions
 */

#pragma once

#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>

#include "DatetimeIntervalDetail.h"
#include "vectorization/Status.h"

namespace omniruntime::vectorization {
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
