/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: Flink-compatible timestamp and day-time interval arithmetic registration
 */

#include "TimestampIntervalArithmetic.h"

#include "vectorization/registration/SimpleFunctionRegistry.h"

namespace omniruntime::vectorization {
void RegisterTimestampIntervalArithmeticFunctions(const std::string &prefix)
{
    RegisterFunction<TimestampPlusDayTimeIntervalFunction, int64_t, int64_t, int64_t>(
        prefix + "datetime_plus_day_time", {OMNI_LONG, OMNI_LONG}, OMNI_LONG);
    RegisterFunction<TimestampMinusDayTimeIntervalFunction, int64_t, int64_t, int64_t>(
        prefix + "datetime_minus_day_time", {OMNI_LONG, OMNI_LONG}, OMNI_LONG);
    // YEAR-MONTH：上游 Adaptor 把 DATE 列映射为 INT（87f8d00 约定），但源列经
    // LogicalType 仍可能物化为 DATE32，故双注册 {DATE32,INT} 与 {INT,INT}；
    // TIMESTAMP 入参为 OMNI_LONG；月份字面量为 OMNI_INT 总月数。
    RegisterFunction<TimestampPlusYearMonthIntervalFunction, int32_t, int32_t, int32_t>(
        prefix + "datetime_plus_year_month", {OMNI_DATE32, OMNI_INT}, OMNI_DATE32);
    RegisterFunction<TimestampPlusYearMonthIntervalFunction, int32_t, int32_t, int32_t>(
        prefix + "datetime_plus_year_month", {OMNI_INT, OMNI_INT}, OMNI_INT);
    RegisterFunction<TimestampPlusYearMonthIntervalFunction, int64_t, int64_t, int32_t>(
        prefix + "datetime_plus_year_month", {OMNI_LONG, OMNI_INT}, OMNI_LONG);
}
} // namespace omniruntime::vectorization
