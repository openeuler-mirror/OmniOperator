/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: Flink-compatible datetime plus interval registration
 */

#include "DateTimePlus.h"

#include "vectorization/registration/SimpleFunctionRegistry.h"

namespace omniruntime::vectorization {
// 注册 datetime + interval 的各重载。同一族函数共用一个 function_name，
// 框架按完整签名（参数类型 + 返回类型）区分重载。interval 参数始终是
// SQL interval 的内部存储：YEAR-MONTH 为 int32 总月数，DAY-TIME 为 int64
// 总毫秒。RegisterFunction 的 template-template-parameter 签名要求把返回类型
// 作为显式类型实参（TReturn）；TArgs... 与执行时选中的 call(...) 重载相匹配。
void RegisterDateTimePlusFunctions(const std::string &prefix)
{
    // --- year-month interval（int32 总月数）---
    // DATE + months -> DATE
    RegisterFunction<DateTimePlusYearMonthFunction, int32_t, int32_t, int32_t>(
        prefix + "datetime_plus_year_month", {OMNI_DATE32, OMNI_INT}, OMNI_DATE32);
    // 上游 Adaptor（87f8d00）把 DATE 列映射为 OMNI_INT。与 DateArithmetic.cpp 的
    // {OMNI_DATE32|OMNI_INT} 双注册模式一致，INT 约定的 DATE 同样接受（同为 int32 epoch 天）。
    RegisterFunction<DateTimePlusYearMonthFunction, int32_t, int32_t, int32_t>(
        prefix + "datetime_plus_year_month", {OMNI_INT, OMNI_INT}, OMNI_INT);
    // TIMESTAMP(ms) + months -> TIMESTAMP(ms)
    RegisterFunction<DateTimePlusYearMonthFunction, int64_t, int64_t, int32_t>(
        prefix + "datetime_plus_year_month", {OMNI_LONG, OMNI_INT}, OMNI_LONG);
    // TIME + year-month interval -> TIME。独立函数名用于区分相同物理签名的 TIMESTAMP。
    RegisterFunction<TimePlusYearMonthFunction, int64_t, int64_t, int32_t>(
        prefix + "time_plus_year_month", {OMNI_LONG, OMNI_INT}, OMNI_LONG);

    // --- day-time interval（int64 总毫秒）---
    // DATE + interval -> DATE：天内毫秒截断，天数须装入 int32。
    RegisterFunction<DateTimePlusDayTimeFunction, int32_t, int32_t, int64_t>(
        prefix + "datetime_plus_day_time", {OMNI_DATE32, OMNI_LONG}, OMNI_DATE32);
    // 上游 Adaptor（87f8d00）把 DATE 列映射为 OMNI_INT；与 DateArithmetic.cpp 的双注册
    // 模式一致，INT 约定的 DATE 同样接受（同为 int32 epoch 天）。
    RegisterFunction<DateTimePlusDayTimeFunction, int32_t, int32_t, int64_t>(
        prefix + "datetime_plus_day_time", {OMNI_INT, OMNI_LONG}, OMNI_INT);
    // DATE + interval -> TIMESTAMP(ms)：date 提升为毫秒，保留时间分量。
    RegisterFunction<DateTimePlusDayTimeFunction, int64_t, int32_t, int64_t>(
        prefix + "datetime_plus_day_time", {OMNI_DATE32, OMNI_LONG}, OMNI_LONG);
    // TIMESTAMP(ms) + interval -> TIMESTAMP(ms)：纯毫秒加法。
    RegisterFunction<DateTimePlusDayTimeFunction, int64_t, int64_t, int64_t>(
        prefix + "datetime_plus_day_time", {OMNI_LONG, OMNI_LONG}, OMNI_LONG);
}
} // namespace omniruntime::vectorization
