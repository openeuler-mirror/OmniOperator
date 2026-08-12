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
}
} // namespace omniruntime::vectorization
