/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: Flink-compatible timestamp and day-time interval arithmetic
 */

#pragma once

#include <cstdint>
#include <string>

#include "DatetimeIntervalDetail.h"
#include "vectorization/Status.h"

namespace omniruntime::vectorization {
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
