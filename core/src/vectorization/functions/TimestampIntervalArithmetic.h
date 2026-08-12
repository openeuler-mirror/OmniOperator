/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: Flink-compatible timestamp and day-time interval arithmetic
 */

#pragma once

#include <cstdint>
#include <limits>
#include <string>

#include "vectorization/Status.h"

namespace omniruntime::vectorization {
namespace detail {
inline int64_t Int64FromBits(uint64_t bits)
{
    if (bits <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
        return static_cast<int64_t>(bits);
    }
    return std::numeric_limits<int64_t>::min()
        + static_cast<int64_t>(bits - (uint64_t{1} << 63));
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

void RegisterTimestampIntervalArithmeticFunctions(const std::string &prefix);
} // namespace omniruntime::vectorization
