/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: base operations implementation
 */

#pragma once
#include "util/config/QueryConfig.h"
#include "vectorization/Status.h"
#include "type/Timestamp.h"
#include "type/string_ref.h"

namespace omniruntime::vectorization {
using namespace type;

// This class provides cast hooks following Spark semantics.
class CastHooks {
public:
    explicit CastHooks(const config::QueryConfig &config);

    // TODO: Spark hook allows more string patterns than Presto.
    Expected<int64_t> castStringToTimestamp(const std::string_view  &view) const;

    /// When casting integral value as timestamp, the input is treated as the
    /// number of seconds since the epoch (1970-01-01 00:00:00 UTC).
    Expected<int64_t> castIntToTimestamp(int64_t seconds) const;

    /// When casting double as timestamp, the input is treated as
    /// the number of seconds since the epoch (1970-01-01 00:00:00 UTC).
    Expected<std::optional<int64_t>> castDoubleToTimestamp(double value) const;

    /// 1) Removes all leading and trailing UTF8 white-spaces before cast. 2) Uses
    /// non-standard cast mode to cast from string to date.
    Expected<int32_t> castStringToDate(const std::string_view &dateString) const;

    // Allows casting 'NaN', 'Infinity', '-Infinity', 'Inf', '-Inf', and these
    // strings with different letter cases to real.
    Expected<float> castStringToReal(const std::string_view &data) const;

    // Allows casting 'NaN', 'Infinity', '-Infinity', 'Inf', '-Inf', and these
    // strings with different letter cases to double.
    Expected<double> castStringToDouble(const std::string_view &data);

    /// When casting from string to integral, floating-point, decimal, date, and
    /// timestamp types, Spark hook trims all leading and trailing UTF8
    /// whitespaces before cast.
    std::string_view removeWhiteSpaces(const std::string_view &view) const;

    const TimestampToStringOptions &timestampToStringOptions() const
    {
        return timestampToStringOptions_;
    }

    bool truncate() const
    {
        return true;
    }

private:
    // Casts a number to a timestamp. The number is treated as the number of
    // seconds since the epoch (1970-01-01 00:00:00 UTC).
    // Supports integer and floating-point types.
    template <typename T>
    Expected<int64_t> castNumberToTimestamp(T seconds) const;

    config::QueryConfig config_;

    /// 1) Does not follow 'isLegacyCast'. 2) The conversion precision is
    /// microsecond. 3) Does not append trailing zeros. 4) Adds a positive
    /// sign at first if the year exceeds 9999. 5) Respects the configured
    /// session timezone.
    TimestampToStringOptions timestampToStringOptions_ = {
        .precision = TimestampToStringOptions::Precision::kMicroseconds,
        .leadingPositiveSign = true,
        .skipTrailingZeros = true,
        .zeroPaddingYear = true,
        .dateTimeSeparator = ' '
    };
};
} // namespace facebook::velox::functions::sparksql
