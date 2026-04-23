/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: DateFormat function for expression system
 */

#pragma once
#include <string>
#include "vectorization/VectorFunction.h"

namespace omniruntime::vectorization {

/// date_format function
/// date_format(timestamp, format) -> varchar
/// Converts a timestamp to a string in the format specified by the date format string.
/// Supported format tokens (JODA-like, same as OmniOperator codegen DateFormat):
///   yyyy - 4-digit year
///   MM   - 2-digit month
///   dd   - 2-digit day of month
///   HH   - 2-digit hour (24-hour)
///   mm   - 2-digit minute
///   ss   - 2-digit second
/// Supports OMNI_TIMESTAMP/OMNI_LONG as timestamp input,
/// and OMNI_VARCHAR/OMNI_CHAR as format string input.
/// Returns NULL if input timestamp or format is NULL, or if formatting fails.
void RegisterDateFormatFunction(const std::string &name);
}
