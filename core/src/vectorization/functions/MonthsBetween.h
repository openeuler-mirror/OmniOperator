/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: MonthsBetween function for expression system
 */

#pragma once
#include <string>
#include "vectorization/VectorFunction.h"

namespace omniruntime::vectorization {

/// months_between function
/// months_between(timestamp, timestamp, bool, varchar) -> double
/// Returns the number of months between timestamp1 and timestamp2.
/// The 4th parameter (timeZoneId) is appended by MonthsBetweenTransformer in Gluten.
/// If timestamp1 is later than timestamp2, the result is positive.
/// If both timestamps are on the same day of month, or both are the last day of month,
/// time of day will be ignored. Otherwise, the difference is calculated based on 31 days
/// per month, and rounded to 8 decimal places when roundOff is true.
void RegisterMonthsBetweenFunction(const std::string &name);
}
