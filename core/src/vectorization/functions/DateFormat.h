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
/// Pattern tokens follow Spark/Java DateTimeFormatter semantics
/// (aligned with FromUnixTime and Spark's DateFormatClass), supporting:
///   y/Y         year (yy: 2-digit, yyyy: 4-digit)
///   M/MM/MMM/MMMM     month (numeric / short name / full name)
///   d/dd        day-of-month
///   D           day-of-year
///   H/HH        hour 0-23
///   h/hh        clock-hour 1-12
///   K/KK        hour 0-11
///   k/kk        clock-hour 1-24
///   m/mm        minute
///   s/ss        second
///   S/SSS/SSSSSS/...  fraction-of-second (real microsecond digits)
///   n           nano-of-second
///   a           AM/PM marker
///   E/EEE/EEEE  day-of-week name
///   F           aligned week of month
///   q/Q         quarter
///   w           week-of-year
///   V/v/z/O/X/x/Z     timezone tokens
///   '...'       literal text escape ('' for a single quote)
/// Supports OMNI_TIMESTAMP/OMNI_LONG as timestamp input,
/// and OMNI_VARCHAR/OMNI_CHAR as format string input.
/// Returns NULL if input timestamp or format is NULL, or if formatting fails.
void RegisterDateFormatFunction(const std::string &name);
}
