/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: DateTrunc function for expression system
 * Truncates a timestamp or date to a specified precision.
 * Supports all Velox date_trunc precision levels:
 *   MICROSECOND, MILLISECOND, SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER, YEAR
 */

#pragma once
#include <string>
#include "vectorization/VectorFunction.h"

namespace omniruntime::vectorization {
void RegisterDateTruncFunction(const std::string &name);
}
