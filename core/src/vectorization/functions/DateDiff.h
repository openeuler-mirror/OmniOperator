/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: DateDiff function for expression system
 */

#pragma once
#include <string>
#include "vectorization/VectorFunction.h"

namespace omniruntime::vectorization {

/// datediff function
/// datediff(date, date) -> int32
/// Returns the number of days from startDate to endDate.
/// Computes endDate - startDate. Supports DATE32 and INT types (equivalent).
/// Integer overflow is allowed to match Spark behavior.
void RegisterDateDiffFunction(const std::string &name);
}
