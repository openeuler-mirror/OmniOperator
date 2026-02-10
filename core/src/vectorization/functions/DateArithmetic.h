/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: Shared implementation for date arithmetic functions (date_add and date_sub)
 */

#pragma once
#include <string>
#include "vectorization/VectorFunction.h"

namespace omniruntime::vectorization {
// Internal shared function for date arithmetic (used by both date_add and date_sub)
// isSubtract: true for date_sub, false for date_add
void RegisterDateArithmeticFunction(const std::string &name, bool isSubtract);
}
