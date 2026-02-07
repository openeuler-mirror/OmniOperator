/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: DateSub function - wrapper for shared date arithmetic implementation
 */

#include "DateSub.h"
#include "DateArithmetic.h"

namespace omniruntime::vectorization {

void RegisterDateSubFunction(const std::string &name)
{
    // Use the shared implementation from DateArithmetic.cpp
    // date_sub(date, n) is equivalent to date_add(date, -n)
    RegisterDateArithmeticFunction(name, true);
}
}
