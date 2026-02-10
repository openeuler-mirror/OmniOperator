/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: DateAdd function - wrapper for shared date arithmetic implementation
 */

#include "DateAdd.h"
#include "DateArithmetic.h"

namespace omniruntime::vectorization {

void RegisterDateAddFunction(const std::string &name)
{
    // Use the shared implementation from DateArithmetic.cpp
    RegisterDateArithmeticFunction(name, false);
}
}
