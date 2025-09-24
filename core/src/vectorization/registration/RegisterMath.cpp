/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#include <string>
#include "../functions/Arithmetic.h"
#include "RegistrationHelpers.h"

namespace omniruntime::vectorization {
void registerMathFunctions(const std::string &prefix)
{
    registerBinaryNumeric<PlusFunction>({prefix + "add"});
}
}
