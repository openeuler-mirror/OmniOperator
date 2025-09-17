/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#include <string>
#include "../functions/Arithmetic.h"
#include "../functions/Comparisons.h"
#include "../functions/IsNull.h"
#include "RegistrationHelpers.h"

namespace omniruntime::vectorization {
void registerMathFunctions(const std::string &prefix)
{
    registerIsNullFunction(prefix + "isnull");
    registerBinaryNumeric<PlusFunction>({prefix + "add"});
    registerBinaryCompare<Greater>(prefix + "greaterThan");
    registerBinaryLogical<And>(prefix + "and");
    registerUnaryIntegral<Not>(prefix + "not");
    registerBinaryNumeric<MinusFunction>({prefix + "subtract"});
    registerBinaryNumeric<MultiplyFunction>({prefix + "multiply"});
    registerBinaryNumeric<DivideFunction>({prefix + "divide"});
    registerBinaryNumeric<RemainderFunction>({prefix + "remainder"});
}
}
