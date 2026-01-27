/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#include <string>
#include "../functions/Arithmetic.h"
#include "../functions/Comparisons.h"
#include "../functions/IsNull.h"
#include "../functions/MathFunctions.h"
#include "RegistrationHelpers.h"

namespace omniruntime::vectorization {
void RegisterMathFunctions(const std::string &prefix)
{
    RegisterIsNullFunction(prefix + "isnull");
    RegisterBinaryNumeric<PlusFunction>({prefix + "add"});
    RegisterBinaryLogical<And>(prefix + "and");
    RegisterUnaryIntegral<Not>(prefix + "not");
    RegisterBinaryNumeric<MinusFunction>({prefix + "subtract"});
    RegisterBinaryNumeric<MultiplyFunction>({prefix + "multiply"});
    RegisterBinaryNumeric<DivideFunction>({prefix + "divide"});
    RegisterBinaryNumeric<RemainderFunction>({prefix + "modulus"});
    RegisterFunction<AcoshFunction, double, double>(prefix + "acosh", {OMNI_DOUBLE}, OMNI_DOUBLE);
    RegisterFunction<AcosFunction, double, double>(prefix + "acos", {OMNI_DOUBLE}, OMNI_DOUBLE);
}
}
