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
    RegisterUnaryNumeric<NegativeFunction>({prefix + "negative"});
	RegisterFunction<AsinFunction, double, double>(prefix + "asin", {OMNI_DOUBLE}, OMNI_DOUBLE);
    RegisterFunction<AsinhFunction, double, double>(prefix + "asinh", {OMNI_DOUBLE}, OMNI_DOUBLE);
    RegisterFunction<AtanFunction, double, double>(prefix + "atan", {OMNI_DOUBLE}, OMNI_DOUBLE);
    RegisterFunction<Atan2Function, double, double, double>(prefix + "atan2", {OMNI_DOUBLE, OMNI_DOUBLE}, OMNI_DOUBLE);
    RegisterFunction<CosFunction, double, double>(prefix + "cos", {OMNI_DOUBLE}, OMNI_DOUBLE);
    RegisterFunction<CoshFunction, double, double>(prefix + "cosh", {OMNI_DOUBLE}, OMNI_DOUBLE);
	RegisterFunction<CbrtFunction, double, double>(prefix + "cbrt", {OMNI_DOUBLE}, OMNI_DOUBLE);
	RegisterFunction<CeilFunction, int64_t, int64_t>(prefix + "ceil", {OMNI_LONG}, OMNI_LONG);
	RegisterFunction<CeilFunction, int64_t, double>(prefix + "ceil", {OMNI_DOUBLE}, OMNI_LONG);
    RegisterFunction<SignFunction, double, double>(prefix + "sign", {OMNI_DOUBLE}, OMNI_DOUBLE);
    RegisterFunction<SinhFunction, double, double>(prefix + "sinh", {OMNI_DOUBLE}, OMNI_DOUBLE);
    RegisterFunction<SqrtFunction, double, double>(prefix + "sqrt", {OMNI_DOUBLE}, OMNI_DOUBLE);
    RegisterFunction<SecFunction, double, double>(prefix + "sec", {OMNI_DOUBLE}, OMNI_DOUBLE);
    RegisterFunction<Log1pFunction, double, double>(prefix + "log1p", {OMNI_DOUBLE}, OMNI_DOUBLE);
    RegisterFunction<Log10Function, double, double>(prefix + "log10", {OMNI_DOUBLE}, OMNI_DOUBLE);
    RegisterFunction<Log2Function, double, double>(prefix + "log2", {OMNI_DOUBLE}, OMNI_DOUBLE);
    RegisterFunction<LogarithmFunction, double, double, double>(prefix + "log", {OMNI_DOUBLE, OMNI_DOUBLE}, OMNI_DOUBLE);
}
}
