/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Decimal Function Registry
 */
#include "func_registry_decimal.h"

using namespace omniruntime;
using namespace omniruntime::type;

namespace omniruntime {
std::vector<Function> DecimalFunctionRegistry::GetFunctions()
{
    std::vector<DataTypeId> paramTypes = { OMNI_DECIMAL128, OMNI_DECIMAL128 };
    DataTypeId retType = OMNI_DECIMAL128;
    static std::vector<Function> decimalFnRegistry = { // Decimal Add
        Function("AddDec128", "add", {}, paramTypes, retType),
        // Decimal Subtract
        Function("SubDec128", "sub", {}, paramTypes, retType),
        // Decimal Division
        Function("DivDec128", "div", {}, paramTypes, retType),
        // Decimal Multiplication
        Function("MulDec128", "mul", {}, paramTypes, retType),
        // Decimal Compare
        Function("Decimal128Compare", "compare", {}, paramTypes, OMNI_INT),
        // Decimal Absolute
        Function("AbsDecimal128", "abs", {}, { OMNI_DECIMAL128 }, OMNI_DECIMAL128),
        // Decimal Cast Long to Decimal128
        Function("CastInt64ToDecimal128", "CAST", {}, { OMNI_LONG }, OMNI_DECIMAL128),
        Function("CastInt64ToDecimal128", "CAST", {}, { OMNI_DECIMAL64 }, OMNI_DECIMAL128) };

    return decimalFnRegistry;
}
}