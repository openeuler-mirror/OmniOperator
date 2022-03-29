/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Decimal Function Registry
 */
#include "func_registry_decimal.h"
#include "functions/decimalfunctions.h"
using namespace omniruntime;
using namespace omniruntime::type;
using namespace omniruntime::codegen;

std::vector<Function> DecimalFunctionRegistry::GetFunctions()
{
    std::vector<DataTypeId> paramTypes = { OMNI_DECIMAL128, OMNI_DECIMAL128 };
    DataTypeId retType = OMNI_DECIMAL128;
    static std::vector<Function> decimalFnRegistry = { // Decimal Add
        Function(reinterpret_cast<void *>(AddDec128), "Add_decimal128", {}, paramTypes, retType),
        // Decimal Subtract
        Function(reinterpret_cast<void *>(SubDec128), "Sub_decimal128", {}, paramTypes, retType),
        // Decimal Division
        Function(reinterpret_cast<void *>(DivDec128), "Div_decimal128", {}, paramTypes, retType),
        // Decimal Multiplication
        Function(reinterpret_cast<void *>(MulDec128), "Mul_decimal128", {}, paramTypes, retType),
        // Decimal Compare
        Function(reinterpret_cast<void *>(Decimal128Compare), "Decimal128Compare", {}, paramTypes, OMNI_INT),
        // Decimal Absolute
        Function(reinterpret_cast<void *>(AbsDecimal128), "abs", {}, { OMNI_DECIMAL128 }, OMNI_DECIMAL128),
        // Decimal Cast Long to Decimal128
        Function(reinterpret_cast<void *>(CastInt64ToDecimal128), "CAST", {}, { OMNI_LONG }, OMNI_DECIMAL128),
        Function(reinterpret_cast<void *>(CastInt64ToDecimal128), "CAST", {}, { OMNI_DECIMAL64 }, OMNI_DECIMAL128) };

    return decimalFnRegistry;
}
