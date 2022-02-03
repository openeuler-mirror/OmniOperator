/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Decimal Function Registry
 */
#include "func_registry_decimal.h"
#include "functions/decimalfunctions.h"
using namespace omniruntime;
using namespace omniruntime::vec;

std::vector<Function> DecimalFunctionRegistry::GetFunctions()
{
    std::vector<VecTypeId> paramTypes = { OMNI_VEC_TYPE_DECIMAL128, OMNI_VEC_TYPE_DECIMAL128 };
    VecTypeId retType = OMNI_VEC_TYPE_DECIMAL128;
    static std::vector<Function> decimalFnRegistry = {
        // Decimal Add
        Function(reinterpret_cast<void*>(AddDec128), "Add_decimal128", {}, paramTypes, retType, true),
        // Decimal Subtract
        Function(reinterpret_cast<void*>(SubDec128), "Sub_decimal128", {}, paramTypes, retType, true),
        // Decimal Division
        Function(reinterpret_cast<void *>(DivDec128), "Div_decimal128", {}, paramTypes, retType, true),
        // Decimal Multiplication
        Function(reinterpret_cast<void*>(MulDec128), "Mul_decimal128", {}, paramTypes, retType, true),
        // Decimal Compare
        Function(reinterpret_cast<void *>(Decimal128Compare), "Decimal128Compare", {},
                 paramTypes, OMNI_VEC_TYPE_INT),
        // Decimal Absolute
        Function(reinterpret_cast<void *>(AbsDecimal128), "abs", {}, {OMNI_VEC_TYPE_DECIMAL128},
                 OMNI_VEC_TYPE_DECIMAL128, true),
        // Decimal Cast Long to Decimal128
        Function(reinterpret_cast<void*>(CastInt64ToDecimal128), "CAST", {},
                 {OMNI_VEC_TYPE_LONG}, OMNI_VEC_TYPE_DECIMAL128, true)

    };

    return decimalFnRegistry;
}
