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
    std::vector<DataTypeId> paramTypes128 = {OMNI_DECIMAL128, OMNI_DECIMAL128};
    std::vector<DataTypeId> paramTypes64 = {OMNI_DECIMAL64, OMNI_DECIMAL64};
    DataTypeId retType128 = OMNI_DECIMAL128;
    DataTypeId retType64 = OMNI_DECIMAL64;
    static std::vector<Function> decimalFnRegistry = {
        Function(reinterpret_cast<void *>(MakeDecimal64), "MakeDecimal", {}, { OMNI_DECIMAL64 }, OMNI_DECIMAL64,
            NULL_RESULT_IF_ANY_NULL_ARG),
        Function(reinterpret_cast<void *>(MakeDecimal128), "MakeDecimal", {},
                 { OMNI_DECIMAL128 }, OMNI_DECIMAL128, NULL_RESULT_IF_ANY_NULL_ARG),
        Function(reinterpret_cast<void *>(MakeDecimal64To128), "MakeDecimal", {}, { OMNI_DECIMAL64 },
            OMNI_DECIMAL128, NULL_RESULT_IF_ANY_NULL_ARG),
        Function(reinterpret_cast<void *>(MakeDecimalLongTo64), "MakeDecimal", {}, { OMNI_LONG }, OMNI_DECIMAL64,
            NULL_RESULT_IF_ANY_NULL_ARG),
        Function(reinterpret_cast<void *>(AddDec128), "Add_decimal128", {}, paramTypes128, retType128,
                 NULL_RESULT_IF_ANY_NULL_ARG),
        Function(reinterpret_cast<void *>(SubDec128), "Sub_decimal128", {}, paramTypes128, retType128,
                 NULL_RESULT_IF_ANY_NULL_ARG),
        Function(reinterpret_cast<void *>(MulDec128), "Mul_decimal128", {}, paramTypes128, retType128,
                 NULL_RESULT_IF_ANY_NULL_ARG),
        Function(reinterpret_cast<void *>(DivDec128), "Div_decimal128", {}, paramTypes128, retType128,
                 NULL_RESULT_IF_ANY_NULL_ARG, true),
        Function(reinterpret_cast<void *>(ModDec128), "Mod_decimal128", {}, paramTypes128, retType128,
                 NULL_RESULT_IF_ANY_NULL_ARG, true),
        Function(reinterpret_cast<void *>(Decimal128Compare), "Decimal128Compare", {}, paramTypes128, OMNI_INT),
        Function(reinterpret_cast<void *>(AbsDecimal128), "abs", {}, {OMNI_DECIMAL128}, retType128,
                 NULL_RESULT_IF_ANY_NULL_ARG),

        Function(reinterpret_cast<void *>(AddDec64Ret64), "Add_decimal64", {}, paramTypes64, retType64,
                 NULL_RESULT_IF_ANY_NULL_ARG),
        Function(reinterpret_cast<void *>(SubDec64Ret64), "Sub_decimal64", {}, paramTypes64, retType64,
                 NULL_RESULT_IF_ANY_NULL_ARG),
        Function(reinterpret_cast<void *>(MulDec64Ret64), "Mul_decimal64", {}, paramTypes64, retType64,
                 NULL_RESULT_IF_ANY_NULL_ARG),
        Function(reinterpret_cast<void *>(DivDec64Ret64), "Div_decimal64", {}, paramTypes64, retType64,
                 NULL_RESULT_IF_ANY_NULL_ARG, true),
        Function(reinterpret_cast<void *>(ModDec64Ret64), "Mod_decimal64", {}, paramTypes64, retType64,
                 NULL_RESULT_IF_ANY_NULL_ARG, true),

        Function(reinterpret_cast<void *>(AddDec64Ret128), "Add_decimal64", {}, paramTypes64, retType128,
                 NULL_RESULT_IF_ANY_NULL_ARG),
        Function(reinterpret_cast<void *>(SubDec64Ret128), "Sub_decimal64", {}, paramTypes64, retType128,
                 NULL_RESULT_IF_ANY_NULL_ARG),
        Function(reinterpret_cast<void *>(MulDec64Ret128), "Mul_decimal64", {}, paramTypes64, retType128,
                 NULL_RESULT_IF_ANY_NULL_ARG),
        Function(reinterpret_cast<void *>(DivDec64Ret128), "Div_decimal64", {}, paramTypes64, retType128,
                 NULL_RESULT_IF_ANY_NULL_ARG, true),
        Function(reinterpret_cast<void *>(ModDec64Ret128), "Mod_decimal64", {}, paramTypes64, retType128,
                 NULL_RESULT_IF_ANY_NULL_ARG, true),
        Function(reinterpret_cast<void *>(Decimal64Compare), "Decimal64Compare", {}, paramTypes64, OMNI_INT),
        Function(reinterpret_cast<void *>(AbsDecimal64), "abs", {}, {OMNI_DECIMAL64}, OMNI_DECIMAL64,
                 NULL_RESULT_IF_ANY_NULL_ARG),

        Function(reinterpret_cast<void *>(CastInt64ToDecimal128), "CAST", {}, {OMNI_LONG}, OMNI_DECIMAL128),
        Function(reinterpret_cast<void *>(CastInt64ToDecimal128), "CAST", {}, {OMNI_DECIMAL64}, OMNI_DECIMAL128,
                 NULL_RESULT_IF_ANY_NULL_ARG),
        Function(reinterpret_cast<void *>(CastDoubleToDecimal64), "CAST", {}, {OMNI_DOUBLE}, OMNI_DECIMAL64,
                 NULL_RESULT_IF_ANY_NULL_ARG),
        // Decimal IsOverflowDecimal
        Function(reinterpret_cast<void *>(IsOverflowDecimal64), "IsOverflowDecimal", {},
                 {OMNI_DECIMAL64, OMNI_INT, OMNI_INT}, OMNI_BOOLEAN, NULL_RESULT_IF_ANY_NULL_ARG),
        Function(reinterpret_cast<void *>(IsOverflowDecimal128), "IsOverflowDecimal", {},
                 {OMNI_DECIMAL128, OMNI_INT, OMNI_INT}, OMNI_BOOLEAN, NULL_RESULT_IF_ANY_NULL_ARG),
        // Decimal UnscaledValue
        Function(reinterpret_cast<void *>(UnscaledValue64), "UnscaledValue", {}, {OMNI_DECIMAL64}, OMNI_LONG,
                 NULL_RESULT_IF_ANY_NULL_ARG)
    };

    return decimalFnRegistry;
}
