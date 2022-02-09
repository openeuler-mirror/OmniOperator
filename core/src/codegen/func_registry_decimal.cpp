/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Decimal Function Registry
 */
#include "func_registry_decimal.h"
#include "functions/decimalfunctions.h"
using namespace omniruntime;
using namespace omniruntime::expressions;

std::vector<Function> GetDecimalFunctionRegistry()
{
    std::vector<DataType> paramTypes = {INT64D, INT64D};
    DataType retType = INT64D;
    std::vector<Function> decimalFnRegistry = {
        // Decimal Add
        Function(reinterpret_cast<void*>(AddDec128), "Add_decimal128", {}, paramTypes, retType, false, true),
        // Decimal Subtract
        Function(reinterpret_cast<void*>(SubDec128), "Sub_decimal128", {}, paramTypes, retType, false, true),
        // Decimal Division
        Function(reinterpret_cast<void *>(DivDec128), "Div_decimal128", {}, paramTypes, retType, false, true),
        // Decimal Multiplication
        Function(reinterpret_cast<void*>(MulDec128), "Mul_decimal128", {}, paramTypes, retType, false, true)
    };
    decimalFnRegistry.insert(decimalFnRegistry.end(), {
            Function(reinterpret_cast<void*>(Decimal128CompareExt), "Decimal128CompareExt", {},
                     {DECIMAL128D, DECIMAL128D}, INT32D, false),
            Function(reinterpret_cast<void *>(AbsDecimal128), "abs", {}, {DECIMAL128D}, DECIMAL128D, true, true),
            Function(reinterpret_cast<void*>(CastInt64ToDecimal128), "CAST", {},
                     {INT64D}, DECIMAL128D, true, true),
    });

    return decimalFnRegistry;
}
