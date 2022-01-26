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
    std::vector<DataType> paramTypes = {INT64D, INT64D, INT64D};
    DataType retType = INT64D;
    std::vector<Function> decimalFnRegistry = {
        // Decimal Add
        Function(reinterpret_cast<void*>(AddDec128), "Add_decimal128", {}, paramTypes, retType, false),
        // Decimal Subtract
        Function(reinterpret_cast<void*>(SubDec128), "Sub_decimal128", {}, paramTypes, retType, false),
        // Decimal Division
        Function(reinterpret_cast<void *>(DivDec128), "Div_decimal128", {}, paramTypes, retType, false),
        // Decimal Multiplication
        Function(reinterpret_cast<void*>(MulDec128), "Mul_decimal128", {}, paramTypes, retType, false)
    };
    paramTypes.pop_back();
    decimalFnRegistry.insert(decimalFnRegistry.end(), {
            Function(reinterpret_cast<void*>(Decimal128CompareExt), "Decimal128CompareExt", {}, paramTypes,
                     INT32D, false),
            Function(reinterpret_cast<void *>(AbsDecimal128), "abs_decimal128_decimal128", {}, {DECIMAL128D, INT64D}, DECIMAL128D, false),
            Function(reinterpret_cast<void*>(CastInt64ToDecimal128), "CAST_int64_decimal128", {},
                     {INT64D, INT64D}, DECIMAL128D, false),
    });

    return decimalFnRegistry;
}
