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
    static std::vector<Function> decimalFnRegistry = { Function(reinterpret_cast<void *>(MakeDecimal64), "MakeDecimal",
        {}, { OMNI_DECIMAL64, OMNI_INT, OMNI_INT, OMNI_INT, OMNI_INT }, OMNI_DECIMAL64),
                                                       Function(reinterpret_cast<void *>(MakeDecimal128), "MakeDecimal", {},
        { OMNI_DECIMAL128, OMNI_INT, OMNI_INT, OMNI_INT, OMNI_INT }, OMNI_DECIMAL128),
                                                       Function(reinterpret_cast<void *>(MakeDecimal64To128), "MakeDecimal", {},
        { OMNI_DECIMAL64, OMNI_INT, OMNI_INT, OMNI_INT, OMNI_INT }, OMNI_DECIMAL128),
                                                       Function(reinterpret_cast<void *>(MakeDecimalLongTo64), "MakeDecimal", {}, { OMNI_LONG, OMNI_INT, OMNI_INT },
        OMNI_DECIMAL64),
                                                       Function(reinterpret_cast<void *>(AddDec128), "Add_decimal128", {}, paramTypes, retType),
                                                       Function(reinterpret_cast<void *>(SubDec128), "Sub_decimal128", {}, paramTypes, retType),
                                                       Function(reinterpret_cast<void *>(DivDec128), "Div_decimal128", {}, paramTypes, retType, true),
                                                       Function(reinterpret_cast<void *>(MulDec128), "Mul_decimal128", {}, paramTypes, retType),
                                                       Function(reinterpret_cast<void *>(Decimal128Compare), "Decimal128Compare", {}, paramTypes, OMNI_INT),
                                                       Function(reinterpret_cast<void *>(AbsDecimal128), "abs", {}, { OMNI_DECIMAL128 }, OMNI_DECIMAL128),
                                                       Function(reinterpret_cast<void *>(CastInt64ToDecimal128), "CAST", {}, { OMNI_LONG }, OMNI_DECIMAL128),
                                                       Function(reinterpret_cast<void *>(CastInt64ToDecimal128), "CAST", {}, { OMNI_DECIMAL64 }, OMNI_DECIMAL128),
                                                       Function(reinterpret_cast<void *>(CastDoubleToDecimal64), "CAST", {}, { OMNI_DOUBLE, OMNI_INT, OMNI_INT },
        OMNI_DECIMAL64),
                                                       Function(reinterpret_cast<void *>(DivDec64), "Div_decimal64", {}, { OMNI_DECIMAL64, OMNI_DECIMAL64 },
        OMNI_DECIMAL64, true),
                                                       Function(reinterpret_cast<void *>(DownScaleDec64), "DownScale_decimal64", {}, { OMNI_DECIMAL64, OMNI_INT },
        OMNI_DECIMAL64),
        // Decimal IsOverflowDecimal
                                                       Function(reinterpret_cast<void *>(IsOverflowDecimal64), "IsOverflowDecimal", {},
        { OMNI_DECIMAL64, OMNI_INT, OMNI_INT, OMNI_INT, OMNI_INT }, OMNI_BOOLEAN),
                                                       Function(reinterpret_cast<void *>(IsOverflowDecimal128), "IsOverflowDecimal", {},
        { OMNI_DECIMAL128, OMNI_INT, OMNI_INT, OMNI_INT, OMNI_INT }, OMNI_BOOLEAN),
        // Decimal UnscaledValue
                                                       Function(reinterpret_cast<void *>(UnscaledValue64), "UnscaledValue", {}, { OMNI_DECIMAL64, OMNI_INT, OMNI_INT },
        OMNI_LONG) };

    return decimalFnRegistry;
}
