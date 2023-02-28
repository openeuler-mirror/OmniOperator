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
    std::vector<DataTypeId> paramTypes128 = { OMNI_DECIMAL128, OMNI_DECIMAL128 };
    std::vector<DataTypeId> paramTypes64 = { OMNI_DECIMAL64, OMNI_DECIMAL64 };
    std::vector<DataTypeId> paramTypes64Op128 = { OMNI_DECIMAL64, OMNI_DECIMAL128 };
    std::vector<DataTypeId> paramTypes128Op64 = { OMNI_DECIMAL128, OMNI_DECIMAL64 };
    DataTypeId retType128 = OMNI_DECIMAL128;
    DataTypeId retType64 = OMNI_DECIMAL64;
    static std::vector<Function> decimalFnRegistry = {
        Function(reinterpret_cast<void *>(AddDec64Dec64Dec64), "Add_decimal64", {}, paramTypes64, retType64,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(AddDec64Dec64Dec128), "Add_decimal64", {}, paramTypes64, retType128,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(AddDec128Dec128Dec128), "Add_decimal128", {}, paramTypes128, retType128,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(AddDec64Dec128Dec128), "Add_decimal64", {}, paramTypes64Op128, retType128,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(AddDec128Dec64Dec128), "Add_decimal128", {}, paramTypes128Op64, retType128,
            INPUT_DATA, true),

        Function(reinterpret_cast<void *>(SubDec64Dec64Dec64), "Sub_decimal64", {}, paramTypes64, retType64,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(SubDec64Dec64Dec128), "Sub_decimal64", {}, paramTypes64, retType128,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(SubDec128Dec128Dec128), "Sub_decimal128", {}, paramTypes128, retType128,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(SubDec64Dec128Dec128), "Sub_decimal64", {}, paramTypes64Op128, retType128,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(SubDec128Dec64Dec128), "Sub_decimal128", {}, paramTypes128Op64, retType128,
            INPUT_DATA, true),

        Function(reinterpret_cast<void *>(MulDec64Dec64Dec64), "Mul_decimal64", {}, paramTypes64, retType64,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(MulDec64Dec64Dec128), "Mul_decimal64", {}, paramTypes64, retType128,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(MulDec128Dec128Dec128), "Mul_decimal128", {}, paramTypes128, retType128,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(MulDec64Dec128Dec128), "Mul_decimal64", {}, paramTypes64Op128, retType128,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(MulDec128Dec64Dec128), "Mul_decimal128", {}, paramTypes128Op64, retType128,
            INPUT_DATA, true),

        Function(reinterpret_cast<void *>(DivDec64Dec64Dec64), "Div_decimal64", {}, paramTypes64, retType64,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(DivDec64Dec128Dec64), "Div_decimal64", {}, paramTypes64Op128, retType64,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(DivDec128Dec64Dec64), "Div_decimal128", {}, paramTypes128Op64, retType64,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(DivDec64Dec64Dec128), "Div_decimal64", {}, paramTypes64, retType128,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(DivDec128Dec128Dec128), "Div_decimal128", {}, paramTypes128, retType128,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(DivDec64Dec128Dec128), "Div_decimal64", {}, paramTypes64Op128, retType128,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(DivDec128Dec64Dec128), "Div_decimal128", {}, paramTypes128Op64, retType128,
            INPUT_DATA, true),

        Function(reinterpret_cast<void *>(ModDec64Dec64Dec64), "Mod_decimal64", {}, paramTypes64, retType64,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(ModDec64Dec128Dec64), "Mod_decimal64", {}, paramTypes64Op128, retType64,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(ModDec128Dec64Dec64), "Mod_decimal128", {}, paramTypes128Op64, retType64,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(ModDec128Dec64Dec128), "Mod_decimal128", {}, paramTypes128Op64, retType128,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(ModDec128Dec128Dec128), "Mod_decimal128", {}, paramTypes128, retType128,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(ModDec128Dec128Dec64), "Mod_decimal128", {}, paramTypes128, retType64,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(ModDec64Dec128Dec128), "Mod_decimal64", {}, paramTypes64Op128, retType128,
            INPUT_DATA, true),

        Function(reinterpret_cast<void *>(CastDecimal64To64), "CAST", {}, { OMNI_DECIMAL64 }, OMNI_DECIMAL64,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(CastDecimal128To128), "CAST", {}, { OMNI_DECIMAL128 }, OMNI_DECIMAL128,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(CastDecimal64To128), "CAST", {}, { OMNI_DECIMAL64 }, OMNI_DECIMAL128,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(CastDecimal128To64), "CAST", {}, { OMNI_DECIMAL128 }, OMNI_DECIMAL64,
            INPUT_DATA, true),

        Function(reinterpret_cast<void *>(CastIntToDecimal64), "CAST", {}, { OMNI_INT }, OMNI_DECIMAL64,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(CastLongToDecimal64), "CAST", {}, { OMNI_LONG }, OMNI_DECIMAL64,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(CastDoubleToDecimal64), "CAST", {}, { OMNI_DOUBLE }, OMNI_DECIMAL64,
            INPUT_DATA, true),

        Function(reinterpret_cast<void *>(CastIntToDecimal128), "CAST", {}, { OMNI_INT }, OMNI_DECIMAL128,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(CastLongToDecimal128), "CAST", {}, { OMNI_LONG }, OMNI_DECIMAL128,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(CastDoubleToDecimal128), "CAST", {}, { OMNI_DOUBLE }, OMNI_DECIMAL128,
            INPUT_DATA, true),

        Function(reinterpret_cast<void *>(CastDecimal64ToLong), "CAST", {}, { OMNI_DECIMAL64 }, OMNI_LONG,
            INPUT_DATA),
        Function(reinterpret_cast<void *>(CastDecimal64ToInt), "CAST", {}, { OMNI_DECIMAL64 }, OMNI_INT,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(CastDecimal64ToDouble), "CAST", {}, { OMNI_DECIMAL64 }, OMNI_DOUBLE,
            INPUT_DATA),

        Function(reinterpret_cast<void *>(CastDecimal128ToLong), "CAST", {}, { OMNI_DECIMAL128 }, OMNI_LONG,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(CastDecimal128ToInt), "CAST", {}, { OMNI_DECIMAL128 }, OMNI_INT,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(CastDecimal128ToDouble), "CAST", {}, { OMNI_DECIMAL128 }, OMNI_DOUBLE,
            INPUT_DATA),

        Function(reinterpret_cast<void *>(AbsDecimal128), "abs", {}, { OMNI_DECIMAL128 }, retType128,
            INPUT_DATA),
        Function(reinterpret_cast<void *>(AbsDecimal64), "abs", {}, { OMNI_DECIMAL64 }, OMNI_DECIMAL64,
            INPUT_DATA),

        Function(reinterpret_cast<void *>(RoundDecimal128), "round", {}, { OMNI_DECIMAL128, OMNI_INT }, retType128,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(RoundDecimal64), "round", {}, { OMNI_DECIMAL64, OMNI_INT }, retType64,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(RoundDecimal128WithoutRound), "round", {}, { OMNI_DECIMAL128 }, retType128,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(RoundDecimal64WithoutRound), "round", {}, { OMNI_DECIMAL64 }, retType64,
            INPUT_DATA, true),

        Function(reinterpret_cast<void *>(Decimal64Compare), "Decimal64Compare", {}, paramTypes64, OMNI_INT),
        Function(reinterpret_cast<void *>(Decimal128Compare), "Decimal128Compare", {}, paramTypes128, OMNI_INT),

        // Return Null
        Function(reinterpret_cast<void *>(RoundDecimal128RetNull), "round_null", {}, { OMNI_DECIMAL128, OMNI_INT },
            retType128, INPUT_DATA),
        Function(reinterpret_cast<void *>(RoundDecimal64RetNull), "round_null", {}, { OMNI_DECIMAL64, OMNI_INT },
            retType64, INPUT_DATA),

        Function(reinterpret_cast<void *>(AddDec64Dec64Dec64RetNull), "Add_decimal64_null", {}, paramTypes64, retType64,
            INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(AddDec64Dec64Dec128RetNull), "Add_decimal64_null", {}, paramTypes64,
            retType128, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(AddDec128Dec128Dec128RetNull), "Add_decimal128_null", {}, paramTypes128,
            retType128, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(AddDec64Dec128Dec128RetNull), "Add_decimal64_null", {}, paramTypes64Op128,
            retType128, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(AddDec128Dec64Dec128RetNull), "Add_decimal128_null", {}, paramTypes128Op64,
            retType128, INPUT_DATA_AND_OVERFLOW_NULL),

        Function(reinterpret_cast<void *>(SubDec64Dec64Dec64RetNull), "Sub_decimal64_null", {}, paramTypes64, retType64,
            INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(SubDec64Dec64Dec128RetNull), "Sub_decimal64_null", {}, paramTypes64,
            retType128, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(SubDec128Dec128Dec128RetNull), "Sub_decimal128_null", {}, paramTypes128,
            retType128, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(SubDec64Dec128Dec128RetNull), "Sub_decimal64_null", {}, paramTypes64Op128,
            retType128, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(SubDec128Dec64Dec128RetNull), "Sub_decimal128_null", {}, paramTypes128Op64,
            retType128, INPUT_DATA_AND_OVERFLOW_NULL),

        Function(reinterpret_cast<void *>(MulDec64Dec64Dec64RetNull), "Mul_decimal64_null", {}, paramTypes64, retType64,
            INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(MulDec64Dec64Dec128RetNull), "Mul_decimal64_null", {}, paramTypes64,
            retType128, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(MulDec128Dec128Dec128RetNull), "Mul_decimal128_null", {}, paramTypes128,
            retType128, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(MulDec64Dec128Dec128RetNull), "Mul_decimal64_null", {}, paramTypes64Op128,
            retType128, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(MulDec128Dec64Dec128RetNull), "Mul_decimal128_null", {}, paramTypes128Op64,
            retType128, INPUT_DATA_AND_OVERFLOW_NULL),

        Function(reinterpret_cast<void *>(DivDec64Dec64Dec64RetNull), "Div_decimal64_null", {}, paramTypes64, retType64,
            INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(DivDec64Dec128Dec64RetNull), "Div_decimal64_null", {}, paramTypes64Op128,
            retType64, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(DivDec128Dec64Dec64RetNull), "Div_decimal128_null", {}, paramTypes128Op64,
            retType64, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(DivDec64Dec64Dec128RetNull), "Div_decimal64_null", {}, paramTypes64,
            retType128, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(DivDec128Dec128Dec128RetNull), "Div_decimal128_null", {}, paramTypes128,
            retType128, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(DivDec64Dec128Dec128RetNull), "Div_decimal64_null", {}, paramTypes64Op128,
            retType128, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(DivDec128Dec64Dec128RetNull), "Div_decimal128_null", {}, paramTypes128Op64,
            retType128, INPUT_DATA_AND_OVERFLOW_NULL),

        Function(reinterpret_cast<void *>(ModDec64Dec64Dec64RetNull), "Mod_decimal64_null", {}, paramTypes64, retType64,
            INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(ModDec64Dec128Dec64RetNull), "Mod_decimal64_null", {}, paramTypes64Op128,
            retType64, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(ModDec128Dec64Dec64RetNull), "Mod_decimal128_null", {}, paramTypes128Op64,
            retType64, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(ModDec128Dec64Dec128RetNull), "Mod_decimal128_null", {}, paramTypes128Op64,
            retType128, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(ModDec128Dec128Dec128RetNull), "Mod_decimal128_null", {}, paramTypes128,
            retType128, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(ModDec128Dec128Dec64RetNull), "Mod_decimal128_null", {}, paramTypes128,
            retType64, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(ModDec64Dec128Dec128RetNull), "Mod_decimal64_null", {}, paramTypes64Op128,
            retType128, INPUT_DATA_AND_OVERFLOW_NULL),

        Function(reinterpret_cast<void *>(CastDecimal64To64RetNull), "CAST_null", {}, { OMNI_DECIMAL64 },
            OMNI_DECIMAL64, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(CastDecimal128To128RetNull), "CAST_null", {}, { OMNI_DECIMAL128 },
            OMNI_DECIMAL128, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(CastDecimal64To128RetNull), "CAST_null", {}, { OMNI_DECIMAL64 },
            OMNI_DECIMAL128, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(CastDecimal128To64RetNull), "CAST_null", {}, { OMNI_DECIMAL128 },
            OMNI_DECIMAL64, INPUT_DATA_AND_OVERFLOW_NULL),

        Function(reinterpret_cast<void *>(CastIntToDecimal64RetNull), "CAST_null", {}, { OMNI_INT }, OMNI_DECIMAL64,
            INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(CastLongToDecimal64RetNull), "CAST_null", {}, { OMNI_LONG }, OMNI_DECIMAL64,
            INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(CastDoubleToDecimal64RetNull), "CAST_null", {}, { OMNI_DOUBLE },
            OMNI_DECIMAL64, INPUT_DATA_AND_OVERFLOW_NULL),

        Function(reinterpret_cast<void *>(CastIntToDecimal128RetNull), "CAST_null", {}, { OMNI_INT }, OMNI_DECIMAL128,
            INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(CastLongToDecimal128RetNull), "CAST_null", {}, { OMNI_LONG }, OMNI_DECIMAL128,
            INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(CastDoubleToDecimal128RetNull), "CAST_null", {}, { OMNI_DOUBLE },
            OMNI_DECIMAL128, INPUT_DATA_AND_OVERFLOW_NULL),

        Function(reinterpret_cast<void *>(CastDecimal64ToIntRetNull), "CAST_null", {}, { OMNI_DECIMAL64 }, OMNI_INT,
            INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(CastDecimal64ToLongRetNull), "CAST_null", {}, { OMNI_DECIMAL64 }, OMNI_LONG,
            INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(CastDecimal64ToDoubleRetNull), "CAST_null", {}, { OMNI_DECIMAL64 },
            OMNI_DOUBLE, INPUT_DATA_AND_OVERFLOW_NULL),

        Function(reinterpret_cast<void *>(CastDecimal128ToIntRetNull), "CAST_null", {}, { OMNI_DECIMAL128 }, OMNI_INT,
            INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(CastDecimal128ToLongRetNull), "CAST_null", {}, { OMNI_DECIMAL128 }, OMNI_LONG,
            INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(CastDecimal128ToDoubleRetNull), "CAST_null", {}, { OMNI_DECIMAL128 },
            OMNI_DOUBLE, INPUT_DATA_AND_OVERFLOW_NULL),

        // UnscaledValue
        Function(reinterpret_cast<void *>(UnscaledValue64), "UnscaledValue", {}, {OMNI_DECIMAL64}, OMNI_LONG,
                 INPUT_DATA),
        // MakeDecimal
        Function(reinterpret_cast<void *>(MakeDecimal64), "MakeDecimal", {}, {OMNI_LONG}, OMNI_DECIMAL64,
                 INPUT_DATA, true),
        Function(reinterpret_cast<void *>(MakeDecimal64RetNull), "MakeDecimal_null", {}, {OMNI_LONG}, OMNI_DECIMAL64,
                 INPUT_DATA_AND_OVERFLOW_NULL),
    };

    return decimalFnRegistry;
}
