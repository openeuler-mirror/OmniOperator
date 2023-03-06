/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: Batch Decimal Function Registry
 */
#include "batch_func_registry_decimal.h"
#include "batch_functions/batch_decimal_arithmetic_functions.h"
#include "batch_functions/batch_decimal_cast_functions.h"

using namespace omniruntime;
using namespace omniruntime::type;
using namespace omniruntime::codegen;

namespace {
const std::string absFnStr = "batch_abs";
const std::string roundFnStr = "batch_round";
const std::string roundNullFnStr = "batch_round_null";
const std::string castFnStr = "batch_CAST";
const std::string castNullFnStr = "batch_CAST_null";
const std::string addNullFnStr = "batch_add_null";
const std::string subtractNullFnStr = "batch_subtract_null";
const std::string multiplyNullFnStr = "batch_multiply_null";
const std::string divideNullFnStr = "batch_divide_null";
const std::string modulusNullFnStr = "batch_modulus_null";
const std::string addFnStr = "batch_add";
const std::string subtractFnStr = "batch_subtract";
const std::string multiplyFnStr = "batch_multiply";
const std::string divideFnStr = "batch_divide";
const std::string modulusFnStr = "batch_modulus";
const std::string lessThanFnStr = "batch_lessThan";
const std::string lessThanEqualFnStr = "batch_lessThanEqual";
const std::string greaterThanFnStr = "batch_greaterThan";
const std::string greaterThanEqualFnStr = "batch_greaterThanEqual";
const std::string equalFnStr = "batch_equal";
const std::string notEqualFnStr = "batch_notEqual";
const std::string makeDecimalStr = "batch_MakeDecimal";
const std::string makeDecimalNullStr = "batch_MakeDecimal_null";
const std::string batchDecimal128CompareStr = "batch_Decimal128Compare";
const std::string batchDecimal64CompareStr = "batch_Decimal64Compare";
const std::string batchUnscaledValueStr = "batch_UnscaledValue";
}

std::vector<Function> BatchDecimalFunctionRegistry::GetFunctions()
{
    std::vector<DataTypeId> paramTypes128 = { OMNI_DECIMAL128, OMNI_DECIMAL128 };
    std::vector<DataTypeId> paramTypes64 = { OMNI_DECIMAL64, OMNI_DECIMAL64 };
    std::vector<DataTypeId> paramTypes64Op128 = { OMNI_DECIMAL64, OMNI_DECIMAL128 };
    std::vector<DataTypeId> paramTypes128Op64 = { OMNI_DECIMAL128, OMNI_DECIMAL64 };

    static std::vector<Function> batchDecimalFunctions = {
        Function(reinterpret_cast<void *>(BatchDecimal128Compare), batchDecimal128CompareStr, {}, paramTypes128,
            OMNI_INT),
        Function(reinterpret_cast<void *>(BatchAbsDecimal128), absFnStr, {}, { OMNI_DECIMAL128 }, OMNI_DECIMAL128,
            INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchDecimal64Compare), batchDecimal64CompareStr, {}, paramTypes64, OMNI_INT),
        Function(reinterpret_cast<void *>(BatchAbsDecimal64), absFnStr, {}, { OMNI_DECIMAL64 }, OMNI_DECIMAL64,
            INPUT_DATA),

        Function(reinterpret_cast<void *>(BatchRoundDecimal128), roundFnStr, {}, { OMNI_DECIMAL128, OMNI_INT },
            OMNI_DECIMAL64, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchRoundDecimal64), roundFnStr, {}, { OMNI_DECIMAL64, OMNI_INT },
            OMNI_DECIMAL64, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchRoundDecimal128WithoutRound), roundFnStr, {}, { OMNI_DECIMAL128 },
            OMNI_DECIMAL64, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchRoundDecimal64WithoutRound), roundFnStr, {}, { OMNI_DECIMAL64 },
            OMNI_DECIMAL64, INPUT_DATA, true),

        // decimal64 compare
        Function(reinterpret_cast<void *>(BatchLessThanDecimal64), lessThanFnStr, {}, paramTypes64, OMNI_BOOLEAN,
            INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchLessThanEqualDecimal64), lessThanEqualFnStr, {}, paramTypes64,
            OMNI_BOOLEAN, INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchGreaterThanDecimal64), greaterThanFnStr, {}, paramTypes64, OMNI_BOOLEAN,
            INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchGreaterThanEqualDecimal64), greaterThanEqualFnStr, {}, paramTypes64,
            OMNI_BOOLEAN, INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchEqualDecimal64), equalFnStr, {}, paramTypes64, OMNI_BOOLEAN, INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchNotEqualDecimal64), notEqualFnStr, {}, paramTypes64, OMNI_BOOLEAN,
            INPUT_DATA),

        // decimal128 compare
        Function(reinterpret_cast<void *>(BatchLessThanDecimal128), lessThanFnStr, {}, paramTypes128, OMNI_BOOLEAN,
            INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchLessThanEqualDecimal128), lessThanEqualFnStr, {}, paramTypes128,
            OMNI_BOOLEAN, INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchGreaterThanDecimal128), greaterThanFnStr, {}, paramTypes128,
            OMNI_BOOLEAN, INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchGreaterThanEqualDecimal128), greaterThanEqualFnStr, {}, paramTypes128,
            OMNI_BOOLEAN, INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchEqualDecimal128), equalFnStr, {}, paramTypes128, OMNI_BOOLEAN,
            INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchNotEqualDecimal128), notEqualFnStr, {}, paramTypes128, OMNI_BOOLEAN,
            INPUT_DATA),

        // Decimal Cast Function
        Function(reinterpret_cast<void *>(BatchCastDecimal64To64), castFnStr, {}, { OMNI_DECIMAL64 }, OMNI_DECIMAL64,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchCastDecimal128To128), castFnStr, {}, { OMNI_DECIMAL128 },
            OMNI_DECIMAL128, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchCastDecimal64To128), castFnStr, {}, { OMNI_DECIMAL64 }, OMNI_DECIMAL128,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchCastDecimal128To64), castFnStr, {}, { OMNI_DECIMAL128 }, OMNI_DECIMAL64,
            INPUT_DATA, true),

        Function(reinterpret_cast<void *>(BatchCastIntToDecimal64), castFnStr, {}, { OMNI_INT }, OMNI_DECIMAL64,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchCastLongToDecimal64), castFnStr, {}, { OMNI_LONG }, OMNI_DECIMAL64,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchCastDoubleToDecimal64), castFnStr, {}, { OMNI_DOUBLE }, OMNI_DECIMAL64,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchCastIntToDecimal128), castFnStr, {}, { OMNI_INT }, OMNI_DECIMAL128,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchCastLongToDecimal128), castFnStr, {}, { OMNI_LONG }, OMNI_DECIMAL128,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchCastDoubleToDecimal128), castFnStr, {}, { OMNI_DOUBLE }, OMNI_DECIMAL128,
            INPUT_DATA, true),

        Function(reinterpret_cast<void *>(BatchCastDecimal128ToLong), castFnStr, {}, { OMNI_DECIMAL128 }, OMNI_LONG,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchCastDecimal128ToInt), castFnStr, {}, { OMNI_DECIMAL128 }, OMNI_INT,
            INPUT_DATA, true),

        // Decimal Cast Function Return Null
        Function(reinterpret_cast<void *>(BatchRoundDecimal128RetNull), roundNullFnStr, {},
            { OMNI_DECIMAL128, OMNI_INT },
            OMNI_DECIMAL64, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(BatchRoundDecimal128RetNull), roundNullFnStr, {},
            { OMNI_DECIMAL64, OMNI_INT },
            OMNI_DECIMAL64, INPUT_DATA_AND_OVERFLOW_NULL),

        Function(reinterpret_cast<void *>(BatchCastDecimal64To64RetNull), castNullFnStr, {}, { OMNI_DECIMAL64 },
            OMNI_DECIMAL64, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(BatchCastDecimal128To128RetNull), castNullFnStr, {}, { OMNI_DECIMAL128 },
            OMNI_DECIMAL128, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(BatchCastDecimal64To128RetNull), castNullFnStr, {}, { OMNI_DECIMAL64 },
            OMNI_DECIMAL128, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(BatchCastDecimal128To64RetNull), castNullFnStr, {}, { OMNI_DECIMAL128 },
            OMNI_DECIMAL64, INPUT_DATA_AND_OVERFLOW_NULL),

        Function(reinterpret_cast<void *>(BatchCastDecimal64ToDoubleRetNull), castNullFnStr, {}, { OMNI_DECIMAL64 },
            OMNI_DOUBLE, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(BatchCastDecimal64ToLongRetNull), castNullFnStr, {}, { OMNI_DECIMAL64 },
            OMNI_LONG, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(BatchCastDecimal128ToDoubleRetNull), castNullFnStr, {}, { OMNI_DECIMAL128 },
            OMNI_DOUBLE, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(BatchCastDecimal64ToIntRetNull), castNullFnStr, {}, { OMNI_DECIMAL64 },
            OMNI_INT, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(BatchCastDecimal128ToIntRetNull), castNullFnStr, {}, { OMNI_DECIMAL128 },
            OMNI_INT, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(BatchCastDecimal128ToLongRetNull), castNullFnStr, {}, { OMNI_DECIMAL128 },
            OMNI_LONG, INPUT_DATA_AND_OVERFLOW_NULL),

        Function(reinterpret_cast<void *>(BatchCastIntToDecimal64RetNull), castNullFnStr, {}, { OMNI_INT },
            OMNI_DECIMAL64, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(BatchCastLongToDecimal64RetNull), castNullFnStr, {}, { OMNI_LONG },
            OMNI_DECIMAL64, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(BatchCastDoubleToDecimal64RetNull), castNullFnStr, {}, { OMNI_DOUBLE },
            OMNI_DECIMAL64, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(BatchCastIntToDecimal128RetNull), castNullFnStr, {}, { OMNI_INT },
            OMNI_DECIMAL128, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(BatchCastLongToDecimal128RetNull), castNullFnStr, {}, { OMNI_LONG },
            OMNI_DECIMAL128, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(BatchCastDoubleToDecimal128RetNull), castNullFnStr, {}, { OMNI_DOUBLE },
            OMNI_DECIMAL128, INPUT_DATA_AND_OVERFLOW_NULL),

        // UnscaledValue
        Function(reinterpret_cast<void *>(BatchUnscaledValue64), batchUnscaledValueStr, {}, { OMNI_DECIMAL64 },
            OMNI_LONG, INPUT_DATA),
        // MakeDecimal
        Function(reinterpret_cast<void *>(BatchMakeDecimal64), makeDecimalStr, {}, { OMNI_LONG }, OMNI_DECIMAL64,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchMakeDecimal64RetNull), makeDecimalNullStr, {}, { OMNI_LONG },
            OMNI_DECIMAL64, INPUT_DATA_AND_OVERFLOW_NULL),

        // Return Null
        Function(reinterpret_cast<void *>(BatchAddDec64Dec64Dec64RetNull), addNullFnStr, {}, paramTypes64,
            OMNI_DECIMAL64, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(BatchAddDec64Dec64Dec128RetNull), addNullFnStr, {}, paramTypes64,
            OMNI_DECIMAL128, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(BatchAddDec128Dec128Dec128RetNull), addNullFnStr, {}, paramTypes128,
            OMNI_DECIMAL128, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(BatchAddDec64Dec128Dec128RetNull), addNullFnStr, {}, paramTypes64Op128,
            OMNI_DECIMAL128, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(BatchAddDec128Dec64Dec128RetNull), addNullFnStr, {}, paramTypes128Op64,
            OMNI_DECIMAL128, INPUT_DATA_AND_OVERFLOW_NULL),

        Function(reinterpret_cast<void *>(BatchSubDec64Dec64Dec64RetNull), subtractNullFnStr, {}, paramTypes64,
            OMNI_DECIMAL64, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(BatchSubDec64Dec64Dec128RetNull), subtractNullFnStr, {}, paramTypes64,
            OMNI_DECIMAL128, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(BatchSubDec128Dec128Dec128RetNull), subtractNullFnStr, {}, paramTypes128,
            OMNI_DECIMAL128, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(BatchSubDec64Dec128Dec128RetNull), subtractNullFnStr, {}, paramTypes64Op128,
            OMNI_DECIMAL128, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(BatchSubDec128Dec64Dec128RetNull), subtractNullFnStr, {}, paramTypes128Op64,
            OMNI_DECIMAL128, INPUT_DATA_AND_OVERFLOW_NULL),

        Function(reinterpret_cast<void *>(BatchMulDec64Dec64Dec64RetNull), multiplyNullFnStr, {}, paramTypes64,
            OMNI_DECIMAL64, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(BatchMulDec64Dec64Dec128RetNull), multiplyNullFnStr, {}, paramTypes64,
            OMNI_DECIMAL128, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(BatchMulDec128Dec128Dec128RetNull), multiplyNullFnStr, {}, paramTypes128,
            OMNI_DECIMAL128, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(BatchMulDec64Dec128Dec128RetNull), multiplyNullFnStr, {}, paramTypes64Op128,
            OMNI_DECIMAL128, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(BatchMulDec128Dec64Dec128RetNull), multiplyNullFnStr, {}, paramTypes128Op64,
            OMNI_DECIMAL128, INPUT_DATA_AND_OVERFLOW_NULL),

        Function(reinterpret_cast<void *>(BatchDivDec64Dec64Dec64RetNull), divideNullFnStr, {}, paramTypes64,
            OMNI_DECIMAL64, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(BatchDivDec64Dec128Dec64RetNull), divideNullFnStr, {}, paramTypes64Op128,
            OMNI_DECIMAL64, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(BatchDivDec128Dec64Dec64RetNull), divideNullFnStr, {}, paramTypes128Op64,
            OMNI_DECIMAL64, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(BatchDivDec64Dec64Dec128RetNull), divideNullFnStr, {}, paramTypes64,
            OMNI_DECIMAL128, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(BatchDivDec128Dec128Dec128RetNull), divideNullFnStr, {}, paramTypes128,
            OMNI_DECIMAL128, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(BatchDivDec64Dec128Dec128RetNull), divideNullFnStr, {}, paramTypes64Op128,
            OMNI_DECIMAL128, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(BatchDivDec128Dec64Dec128RetNull), divideNullFnStr, {}, paramTypes128Op64,
            OMNI_DECIMAL128, INPUT_DATA_AND_OVERFLOW_NULL),

        Function(reinterpret_cast<void *>(BatchModDec64Dec64Dec64RetNull), modulusNullFnStr, {}, paramTypes64,
            OMNI_DECIMAL64, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(BatchModDec64Dec128Dec64RetNull), modulusNullFnStr, {}, paramTypes64Op128,
            OMNI_DECIMAL64, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(BatchModDec128Dec64Dec64RetNull), modulusNullFnStr, {}, paramTypes128Op64,
            OMNI_DECIMAL64, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(BatchModDec128Dec64Dec128RetNull), modulusNullFnStr, {}, paramTypes128Op64,
            OMNI_DECIMAL128, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(BatchModDec128Dec128Dec128RetNull), modulusNullFnStr, {}, paramTypes128,
            OMNI_DECIMAL128, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(BatchModDec128Dec128Dec64RetNull), modulusNullFnStr, {}, paramTypes128,
            OMNI_DECIMAL64, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(BatchModDec64Dec128Dec128RetNull), modulusNullFnStr, {}, paramTypes64Op128,
            OMNI_DECIMAL128, INPUT_DATA_AND_OVERFLOW_NULL),
    };

    return batchDecimalFunctions;
}

std::vector<Function> BatchDecimalFunctionRegistryDown::GetFunctions()
{
    static std::vector<Function> batchDecimalFunctions = {
        Function(reinterpret_cast<void *>(BatchCastDecimal64ToLongDown), castFnStr, {}, { OMNI_DECIMAL64 }, OMNI_LONG,
            INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchCastDecimal64ToIntDown), castFnStr, {}, { OMNI_DECIMAL64 }, OMNI_INT,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchCastDecimal64ToDoubleDown), castFnStr, {}, { OMNI_DECIMAL64 },
            OMNI_DOUBLE,
            INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchCastDecimal128ToDoubleDown), castFnStr, {}, { OMNI_DECIMAL128 },
            OMNI_DOUBLE,
            INPUT_DATA),
    };

    return batchDecimalFunctions;
}

std::vector<Function> BatchDecimalFunctionRegistryHalfUp::GetFunctions()
{
    static std::vector<Function> batchDecimalFunctions = {
        Function(reinterpret_cast<void *>(BatchCastDecimal64ToLongHalfUp), castFnStr, {}, { OMNI_DECIMAL64 }, OMNI_LONG,
            INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchCastDecimal64ToIntHalfUp), castFnStr, {}, { OMNI_DECIMAL64 }, OMNI_INT,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchCastDecimal64ToDoubleHalfUp), castFnStr, {}, { OMNI_DECIMAL64 },
            OMNI_DOUBLE,
            INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchCastDecimal128ToDoubleHalfUp), castFnStr, {}, { OMNI_DECIMAL128 },
            OMNI_DOUBLE,
            INPUT_DATA),
    };

    return batchDecimalFunctions;
}

std::vector<Function> BatchDecimalFunctionRegistryReScale::GetFunctions()
{
    std::vector<DataTypeId> paramTypes128 = { OMNI_DECIMAL128, OMNI_DECIMAL128 };
    std::vector<DataTypeId> paramTypes64 = { OMNI_DECIMAL64, OMNI_DECIMAL64 };
    std::vector<DataTypeId> paramTypes64Op128 = { OMNI_DECIMAL64, OMNI_DECIMAL128 };
    std::vector<DataTypeId> paramTypes128Op64 = { OMNI_DECIMAL128, OMNI_DECIMAL64 };

    static std::vector<Function> batchDecimalFunctions = {
        // decimal arith function
        Function(reinterpret_cast<void *>(BatchAddDec64Dec64Dec64ReScale), addFnStr, {}, paramTypes64, OMNI_DECIMAL64,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchAddDec64Dec64Dec128ReScale), addFnStr, {}, paramTypes64, OMNI_DECIMAL128,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchAddDec128Dec128Dec128ReScale), addFnStr, {}, paramTypes128,
            OMNI_DECIMAL128,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchAddDec64Dec128Dec128ReScale), addFnStr, {}, paramTypes64Op128,
            OMNI_DECIMAL128,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchAddDec128Dec64Dec128ReScale), addFnStr, {}, paramTypes128Op64,
            OMNI_DECIMAL128,
            INPUT_DATA, true),

        Function(reinterpret_cast<void *>(BatchSubDec64Dec64Dec64ReScale), subtractFnStr, {}, paramTypes64,
            OMNI_DECIMAL64,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchSubDec64Dec64Dec128ReScale), subtractFnStr, {}, paramTypes64,
            OMNI_DECIMAL128,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchSubDec128Dec128Dec128ReScale), subtractFnStr, {}, paramTypes128,
            OMNI_DECIMAL128, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchSubDec64Dec128Dec128ReScale), subtractFnStr, {}, paramTypes64Op128,
            OMNI_DECIMAL128, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchSubDec128Dec64Dec128ReScale), subtractFnStr, {}, paramTypes128Op64,
            OMNI_DECIMAL128, INPUT_DATA, true),

        Function(reinterpret_cast<void *>(BatchMulDec64Dec64Dec64ReScale), multiplyFnStr, {}, paramTypes64,
            OMNI_DECIMAL64,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchMulDec64Dec64Dec128ReScale), multiplyFnStr, {}, paramTypes64,
            OMNI_DECIMAL128,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchMulDec128Dec128Dec128ReScale), multiplyFnStr, {}, paramTypes128,
            OMNI_DECIMAL128, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchMulDec64Dec128Dec128ReScale), multiplyFnStr, {}, paramTypes64Op128,
            OMNI_DECIMAL128, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchMulDec128Dec64Dec128ReScale), multiplyFnStr, {}, paramTypes128Op64,
            OMNI_DECIMAL128, INPUT_DATA, true),

        Function(reinterpret_cast<void *>(BatchDivDec64Dec64Dec64ReScale), divideFnStr, {}, paramTypes64,
            OMNI_DECIMAL64,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchDivDec64Dec128Dec64ReScale), divideFnStr, {}, paramTypes64Op128,
            OMNI_DECIMAL64,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchDivDec128Dec64Dec64ReScale), divideFnStr, {}, paramTypes128Op64,
            OMNI_DECIMAL64,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchDivDec64Dec64Dec128ReScale), divideFnStr, {}, paramTypes64,
            OMNI_DECIMAL128,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchDivDec128Dec128Dec128ReScale), divideFnStr, {}, paramTypes128,
            OMNI_DECIMAL128,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchDivDec64Dec128Dec128ReScale), divideFnStr, {}, paramTypes64Op128,
            OMNI_DECIMAL128, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchDivDec128Dec64Dec128ReScale), divideFnStr, {}, paramTypes128Op64,
            OMNI_DECIMAL128, INPUT_DATA, true),

        Function(reinterpret_cast<void *>(BatchModDec64Dec64Dec64ReScale), modulusFnStr, {}, paramTypes64,
            OMNI_DECIMAL64,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchModDec64Dec128Dec64ReScale), modulusFnStr, {}, paramTypes64Op128,
            OMNI_DECIMAL64, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchModDec128Dec64Dec64ReScale), modulusFnStr, {}, paramTypes128Op64,
            OMNI_DECIMAL64, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchModDec128Dec64Dec128ReScale), modulusFnStr, {}, paramTypes128Op64,
            OMNI_DECIMAL128, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchModDec128Dec128Dec128ReScale), modulusFnStr, {}, paramTypes128,
            OMNI_DECIMAL128,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchModDec128Dec128Dec64ReScale), modulusFnStr, {}, paramTypes128,
            OMNI_DECIMAL64,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchModDec64Dec128Dec128ReScale), modulusFnStr, {}, paramTypes64Op128,
            OMNI_DECIMAL128, INPUT_DATA, true),
    };

    return batchDecimalFunctions;
}

std::vector<Function> BatchDecimalFunctionRegistryNotReScale::GetFunctions()
{
    std::vector<DataTypeId> paramTypes128 = { OMNI_DECIMAL128, OMNI_DECIMAL128 };
    std::vector<DataTypeId> paramTypes64 = { OMNI_DECIMAL64, OMNI_DECIMAL64 };
    std::vector<DataTypeId> paramTypes64Op128 = { OMNI_DECIMAL64, OMNI_DECIMAL128 };
    std::vector<DataTypeId> paramTypes128Op64 = { OMNI_DECIMAL128, OMNI_DECIMAL64 };

    static std::vector<Function> batchDecimalFunctions = {
        // decimal arith function
        Function(reinterpret_cast<void *>(BatchAddDec64Dec64Dec64NotReScale), addFnStr, {}, paramTypes64,
            OMNI_DECIMAL64,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchAddDec64Dec64Dec128NotReScale), addFnStr, {}, paramTypes64,
            OMNI_DECIMAL128,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchAddDec128Dec128Dec128NotReScale), addFnStr, {}, paramTypes128,
            OMNI_DECIMAL128,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchAddDec64Dec128Dec128NotReScale), addFnStr, {}, paramTypes64Op128,
            OMNI_DECIMAL128,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchAddDec128Dec64Dec128NotReScale), addFnStr, {}, paramTypes128Op64,
            OMNI_DECIMAL128,
            INPUT_DATA, true),

        Function(reinterpret_cast<void *>(BatchSubDec64Dec64Dec64NotReScale), subtractFnStr, {}, paramTypes64,
            OMNI_DECIMAL64,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchSubDec64Dec64Dec128NotReScale), subtractFnStr, {}, paramTypes64,
            OMNI_DECIMAL128,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchSubDec128Dec128Dec128NotReScale), subtractFnStr, {}, paramTypes128,
            OMNI_DECIMAL128, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchSubDec64Dec128Dec128NotReScale), subtractFnStr, {}, paramTypes64Op128,
            OMNI_DECIMAL128, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchSubDec128Dec64Dec128NotReScale), subtractFnStr, {}, paramTypes128Op64,
            OMNI_DECIMAL128, INPUT_DATA, true),

        Function(reinterpret_cast<void *>(BatchMulDec64Dec64Dec64NotReScale), multiplyFnStr, {}, paramTypes64,
            OMNI_DECIMAL64,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchMulDec64Dec64Dec128NotReScale), multiplyFnStr, {}, paramTypes64,
            OMNI_DECIMAL128,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchMulDec128Dec128Dec128NotReScale), multiplyFnStr, {}, paramTypes128,
            OMNI_DECIMAL128, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchMulDec64Dec128Dec128NotReScale), multiplyFnStr, {}, paramTypes64Op128,
            OMNI_DECIMAL128, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchMulDec128Dec64Dec128NotReScale), multiplyFnStr, {}, paramTypes128Op64,
            OMNI_DECIMAL128, INPUT_DATA, true),

        Function(reinterpret_cast<void *>(BatchDivDec64Dec64Dec64NotReScale), divideFnStr, {}, paramTypes64,
            OMNI_DECIMAL64,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchDivDec64Dec128Dec64NotReScale), divideFnStr, {}, paramTypes64Op128,
            OMNI_DECIMAL64,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchDivDec128Dec64Dec64NotReScale), divideFnStr, {}, paramTypes128Op64,
            OMNI_DECIMAL64,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchDivDec64Dec64Dec128NotReScale), divideFnStr, {}, paramTypes64,
            OMNI_DECIMAL128,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchDivDec128Dec128Dec128NotReScale), divideFnStr, {}, paramTypes128,
            OMNI_DECIMAL128,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchDivDec64Dec128Dec128NotReScale), divideFnStr, {}, paramTypes64Op128,
            OMNI_DECIMAL128, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchDivDec128Dec64Dec128NotReScale), divideFnStr, {}, paramTypes128Op64,
            OMNI_DECIMAL128, INPUT_DATA, true),

        Function(reinterpret_cast<void *>(BatchModDec64Dec64Dec64NotReScale), modulusFnStr, {}, paramTypes64,
            OMNI_DECIMAL64,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchModDec64Dec128Dec64NotReScale), modulusFnStr, {}, paramTypes64Op128,
            OMNI_DECIMAL64, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchModDec128Dec64Dec64NotReScale), modulusFnStr, {}, paramTypes128Op64,
            OMNI_DECIMAL64, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchModDec128Dec64Dec128NotReScale), modulusFnStr, {}, paramTypes128Op64,
            OMNI_DECIMAL128, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchModDec128Dec128Dec128NotReScale), modulusFnStr, {}, paramTypes128,
            OMNI_DECIMAL128,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchModDec128Dec128Dec64NotReScale), modulusFnStr, {}, paramTypes128,
            OMNI_DECIMAL64,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchModDec64Dec128Dec128NotReScale), modulusFnStr, {}, paramTypes64Op128,
            OMNI_DECIMAL128, INPUT_DATA, true),
    };

    return batchDecimalFunctions;
}
