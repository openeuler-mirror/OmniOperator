/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: Batch Decimal Function Registry
 */
#include "batch_func_registry_decimal.h"
#include "batch_functions/batch_decimalfunctions.h"

using namespace omniruntime;
using namespace omniruntime::type;
using namespace omniruntime::codegen;

std::vector<Function> BatchDecimalFunctionRegistry::GetFunctions()
{
    std::string absFnStr = "batch_abs";
    std::string castFnStr = "batch_CAST";
    std::string castNullFnStr = "batch_CAST_null";
    std::string addFnStr = "batch_add";
    std::string subtractFnStr = "batch_subtract";
    std::string multiplyFnStr = "batch_multiply";
    std::string divideFnStr = "batch_divide";
    std::string modulusFnStr = "batch_modulus";
    std::string addNullFnStr = "batch_add_null";
    std::string subtractNullFnStr = "batch_subtract_null";
    std::string multiplyNullFnStr = "batch_multiply_null";
    std::string divideNullFnStr = "batch_divide_null";
    std::string modulusNullFnStr = "batch_modulus_null";
    std::string lessThanFnStr = "batch_lessThan";
    std::string lessThanEqualFnStr = "batch_lessThanEqual";
    std::string greaterThanFnStr = "batch_greaterThan";
    std::string greaterThanEqualFnStr = "batch_greaterThanEqual";
    std::string equalFnStr = "batch_equal";
    std::string notEqualFnStr = "batch_notEqual";
    std::string makeDecimalStr = "batch_MakeDecimal";
    std::string makeDecimalNullStr = "batch_MakeDecimal_null";

    std::vector<DataTypeId> paramTypes128 = { OMNI_DECIMAL128, OMNI_DECIMAL128 };
    std::vector<DataTypeId> paramTypes64 = { OMNI_DECIMAL64, OMNI_DECIMAL64 };
    std::vector<DataTypeId> paramTypes64Op128 = { OMNI_DECIMAL64, OMNI_DECIMAL128 };
    std::vector<DataTypeId> paramTypes128Op64 = { OMNI_DECIMAL128, OMNI_DECIMAL64 };

    static std::vector<Function> batchDecimalFunctions = {
        Function(reinterpret_cast<void *>(BatchDecimal128Compare), "batch_Decimal128Compare", {}, paramTypes128,
            OMNI_INT),
        Function(reinterpret_cast<void *>(BatchAbsDecimal128), absFnStr, {}, { OMNI_DECIMAL128 }, OMNI_DECIMAL128,
            INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchDecimal64Compare), "batch_Decimal64Compare", {}, paramTypes64, OMNI_INT),
        Function(reinterpret_cast<void *>(BatchAbsDecimal64), absFnStr, {}, { OMNI_DECIMAL64 }, OMNI_DECIMAL64,
            INPUT_DATA),
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

        Function(reinterpret_cast<void *>(BatchCastDecimal64ToLong), castFnStr, {}, { OMNI_DECIMAL64 }, OMNI_LONG,
            INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchCastDecimal64ToInt), castFnStr, {}, { OMNI_DECIMAL64 }, OMNI_INT,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchCastDecimal64ToDouble), castFnStr, {}, { OMNI_DECIMAL64 }, OMNI_DOUBLE,
            INPUT_DATA),

        Function(reinterpret_cast<void *>(BatchCastDecimal128ToLong), castFnStr, {}, { OMNI_DECIMAL128 }, OMNI_LONG,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchCastDecimal128ToInt), castFnStr, {}, { OMNI_DECIMAL128 }, OMNI_INT,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchCastDecimal128ToDouble), castFnStr, {}, { OMNI_DECIMAL128 }, OMNI_DOUBLE,
            INPUT_DATA),

        // Decimal Cast Function Return Null
        Function(reinterpret_cast<void *>(BatchCastDecimal64To64RetNull), castNullFnStr, {}, { OMNI_DECIMAL64 },
            OMNI_DECIMAL64, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(BatchCastDecimal64ToDoubleRetNull), castNullFnStr, {}, { OMNI_DECIMAL64 },
            OMNI_DOUBLE, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(BatchCastDecimal64ToLongRetNull), castNullFnStr, {}, { OMNI_DECIMAL64 },
            OMNI_LONG, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(BatchCastDecimal128To128RetNull), castNullFnStr, {}, { OMNI_DECIMAL128 },
            OMNI_DECIMAL128, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(BatchCastDecimal64To128RetNull), castNullFnStr, {}, { OMNI_DECIMAL64 },
            OMNI_DECIMAL128, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(BatchCastDecimal128To64RetNull), castNullFnStr, {}, { OMNI_DECIMAL128 },
            OMNI_DECIMAL64, INPUT_DATA_AND_OVERFLOW_NULL),
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
        Function(reinterpret_cast<void *>(BatchUnscaledValue64), "batch_UnscaledValue", {}, { OMNI_DECIMAL64 },
            OMNI_LONG, INPUT_DATA),
        // MakeDecimal
        Function(reinterpret_cast<void *>(BatchMakeDecimal64), makeDecimalStr, {}, { OMNI_LONG }, OMNI_DECIMAL64,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchMakeDecimal64RetNull), makeDecimalNullStr, {}, { OMNI_LONG },
            OMNI_DECIMAL64, INPUT_DATA_AND_OVERFLOW_NULL),

    };

    return batchDecimalFunctions;
}
