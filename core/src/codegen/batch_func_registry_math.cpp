/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: batch math functions registry
 */
#include "batch_func_registry_math.h"
#include "batch_functions/batch_mathfunctions.h"

using namespace omniruntime;
using namespace omniruntime::type;

namespace {
const std::string absFnStr = "batch_abs";
const std::string castFnStr = "batch_CAST";
const std::string roundFnStr = "batch_round";
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
const std::string pModFnStr = "batch_pmod";
}

std::vector<Function> BatchMathFunctionRegistry::GetFunctions()
{
    const std::vector<omniruntime::type::DataTypeId> doubleParams = { OMNI_DOUBLE, OMNI_DOUBLE };
    const std::vector<omniruntime::type::DataTypeId> longParams = { OMNI_LONG, OMNI_LONG };
    const std::vector<omniruntime::type::DataTypeId> intParams = { OMNI_INT, OMNI_INT };
    const std::vector<omniruntime::type::DataTypeId> boolParams = { OMNI_BOOLEAN, OMNI_BOOLEAN };

    std::vector<Function> batchMathFunctions = {
        Function(reinterpret_cast<void *>(BatchAbs<int32_t>), absFnStr, {}, { OMNI_INT }, OMNI_INT, INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchAbs<int64_t>), absFnStr, {}, { OMNI_LONG }, OMNI_LONG, INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchAbs<double>), absFnStr, {}, { OMNI_DOUBLE }, OMNI_DOUBLE, INPUT_DATA),

        Function(reinterpret_cast<void *>(BatchCastInt32ToDouble), castFnStr, {}, { OMNI_INT }, OMNI_DOUBLE,
            INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchCastInt64ToDouble), castFnStr, {}, { OMNI_LONG }, OMNI_DOUBLE,
            INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchCastInt32ToInt64), castFnStr, {}, { OMNI_INT }, OMNI_LONG, INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchCastInt64ToInt32), castFnStr, {}, { OMNI_LONG }, OMNI_INT, INPUT_DATA),

        Function(reinterpret_cast<void *>(BatchAddDouble), addFnStr, {}, doubleParams, OMNI_DOUBLE, INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchSubtractDouble), subtractFnStr, {}, doubleParams, OMNI_DOUBLE,
            INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchMultiplyDouble), multiplyFnStr, {}, doubleParams, OMNI_DOUBLE,
            INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchDivideDouble), divideFnStr, {}, doubleParams, OMNI_DOUBLE, INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchModulusDouble), modulusFnStr, {}, doubleParams, OMNI_DOUBLE, INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchLessThanDouble), lessThanFnStr, {}, doubleParams, OMNI_BOOLEAN,
            INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchLessThanEqualDouble), lessThanEqualFnStr, {}, doubleParams, OMNI_BOOLEAN,
            INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchGreaterThanDouble), greaterThanFnStr, {}, doubleParams, OMNI_BOOLEAN,
            INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchGreaterThanEqualDouble), greaterThanEqualFnStr, {}, doubleParams,
            OMNI_BOOLEAN, INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchEqualDouble), equalFnStr, {}, doubleParams, OMNI_BOOLEAN, INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchNotEqualDouble), notEqualFnStr, {}, doubleParams, OMNI_BOOLEAN,
            INPUT_DATA),

        Function(reinterpret_cast<void *>(BatchAddInt64), addFnStr, {}, longParams, OMNI_LONG, INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchSubtractInt64), subtractFnStr, {}, longParams, OMNI_LONG, INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchMultiplyInt64), multiplyFnStr, {}, longParams, OMNI_LONG, INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchDivideInt64), divideFnStr, {}, longParams, OMNI_LONG, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchModulusInt64), modulusFnStr, {}, longParams, OMNI_LONG, INPUT_DATA,
            true),
        Function(reinterpret_cast<void *>(BatchLessThanInt64), lessThanFnStr, {}, longParams, OMNI_BOOLEAN, INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchLessThanEqualInt64), lessThanEqualFnStr, {}, longParams, OMNI_BOOLEAN,
            INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchGreaterThanInt64), greaterThanFnStr, {}, longParams, OMNI_BOOLEAN,
            INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchGreaterThanEqualInt64), greaterThanEqualFnStr, {}, longParams,
            OMNI_BOOLEAN, INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchEqualInt64), equalFnStr, {}, longParams, OMNI_BOOLEAN, INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchNotEqualInt64), notEqualFnStr, {}, longParams, OMNI_BOOLEAN, INPUT_DATA),

        Function(reinterpret_cast<void *>(BatchAddInt32), addFnStr, {}, intParams, OMNI_INT, INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchSubtractInt32), subtractFnStr, {}, intParams, OMNI_INT, INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchMultiplyInt32), multiplyFnStr, {}, intParams, OMNI_INT, INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchDivideInt32), divideFnStr, {}, intParams, OMNI_INT, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchModulusInt32), modulusFnStr, {}, intParams, OMNI_INT, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchLessThanInt32), lessThanFnStr, {}, intParams, OMNI_BOOLEAN, INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchLessThanEqualInt32), lessThanEqualFnStr, {}, intParams, OMNI_BOOLEAN,
            INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchGreaterThanInt32), greaterThanFnStr, {}, intParams, OMNI_BOOLEAN,
            INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchGreaterThanEqualInt32), greaterThanEqualFnStr, {}, intParams,
            OMNI_BOOLEAN, INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchEqualInt32), equalFnStr, {}, intParams, OMNI_BOOLEAN, INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchNotEqualInt32), notEqualFnStr, {}, intParams, OMNI_BOOLEAN, INPUT_DATA),

        Function(reinterpret_cast<void *>(BatchEqualBool), equalFnStr, {}, boolParams, OMNI_BOOLEAN, INPUT_DATA),

        Function(reinterpret_cast<void *>(BatchPmod), pModFnStr, {}, intParams, OMNI_INT, INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchRound<int32_t>), roundFnStr, {}, intParams, OMNI_INT, INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchRound<int64_t>), roundFnStr, {}, { OMNI_LONG, OMNI_INT }, OMNI_LONG,
            INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchRound<double>), roundFnStr, {}, { OMNI_DOUBLE, OMNI_INT }, OMNI_DOUBLE,
            INPUT_DATA)
    };

    return batchMathFunctions;
}

std::vector<Function> BatchMathFunctionRegistryHalfUp::GetFunctions()
{
    std::vector<Function> batchMathFunctions = {
        Function(reinterpret_cast<void *>(BatchCastDoubleToInt64HalfUp), castFnStr, {}, { OMNI_DOUBLE }, OMNI_LONG,
            INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchCastDoubleToInt32HalfUp), castFnStr, {}, { OMNI_DOUBLE }, OMNI_INT,
            INPUT_DATA),
    };

    return batchMathFunctions;
}

std::vector<Function> BatchMathFunctionRegistryDown::GetFunctions()
{
    std::vector<Function> batchMathFunctions = {
        Function(reinterpret_cast<void *>(BatchCastDoubleToInt64Down), castFnStr, {}, { OMNI_DOUBLE }, OMNI_LONG,
            INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchCastDoubleToInt32Down), castFnStr, {}, { OMNI_DOUBLE }, OMNI_INT,
            INPUT_DATA),
    };

    return batchMathFunctions;
}