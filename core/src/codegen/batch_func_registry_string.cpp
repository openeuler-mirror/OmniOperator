/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: Batch String Function Registry
 */
#include "batch_func_registry_string.h"
#include "batch_functions/batch_stringfunctions.h"

namespace omniruntime::codegen {
using namespace omniruntime::type;
using namespace codegen::function;

namespace {
const std::string LESS_THAN_FN_STR = "batch_lessThan";
const std::string LESS_THAN_EQUAL_FN_STR = "batch_lessThanEqual";
const std::string GREATER_THAN_FN_STR = "batch_greaterThan";
const std::string GREATER_THAN_EQUAL_FN_STR = "batch_greaterThanEqual";
const std::string EQUAL_FN_STR = "batch_equal";
const std::string NOT_EQUAL_FN_STR = "batch_notEqual";
const std::string SUBSTR_FN_STR = "batch_substr";
const std::string CONCAT_FN_STR = "batch_concat";
const std::string LIKE_FN_STR = "batch_LIKE";
const std::string CAST_FN_STR = "batch_CAST";
const std::string UPPER_FN_STR = "batch_upper";
const std::string LOWER_FN_STR = "batch_lower";
const std::string COMPARE_FN_STR = "batch_compare";
const std::string LENGTH_FN_STR = "batch_length";
const std::string REPLACE_FN_STR = "batch_replace";
const std::string CONCAT_FN_STR_RETNULL = "batch_concat_null";
const std::string CAST_FN_STR_RETNULL = "batch_CAST_null";
}

std::vector<Function> BatchStringFunctionRegistry::GetFunctions()
{
    std::vector<Function> batchStringFnRegistry = { Function(reinterpret_cast<void *>(BatchLessThanStr),
        LESS_THAN_FN_STR, {}, { OMNI_VARCHAR, OMNI_VARCHAR }, OMNI_BOOLEAN, INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchLessThanEqualStr), LESS_THAN_EQUAL_FN_STR, {},
            { OMNI_VARCHAR, OMNI_VARCHAR }, OMNI_BOOLEAN, INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchGreaterThanStr), GREATER_THAN_FN_STR, {}, { OMNI_VARCHAR, OMNI_VARCHAR },
            OMNI_BOOLEAN, INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchGreaterThanEqualStr), GREATER_THAN_EQUAL_FN_STR, {},
            { OMNI_VARCHAR, OMNI_VARCHAR }, OMNI_BOOLEAN, INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchEqualStr), EQUAL_FN_STR, {}, { OMNI_VARCHAR, OMNI_VARCHAR },
            OMNI_BOOLEAN, INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchNotEqualStr), NOT_EQUAL_FN_STR, {}, { OMNI_VARCHAR, OMNI_VARCHAR },
            OMNI_BOOLEAN, INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchStrCompare), COMPARE_FN_STR, {}, { OMNI_VARCHAR, OMNI_VARCHAR },
            OMNI_INT),

        // concat functions
        Function(reinterpret_cast<void *>(BatchConcatStrStr), CONCAT_FN_STR, {}, { OMNI_VARCHAR, OMNI_VARCHAR },
            OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchConcatCharChar), CONCAT_FN_STR, {}, { OMNI_CHAR, OMNI_CHAR }, OMNI_CHAR,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchConcatCharStr), CONCAT_FN_STR, {}, { OMNI_CHAR, OMNI_VARCHAR },
            OMNI_CHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchConcatStrChar), CONCAT_FN_STR, {}, { OMNI_VARCHAR, OMNI_CHAR },
            OMNI_CHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchConcatStrStrRetNull), CONCAT_FN_STR_RETNULL, {},
            { OMNI_VARCHAR, OMNI_VARCHAR }, OMNI_CHAR, INPUT_DATA_AND_OVERFLOW_NULL, true),
        Function(reinterpret_cast<void *>(BatchConcatCharCharRetNull), CONCAT_FN_STR_RETNULL, {},
            { OMNI_CHAR, OMNI_CHAR }, OMNI_CHAR, INPUT_DATA_AND_OVERFLOW_NULL, true),
        Function(reinterpret_cast<void *>(BatchConcatCharStrRetNull), CONCAT_FN_STR_RETNULL, {},
            { OMNI_CHAR, OMNI_VARCHAR }, OMNI_CHAR, INPUT_DATA_AND_OVERFLOW_NULL, true),
        Function(reinterpret_cast<void *>(BatchConcatStrCharRetNull), CONCAT_FN_STR_RETNULL, {},
            { OMNI_VARCHAR, OMNI_CHAR }, OMNI_CHAR, INPUT_DATA_AND_OVERFLOW_NULL, true),

        Function(reinterpret_cast<void *>(BatchLikeStr), LIKE_FN_STR, {}, { OMNI_VARCHAR, OMNI_VARCHAR }, OMNI_BOOLEAN,
            INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchLikeChar), LIKE_FN_STR, {}, { OMNI_CHAR, OMNI_VARCHAR }, OMNI_BOOLEAN,
            INPUT_DATA),

        Function(reinterpret_cast<void *>(BatchToUpperStr), UPPER_FN_STR, {}, { OMNI_VARCHAR }, OMNI_VARCHAR,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchToUpperChar), UPPER_FN_STR, {}, { OMNI_CHAR }, OMNI_CHAR, INPUT_DATA,
            true),
        Function(reinterpret_cast<void *>(BatchToLowerStr), LOWER_FN_STR, {}, { OMNI_VARCHAR }, OMNI_VARCHAR,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchToLowerChar), LOWER_FN_STR, {}, { OMNI_CHAR }, OMNI_CHAR, INPUT_DATA,
            true),

        // length functions
        Function(reinterpret_cast<void *>(BatchLengthChar), LENGTH_FN_STR, {}, { OMNI_CHAR }, OMNI_LONG, INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchLengthStr), LENGTH_FN_STR, {}, { OMNI_VARCHAR }, OMNI_LONG, INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchLengthCharReturnInt32), LENGTH_FN_STR, {}, { OMNI_CHAR }, OMNI_INT,
            INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchLengthStrReturnInt32), LENGTH_FN_STR, {}, { OMNI_VARCHAR }, OMNI_INT,
            INPUT_DATA),

        // cast to string
        Function(reinterpret_cast<void *>(BatchCastIntToString), CAST_FN_STR, {}, { OMNI_INT }, OMNI_VARCHAR,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchCastLongToString), CAST_FN_STR, {}, { OMNI_LONG }, OMNI_VARCHAR,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchCastDoubleToString), CAST_FN_STR, {}, { OMNI_DOUBLE }, OMNI_VARCHAR,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchCastDecimal64ToString), CAST_FN_STR, {}, { OMNI_DECIMAL64 },
            OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchCastDecimal128ToString), CAST_FN_STR, {}, { OMNI_DECIMAL128 },
            OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchCastIntToStringRetNull), CAST_FN_STR_RETNULL, {}, { OMNI_INT },
            OMNI_VARCHAR, INPUT_DATA_AND_OVERFLOW_NULL, true),
        Function(reinterpret_cast<void *>(BatchCastLongToStringRetNull), CAST_FN_STR_RETNULL, {}, { OMNI_LONG },
            OMNI_VARCHAR, INPUT_DATA_AND_OVERFLOW_NULL, true),
        Function(reinterpret_cast<void *>(BatchCastDoubleToStringRetNull), CAST_FN_STR_RETNULL, {}, { OMNI_DOUBLE },
            OMNI_VARCHAR, INPUT_DATA_AND_OVERFLOW_NULL, true),
        Function(reinterpret_cast<void *>(BatchCastDecimal64ToStringRetNull), CAST_FN_STR_RETNULL, {},
            { OMNI_DECIMAL64 }, OMNI_VARCHAR, INPUT_DATA_AND_OVERFLOW_NULL, true),
        Function(reinterpret_cast<void *>(BatchCastDecimal128ToStringRetNull), CAST_FN_STR_RETNULL, {},
            { OMNI_DECIMAL128 }, OMNI_VARCHAR, INPUT_DATA_AND_OVERFLOW_NULL, true),

        // cast string to
        Function(reinterpret_cast<void *>(BatchCastStrWithDiffWidths), CAST_FN_STR, {}, { OMNI_VARCHAR }, OMNI_VARCHAR,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchCastStringToDecimal64), CAST_FN_STR, {}, { OMNI_VARCHAR },
            OMNI_DECIMAL64, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchCastStringToDecimal128), CAST_FN_STR, {}, { OMNI_VARCHAR },
            OMNI_DECIMAL128, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchCastStringToDecimal64RetNull), CAST_FN_STR_RETNULL, {}, { OMNI_VARCHAR },
            OMNI_DECIMAL64, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(BatchCastStringToDecimal128RetNull), CAST_FN_STR_RETNULL, {},
            { OMNI_VARCHAR }, OMNI_DECIMAL128, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(BatchCastStringToIntRetNull), CAST_FN_STR_RETNULL, {}, { OMNI_VARCHAR },
            OMNI_INT, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(BatchCastStringToLongRetNull), CAST_FN_STR_RETNULL, {}, { OMNI_VARCHAR },
            OMNI_LONG, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(BatchCastStringToDoubleRetNull), CAST_FN_STR_RETNULL, {}, { OMNI_VARCHAR },
            OMNI_DOUBLE, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(BatchCastStringToInt), CAST_FN_STR, {}, { OMNI_VARCHAR }, OMNI_INT,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchCastStringToLong), CAST_FN_STR, {}, { OMNI_VARCHAR }, OMNI_LONG,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchCastStringToDouble), CAST_FN_STR, {}, { OMNI_VARCHAR }, OMNI_DOUBLE,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchCastStrWithDiffWidthsRetNull), CAST_FN_STR_RETNULL, {}, { OMNI_VARCHAR },
            OMNI_VARCHAR, INPUT_DATA_AND_OVERFLOW_NULL, true) };

    return batchStringFnRegistry;
}

std::vector<Function> BatchStringFunctionRegistryNotAllowReducePrecison::GetFunctions()
{
    std::vector<Function> batchStringFnRegistry = {
        Function(reinterpret_cast<void *>(BatchCastStringToDateNotAllowReducePrecison), CAST_FN_STR, {},
            { OMNI_VARCHAR }, OMNI_DATE32, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchCastStringToDateRetNullNotAllowReducePrecison), CAST_FN_STR_RETNULL, {},
            { OMNI_VARCHAR }, OMNI_DATE32, INPUT_DATA_AND_OVERFLOW_NULL),
    };

    return batchStringFnRegistry;
}

std::vector<Function> BatchStringFunctionRegistryAllowReducePrecison::GetFunctions()
{
    std::vector<Function> batchStringFnRegistry = {
        Function(reinterpret_cast<void *>(BatchCastStringToDateAllowReducePrecison), CAST_FN_STR, {}, { OMNI_VARCHAR },
            OMNI_DATE32, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchCastStringToDateRetNullAllowReducePrecison), CAST_FN_STR_RETNULL, {},
            { OMNI_VARCHAR }, OMNI_DATE32, INPUT_DATA_AND_OVERFLOW_NULL),
    };

    return batchStringFnRegistry;
}

std::vector<Function> BatchStringFunctionRegistryNotReplace::GetFunctions()
{
    std::vector<Function> batchStringFnRegistry = {
        // replace functions
        Function(reinterpret_cast<void *>(BatchReplaceStrStrStrWithRepNotReplace), REPLACE_FN_STR, {},
            { OMNI_VARCHAR, OMNI_VARCHAR, OMNI_VARCHAR }, OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchReplaceStrStrWithoutRepNotReplace), REPLACE_FN_STR, {},
            { OMNI_VARCHAR, OMNI_VARCHAR }, OMNI_VARCHAR, INPUT_DATA, true),
    };

    return batchStringFnRegistry;
}

std::vector<Function> BatchStringFunctionRegistryReplace::GetFunctions()
{
    std::vector<Function> batchStringFnRegistry = {
        // replace functions
        Function(reinterpret_cast<void *>(BatchReplaceStrStrStrWithRepReplace), REPLACE_FN_STR, {},
            { OMNI_VARCHAR, OMNI_VARCHAR, OMNI_VARCHAR }, OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchReplaceStrStrWithoutRepReplace), REPLACE_FN_STR, {},
            { OMNI_VARCHAR, OMNI_VARCHAR }, OMNI_VARCHAR, INPUT_DATA, true),
    };

    return batchStringFnRegistry;
}

std::vector<Function> BatchStringFunctionRegistryReplaceEmptyString::GetFunctions()
{
    std::vector<Function> batchStringFnRegistry = {
        // substr functions
        Function(reinterpret_cast<void *>(BatchSubstrEmptyString<int32_t>), SUBSTR_FN_STR, {},
            { OMNI_VARCHAR, OMNI_INT, OMNI_INT }, OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchSubstrCharEmptyString<int32_t>), SUBSTR_FN_STR, {},
            { OMNI_CHAR, OMNI_INT, OMNI_INT }, OMNI_CHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchSubstrEmptyString<int64_t>), SUBSTR_FN_STR, {},
            { OMNI_VARCHAR, OMNI_LONG, OMNI_LONG }, OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchSubstrCharEmptyString<int64_t>), SUBSTR_FN_STR, {},
            { OMNI_CHAR, OMNI_LONG, OMNI_LONG }, OMNI_CHAR, INPUT_DATA, true),

        // substr with start index functions
        Function(reinterpret_cast<void *>(BatchSubstrWithStartEmptyString<int32_t>), SUBSTR_FN_STR, {},
            { OMNI_VARCHAR, OMNI_INT }, OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchSubstrCharWithStartEmptyString<int32_t>), SUBSTR_FN_STR, {},
            { OMNI_CHAR, OMNI_INT }, OMNI_CHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchSubstrWithStartEmptyString<int64_t>), SUBSTR_FN_STR, {},
            { OMNI_VARCHAR, OMNI_LONG }, OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchSubstrCharWithStartEmptyString<int64_t>), SUBSTR_FN_STR, {},
            { OMNI_CHAR, OMNI_LONG }, OMNI_CHAR, INPUT_DATA, true),
    };

    return batchStringFnRegistry;
}

std::vector<Function> BatchStringFunctionRegistryReplaceInterceptFromBeyond::GetFunctions()
{
    std::vector<Function> batchStringFnRegistry = {
        // substr functions
        Function(reinterpret_cast<void *>(BatchSubstrInterceptFromBeyond<int32_t>), SUBSTR_FN_STR, {},
            { OMNI_VARCHAR, OMNI_INT, OMNI_INT }, OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchSubstrCharInterceptFromBeyond<int32_t>), SUBSTR_FN_STR, {},
            { OMNI_CHAR, OMNI_INT, OMNI_INT }, OMNI_CHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchSubstrInterceptFromBeyond<int64_t>), SUBSTR_FN_STR, {},
            { OMNI_VARCHAR, OMNI_LONG, OMNI_LONG }, OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchSubstrCharInterceptFromBeyond<int64_t>), SUBSTR_FN_STR, {},
            { OMNI_CHAR, OMNI_LONG, OMNI_LONG }, OMNI_CHAR, INPUT_DATA, true),

        // substr with start index functions
        Function(reinterpret_cast<void *>(BatchSubstrWithStartInterceptFromBeyond<int32_t>), SUBSTR_FN_STR, {},
            { OMNI_VARCHAR, OMNI_INT }, OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchSubstrCharWithStartInterceptFromBeyond<int32_t>), SUBSTR_FN_STR, {},
            { OMNI_CHAR, OMNI_INT }, OMNI_CHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchSubstrWithStartInterceptFromBeyond<int64_t>), SUBSTR_FN_STR, {},
            { OMNI_VARCHAR, OMNI_LONG }, OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchSubstrCharWithStartInterceptFromBeyond<int64_t>), SUBSTR_FN_STR, {},
            { OMNI_CHAR, OMNI_LONG }, OMNI_CHAR, INPUT_DATA, true),
    };

    return batchStringFnRegistry;
}
}