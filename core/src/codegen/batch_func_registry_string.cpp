/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: Batch String Function Registry
 */
#include "batch_func_registry_string.h"
#include "batch_functions/batch_stringfunctions.h"

using namespace omniruntime;
using namespace omniruntime::type;

namespace {
const std::string lessThanFnStr = "batch_lessThan";
const std::string lessThanEqualFnStr = "batch_lessThanEqual";
const std::string greaterThanFnStr = "batch_greaterThan";
const std::string greaterThanEqualFnStr = "batch_greaterThanEqual";
const std::string equalFnStr = "batch_equal";
const std::string notEqualFnStr = "batch_notEqual";
const std::string substrFnStr = "batch_substr";
const std::string concatFnStr = "batch_concat";
const std::string likeFnStr = "batch_LIKE";
const std::string castFnStr = "batch_CAST";
const std::string upperFnStr = "batch_upper";
const std::string lowerFnStr = "batch_lower";
const std::string compareFnStr = "batch_compare";
const std::string lengthFnStr = "batch_length";
const std::string replaceFnStr = "batch_replace";
const std::string concatFnStrRetNull = "batch_concat_null";
const std::string castFnStrRetNull = "batch_CAST_null";
}

std::vector<Function> BatchStringFunctionRegistry::GetFunctions()
{
    std::vector<Function> batchStringFnRegistry = { Function(reinterpret_cast<void *>(BatchLessThanStr), lessThanFnStr,
        {}, { OMNI_VARCHAR, OMNI_VARCHAR }, OMNI_BOOLEAN, INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchLessThanEqualStr), lessThanEqualFnStr, {},
            { OMNI_VARCHAR, OMNI_VARCHAR }, OMNI_BOOLEAN, INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchGreaterThanStr), greaterThanFnStr, {}, { OMNI_VARCHAR, OMNI_VARCHAR },
            OMNI_BOOLEAN, INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchGreaterThanEqualStr), greaterThanEqualFnStr, {},
            { OMNI_VARCHAR, OMNI_VARCHAR }, OMNI_BOOLEAN, INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchEqualStr), equalFnStr, {}, { OMNI_VARCHAR, OMNI_VARCHAR }, OMNI_BOOLEAN,
            INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchNotEqualStr), notEqualFnStr, {}, { OMNI_VARCHAR, OMNI_VARCHAR },
            OMNI_BOOLEAN, INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchStrCompare), compareFnStr, {}, { OMNI_VARCHAR, OMNI_VARCHAR }, OMNI_INT),

        // concat functions
        Function(reinterpret_cast<void *>(BatchConcatStrStr), concatFnStr, {}, { OMNI_VARCHAR, OMNI_VARCHAR },
            OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchConcatCharChar), concatFnStr, {}, { OMNI_CHAR, OMNI_CHAR }, OMNI_CHAR,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchConcatCharStr), concatFnStr, {}, { OMNI_CHAR, OMNI_VARCHAR }, OMNI_CHAR,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchConcatStrChar), concatFnStr, {}, { OMNI_VARCHAR, OMNI_CHAR }, OMNI_CHAR,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchConcatStrStrRetNull), concatFnStrRetNull, {},
            { OMNI_VARCHAR, OMNI_VARCHAR }, OMNI_CHAR, INPUT_DATA_AND_OVERFLOW_NULL, true),
        Function(reinterpret_cast<void *>(BatchConcatCharCharRetNull), concatFnStrRetNull, {}, { OMNI_CHAR, OMNI_CHAR },
            OMNI_CHAR, INPUT_DATA_AND_OVERFLOW_NULL, true),
        Function(reinterpret_cast<void *>(BatchConcatCharStrRetNull), concatFnStrRetNull, {},
            { OMNI_CHAR, OMNI_VARCHAR }, OMNI_CHAR, INPUT_DATA_AND_OVERFLOW_NULL, true),
        Function(reinterpret_cast<void *>(BatchConcatStrCharRetNull), concatFnStrRetNull, {},
            { OMNI_VARCHAR, OMNI_CHAR }, OMNI_CHAR, INPUT_DATA_AND_OVERFLOW_NULL, true),

        Function(reinterpret_cast<void *>(BatchLikeStr), likeFnStr, {}, { OMNI_VARCHAR, OMNI_VARCHAR }, OMNI_BOOLEAN,
            INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchLikeChar), likeFnStr, {}, { OMNI_CHAR, OMNI_VARCHAR }, OMNI_BOOLEAN,
            INPUT_DATA),

        Function(reinterpret_cast<void *>(BatchToUpperStr), upperFnStr, {}, { OMNI_VARCHAR }, OMNI_VARCHAR, INPUT_DATA,
            true),
        Function(reinterpret_cast<void *>(BatchToUpperChar), upperFnStr, {}, { OMNI_CHAR }, OMNI_CHAR, INPUT_DATA,
            true),
        Function(reinterpret_cast<void *>(BatchToLowerStr), lowerFnStr, {}, { OMNI_VARCHAR }, OMNI_VARCHAR, INPUT_DATA,
            true),
        Function(reinterpret_cast<void *>(BatchToLowerChar), lowerFnStr, {}, { OMNI_CHAR }, OMNI_CHAR, INPUT_DATA,
            true),

        // length functions
        Function(reinterpret_cast<void *>(BatchLengthChar), lengthFnStr, {}, { OMNI_CHAR }, OMNI_LONG, INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchLengthStr), lengthFnStr, {}, { OMNI_VARCHAR }, OMNI_LONG, INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchLengthCharReturnInt32), lengthFnStr, {}, { OMNI_CHAR }, OMNI_INT,
            INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchLengthStrReturnInt32), lengthFnStr, {}, { OMNI_VARCHAR }, OMNI_INT,
            INPUT_DATA),

        // cast to string
        Function(reinterpret_cast<void *>(BatchCastIntToString), castFnStr, {}, { OMNI_INT }, OMNI_VARCHAR, INPUT_DATA,
            true),
        Function(reinterpret_cast<void *>(BatchCastLongToString), castFnStr, {}, { OMNI_LONG }, OMNI_VARCHAR,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchCastDoubleToString), castFnStr, {}, { OMNI_DOUBLE }, OMNI_VARCHAR,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchCastDecimal64ToString), castFnStr, {}, { OMNI_DECIMAL64 }, OMNI_VARCHAR,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchCastDecimal128ToString), castFnStr, {}, { OMNI_DECIMAL128 },
            OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchCastIntToStringRetNull), castFnStrRetNull, {}, { OMNI_INT },
            OMNI_VARCHAR, INPUT_DATA_AND_OVERFLOW_NULL, true),
        Function(reinterpret_cast<void *>(BatchCastLongToStringRetNull), castFnStrRetNull, {}, { OMNI_LONG },
            OMNI_VARCHAR, INPUT_DATA_AND_OVERFLOW_NULL, true),
        Function(reinterpret_cast<void *>(BatchCastDoubleToStringRetNull), castFnStrRetNull, {}, { OMNI_DOUBLE },
            OMNI_VARCHAR, INPUT_DATA_AND_OVERFLOW_NULL, true),
        Function(reinterpret_cast<void *>(BatchCastDecimal64ToStringRetNull), castFnStrRetNull, {}, { OMNI_DECIMAL64 },
            OMNI_VARCHAR, INPUT_DATA_AND_OVERFLOW_NULL, true),
        Function(reinterpret_cast<void *>(BatchCastDecimal128ToStringRetNull), castFnStrRetNull, {},
            { OMNI_DECIMAL128 }, OMNI_VARCHAR, INPUT_DATA_AND_OVERFLOW_NULL, true),

        // cast string to
        Function(reinterpret_cast<void *>(BatchCastStrWithDiffWidths), castFnStr, {}, { OMNI_VARCHAR }, OMNI_VARCHAR,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchCastStringToDecimal64), castFnStr, {}, { OMNI_VARCHAR }, OMNI_DECIMAL64,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchCastStringToDecimal128), castFnStr, {}, { OMNI_VARCHAR },
            OMNI_DECIMAL128, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchCastStringToDecimal64RetNull), castFnStrRetNull, {}, { OMNI_VARCHAR },
            OMNI_DECIMAL64, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(BatchCastStringToDecimal128RetNull), castFnStrRetNull, {}, { OMNI_VARCHAR },
            OMNI_DECIMAL128, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(BatchCastStringToIntRetNull), castFnStrRetNull, {}, { OMNI_VARCHAR },
            OMNI_INT, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(BatchCastStringToLongRetNull), castFnStrRetNull, {}, { OMNI_VARCHAR },
            OMNI_LONG, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(BatchCastStringToDoubleRetNull), castFnStrRetNull, {}, { OMNI_VARCHAR },
            OMNI_DOUBLE, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(BatchCastStringToInt), castFnStr, {}, { OMNI_VARCHAR }, OMNI_INT, INPUT_DATA,
            true),
        Function(reinterpret_cast<void *>(BatchCastStringToLong), castFnStr, {}, { OMNI_VARCHAR }, OMNI_LONG,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchCastStringToDouble), castFnStr, {}, { OMNI_VARCHAR }, OMNI_DOUBLE,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchCastStrWithDiffWidthsRetNull), castFnStrRetNull, {}, { OMNI_VARCHAR },
            OMNI_VARCHAR, INPUT_DATA_AND_OVERFLOW_NULL, true)
    };

    return batchStringFnRegistry;
}

std::vector<Function> BatchStringFunctionRegistryNotAllowReducePrecison::GetFunctions()
{
    std::vector<Function> batchStringFnRegistry = {
        Function(reinterpret_cast<void *>(BatchCastStringToDateNotAllowReducePrecison), castFnStr, {}, { OMNI_VARCHAR },
            OMNI_DATE32,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchCastStringToDateRetNullNotAllowReducePrecison), castFnStrRetNull, {},
            { OMNI_VARCHAR },
            OMNI_DATE32, INPUT_DATA_AND_OVERFLOW_NULL),
    };

    return batchStringFnRegistry;
}

std::vector<Function> BatchStringFunctionRegistryAllowReducePrecison::GetFunctions()
{
    std::vector<Function> batchStringFnRegistry = {
        Function(reinterpret_cast<void *>(BatchCastStringToDateAllowReducePrecison), castFnStr, {}, { OMNI_VARCHAR },
            OMNI_DATE32,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchCastStringToDateRetNullAllowReducePrecison), castFnStrRetNull, {},
            { OMNI_VARCHAR },
            OMNI_DATE32, INPUT_DATA_AND_OVERFLOW_NULL),
    };

    return batchStringFnRegistry;
}

std::vector<Function> BatchStringFunctionRegistryNotReplace::GetFunctions()
{
    std::vector<Function> batchStringFnRegistry = {
        // replace functions
        Function(reinterpret_cast<void *>(BatchReplaceStrStrStrWithRepNotReplace), replaceFnStr, {},
            { OMNI_VARCHAR, OMNI_VARCHAR, OMNI_VARCHAR }, OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchReplaceStrStrWithoutRepNotReplace), replaceFnStr, {},
            { OMNI_VARCHAR, OMNI_VARCHAR }, OMNI_VARCHAR, INPUT_DATA, true),
    };

    return batchStringFnRegistry;
}

std::vector<Function> BatchStringFunctionRegistryReplace::GetFunctions()
{
    std::vector<Function> batchStringFnRegistry = {
        // replace functions
        Function(reinterpret_cast<void *>(BatchReplaceStrStrStrWithRepReplace), replaceFnStr, {},
            { OMNI_VARCHAR, OMNI_VARCHAR, OMNI_VARCHAR }, OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchReplaceStrStrWithoutRepReplace), replaceFnStr, {},
            { OMNI_VARCHAR, OMNI_VARCHAR }, OMNI_VARCHAR, INPUT_DATA, true),
    };

    return batchStringFnRegistry;
}

std::vector<Function> BatchStringFunctionRegistryReplaceEmptyString::GetFunctions()
{
    std::vector<Function> batchStringFnRegistry = {
        // substr functions
        Function(reinterpret_cast<void *>(BatchSubstrEmptyString<int32_t>), substrFnStr, {},
            { OMNI_VARCHAR, OMNI_INT, OMNI_INT },
            OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchSubstrCharEmptyString<int32_t>), substrFnStr, {},
            { OMNI_CHAR, OMNI_INT, OMNI_INT },
            OMNI_CHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchSubstrEmptyString<int64_t>), substrFnStr, {},
            { OMNI_VARCHAR, OMNI_LONG, OMNI_LONG }, OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchSubstrCharEmptyString<int64_t>), substrFnStr, {},
            { OMNI_CHAR, OMNI_LONG, OMNI_LONG }, OMNI_CHAR, INPUT_DATA, true),

        // substr with start index functions
        Function(reinterpret_cast<void *>(BatchSubstrWithStartEmptyString<int32_t>), substrFnStr, {},
            { OMNI_VARCHAR, OMNI_INT },
            OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchSubstrCharWithStartEmptyString<int32_t>), substrFnStr, {},
            { OMNI_CHAR, OMNI_INT },
            OMNI_CHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchSubstrWithStartEmptyString<int64_t>), substrFnStr, {},
            { OMNI_VARCHAR, OMNI_LONG },
            OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchSubstrCharWithStartEmptyString<int64_t>), substrFnStr, {},
            { OMNI_CHAR, OMNI_LONG },
            OMNI_CHAR, INPUT_DATA, true),
    };

    return batchStringFnRegistry;
}

std::vector<Function> BatchStringFunctionRegistryReplaceInterceptFromBeyond::GetFunctions()
{
    std::vector<Function> batchStringFnRegistry = {
        // substr functions
        Function(reinterpret_cast<void *>(BatchSubstrInterceptFromBeyond<int32_t>), substrFnStr, {},
            { OMNI_VARCHAR, OMNI_INT, OMNI_INT },
            OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchSubstrCharInterceptFromBeyond<int32_t>), substrFnStr, {},
            { OMNI_CHAR, OMNI_INT, OMNI_INT },
            OMNI_CHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchSubstrInterceptFromBeyond<int64_t>), substrFnStr, {},
            { OMNI_VARCHAR, OMNI_LONG, OMNI_LONG }, OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchSubstrCharInterceptFromBeyond<int64_t>), substrFnStr, {},
            { OMNI_CHAR, OMNI_LONG, OMNI_LONG }, OMNI_CHAR, INPUT_DATA, true),

        // substr with start index functions
        Function(reinterpret_cast<void *>(BatchSubstrWithStartInterceptFromBeyond<int32_t>), substrFnStr, {},
            { OMNI_VARCHAR, OMNI_INT },
            OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchSubstrCharWithStartInterceptFromBeyond<int32_t>), substrFnStr, {},
            { OMNI_CHAR, OMNI_INT },
            OMNI_CHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchSubstrWithStartInterceptFromBeyond<int64_t>), substrFnStr, {},
            { OMNI_VARCHAR, OMNI_LONG },
            OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchSubstrCharWithStartInterceptFromBeyond<int64_t>), substrFnStr, {},
            { OMNI_CHAR, OMNI_LONG },
            OMNI_CHAR, INPUT_DATA, true),
    };

    return batchStringFnRegistry;
}

