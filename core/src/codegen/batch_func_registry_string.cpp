/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: Batch String Function Registry
 */
#include "batch_func_registry_string.h"
#include "batch_functions/batch_stringfunctions.h"

using namespace omniruntime;
using namespace omniruntime::type;

std::vector<Function> BatchStringFunctionRegistry::GetFunctions()
{
    std::string lessThanFnStr = "batch_lessThan";
    std::string lessThanEqualFnStr = "batch_lessThanEqual";
    std::string greaterThanFnStr = "batch_greaterThan";
    std::string greaterThanEqualFnStr = "batch_greaterThanEqual";
    std::string equalFnStr = "batch_equal";
    std::string notEqualFnStr = "batch_notEqual";

    std::string substrFnStr = "batch_substr";
    std::string concatFnStr = "batch_concat";
    std::string likeFnStr = "batch_LIKE";
    std::string castFnStr = "batch_CAST";
    std::string upperFnStr = "batch_upper";
    std::string lowerFnStr = "batch_lower";
    std::string compareFnStr = "batch_compare";
    std::string lengthFnStr = "batch_length";
    std::string replaceFnStr = "batch_replace";

    std::string substrFnStrRetNull = "batch_substr_null";
    std::string concatFnStrRetNull = "batch_concat_null";
    std::string likeFnStrRetNull = "batch_LIKE_null";
    std::string castFnStrRetNull = "batch_CAST_null";
    std::string upperFnStrRetNull = "batch_upper_null";
    std::string lowerFnStrRetNull = "batch_lower_null";
    std::string compareFnStrRetNull = "batch_compare_null";
    std::string lengthFnStrRetNull = "batch_length_null";
    std::string replaceFnStrRetNull = "batch_replace_null";

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

        // substr functions
        Function(reinterpret_cast<void *>(BatchSubstr<int32_t>), substrFnStr, {}, { OMNI_VARCHAR, OMNI_INT, OMNI_INT },
            OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchSubstrChar<int32_t>), substrFnStr, {}, { OMNI_CHAR, OMNI_INT, OMNI_INT },
            OMNI_CHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchSubstr<int64_t>), substrFnStr, {},
            { OMNI_VARCHAR, OMNI_LONG, OMNI_LONG }, OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchSubstrChar<int64_t>), substrFnStr, {},
            { OMNI_CHAR, OMNI_LONG, OMNI_LONG }, OMNI_CHAR, INPUT_DATA, true),

        // substr with start index functions
        Function(reinterpret_cast<void *>(BatchSubstrWithStart<int32_t>), substrFnStr, {}, { OMNI_VARCHAR, OMNI_INT },
            OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchSubstrCharWithStart<int32_t>), substrFnStr, {}, { OMNI_CHAR, OMNI_INT },
            OMNI_CHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchSubstrWithStart<int64_t>), substrFnStr, {}, { OMNI_VARCHAR, OMNI_LONG },
            OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchSubstrCharWithStart<int64_t>), substrFnStr, {}, { OMNI_CHAR, OMNI_LONG },
            OMNI_CHAR, INPUT_DATA, true),

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

        // replace functions
        Function(reinterpret_cast<void *>(BatchReplaceStrStrStrWithRep), replaceFnStr, {},
            { OMNI_VARCHAR, OMNI_VARCHAR, OMNI_VARCHAR }, OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchReplaceStrStrWithoutRep), replaceFnStr, {},
            { OMNI_VARCHAR, OMNI_VARCHAR }, OMNI_VARCHAR, INPUT_DATA, true),

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
        Function(reinterpret_cast<void *>(BatchCastStringToDate), castFnStr, {}, { OMNI_VARCHAR }, OMNI_INT, INPUT_DATA,
            true),
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
        Function(reinterpret_cast<void *>(BatchCastStringToDateRetNull), castFnStrRetNull, {}, { OMNI_VARCHAR },
            OMNI_INT, INPUT_DATA_AND_OVERFLOW_NULL),
        Function(reinterpret_cast<void *>(BatchCastStrWithDiffWidthsRetNull), castFnStrRetNull, {}, { OMNI_VARCHAR },
            OMNI_VARCHAR, INPUT_DATA_AND_OVERFLOW_NULL, true) };
    return batchStringFnRegistry;
}