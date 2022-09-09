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

    std::string substrStr = "batch_substr";
    std::string concatStr = "batch_concat";
    std::string concatNullStr = "batch_concat_null";
    std::string castStr = "batch_CAST";
    std::string likeStr = "batch_LIKE";
    std::string upperStr = "batch_upper";
    std::string lowerStr = "batch_lower";
    std::string lengthStr = "batch_length";
    std::string replaceStr = "batch_replace";

    std::vector<Function> batchStringFnRegistry = {
        // substr functions
        Function(reinterpret_cast<void *>(BatchSubstr<int32_t>), substrStr, {}, { OMNI_VARCHAR, OMNI_INT, OMNI_INT },
            OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchSubstrChar<int32_t>), substrStr, {}, { OMNI_CHAR, OMNI_INT, OMNI_INT },
            OMNI_CHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchSubstr<int64_t>), substrStr, {}, { OMNI_VARCHAR, OMNI_LONG, OMNI_LONG },
            OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchSubstrChar<int64_t>), substrStr, {}, { OMNI_CHAR, OMNI_LONG, OMNI_LONG },
            OMNI_CHAR, INPUT_DATA, true),

        // substr with start index functions
        Function(reinterpret_cast<void *>(BatchSubstrWithStart<int32_t>), substrStr, {}, { OMNI_VARCHAR, OMNI_INT },
            OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchSubstrCharWithStart<int32_t>), substrStr, {}, { OMNI_CHAR, OMNI_INT },
            OMNI_CHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchSubstrWithStart<int64_t>), substrStr, {}, { OMNI_VARCHAR, OMNI_LONG },
            OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchSubstrCharWithStart<int64_t>), substrStr, {}, { OMNI_CHAR, OMNI_LONG },
            OMNI_CHAR, INPUT_DATA, true),

        // concat functions
        Function(reinterpret_cast<void *>(BatchConcatStrStr), concatStr, {}, { OMNI_VARCHAR, OMNI_VARCHAR },
            OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchConcatCharChar), concatStr, {}, { OMNI_CHAR, OMNI_CHAR }, OMNI_CHAR,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchConcatCharStr), concatStr, {}, { OMNI_CHAR, OMNI_VARCHAR }, OMNI_CHAR,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchConcatStrChar), concatStr, {}, { OMNI_VARCHAR, OMNI_CHAR }, OMNI_CHAR,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchConcatStrStrRetNull), concatNullStr, {},
                 { OMNI_VARCHAR, OMNI_VARCHAR }, OMNI_CHAR, INPUT_DATA_AND_OVERFLOW_NULL, true),
        Function(reinterpret_cast<void *>(BatchConcatCharCharRetNull), concatNullStr, {},
                 { OMNI_CHAR, OMNI_CHAR }, OMNI_CHAR, INPUT_DATA_AND_OVERFLOW_NULL, true),
        Function(reinterpret_cast<void *>(BatchConcatCharStrRetNull), concatNullStr, {},
                 { OMNI_CHAR, OMNI_VARCHAR }, OMNI_CHAR, INPUT_DATA_AND_OVERFLOW_NULL, true),
        Function(reinterpret_cast<void *>(BatchConcatStrCharRetNull), concatNullStr, {},
                 { OMNI_VARCHAR, OMNI_CHAR }, OMNI_CHAR, INPUT_DATA_AND_OVERFLOW_NULL, true),

        Function(reinterpret_cast<void *>(BatchLikeStr), likeStr, {}, { OMNI_VARCHAR, OMNI_VARCHAR }, OMNI_BOOLEAN,
            INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchLikeChar), likeStr, {}, { OMNI_CHAR, OMNI_VARCHAR }, OMNI_BOOLEAN,
            INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchCastString), castStr, {}, { OMNI_VARCHAR }, OMNI_INT, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchToUpperStr), upperStr, {}, { OMNI_VARCHAR }, OMNI_VARCHAR, INPUT_DATA,
            true),
        Function(reinterpret_cast<void *>(BatchToUpperChar), upperStr, {}, { OMNI_CHAR }, OMNI_CHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchToLowerStr), lowerStr, {}, { OMNI_VARCHAR }, OMNI_VARCHAR, INPUT_DATA,
            true),
        Function(reinterpret_cast<void *>(BatchToLowerChar), lowerStr, {}, { OMNI_CHAR }, OMNI_CHAR, INPUT_DATA, true),

        // length functions
        Function(reinterpret_cast<void *>(BatchLengthChar), lengthStr, {}, { OMNI_CHAR }, OMNI_LONG, INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchLengthStr), lengthStr, {}, { OMNI_VARCHAR }, OMNI_LONG, INPUT_DATA),

        // replace functions
        Function(reinterpret_cast<void *>(BatchReplaceStrStrStrWithRep), replaceStr, {},
            { OMNI_VARCHAR, OMNI_VARCHAR, OMNI_VARCHAR }, OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(BatchReplaceStrStrWithoutRep), replaceStr, {}, { OMNI_VARCHAR, OMNI_VARCHAR },
            OMNI_VARCHAR, INPUT_DATA, true),

        Function(reinterpret_cast<void *>(BatchStrCompare), "batch_compare", {}, { OMNI_VARCHAR, OMNI_VARCHAR },
            OMNI_INT),
        Function(reinterpret_cast<void *>(BatchLessThanStr), lessThanFnStr, {}, { OMNI_VARCHAR, OMNI_VARCHAR },
            OMNI_BOOLEAN, INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchLessThanEqualStr), lessThanEqualFnStr, {},
            { OMNI_VARCHAR, OMNI_VARCHAR }, OMNI_BOOLEAN, INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchGreaterThanStr), greaterThanFnStr, {}, { OMNI_VARCHAR, OMNI_VARCHAR },
            OMNI_BOOLEAN, INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchGreaterThanEqualStr), greaterThanEqualFnStr, {},
            { OMNI_VARCHAR, OMNI_VARCHAR }, OMNI_BOOLEAN, INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchEqualStr), equalFnStr, {}, { OMNI_VARCHAR, OMNI_VARCHAR }, OMNI_BOOLEAN,
            INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchEqualStr), equalFnStr, {}, { OMNI_CHAR, OMNI_CHAR }, OMNI_BOOLEAN,
            INPUT_DATA),
        Function(reinterpret_cast<void *>(BatchNotEqualStr), notEqualFnStr, {}, { OMNI_VARCHAR, OMNI_VARCHAR },
            OMNI_BOOLEAN, INPUT_DATA),
    };
    return batchStringFnRegistry;
}
