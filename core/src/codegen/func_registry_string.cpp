/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: String Function Registry
 */
#include "func_registry_string.h"
#include "functions/stringfunctions.h"

using namespace omniruntime;
using namespace omniruntime::type;

std::vector<Function> StringFunctionRegistry::GetFunctions()
{
    std::string substrStr = "substr";
    std::string concatFnStr = "concat";
    std::string likeFnStr = "LIKE";
    std::string castFnStr = "CAST";
    std::string upperFnStr = "upper";
    std::string lowerFnStr = "lower";
    std::string compareFnStr = "compare";
    std::string lengthFnStr = "length";
    std::string replaceFnStr = "replace";

    std::vector<Function> stringFnRegistry = { // substr functions
        Function(reinterpret_cast<void *>(Substr<int32_t>), substrStr, {}, { OMNI_VARCHAR, OMNI_INT, OMNI_INT },
            OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(SubstrChar<int32_t>), substrStr, {}, { OMNI_CHAR, OMNI_INT, OMNI_INT },
            OMNI_CHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(Substr<int64_t>), substrStr, {}, { OMNI_VARCHAR, OMNI_LONG, OMNI_LONG },
            OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(SubstrChar<int64_t>), substrStr, {}, { OMNI_CHAR, OMNI_LONG, OMNI_LONG },
            OMNI_CHAR, INPUT_DATA, true),

        // substr with start index functions
        Function(reinterpret_cast<void *>(SubstrWithStart<int32_t>), substrStr, {}, { OMNI_VARCHAR, OMNI_INT },
            OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(SubstrCharWithStart<int32_t>), substrStr, {}, { OMNI_CHAR, OMNI_INT },
            OMNI_CHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(SubstrWithStart<int64_t>), substrStr, {}, { OMNI_VARCHAR, OMNI_LONG },
            OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(SubstrCharWithStart<int64_t>), substrStr, {}, { OMNI_CHAR, OMNI_LONG },
            OMNI_CHAR, INPUT_DATA, true),

        // concat functions
        Function(reinterpret_cast<void *>(ConcatStrStr), concatFnStr, {},
            { OMNI_VARCHAR, OMNI_VARCHAR }, OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(ConcatCharChar), concatFnStr, {},
            { OMNI_CHAR, OMNI_CHAR }, OMNI_CHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(ConcatCharStr), concatFnStr, {},
            { OMNI_CHAR, OMNI_VARCHAR }, OMNI_CHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(ConcatStrChar), concatFnStr, {},
            { OMNI_VARCHAR, OMNI_CHAR }, OMNI_CHAR, INPUT_DATA, true),

        Function(reinterpret_cast<void *>(LikeStr), likeFnStr, {},
            { OMNI_VARCHAR, OMNI_VARCHAR }, OMNI_BOOLEAN, INPUT_DATA),
        Function(reinterpret_cast<void *>(LikeChar), likeFnStr, {},
            { OMNI_CHAR, OMNI_VARCHAR }, OMNI_BOOLEAN, INPUT_DATA),
        Function(reinterpret_cast<void *>(CastString), castFnStr, {},
            { OMNI_VARCHAR }, OMNI_INT, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(StrCompare), compareFnStr, {}, { OMNI_VARCHAR, OMNI_VARCHAR }, OMNI_INT),

        Function(reinterpret_cast<void *>(ToUpperStr), upperFnStr, {},
            { OMNI_VARCHAR }, OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(ToUpperChar), upperFnStr, {},
            { OMNI_CHAR }, OMNI_CHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(ToLowerStr), lowerFnStr, {}, { OMNI_VARCHAR }, OMNI_VARCHAR,
            INPUT_DATA, true),
        Function(reinterpret_cast<void *>(ToLowerChar), lowerFnStr, {}, { OMNI_CHAR }, OMNI_CHAR,
            INPUT_DATA, true),

        // length functions
        Function(reinterpret_cast<void *>(LengthChar), lengthFnStr, {}, { OMNI_CHAR }, OMNI_LONG,
            INPUT_DATA),
        Function(reinterpret_cast<void *>(LengthStr), lengthFnStr, {}, { OMNI_VARCHAR }, OMNI_LONG,
            INPUT_DATA),
        Function(reinterpret_cast<void *>(LengthCharReturnInt32), lengthFnStr, {}, { OMNI_CHAR }, OMNI_INT,
            INPUT_DATA),
        Function(reinterpret_cast<void *>(LengthStrReturnInt32), lengthFnStr, {}, { OMNI_VARCHAR }, OMNI_INT,
            INPUT_DATA),

        // replace functions
        Function(reinterpret_cast<void *>(ReplaceStrStrStrWithRep), replaceFnStr, {},
            { OMNI_VARCHAR, OMNI_VARCHAR, OMNI_VARCHAR }, OMNI_VARCHAR, INPUT_DATA, true),
        Function(reinterpret_cast<void *>(ReplaceStrStrWithoutRep), replaceFnStr, {}, { OMNI_VARCHAR, OMNI_VARCHAR },
            OMNI_VARCHAR, INPUT_DATA, true)
    };
    return stringFnRegistry;
}
