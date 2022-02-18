/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: String Function Registry
 */
#include "func_registry_string.h"
#include "functions/stringfunctions.h"
#include "../vector/vector_type.h"
using namespace omniruntime;
using namespace omniruntime::vec;

std::vector<Function> StringFunctionRegistry::GetFunctions()
{
    std::string substrStr = "substr";
    std::vector<Function> stringFnRegistry = { // substr functions
        Function(reinterpret_cast<void *>(Substr<int32_t>), substrStr, {},
            { OMNI_VEC_TYPE_VARCHAR, OMNI_VEC_TYPE_INT, OMNI_VEC_TYPE_INT }, OMNI_VEC_TYPE_VARCHAR, true),
        Function(reinterpret_cast<void *>(SubstrChar<int32_t>), substrStr, {},
            { OMNI_VEC_TYPE_CHAR, OMNI_VEC_TYPE_INT, OMNI_VEC_TYPE_INT }, OMNI_VEC_TYPE_CHAR, true),
        Function(reinterpret_cast<void *>(Substr<int64_t>), substrStr, {},
            { OMNI_VEC_TYPE_VARCHAR, OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG }, OMNI_VEC_TYPE_VARCHAR, true),
        Function(reinterpret_cast<void *>(SubstrChar<int64_t>), substrStr, {},
            { OMNI_VEC_TYPE_CHAR, OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG }, OMNI_VEC_TYPE_CHAR, true),

        // substr with start index functions
        Function(reinterpret_cast<void *>(SubstrWithStart<int32_t>), substrStr, {},
            { OMNI_VEC_TYPE_VARCHAR, OMNI_VEC_TYPE_INT }, OMNI_VEC_TYPE_VARCHAR, true),
        Function(reinterpret_cast<void *>(SubstrCharWithStart<int32_t>), substrStr, {},
            { OMNI_VEC_TYPE_CHAR, OMNI_VEC_TYPE_INT }, OMNI_VEC_TYPE_CHAR, true),
        Function(reinterpret_cast<void *>(SubstrWithStart<int64_t>), substrStr, {},
            { OMNI_VEC_TYPE_VARCHAR, OMNI_VEC_TYPE_LONG }, OMNI_VEC_TYPE_VARCHAR, true),
        Function(reinterpret_cast<void *>(SubstrCharWithStart<int64_t>), substrStr, {},
            { OMNI_VEC_TYPE_CHAR, OMNI_VEC_TYPE_LONG }, OMNI_VEC_TYPE_CHAR, true),

        // concat functions
        Function(reinterpret_cast<void *>(ConcatStrStr), "concat", {}, { OMNI_VEC_TYPE_VARCHAR, OMNI_VEC_TYPE_VARCHAR },
            OMNI_VEC_TYPE_VARCHAR, true),
        Function(reinterpret_cast<void *>(ConcatCharChar), "concat", {}, { OMNI_VEC_TYPE_CHAR, OMNI_VEC_TYPE_CHAR },
            OMNI_VEC_TYPE_CHAR, true),
        Function(reinterpret_cast<void *>(ConcatCharStr), "concat", {}, { OMNI_VEC_TYPE_CHAR, OMNI_VEC_TYPE_VARCHAR },
            OMNI_VEC_TYPE_CHAR, true),
        Function(reinterpret_cast<void *>(ConcatStrChar), "concat", {}, { OMNI_VEC_TYPE_VARCHAR, OMNI_VEC_TYPE_CHAR },
            OMNI_VEC_TYPE_CHAR, true),

        Function(reinterpret_cast<void *>(Like), "LIKE", {}, { OMNI_VEC_TYPE_VARCHAR, OMNI_VEC_TYPE_VARCHAR },
            OMNI_VEC_TYPE_BOOLEAN),
        Function(reinterpret_cast<void *>(CastString), "CAST", {}, { OMNI_VEC_TYPE_VARCHAR }, OMNI_VEC_TYPE_INT),

        Function(reinterpret_cast<void *>(ToUpper), "upper", {}, { OMNI_VEC_TYPE_VARCHAR }, OMNI_VEC_TYPE_VARCHAR,
            true),
        Function(reinterpret_cast<void *>(ToUpperChar), "upper", {}, { OMNI_VEC_TYPE_CHAR }, OMNI_VEC_TYPE_CHAR, true),

        Function(reinterpret_cast<void *>(StrCompare), "compare", {}, { OMNI_VEC_TYPE_VARCHAR, OMNI_VEC_TYPE_VARCHAR },
            OMNI_VEC_TYPE_INT)
    };
    return stringFnRegistry;
}
