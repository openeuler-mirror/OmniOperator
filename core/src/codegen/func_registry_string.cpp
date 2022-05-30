/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: String Function Registry
 */
#include "func_registry_string.h"
#include "functions/stringfunctions.h"
#include "../type/data_type.h"
using namespace omniruntime;
using namespace omniruntime::type;

std::vector<Function> StringFunctionRegistry::GetFunctions()
{
    std::string substrStr = "substr";
    std::vector<Function> stringFnRegistry = { // substr functions
        Function(reinterpret_cast<void *>(Substr<int32_t>), substrStr, {}, { OMNI_VARCHAR, OMNI_INT, OMNI_INT },
                 OMNI_VARCHAR, NULL_RESULT_IF_ANY_NULL_ARG, true),
        Function(reinterpret_cast<void *>(SubstrChar<int32_t>), substrStr, {}, { OMNI_CHAR, OMNI_INT, OMNI_INT },
                 OMNI_CHAR, NULL_RESULT_IF_ANY_NULL_ARG, true),
        Function(reinterpret_cast<void *>(Substr<int64_t>), substrStr, {}, { OMNI_VARCHAR, OMNI_LONG, OMNI_LONG },
                 OMNI_VARCHAR, NULL_RESULT_IF_ANY_NULL_ARG, true),
        Function(reinterpret_cast<void *>(SubstrChar<int64_t>), substrStr, {}, { OMNI_CHAR, OMNI_LONG, OMNI_LONG },
                 OMNI_CHAR, NULL_RESULT_IF_ANY_NULL_ARG, true),

        // substr with start index functions
        Function(reinterpret_cast<void *>(SubstrWithStart<int32_t>), substrStr, {}, { OMNI_VARCHAR, OMNI_INT },
                 OMNI_VARCHAR, NULL_RESULT_IF_ANY_NULL_ARG, true),
        Function(reinterpret_cast<void *>(SubstrCharWithStart<int32_t>), substrStr, {}, { OMNI_CHAR, OMNI_INT },
                 OMNI_CHAR, NULL_RESULT_IF_ANY_NULL_ARG, true),
        Function(reinterpret_cast<void *>(SubstrWithStart<int64_t>), substrStr, {}, { OMNI_VARCHAR, OMNI_LONG },
                 OMNI_VARCHAR, NULL_RESULT_IF_ANY_NULL_ARG, true),
        Function(reinterpret_cast<void *>(SubstrCharWithStart<int64_t>), substrStr, {}, { OMNI_CHAR, OMNI_LONG },
                 OMNI_CHAR, NULL_RESULT_IF_ANY_NULL_ARG, true),

        // concat functions
        Function(reinterpret_cast<void *>(ConcatStrStr), "concat", {},
                 { OMNI_VARCHAR, OMNI_VARCHAR }, OMNI_VARCHAR, NULL_RESULT_IF_ANY_NULL_ARG, true),
        Function(reinterpret_cast<void *>(ConcatCharChar), "concat", {},
                 { OMNI_CHAR, OMNI_CHAR }, OMNI_CHAR, NULL_RESULT_IF_ANY_NULL_ARG, true),
        Function(reinterpret_cast<void *>(ConcatCharStr), "concat", {},
                 { OMNI_CHAR, OMNI_VARCHAR }, OMNI_CHAR, NULL_RESULT_IF_ANY_NULL_ARG, true),
        Function(reinterpret_cast<void *>(ConcatStrChar), "concat", {},
                 { OMNI_VARCHAR, OMNI_CHAR }, OMNI_CHAR, NULL_RESULT_IF_ANY_NULL_ARG, true),

        Function(reinterpret_cast<void *>(Like), "LIKE", {},
                 { OMNI_VARCHAR, OMNI_VARCHAR }, OMNI_BOOLEAN, NULL_RESULT_IF_ANY_NULL_ARG),
        Function(reinterpret_cast<void *>(CastString), "CAST", {},
                 { OMNI_VARCHAR }, OMNI_INT, NULL_RESULT_IF_ANY_NULL_ARG, true),

        Function(reinterpret_cast<void *>(ToUpper), "upper", {},
                 { OMNI_VARCHAR }, OMNI_VARCHAR, NULL_RESULT_IF_ANY_NULL_ARG, true),
        Function(reinterpret_cast<void *>(ToUpperChar), "upper", {},
                 { OMNI_CHAR }, OMNI_CHAR, NULL_RESULT_IF_ANY_NULL_ARG, true),

        Function(reinterpret_cast<void *>(StrCompare), "compare", {},
            { OMNI_VARCHAR, OMNI_VARCHAR }, OMNI_INT)
    };
    return stringFnRegistry;
}
