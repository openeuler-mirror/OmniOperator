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
    std::vector<Function> stringFnRegistry = {
        // substr functions
        Function(reinterpret_cast<void*>(Substr), substrStr,
                 {}, {OMNI_VEC_TYPE_VARCHAR, OMNI_VEC_TYPE_INT, OMNI_VEC_TYPE_INT}, OMNI_VEC_TYPE_VARCHAR, true),
        Function(reinterpret_cast<void*>(SubstrChar), substrStr,
                 {}, {OMNI_VEC_TYPE_CHAR, OMNI_VEC_TYPE_INT, OMNI_VEC_TYPE_INT}, OMNI_VEC_TYPE_CHAR, true),
        Function(reinterpret_cast<void*>(Substr_int64), substrStr,
                 {}, {OMNI_VEC_TYPE_VARCHAR, OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG}, OMNI_VEC_TYPE_VARCHAR, true),

        // substr with start index functions
        Function(reinterpret_cast<void*>(SubstrWithStart), substrStr,
                 {}, {OMNI_VEC_TYPE_VARCHAR, OMNI_VEC_TYPE_INT}, OMNI_VEC_TYPE_VARCHAR, true),
        Function(reinterpret_cast<void*>(SubstrCharWithStart), substrStr,
                 {}, {OMNI_VEC_TYPE_CHAR, OMNI_VEC_TYPE_INT}, OMNI_VEC_TYPE_CHAR, true),
        Function(reinterpret_cast<void*>(SubstrWithStart_int64), substrStr,
                 {}, {OMNI_VEC_TYPE_VARCHAR, OMNI_VEC_TYPE_LONG}, OMNI_VEC_TYPE_VARCHAR, true),

        // concat functions
        Function(reinterpret_cast<void *>(ConcatStr), "concat",
                 {}, {OMNI_VEC_TYPE_VARCHAR, OMNI_VEC_TYPE_VARCHAR}, OMNI_VEC_TYPE_VARCHAR, true),
        Function(reinterpret_cast<void *>(ConcatChar), "concat",
                 {}, {OMNI_VEC_TYPE_CHAR, OMNI_VEC_TYPE_CHAR}, OMNI_VEC_TYPE_CHAR, true),

        Function(reinterpret_cast<void *>(Like), "LIKE",
                 {}, {OMNI_VEC_TYPE_VARCHAR, OMNI_VEC_TYPE_VARCHAR}, OMNI_VEC_TYPE_BOOLEAN),
        Function(reinterpret_cast<void*>(CastString), "CAST",
                 {}, {OMNI_VEC_TYPE_VARCHAR}, OMNI_VEC_TYPE_INT),

        Function(reinterpret_cast<void *>(StrCompare), "compare",
                 {}, {OMNI_VEC_TYPE_VARCHAR, OMNI_VEC_TYPE_VARCHAR}, OMNI_VEC_TYPE_INT)
    };
    return stringFnRegistry;
}
