/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: String Function Registry
 */
#include "func_registry_string.h"
#include "functions/stringfunctions.h"
#include "../vector/vector_type.h"
using namespace omniruntime;
using namespace omniruntime::vec;

std::vector<Function> GetStringFunctionRegistry()
{
    std::string substrStr = "substr_";
    std::string substrWithStartStr = substrStr + "start_";
    static std::vector<Function> stringFnRegistry = {
        // substr functions
        Function(reinterpret_cast<void*>(SubstrExt), substrStr + TypeUtil::TypeToString(OMNI_VEC_TYPE_INT),
                 {}, {OMNI_VEC_TYPE_VARCHAR, OMNI_VEC_TYPE_INT, OMNI_VEC_TYPE_INT}, OMNI_VEC_TYPE_VARCHAR, false, true),
        Function(reinterpret_cast<void*>(SubstrExt64), substrStr + TypeUtil::TypeToString(OMNI_VEC_TYPE_LONG),
                 {}, {OMNI_VEC_TYPE_VARCHAR, OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_LONG}, OMNI_VEC_TYPE_VARCHAR, false, true),

        // substr with start index functions
        Function(reinterpret_cast<void*>(SubstrWithStartExt), substrWithStartStr +
                         TypeUtil::TypeToString(OMNI_VEC_TYPE_INT),
                 {}, {OMNI_VEC_TYPE_VARCHAR, OMNI_VEC_TYPE_INT}, OMNI_VEC_TYPE_VARCHAR, false, true),
        Function(reinterpret_cast<void*>(SubstrWithStartExt64), substrWithStartStr +
                         TypeUtil::TypeToString(OMNI_VEC_TYPE_LONG),
                 {}, {OMNI_VEC_TYPE_VARCHAR, OMNI_VEC_TYPE_LONG}, OMNI_VEC_TYPE_VARCHAR, false, true),

        // concat functions
        Function(reinterpret_cast<void*>(ConcatStrExt), "concat_string",
                 {}, {OMNI_VEC_TYPE_VARCHAR, OMNI_VEC_TYPE_VARCHAR}, OMNI_VEC_TYPE_VARCHAR, false, true),
        Function(reinterpret_cast<void*>(ConcatCharExt), "concat_char",
                 {}, {OMNI_VEC_TYPE_CHAR, OMNI_VEC_TYPE_VARCHAR}, OMNI_VEC_TYPE_VARCHAR, false, true),

        Function(reinterpret_cast<void*>(LikeExt), "LIKE",
                 {}, {OMNI_VEC_TYPE_VARCHAR, OMNI_VEC_TYPE_VARCHAR}, OMNI_VEC_TYPE_BOOLEAN, false),
        Function(reinterpret_cast<void*>(CastString), "CAST",
                 {}, {OMNI_VEC_TYPE_VARCHAR}, OMNI_VEC_TYPE_INT)
    };
    return stringFnRegistry;
}

std::vector<Function> GetStringCmpFn()
{
    static std::vector<Function> stringFnRegistry = {
        Function(reinterpret_cast<void*>(StrCompareExt), "StrCompareExt", {},
            {OMNI_VEC_TYPE_VARCHAR, OMNI_VEC_TYPE_VARCHAR}, OMNI_VEC_TYPE_INT, false)
    };
    return stringFnRegistry;
}
