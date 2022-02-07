/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Dictionary Functions Registry
 */
#include "func_registry_dictionary.h"
#include "functions/dictionaryfunctions.h"
using namespace omniruntime;
using namespace omniruntime::vec;

std::vector<Function> GetDictionaryFunctionRegistry()
{
    std::vector<VecTypeId> paramTypes = {OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_INT};
    static std::vector<Function> dictionaryFnRegistry = {
        Function(reinterpret_cast<void*>(GetIntFromDictionaryVector), "DictionaryGetInt", {}, paramTypes,
                 OMNI_VEC_TYPE_INT, false),
        Function(reinterpret_cast<void*>(GetLongFromDictionaryVector), "DictionaryGetLong", {}, paramTypes,
                 OMNI_VEC_TYPE_LONG, false),
        Function(reinterpret_cast<void*>(GetDoubleFromDictionaryVector), "DictionaryGetDouble", {},
                 paramTypes, OMNI_VEC_TYPE_DOUBLE, false),
        Function(reinterpret_cast<void*>(GetBooleanFromDictionaryVector), "DictionaryGetBoolean", {},
                 paramTypes, OMNI_VEC_TYPE_BOOLEAN, false),
        Function(reinterpret_cast<void*>(GetVarcharFromDictionaryVector),
                 "DictionaryGetVarchar", {}, {OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_INT}, OMNI_VEC_TYPE_VARCHAR, false),
        Function(reinterpret_cast<void*>(GetDecimalFromDictionaryVector),
                 "DictionaryGetDecimal", {}, {OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_INT}, OMNI_VEC_TYPE_DECIMAL128,
                 false, true)
    };
    return dictionaryFnRegistry;
}
