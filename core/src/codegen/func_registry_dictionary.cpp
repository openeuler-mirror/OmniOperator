/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Dictionary Functions Registry
 */
#include "func_registry_dictionary.h"
#include "functions/dictionaryfunctions.h"
using namespace omniruntime;
using namespace omniruntime::vec;

std::vector<Function> DictionaryFunctionRegistry::GetFunctions()
{
    std::vector<VecTypeId> paramTypes = {OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_INT};
    std::vector<Function> dictionaryFnRegistry = {
        Function(reinterpret_cast<void*>(GetIntFromDictionaryVector), "DictionaryGetInt", {}, paramTypes,
                 OMNI_VEC_TYPE_INT),
        Function(reinterpret_cast<void*>(GetLongFromDictionaryVector), "DictionaryGetLong", {}, paramTypes,
                 OMNI_VEC_TYPE_LONG),
        Function(reinterpret_cast<void*>(GetDoubleFromDictionaryVector), "DictionaryGetDouble", {},
                 paramTypes, OMNI_VEC_TYPE_DOUBLE),
        Function(reinterpret_cast<void*>(GetBooleanFromDictionaryVector), "DictionaryGetBoolean", {},
                 paramTypes, OMNI_VEC_TYPE_BOOLEAN),
        Function(reinterpret_cast<void*>(GetVarcharFromDictionaryVector),
                 "DictionaryGetVarchar", {}, paramTypes, OMNI_VEC_TYPE_VARCHAR),
        Function(reinterpret_cast<void*>(GetDecimalFromDictionaryVector),
                 "DictionaryGetDecimal", {}, paramTypes, OMNI_VEC_TYPE_DECIMAL128)
    };
    return dictionaryFnRegistry;
}
