/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Dictionary Functions Registry
 */
#include "func_registry_dictionary.h"

using namespace omniruntime;
using namespace omniruntime::type;

namespace omniruntime {
std::vector<Function> DictionaryFunctionRegistry::GetFunctions()
{
    std::vector<DataTypeId> paramTypes = { OMNI_LONG, OMNI_INT };
    std::vector<Function> dictionaryFnRegistry = { Function("GetIntFromDictionaryVector", "get_dictionary_value", {},
        paramTypes, OMNI_INT),
        Function("GetLongFromDictionaryVector", "get_dictionary_value", {}, paramTypes, OMNI_LONG),
        Function("GetDoubleFromDictionaryVector", "get_dictionary_value", {}, paramTypes, OMNI_DOUBLE),
        Function("GetBooleanFromDictionaryVector", "get_dictionary_value", {}, paramTypes, OMNI_BOOLEAN),
        Function("GetVarcharFromDictionaryVector", "get_dictionary_value", {}, paramTypes, OMNI_VARCHAR),
        Function("GetDecimalFromDictionaryVector", "get_dictionary_value", {}, paramTypes, OMNI_DECIMAL128) };
    return dictionaryFnRegistry;
}
}
