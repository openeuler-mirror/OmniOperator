/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Dictionary Functions Registry
 */
#include "func_registry_dictionary.h"
#include "functions/dictionaryfunctions.h"
using namespace omniruntime;
using namespace omniruntime::expressions;

std::vector<Function> GetDictionaryFunctionRegistry()
{
    std::vector<DataType> paramTypes = {INT64D, INT32D};
    static std::vector<Function> dictionaryFnRegistry = {
        Function(reinterpret_cast<void*>(GetIntFromDictionaryVector), "DictionaryGetInt", {}, paramTypes,
                 INT32D, false),
        Function(reinterpret_cast<void*>(GetLongFromDictionaryVector), "DictionaryGetLong", {}, paramTypes,
                 INT64D, false),
        Function(reinterpret_cast<void*>(GetDoubleFromDictionaryVector), "DictionaryGetDouble", {},
                 paramTypes, DOUBLED, false),
        Function(reinterpret_cast<void*>(GetBooleanFromDictionaryVector), "DictionaryGetBoolean", {},
                 paramTypes, BOOLD, false),
        Function(reinterpret_cast<void*>(GetVarcharFromDictionaryVector),
                 "DictionaryGetVarchar", {}, {INT64D, INT32D, INT32PTRD}, INT8PTRD, false),
        Function(reinterpret_cast<void*>(GetDecimalFromDictionaryVector),
                 "DictionaryGetDecimal", {}, {INT64D, INT32D, INT64D}, INT64D, false)
    };
    return dictionaryFnRegistry;
}
