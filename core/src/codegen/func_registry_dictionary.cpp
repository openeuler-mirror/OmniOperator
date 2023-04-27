/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Dictionary Functions Registry
 */
#include "func_registry_dictionary.h"
#include "functions/dictionaryfunctions.h"

namespace omniruntime::codegen {
using namespace omniruntime::type;
using namespace omniruntime::codegen::function;

std::vector<Function> DictionaryFunctionRegistry::GetFunctions()
{
    std::vector<DataTypeId> paramTypes = { OMNI_LONG, OMNI_INT };
    std::vector<DataTypeId> getStringParamTypes = { OMNI_LONG, OMNI_INT, OMNI_INT };
    std::vector<Function> dictionaryFnRegistry = { Function(reinterpret_cast<void *>(GetIntFromDictionaryVector),
        "DictionaryGetInt", {}, paramTypes, OMNI_INT),
        Function(reinterpret_cast<void *>(GetLongFromDictionaryVector), "DictionaryGetLong", {}, paramTypes, OMNI_LONG),
        Function(reinterpret_cast<void *>(GetDoubleFromDictionaryVector), "DictionaryGetDouble", {}, paramTypes,
            OMNI_DOUBLE),
        Function(reinterpret_cast<void *>(GetBooleanFromDictionaryVector), "DictionaryGetBoolean", {}, paramTypes,
            OMNI_BOOLEAN),
        Function(reinterpret_cast<void *>(GetVarcharFromDictionaryVector), "DictionaryGetVarchar", {}, paramTypes,
            OMNI_VARCHAR),
        Function(reinterpret_cast<void *>(GetDecimalFromDictionaryVector), "DictionaryGetDecimal", {}, paramTypes,
            OMNI_DECIMAL128)
    };
    return dictionaryFnRegistry;
}
}
