/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: Batch Dictionary Function Registry
 */

#include "batch_func_registry_dictionary.h"
#include "batch_functions/batch_dictionaryfunctions.h"

namespace omniruntime::codegen {
using namespace omniruntime::type;
using namespace codegen::function;

std::vector<Function> BatchDictionaryFunctionRegistry::GetFunctions()
{
    std::vector<DataTypeId> paramTypes = { OMNI_LONG };
    std::vector<Function> batchDictionaryFnRegistry = {
        Function(reinterpret_cast<void *>(BatchGetIntFromDictionaryVector), "batch_GetDic", {}, paramTypes, OMNI_INT),
        Function(reinterpret_cast<void *>(BatchGetIntFromDictionaryVector), "batch_GetDic", {}, paramTypes,
            OMNI_DATE32),
        Function(reinterpret_cast<void *>(BatchGetLongFromDictionaryVector), "batch_GetDic", {}, paramTypes, OMNI_LONG),
        Function(reinterpret_cast<void *>(BatchGetLongFromDictionaryVector), "batch_GetDic", {}, paramTypes,
            OMNI_DECIMAL64),
        Function(reinterpret_cast<void *>(BatchGetLongFromDictionaryVector), "batch_GetDic", {}, paramTypes,
            OMNI_TIMESTAMP),
        Function(reinterpret_cast<void *>(BatchGetDoubleFromDictionaryVector), "batch_GetDic", {}, paramTypes,
            OMNI_DOUBLE),
        Function(reinterpret_cast<void *>(BatchGetBooleanFromDictionaryVector), "batch_GetDic", {}, paramTypes,
            OMNI_BOOLEAN),
        Function(reinterpret_cast<void *>(BatchGetVarcharFromDictionaryVector), "batch_GetDic", {}, paramTypes,
            OMNI_VARCHAR),
        Function(reinterpret_cast<void *>(BatchGetVarcharFromDictionaryVector), "batch_GetDic", {}, paramTypes,
            OMNI_CHAR),
        Function(reinterpret_cast<void *>(BatchGetDecimalFromDictionaryVector), "batch_GetDic", {}, paramTypes,
            OMNI_DECIMAL128),
        Function(reinterpret_cast<void *>(BatchGetIntFromVector), "batch_GetData", {}, paramTypes, OMNI_INT),
        Function(reinterpret_cast<void *>(BatchGetIntFromVector), "batch_GetData", {}, paramTypes, OMNI_DATE32),
        Function(reinterpret_cast<void *>(BatchGetLongFromVector), "batch_GetData", {}, paramTypes, OMNI_LONG),
        Function(reinterpret_cast<void *>(BatchGetLongFromVector), "batch_GetData", {}, paramTypes, OMNI_DECIMAL64),
        Function(reinterpret_cast<void *>(BatchGetLongFromVector), "batch_GetData", {}, paramTypes, OMNI_TIMESTAMP),
        Function(reinterpret_cast<void *>(BatchGetDoubleFromVector), "batch_GetData", {}, paramTypes, OMNI_DOUBLE),
        Function(reinterpret_cast<void *>(BatchGetBooleanFromVector), "batch_GetData", {}, paramTypes, OMNI_BOOLEAN),
        Function(reinterpret_cast<void *>(BatchGetVarcharFromVector), "batch_GetData", {}, paramTypes, OMNI_VARCHAR),
        Function(reinterpret_cast<void *>(BatchGetVarcharFromVector), "batch_GetData", {}, paramTypes, OMNI_CHAR),
        Function(reinterpret_cast<void *>(BatchGetDecimalFromVector), "batch_GetData", {}, paramTypes, OMNI_DECIMAL128)
    };
    return batchDictionaryFnRegistry;
}
}