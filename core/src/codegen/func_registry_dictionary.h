/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Dictionary Functions Registry
 */
#ifndef OMNI_RUNTIME_FUNC_REGISTRY_DICTIONARY_H
#define OMNI_RUNTIME_FUNC_REGISTRY_DICTIONARY_H
#include "function.h"

std::vector<omniruntime::Function> GetDictionaryFunctionRegistry();

// functions called directly from codegen
const std::string dictionaryGetIntStr = "DictionaryGetInt";
const std::string dictionaryGetLongStr = "DictionaryGetLong";
const std::string dictionaryGetDoubleStr = "DictionaryGetDouble";
const std::string dictionaryGetBooleanStr = "DictionaryGetBoolean";
const std::string dictionaryGetVarcharStr = "DictionaryGetVarchar";
const std::string dictionaryGetDecimalStr = "DictionaryGetDecimal";

#endif // OMNI_RUNTIME_FUNC_REGISTRY_DICTIONARY_H
