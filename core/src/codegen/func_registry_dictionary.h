/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Dictionary Functions Registry
 */
#ifndef OMNI_RUNTIME_FUNC_REGISTRY_DICTIONARY_H
#define OMNI_RUNTIME_FUNC_REGISTRY_DICTIONARY_H
#include "function.h"
#include "func_registry_base.h"

// functions called directly from codegen
const std::string dictionaryGetIntStr = "get_dictionary_value";
const std::string dictionaryGetLongStr = "get_dictionary_value";
const std::string dictionaryGetDoubleStr = "get_dictionary_value";
const std::string dictionaryGetBooleanStr = "get_dictionary_value";
const std::string dictionaryGetVarcharStr = "get_dictionary_value";
const std::string dictionaryGetDecimalStr = "get_dictionary_value";

namespace omniruntime {
class DictionaryFunctionRegistry : public BaseFunctionRegistry {
public:
    std::vector<Function> GetFunctions() override;
};
}

#endif // OMNI_RUNTIME_FUNC_REGISTRY_DICTIONARY_H
