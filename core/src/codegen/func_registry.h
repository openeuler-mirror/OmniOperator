/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry function
 */
#ifndef OMNI_RUNTIME_FUNC_REGISTRY_H
#define OMNI_RUNTIME_FUNC_REGISTRY_H
#include "function.h"
#include "external_func_registry.h"
#include "func_registry_context.h"
#include "func_registry_decimal.h"
#include "func_registry_dictionary.h"
#include "func_registry_math.h"
#include "func_registry_hash.h"
#include "func_registry_string.h"

class FunctionRegistry {
public:
    ~FunctionRegistry();
    static omniruntime::Function *LookupFunction(const std::string& fnID);
    static std::vector<omniruntime::Function> &GetFunctions();

private:
    ExternalFuncRegistry efr;
    static std::vector<omniruntime::Function> functionRegistry;

    static std::vector<omniruntime::Function> Initialize();
};
#endif // OMNI_RUNTIME_FUNC_REGISTRY_H
