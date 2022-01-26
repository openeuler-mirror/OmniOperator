/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry function
 */
#include "func_registry.h"
#include <algorithm>
#include "util/debug.h"

using omniruntime::Function;

std::vector<Function> FunctionRegistry::functionRegistry = Initialize();

std::vector<omniruntime::Function> FunctionRegistry::Initialize()
{
    std::vector<Function> combinedRegistry;
    auto registry = GetContextFunctionRegistry();
    combinedRegistry.insert(std::end(combinedRegistry), registry.begin(), registry.end());

    registry = GetDecimalFunctionRegistry();
    combinedRegistry.insert(std::end(combinedRegistry), registry.begin(), registry.end());

    registry = GetDictionaryFunctionRegistry();
    combinedRegistry.insert(std::end(combinedRegistry), registry.begin(), registry.end());

    registry = GetMathFunctionRegistry();
    combinedRegistry.insert(std::end(combinedRegistry), registry.begin(), registry.end());

    registry = GetHashRegistry();
    combinedRegistry.insert(std::end(combinedRegistry), registry.begin(), registry.end());

    registry = GetStringFunctionRegistry();
    combinedRegistry.insert(std::end(combinedRegistry), registry.begin(), registry.end());

    registry = GetStringCmpFn();
    combinedRegistry.insert(std::end(combinedRegistry), registry.begin(), registry.end());

    // External functions
    // TODO: Need to find a way to statically initialize the external functions

    return combinedRegistry;
}

FunctionRegistry::~FunctionRegistry() = default;

Function *FunctionRegistry::LookupFunction(const std::string& fnID)
{
    auto it = find_if(functionRegistry.begin(), functionRegistry.end(), [&fnID](const
    Function& obj)
    {return obj.GetFuncID() == fnID;});
    if (it == functionRegistry.end()) {
        LogWarn("Function not supported: %s", fnID.c_str());
        return nullptr;
    }
    return &(*it);
}

std::vector<Function> &FunctionRegistry::GetFunctions()
{
    return functionRegistry;
}
