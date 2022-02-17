/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry function
 */
#include "func_registry.h"
#include <algorithm>
#include "util/debug.h"

using namespace std;
using namespace omniruntime;
using omniruntime::Function;

vector<Function> FunctionRegistry::registeredFunctions = Initialize();
FunctionMapPtr FunctionRegistry::functionRegistry;

vector<unique_ptr<BaseFunctionRegistry>> FunctionRegistry::GetFunctionRegistries()
{
    vector<unique_ptr<BaseFunctionRegistry>> functionRegistries;
    functionRegistries.push_back(make_unique<ContextFunctionRegistry>());
    functionRegistries.push_back(make_unique<DecimalFunctionRegistry>());
    functionRegistries.push_back(make_unique<DictionaryFunctionRegistry>());
    functionRegistries.push_back(make_unique<MathFunctionRegistry>());
    functionRegistries.push_back(make_unique<StringFunctionRegistry>());
    functionRegistries.push_back(make_unique<HashFunctionRegistry>());

    // External functions
    functionRegistries.push_back(make_unique<ExternalFunctionRegistry>());

    return functionRegistries;
}

std::vector<Function> FunctionRegistry::Initialize()
{
    std::vector<Function> allFunctions;
    functionRegistry = std::make_unique<std::unordered_map<const FunctionSignature*, const Function*, Hash, Equals>>();

    auto registries = GetFunctionRegistries();
    for (auto const &registry : registries) {
        auto functions = registry->GetFunctions();
        allFunctions.insert(std::end(allFunctions), functions.begin(), functions.end());
    }
    for (auto &function : allFunctions) {
        for (auto &signature : function.GetSignatures()) {
            if (functionRegistry->find(&signature) != functionRegistry->end()) {
                LogWarn("Trying to register functions with same signature: %s", signature.ToString().c_str());
            }
            functionRegistry->insert(std::make_pair(&signature, &function));
        }
    }

    return allFunctions;
}

FunctionRegistry::~FunctionRegistry() = default;

const Function *FunctionRegistry::LookupFunction(FunctionSignature *signature)
{
    auto result = functionRegistry->find(signature);
    if (result == functionRegistry->end()) {
        LogWarn("Function not supported: %s", signature->ToString().c_str());
        return nullptr;
    }
    return result->second;
}

std::vector<Function> &FunctionRegistry::GetFunctions()
{
    return registeredFunctions;
}
