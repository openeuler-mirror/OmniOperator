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
const std::string INVALID_HIVE_UDF = "";

vector<Function> FunctionRegistry::registeredFunctions = Initialize();
FunctionMapPtr FunctionRegistry::functionRegistry;
FunctionMapPtr FunctionRegistry::functionNullRegistry;
HiveUdfMapPtr FunctionRegistry::hiveUdfMap;
std::once_flag FunctionRegistry::initHiveUdfMap;

vector<unique_ptr<BaseFunctionRegistry>> FunctionRegistry::GetFunctionRegistries()
{
    vector<unique_ptr<BaseFunctionRegistry>> functionRegistries;
    functionRegistries.push_back(make_unique<ContextFunctionRegistry>());
    functionRegistries.push_back(make_unique<DecimalFunctionRegistry>());
    functionRegistries.push_back(make_unique<DictionaryFunctionRegistry>());
    functionRegistries.push_back(make_unique<MathFunctionRegistry>());
    functionRegistries.push_back(make_unique<StringFunctionRegistry>());
    functionRegistries.push_back(make_unique<HashFunctionRegistry>());
    functionRegistries.push_back(make_unique<VarcharVectorFunctionRegistry>());
    functionRegistries.push_back(make_unique<HiveUdfRegistry>());

    functionRegistries.push_back(make_unique<BatchMathFunctionRegistry>());
    functionRegistries.push_back(make_unique<BatchHashFunctionRegistry>());

    // External functions
    functionRegistries.push_back(make_unique<ExternalFunctionRegistry>());

    return functionRegistries;
}

std::vector<Function> FunctionRegistry::Initialize()
{
    hiveUdfMap = std::make_unique<std::unordered_map<std::string, std::string>>();

    std::vector<Function> allFunctions;
    functionRegistry =
        std::make_unique<std::unordered_map<const FunctionSignature *, const Function *, Hash, Equals>>();
    functionNullRegistry =
        std::make_unique<std::unordered_map<const FunctionSignature *, const Function *, Hash, Equals>>();

    auto registries = GetFunctionRegistries();
    for (auto const & registry : registries) {
        auto functions = registry->GetFunctions();
        allFunctions.insert(std::end(allFunctions), functions.begin(), functions.end());
    }
    for (auto &function : allFunctions) {
        for (auto &signature : function.GetSignatures()) {
            if (functionRegistry->find(&signature) != functionRegistry->end()) {
                LogWarn("Trying to register functions with same signature: %s", signature.ToString().c_str());
            }
            functionRegistry->insert(std::make_pair(&signature, &function));
            if (function.GetNullableResultType() == INPUT_DATA_AND_OVERFLOW_NULL) {
                functionNullRegistry->insert(std::make_pair(&signature, &function));
            }
        }
    }

    return allFunctions;
}

FunctionRegistry::~FunctionRegistry() = default;

const Function *FunctionRegistry::LookupFunction(FunctionSignature *signature)
{
    auto result = functionRegistry->find(signature);
    if (result == functionRegistry->end()) {
        return nullptr;
    }
    return result->second;
}

bool FunctionRegistry::LookupNullFunction(FunctionSignature *signature)
{
    auto signatureNull = FunctionSignature(signature->GetName() + "_null", signature->GetParams(),
        signature->GetReturnType(), signature->GetFunctionAddress());
    auto result = functionNullRegistry->find(&signatureNull);
    return result != functionNullRegistry->end();
}

// Some functions such as CastDecimal128ToStringRetNull(), it needs both contextPtr and overflowConfig as parameters.
// The purpose of the below function is to find this functions.
bool FunctionRegistry::IsNullExecutionContextSet(FunctionSignature *signature)
{
    auto signatureNull = FunctionSignature(signature->GetName() + "_null", signature->GetParams(),
        signature->GetReturnType(), signature->GetFunctionAddress());
    auto result = functionNullRegistry->find(&signatureNull);
    return result->second->IsExecutionContextSet();
}

void FunctionRegistry::InitHiveUdfMap()
{
    HiveUdfRegistry::GenerateHiveUdfMap(*(hiveUdfMap.get()));
}

const std::string &FunctionRegistry::LookupHiveUdf(const std::string &udfName)
{
    std::call_once(initHiveUdfMap, InitHiveUdfMap);
    auto result = hiveUdfMap->find(udfName);
    if (result == hiveUdfMap->end()) {
        return INVALID_HIVE_UDF;
    }
    return result->second;
}

std::vector<Function> &FunctionRegistry::GetFunctions()
{
    return registeredFunctions;
}
