/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Maps a function in function expression to a precompiled function
 */
#include "function.h"

using namespace omniruntime::type;

namespace omniruntime {
Function::Function(const std::string& functionName, const std::string &name, const std::vector<std::string> &aliases,
    const std::vector<DataTypeId> &paramTypes, const DataTypeId &retType, bool setExecutionContext)
{
    this->name = name;
    this->functionName = functionName;
    // update function name used for lookup in codegen
    this->isExecContextSet = setExecutionContext;
    // create function sig to register for codegen
    this->signatures.emplace_back(name, paramTypes, retType);
    // create function sigs for different functions calls in omni-runtime
    for (auto &alias : aliases) {
        this->signatures.emplace_back(alias, paramTypes, retType);
    }
}

Function::Function(const std::string &fnID, const FunctionSignature &signature)
{
    this->signatures.push_back(signature);
}

Function::~Function() = default;

const std::vector<FunctionSignature> &Function::GetSignatures() const
{
    return this->signatures;
}

std::string Function::GetFunctionName() const
{
    return this->functionName;
}

DataTypeId Function::GetReturnType() const
{
    return this->signatures.at(0).GetReturnType();
}
const std::vector<DataTypeId> &Function::GetParamTypes() const
{
    return this->signatures.at(0).GetParams();
}

bool Function::IsExecutionContextSet() const
{
    return this->isExecContextSet;
}
}
