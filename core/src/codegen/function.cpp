/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Maps a function in function expression to a precompiled function
 */
#include <algorithm>
#include "function.h"

using namespace omniruntime::type;

namespace omniruntime {
    Function::Function(void* address, const std::string& name, const std::vector<std::string>& aliases,
                       const std::vector<DataTypeId>& paramTypes, const DataTypeId &retType, bool setExecutionContext)
    {
        this->address = address;
        // update function name used for lookup in codegen
        this->isExecContextSet = setExecutionContext;
        // create function sig to register for codegen
        this->signatures.emplace_back(name, paramTypes, retType, address);
        // create function sigs for different functions calls in omni-runtime
        for (auto& alias : aliases) {
            this->signatures.emplace_back(alias, paramTypes, retType, address);
        }
    }

    Function::Function(const std::string& fnID, const FunctionSignature& signature)
    {
        this->signatures.push_back(signature);
    }

    Function::~Function() = default;

    const std::vector<FunctionSignature>& Function::GetSignatures() const
    {
        return this->signatures;
    }

    std::string Function::GetId() const
    {
        return this->signatures.at(0).ToString();
    }

    DataTypeId Function::GetReturnType() const
    {
        return this->signatures.at(0).GetReturnType();
    }
    const std::vector<DataTypeId> &Function::GetParamTypes() const
    {
        return this->signatures.at(0).GetParams();
    }

    const void *Function::GetAddress() const
    {
        return this->address;
    }

    bool Function::IsExecutionContextSet() const
    {
        return this->isExecContextSet;
    }
}
