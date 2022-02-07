/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Maps a function in function expression to a precompiled function
 */
#include <algorithm>
#include "function.h"

using namespace omniruntime::vec;

namespace omniruntime {
    std::string GetFuncIdSuffix(const std::vector<VecTypeId>& paramTypes, const omniruntime::vec::VecTypeId &retType, int32_t numArgs)
    {
        std::string id;
        for (std::vector<VecTypeId>::size_type i = 0; i != numArgs; i++) {
            id += "_" + TypeUtil::TypeToString(paramTypes[i]);
        }
        return id += "_" + TypeUtil::TypeToString(retType);
    }

    Function::Function(void* address, const std::string& fnID, const std::vector<std::string>& aliases,
         const std::vector<VecTypeId>& paramTypes, const omniruntime::vec::VecTypeId &retType, bool generateFuncID,
         bool setExecutionContext)
    {
        // update function name used for lookup in codegen
        this->funcID = fnID;
        this->isExecContextSet = setExecutionContext;
        // number of args expected for a valid function
        int32_t numArgs;
        if (FUNC_TO_NUM_ARGS.count(fnID)) {
            numArgs = FUNC_TO_NUM_ARGS.find(fnID)->second;
        } else {
            numArgs = paramTypes.size();
        }
        if (generateFuncID) {
            this->funcID += GetFuncIdSuffix(paramTypes, retType, numArgs);
        }
        std::vector<VecTypeId> args = paramTypes;
        // create function sig to register for codegen
        this->signatures.emplace_back(this->funcID, args, retType, address);
        // create function sigs for different functions calls in omni-runtime
        for (auto& alias : aliases) {
            this->signatures.emplace_back(alias, args, retType, address);
        }
    }

    Function::Function(const std::string& fnID, const FunctionSignature& signature)
    {
        this->funcID = fnID;
        this->signatures.push_back(signature);
    }

    Function::~Function() = default;

    const std::vector<FunctionSignature>& Function::GetSignatures() const
    {
        return this->signatures;
    }

    std::string Function::GetFuncID() const
    {
        return this->funcID;
    }

    bool Function::IsExecutionContextSet() const
    {
        return this->isExecContextSet;
    }
}
