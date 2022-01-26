/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Maps a function in function expression to a precompiled function
 */
#include <algorithm>
#include "function.h"

using namespace omniruntime::expressions;

namespace omniruntime {
    void ReplaceArgDataTypes(std::vector<DataType>& vec, std::string fnID)
    {
        if (fnID == "mm3hash") {
            vec.insert(vec.begin() + vec.size() - 1, DataType::BOOLD);
        }
        // replace all decimal128 datatypes to int64 for signature
        std::replace(vec.begin(), vec.end(), DECIMAL128D, INT64D);

        // replace varchar data type in argument to int8ptr and length of string for signature
        while (std::count(vec.begin(), vec.end(), VARCHARD) != 0) {
            auto itr = std::find(vec.begin(), vec.end(), VARCHARD);
            vec.insert(++itr, INT32D);
            std::replace(vec.begin(), ++(std::find(vec.begin(), vec.end(), VARCHARD)), VARCHARD, INT8PTRD);
        }

        // replace char data type in argument to int8ptr, length and width for signature
        while (std::count(vec.begin(), vec.end(), CHARD) != 0) {
            auto itr = std::find(vec.begin(), vec.end(), CHARD);
            vec.insert(++itr, INT32D);
            vec.insert(++itr, INT32D);
            std::replace(vec.begin(), ++(std::find(vec.begin(), vec.end(), CHARD)), CHARD, INT8PTRD);
        }
    }

    void ReplaceRetDataType(DataType& ret)
    {
        if (ret == DECIMAL128D) {
            ret = INT64D;
        }
        if (ret == VARCHARD || ret == CHARD) {
            ret = INT8PTRD;
        }
    }

    std::string GetFuncIdSuffix(const std::vector<DataType>& paramTypes, const DataType& retType, int32_t numArgs)
    {
        std::string id;
        for (std::vector<DataType>::size_type i = 0; i != numArgs; i++) {
            id += "_" + DataTypeString(paramTypes[i]);
        }
        return id += "_" + DataTypeString(retType);
    }

    Function::Function(void* address, const std::string& fnID, const std::vector<std::string>& aliases, const
    std::vector<DataType>& paramTypes, const DataType& retType, bool generateFuncID, bool setExecutionContext)
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
        // update paramTypes for signature
        std::vector<DataType> args = paramTypes;
        if (setExecutionContext)
            args.push_back(INT64D);
        ReplaceArgDataTypes(args, fnID);
        // update ret for signature
        DataType ret = retType;
        ReplaceRetDataType(ret);
        // create a function sig to register for codegen
        this->signatures.emplace_back(this->funcID, args, ret, address);
        // create function sigs for different functions calls in omni-runtime
        for (auto& alias : aliases) {
            this->signatures.emplace_back(alias, args, ret, address);
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
