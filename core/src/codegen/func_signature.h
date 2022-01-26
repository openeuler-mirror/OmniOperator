/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description:
 */
#ifndef __FUNC_SIGNATURE_H__
#define __FUNC_SIGNATURE_H__
#include <vector>
#include <map>
#include <set>
#include <string>

#include "common/datatype.h"

class FunctionSignature {
public:
    FunctionSignature();
    FunctionSignature(std::string name, std::vector<omniruntime::expressions::DataType> params,
                      omniruntime::expressions::DataType returnType, void* address);
    FunctionSignature(const FunctionSignature &fs);
    FunctionSignature &operator=(FunctionSignature other)
    {
        std::swap(funcName, other.funcName);
        std::swap(paramTypes, other.paramTypes);
        std::swap(retType, other.retType);
        std::swap(funcAddress, other.funcAddress);
        return *this;
    }
    ~FunctionSignature();
    std::string GetId() const;
    std::string GetName() const;
    std::vector<omniruntime::expressions::DataType> GetParams() const;
    omniruntime::expressions::DataType GetReturnType() const;
    void* GetFunctionAddress() const;
private:
    std::string funcName = "";
    std::vector<omniruntime::expressions::DataType> paramTypes {};
    omniruntime::expressions::DataType retType = omniruntime::expressions::DataType::INVALIDDATAD;
    void* funcAddress = nullptr;
};

#endif