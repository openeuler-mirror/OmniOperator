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
#include <cstring>

#include "../common/expressions.h"

using namespace omniruntime::expressions;

class FunctionSignature {
public:
    FunctionSignature(std::string name, std::vector<DataType> params, DataType returnType, void* address);
    ~FunctionSignature();
    std::string GetId();
    std::string GetName();
    std::vector<DataType> GetParams();
    DataType GetReturnType();
    void* GetFunctionAddress();
private:
    std::string funcName;
    std::vector<DataType> paramTypes;
    DataType retType;
    void* funcAddress;
};

std::vector<FunctionSignature> GetFunctionSignatures();

#endif