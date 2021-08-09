/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description:
 */
#include "./func_signature.h"

FunctionSignature::FunctionSignature(std::string name, std::vector<DataType> params, DataType returnType, void* address)
{
    this->funcName = name;
    this->paramTypes = params;
    this->retType = returnType;
    this->funcAddress = address;
}

FunctionSignature::~FunctionSignature(){}

std::string FunctionSignature::GetName()
{
    return this->funcName;
}

std::vector<DataType> FunctionSignature::GetParams()
{
    return this->paramTypes;
}

DataType FunctionSignature::GetReturnType()
{
    return this-> retType;
}

void* FunctionSignature::GetFunctionAddress()
{
    return this->funcAddress;
}