/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description:
 */
#include "./func_signature.h"

using namespace omniruntime::expressions;

FunctionSignature::FunctionSignature()
{
}

FunctionSignature::FunctionSignature(std::string name, std::vector<DataType> params, DataType returnType, void* address)
{
    this->funcName = name;
    this->paramTypes = params;
    this->retType = returnType;
    this->funcAddress = address;
}

// Copy constructor
FunctionSignature::FunctionSignature(const FunctionSignature &fs) : funcName(fs.funcName), paramTypes(fs.paramTypes),
    retType(fs.retType), funcAddress(fs.funcAddress)
{
}

FunctionSignature::~FunctionSignature() {
}

std::string FunctionSignature::GetName() const
{
    return this->funcName;
}

std::vector<DataType> FunctionSignature::GetParams() const
{
    return this->paramTypes;
}

DataType FunctionSignature::GetReturnType() const
{
    return this-> retType;
}

void* FunctionSignature::GetFunctionAddress() const
{
    return this->funcAddress;
}