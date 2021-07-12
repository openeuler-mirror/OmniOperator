#include "./func_signature.h"

FunctionSignature::FunctionSignature(std::string name, std::vector<DataType> params, DataType returnType, void* address)
{
    this->func_name = name;
    this->param_types = params;
    this->ret_type = returnType;
    this->func_address = address;
}

FunctionSignature::~FunctionSignature() {
}

std::string FunctionSignature::getName() {
    return this->func_name;
}

std::vector<DataType> FunctionSignature::getParams()
{
    return this->param_types;
}

DataType FunctionSignature::getReturnType()
{
    return this-> ret_type;
}

void* FunctionSignature::getFunctionAddress()
{
    return this->func_address;
}