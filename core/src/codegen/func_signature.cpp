/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description:
 */
#include "util/type_util.h"
#include "func_signature.h"

using namespace omniruntime::type;

static std::string ToLower(std::string s)
{
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return std::tolower(c); });
    return s;
}

FunctionSignature::FunctionSignature(const std::string name, std::vector<DataTypeId> params,
    const omniruntime::type::DataTypeId &returnType)
    : funcName(name), paramTypes(params), retType(returnType)
{}

// Copy constructor
FunctionSignature::FunctionSignature(const FunctionSignature &fs)
    : funcName(fs.funcName), paramTypes(fs.paramTypes), retType(fs.retType)
{}

FunctionSignature::~FunctionSignature() {}

std::string FunctionSignature::GetName() const
{
    return this->funcName;
}

const std::vector<DataTypeId> &FunctionSignature::GetParams() const
{
    return this->paramTypes;
}

DataTypeId FunctionSignature::GetReturnType() const
{
    return this->retType;
}

FunctionSignature &FunctionSignature::operator = (FunctionSignature other)
{
    std::swap(funcName, other.funcName);
    std::swap(paramTypes, other.paramTypes);
    std::swap(retType, other.retType);
    return *this;
}

bool FunctionSignature::operator == (const FunctionSignature &other) const
{
    if (ToLower(this->funcName) != ToLower(other.funcName) || this->retType != other.retType ||
        this->paramTypes.size() != other.paramTypes.size()) {
        return false;
    }

    for (uint32_t i = 0; i < this->paramTypes.size(); i++) {
        if (this->paramTypes.at(i) != other.paramTypes.at(i)) {
            return false;
        }
    }
    return true;
}

size_t FunctionSignature::HashCode() const
{
    auto hashName = std::hash<std::string> {}(ToLower(this->funcName));
    auto hashReturnType = std::hash<int> {}(static_cast<int>(this->retType));
    auto combinedHash = hashName ^ (hashReturnType << 1);
    for (auto param : this->paramTypes) {
        auto hashParamType = std::hash<int> {}(static_cast<int>(param));
        combinedHash = hashParamType ^ (combinedHash << 1);
    }
    return combinedHash;
}

std::string FunctionSignature::ToString() const
{
    auto result = this->funcName;
    for (auto const & param : this->paramTypes) {
        result += "_";
        result += TypeUtil::TypeToString(param);
    }
    result = result + "_" + TypeUtil::TypeToString(this->retType);
    return result;
}