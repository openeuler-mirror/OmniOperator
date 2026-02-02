/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description:
 */
#include <utility>
#include "util/type_util.h"
#include "func_signature.h"

namespace omniruntime::codegen {
using namespace omniruntime::type;

FunctionSignature::FunctionSignature() = default;

FunctionSignature::FunctionSignature(const std::string &name, std::vector<DataTypeId> params,
    const omniruntime::type::DataTypeId &returnType, void *address)
    : funcName(name), paramTypes(std::move(params)), retType(returnType), funcAddress(address)
{}

// Copy constructor
FunctionSignature::FunctionSignature(const FunctionSignature &fs)
    //: funcName(fs.funcName), paramTypes(fs.paramTypes), retType(fs.retType), funcAddress(fs.funcAddress)
    : funcName(fs.funcName), paramTypes(fs.paramTypes), retType(fs.retType), funcAddress(fs.funcAddress),
    isVariadic_(fs.isVariadic_), variadicType_(fs.variadicType_), minArgs_(fs.minArgs_)
{}

FunctionSignature::~FunctionSignature() = default;

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

void *FunctionSignature::GetFunctionAddress() const
{
    return this->funcAddress;
}

FunctionSignature &FunctionSignature::operator = (FunctionSignature other)
{
    std::swap(funcName, other.funcName);
    std::swap(paramTypes, other.paramTypes);
    std::swap(retType, other.retType);
    std::swap(funcAddress, other.funcAddress);
    std::swap(isVariadic_, other.isVariadic_);
    std::swap(variadicType_, other.variadicType_);
    std::swap(minArgs_, other.minArgs_);
    return *this;
}

bool FunctionSignature::operator == (const FunctionSignature &other) const
{
    // Function name and return type must always match
    if (this->funcName != other.funcName || this->retType != other.retType) {
        return false;
    }

    // Handle variadic function matching
    // Case 1: This is a variadic signature, other is a concrete call
    if (this->isVariadic_ && !other.isVariadic_) {
        // Check minimum argument count
        if (static_cast<int>(other.paramTypes.size()) < this->minArgs_) {
            return false;
        }
        // All arguments must match the variadic type
        for (const auto& paramType : other.paramTypes) {
            if (paramType != this->variadicType_) {
                return false;
            }
        }
        return true;
    }

    // Case 2: Other is a variadic signature, this is a concrete call
    if (other.isVariadic_ && !this->isVariadic_) {
        // Check minimum argument count
        if (static_cast<int>(this->paramTypes.size()) < other.minArgs_) {
            return false;
        }
        // All arguments must match the variadic type
        for (const auto& paramType : this->paramTypes) {
            if (paramType != other.variadicType_) {
                return false;
            }
        }
        return true;
    }

    // Case 3: Both are variadic - must have same variadic type and min args
    if (this->isVariadic_ && other.isVariadic_) {
        return this->variadicType_ == other.variadicType_ && this->minArgs_ == other.minArgs_;
    }

    // Case 4: Neither is variadic - exact match required (original logic)
    if (this->paramTypes.size() != other.paramTypes.size()) {
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
    auto hashName = std::hash<std::string> {}(this->funcName);
    auto hashReturnType = std::hash<int> {}(static_cast<int>(this->retType));
    auto combinedHash = hashName ^ (hashReturnType << 1);

    // For variadic signatures, use only function name + return type + variadic type
    // This ensures that calls with different argument counts can find the same signature
    if (this->isVariadic_) {
        auto hashVariadicType = std::hash<int> {}(static_cast<int>(this->variadicType_));
        // Use a special marker (0xVARIADIC) to distinguish from non-variadic signatures
        combinedHash = hashVariadicType ^ (combinedHash << 1) ^ 0x56415249;  // "VARI" in hex
        return combinedHash;
    }

    // For non-variadic signatures with all same-type parameters, compute a "potential variadic" hash
    // This allows concrete calls like greatest(INT, INT, INT) to match variadic greatest<INT>
    if (!this->paramTypes.empty()) {
        bool allSameType = true;
        DataTypeId firstType = this->paramTypes[0];
        for (const auto& param : this->paramTypes) {
            if (param != firstType) {
                allSameType = false;
                break;
            }
        }

        if (allSameType) {
            // Compute the same hash as a variadic signature would
            auto hashVariadicType = std::hash<int> {}(static_cast<int>(firstType));
            return hashVariadicType ^ (combinedHash << 1) ^ 0x56415249;
        }
    }

    // Original logic for non-variadic signatures with mixed types
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

std::string FunctionSignature::ToString(omniruntime::op::OverflowConfig *overflowConfig) const
{
    auto result = this->funcName;
    if (overflowConfig != nullptr && overflowConfig->GetOverflowConfigId() == omniruntime::op::OVERFLOW_CONFIG_NULL) {
        result += "_null";
    }
    for (auto const & param : this->paramTypes) {
        result += "_";
        result += TypeUtil::TypeToString(param);
    }
    result = result + "_" + TypeUtil::TypeToString(this->retType);
    return result;
}

std::shared_ptr<FunctionSignature> FunctionSignatureBuilder::Build()
{
    auto signature = std::make_shared<FunctionSignature>(funcName_, argumentTypes_, returnType_);
    if (variableArity_) {
        signature->SetVariadic(true, variadicType_, minArgs_);
    }
    return signature;
}

std::shared_ptr<FunctionSignature> FunctionSignature::Variadic(const std::string &name,
                                                            omniruntime::type::DataTypeId variadicType,
                                                            omniruntime::type::DataTypeId returnType,
                                                            int minArgs)
{
    auto signature = std::make_shared<FunctionSignature>(name, std::vector<DataTypeId>{}, returnType);
    signature->SetVariadic(true, variadicType, minArgs);
    return signature;
}
}
