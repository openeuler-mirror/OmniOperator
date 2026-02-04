/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description:
 */
#include <utility>
#include <algorithm>
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
    // 1. Basic matching: Function name and return type must match.
    if (funcName != other.funcName || retType != other.retType) {
        return false;
    }

    // 2. Neither is a variable parameter: the parameter lists must be exactly the same (most common scenario).
    if (!isVariadic_ && !other.isVariadic_) {
        if (paramTypes.size() != other.paramTypes.size()) {
            return false;
        }
        return std::equal(paramTypes.begin(), paramTypes.end(), other.paramTypes.begin());
    }

    // 3. Both are variable parameters: Compare the variable parameter types + minimum number of parameters.
    if (isVariadic_ && other.isVariadic_) {
        return variadicType_ == other.variadicType_ && minArgs_ == other.minArgs_;
    }

    // 4. Only one side is a variadic parameter.
    const FunctionSignature &variadicSig = isVariadic_ ? *this : other;
    const FunctionSignature &concrete = isVariadic_ ? other : *this;

    // 4.1 The number of parameters must meet the variable parameter requirement.
    if (static_cast<int>(concrete.paramTypes.size()) < variadicSig.minArgs_) {
        return false;
    }

    // 4.2 All actual parameters must match the variable parameter types.
    for (const auto &t : concrete.paramTypes) {
        if (t != variadicSig.variadicType_) {
            return false;
        }
    }
    return true;
}

static constexpr size_t kHashMultiplier = 2654435761UL;

// Special hash flag for variable parameter signature
static constexpr size_t kVariadicHashMarker = 0x56415249;  // "VARI" in hex

static constexpr size_t kTypeHashTable[] = {
    0  * kHashMultiplier,  // OMNI_NONE = 0
    1  * kHashMultiplier,  // OMNI_INT = 1
    2  * kHashMultiplier,  // OMNI_LONG = 2
    3  * kHashMultiplier,  // OMNI_DOUBLE = 3
    4  * kHashMultiplier,  // OMNI_BOOLEAN = 4
    5  * kHashMultiplier,  // OMNI_SHORT = 5
    6  * kHashMultiplier,  // OMNI_DECIMAL64 = 6
    7  * kHashMultiplier,  // OMNI_DECIMAL128 = 7
    8  * kHashMultiplier,  // OMNI_DATE32 = 8
    9  * kHashMultiplier,  // OMNI_DATE64 = 9
    10 * kHashMultiplier,  // OMNI_TIME32 = 10
    11 * kHashMultiplier,  // OMNI_TIME64 = 11
    12 * kHashMultiplier,  // OMNI_TIMESTAMP = 12
    13 * kHashMultiplier,  // OMNI_INTERVAL_MONTHS = 13
    14 * kHashMultiplier,  // OMNI_INTERVAL_DAY_TIME = 14
    15 * kHashMultiplier,  // OMNI_VARCHAR = 15
    16 * kHashMultiplier,  // OMNI_CHAR = 16
    17 * kHashMultiplier,  // OMNI_CONTAINER = 17
    18 * kHashMultiplier,  // OMNI_BYTE = 18
    19 * kHashMultiplier,  // OMNI_FLOAT = 19
    20 * kHashMultiplier,  // OMNI_VARBINARY = 20
    21 * kHashMultiplier,  // OMNI_TIME_WITHOUT_TIME_ZONE = 21
    22 * kHashMultiplier,  // OMNI_TIMESTAMP_WITHOUT_TIME_ZONE = 22
    23 * kHashMultiplier,  // OMNI_TIMESTAMP_WITH_TIME_ZONE = 23
    24 * kHashMultiplier,  // OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE = 24
    25 * kHashMultiplier,  // OMNI_MULTISET = 25
    26 * kHashMultiplier,  // reserved
    27 * kHashMultiplier,  // reserved
    28 * kHashMultiplier,  // reserved
    29 * kHashMultiplier,  // reserved
    30 * kHashMultiplier,  // OMNI_ARRAY = 30
    31 * kHashMultiplier,  // OMNI_MAP = 31
    32 * kHashMultiplier,  // OMNI_ROW = 32
    33 * kHashMultiplier,  // OMNI_UNKNOWN = 33
    34 * kHashMultiplier,  // OMNI_FUNCTION = 34
    35 * kHashMultiplier,  // OMNI_OPAQUE = 35
    36 * kHashMultiplier,  // OMNI_INVALID = 36 (fallback)
    37 * kHashMultiplier,  // reserved for future
    38 * kHashMultiplier,  // reserved for future
    39 * kHashMultiplier,  // reserved for future
};

static constexpr size_t kTypeHashTableSize = sizeof(kTypeHashTable) / sizeof(kTypeHashTable[0]);

static inline size_t hashTypeId(DataTypeId typeId) {
    size_t idx = static_cast<size_t>(typeId);
    if (idx < kTypeHashTableSize) {
        return kTypeHashTable[idx];
    }
    return idx * kHashMultiplier;
}

size_t FunctionSignature::HashCode() const
{
    auto hashName = std::hash<std::string>{}(funcName);
    size_t combinedHash = hashName ^ (hashTypeId(retType) << 1);

    if (isVariadic_) {
        return hashTypeId(variadicType_) ^ (combinedHash << 1) ^ kVariadicHashMarker;
    }

    if (!paramTypes.empty()) {
        DataTypeId firstType = paramTypes[0];
        bool allSameType = std::all_of(paramTypes.begin(), paramTypes.end(),
            [firstType](DataTypeId t) { return t == firstType; });

        if (allSameType) {
            return hashTypeId(firstType) ^ (combinedHash << 1) ^ kVariadicHashMarker;
        }
    }

    for (const auto &param : paramTypes) {
        combinedHash = hashTypeId(param) ^ (combinedHash << 1);
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
