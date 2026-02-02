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
#include <type/data_type.h>
#include "operator/config/operator_config.h"

namespace omniruntime::codegen {
class FunctionSignature {
public:
    FunctionSignature();
    FunctionSignature(const std::string &name, std::vector<omniruntime::type::DataTypeId> params,
        const omniruntime::type::DataTypeId &returnType, void *address = nullptr);
    FunctionSignature(const FunctionSignature &fs);
    FunctionSignature &operator = (FunctionSignature other);
    bool operator == (const FunctionSignature &other) const;
    ~FunctionSignature();
    std::string GetName() const;
    const std::vector<omniruntime::type::DataTypeId> &GetParams() const;
    omniruntime::type::DataTypeId GetReturnType() const;
    void *GetFunctionAddress() const;
    size_t HashCode() const;
    std::string ToString() const;
    std::string ToString(omniruntime::op::OverflowConfig *overflowConfig) const;

    /// Variadic function support
    bool IsVariadic() const { return isVariadic_; }
    omniruntime::type::DataTypeId GetVariadicType() const { return variadicType_; }
    int GetMinArgs() const { return minArgs_; }
    void SetVariadic(bool variadic, omniruntime::type::DataTypeId variadicType, int minArgs) {
        isVariadic_ = variadic;
        variadicType_ = variadicType;
        minArgs_ = minArgs;
    }

    /// Create a variadic function signature
    /// @param name Function name
    /// @param variadicType The type that all variadic arguments must have
    /// @param returnType The return type
    /// @param minArgs Minimum number of arguments (default 2 for functions like greatest/least)
    static std::shared_ptr<FunctionSignature> Variadic(const std::string &name,
                                                    omniruntime::type::DataTypeId variadicType,
                                                    omniruntime::type::DataTypeId returnType,
                                                    int minArgs = 2);

private:
    std::string funcName;
    std::vector<omniruntime::type::DataTypeId> paramTypes {};
    omniruntime::type::DataTypeId retType;
    void *funcAddress = nullptr;

    // Variadic function support
    bool isVariadic_ = false;
    omniruntime::type::DataTypeId variadicType_ = omniruntime::type::OMNI_NONE;
    int minArgs_ = 0;
};

class FunctionSignatureBuilder {
public:
    FunctionSignatureBuilder() = default;

    FunctionSignatureBuilder &FuncName(const std::string &name)
    {
        this->funcName_ = name;
        return *this;
    }

    FunctionSignatureBuilder &ReturnType(type::DataTypeId type)
    {
        returnType_ = type;
        return *this;
    }

    FunctionSignatureBuilder &ArgumentType(type::DataTypeId type)
    {
        argumentTypes_.emplace_back(type);
        return *this;
    }

    /// Mark this function as variadic (variable number of arguments)
    /// @param variadicType The type that all variadic arguments must have
    /// @param minArgs Minimum number of arguments required
    FunctionSignatureBuilder &VariableArity(type::DataTypeId variadicType, int minArgs = 2)
    {
        variableArity_ = true;
        variadicType_ = variadicType;
        minArgs_ = minArgs;
        return *this;
    }

    std::shared_ptr<FunctionSignature> Build();

private:
    std::string funcName_;
    type::DataTypeId returnType_;
    std::vector<type::DataTypeId> argumentTypes_;
    bool variableArity_{false};
    type::DataTypeId variadicType_{type::OMNI_NONE};
    int minArgs_{0};
};
}

#endif