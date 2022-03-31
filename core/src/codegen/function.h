/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: OmniRuntime Function Header
 */
#ifndef OMNI_RUNTIME_FUNCTION_H
#define OMNI_RUNTIME_FUNCTION_H

#include "func_signature.h"
#include "util/type_util.h"

namespace omniruntime {
class Function {
public:
    Function() = default;

    /* *
     * Constructs a omni-runtime Function object that contains the functionality and attributes of an omni-runtime
     * function
     *
     * @param name external name for the function
     * @param functionName function name, needs to be exact same as the function that will be called
     * @param aliases allows to specify multiple names for the same function
     * @param paramTypes vector of datatypes of arguments - VARCHAR AND CHAR are expanded to their corresponding
     * function signature equivalents to contain value and length for VARCHAR and value, length and width for CHAR
     * @param retType data type of return value
     * @param setExecutionContext if true - pass the execution context to func signature as a param,
     * it will always be the first parameter in your function, default to false
     */
    Function(const std::string &functionName, const std::string &name, const std::vector<std::string> &aliases,
        const std::vector<omniruntime::type::DataTypeId> &paramTypes, const omniruntime::type::DataTypeId &retType,
        bool setExecutionContext = false);

    Function(const std::string &fnID, const FunctionSignature &signature);

    // Copy constructor
    Function &operator = (Function other)
    {
        std::swap(signatures, other.signatures);
        return *this;
    }

    ~Function();
    const std::vector<FunctionSignature> &GetSignatures() const;
    omniruntime::type::DataTypeId GetReturnType() const;
    const std::vector<omniruntime::type::DataTypeId> &GetParamTypes() const;
    std::string GetFunctionName() const;
    bool IsExecutionContextSet() const;

private:
    std::string name;
    std::string functionName;
    // signatures corresponding to that function
    std::vector<FunctionSignature> signatures = {};
    bool isExecContextSet = false;
};
}

#endif // OMNI_RUNTIME_FUNCTION_H
