/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: OmniRuntime Function Header
 */
#ifndef OMNI_RUNTIME_FUNCTION_H
#define OMNI_RUNTIME_FUNCTION_H

#include "func_signature.h"
#include "util/type_util.h"

namespace omniruntime::codegen {
enum NullableResultType {
    INPUT_DATA,
    INPUT_DATA_AND_NULL,
    INPUT_DATA_AND_OVERFLOW_NULL,
    INPUT_DATA_AND_NULL_AND_RETURN_NULL,
    DEFAULT
};

class Function {
public:
    Function() = default;

    /**
     * Constructs an omni-runtime Function object that contains the functionality and attributes of an omni-runtime
     * function
     *
     * @param name function name
     * @param address contains a void pointer of the function
     * @param aliases allows to specify multiple names for the same function
     * @param paramTypes vector of datatypes of arguments - VARCHAR AND CHAR are expanded to their corresponding
     * function signature equivalents to contain value and length for VARCHAR and value, length and width for CHAR
     * @param retType data type of return value
     * @param setExecutionContext if true - pass the execution context to func signature as a param,
     * it will always be the first parameter in your function, default to false
     */
    Function(void *address, const std::string &name, const std::vector<std::string> &aliases,
        const std::vector<omniruntime::type::DataTypeId> &paramTypes, const omniruntime::type::DataTypeId &retType,
        NullableResultType = DEFAULT, bool setExecutionContext = false);

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
    std::string GetId() const;
    const void *GetAddress() const;
    const NullableResultType GetNullableResultType() const;
    bool IsExecutionContextSet() const;

private:
    void *address;
    // signatures corresponding to that function
    std::vector<FunctionSignature> signatures = {};
    NullableResultType nullableResultType;
    bool isExecContextSet = false;
};
}

#endif // OMNI_RUNTIME_FUNCTION_H
