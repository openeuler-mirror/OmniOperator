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

        /**
         * Constructs a omni-runtime Function object that contains the functionality and attributes of an omni-runtime
         * function
         *
         * @param name function name
         * @param address contains a void pointer of the function
         * @param aliases allows to specify multiple names for the same function
         * @param paramTypes vector of datatypes of arguments - VARCHAR AND CHAR are expanded to their corresponding
         * function signature equivalents to contain value and length for VARCHAR and value, length and width for CHAR
         * @param retType datatype of return value
         * @param generateFuncID if true - unique funcID is generated to match funcID in parser
         * @param setExecutionContext if true - pass the execution context to func signature as a param
         */
        Function(void *address, const std::string &name, const std::vector<std::string> &aliases, const
        std::vector<omniruntime::vec::VecTypeId> &paramTypes, const omniruntime::vec::VecTypeId
        &retType, bool setExecutionContext = false);

        Function(const std::string &fnID, const FunctionSignature &signature);

        // Copy constructor
        Function &operator=(Function other)
        {
            std::swap(signatures, other.signatures);
            return *this;
        }

        ~Function();
        const std::vector<FunctionSignature> &GetSignatures() const;
        omniruntime::vec::VecTypeId GetReturnType() const;
        const std::vector<omniruntime::vec::VecTypeId> &GetParamTypes() const;
        std::string GetId() const;
        const void *GetAddress() const;
        bool IsExecutionContextSet() const;
    private:
        void *address;
        // signatures corresponding to that function
        std::vector<FunctionSignature> signatures = {};
        bool isExecContextSet = false;
    };
}

#endif // OMNI_RUNTIME_FUNCTION_H
