/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: OmniRuntime Function Header
 */
#ifndef OMNI_RUNTIME_FUNCTION_H
#define OMNI_RUNTIME_FUNCTION_H

#include "func_signature.h"
#include "../common/datatype.h"

// used by parser to validate the number of args in each function
const std::map<std::string, int32_t> FUNC_TO_NUM_ARGS = {
    {"CAST", 1},
    {"substr_start", 2},
    {"substr", 3},
    {"concat", 2},
    {"abs", 1},
    {"LIKE", 2},
    {"combine_hash", 2},
    {"mm3hash", 2},
    {"pmod", 2}
};

namespace omniruntime {
    class Function {
    public:
        Function() = default;

        /**
         * Constructs a omni-runtime Function object that contains the functionality and attributes of an omni-runtime
         * function
         *
         * @param fnID uniquely identifies each individual function - assigned to function name or funcID based on
         * value of generateFuncID parameter
         * @param address contains a void pointer of the function
         * @param aliases allows to specify multiple names for the same function
         * @param paramTypes vector of datatypes of arguments - VARCHAR AND CHAR are expanded to their corresponding
         * function signature equivalents to contain value and length for VARCHAR and value, length and width for CHAR
         * @param retType datatype of return value
         * @param generateFuncID if true - unique funcID is generated to match funcID in parser
         * @param setExecutionContext if true - pass the execution context to func signature as a param
         */
        Function(void *address, const std::string &fnID, const std::vector<std::string> &aliases, const
        std::vector<omniruntime::expressions::DataType> &paramTypes, const omniruntime::expressions::DataType
        &retType, bool generateFuncID = true, bool setExecutionContext = false);

        Function(const std::string &fnID, const FunctionSignature &signature);

        // Copy constructor
        Function &operator=(Function other)
        {
            std::swap(funcID, other.funcID);
            std::swap(signatures, other.signatures);
            return *this;
        }

        ~Function();
        const std::vector<FunctionSignature> &GetSignatures() const;
        std::string GetFuncID() const;
        bool IsExecutionContextSet() const;
    private:
        // signatures corresponding to that function
        std::vector<FunctionSignature> signatures = {};
        std::string funcID  = "";
        bool isExecContextSet = false;
    };
}

#endif // OMNI_RUNTIME_FUNCTION_H
