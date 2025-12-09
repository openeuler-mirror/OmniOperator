/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#pragma once
#include <string>
#include <vector>
#include <memory>
#include "vectorization/VectorFunction.h"

namespace omniruntime::vectorization {
void RegisterIsNullFunction(const std::string &name);

inline std::vector<std::shared_ptr<codegen::FunctionSignature>> IsNullSignatures(const std::string &name)
{
    std::vector<std::shared_ptr<codegen::FunctionSignature>> signatures;
    for (const auto &inputType : {
             type::OMNI_BOOLEAN,
             type::OMNI_BYTE,
             type::OMNI_INT,
             type::OMNI_LONG,
             type::OMNI_DOUBLE,
             type::OMNI_VARBINARY,
             type::OMNI_VARCHAR
         }) {
        signatures.emplace_back(
            codegen::FunctionSignatureBuilder().FuncName(name).ReturnType(type::OMNI_BOOLEAN).ArgumentType(inputType).
            Build());
    }
    return signatures;
}
}
