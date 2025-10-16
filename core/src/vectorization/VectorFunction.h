/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#pragma once

#include <stack>
#include "vector/vector.h"
#include "operator/execution_context.h"
#include "vector/vector_helper.h"
#include "codegen/func_signature.h"
#include "type/data_type.h"

namespace omniruntime::vectorization {
using namespace codegen;
using namespace type;

using FunctionSignaturePtr = std::shared_ptr<FunctionSignature>;

struct Hash {
    std::size_t operator ()(const FunctionSignaturePtr &signature) const
    {
        return signature->HashCode();
    }
};

struct Equals {
    bool operator ()(FunctionSignaturePtr s1, const FunctionSignaturePtr s2) const
    {
        return *s1 == *s2;
    }
};

class VectorFunction;
using FunctionMap = std::unique_ptr<std::unordered_map<FunctionSignaturePtr, std::shared_ptr<VectorFunction>, Hash,
    Equals>>;

using VectorPtr = vec::BaseVector *;

class VectorFunction {
public:
    virtual ~VectorFunction() = default;

    virtual void apply(std::stack<VectorPtr> &args, const type::DataTypePtr &outputType, vec::BaseVector *result,
        op::ExecutionContext *context) const = 0;

    /// Registers stateless VectorFunction. The same instance will be used for all
    /// expressions.
    /// Returns true iff an new function is inserted
    static bool RegisterVectorFunction(const std::string &name, std::vector<DataTypeId> paramsType,
        DataTypeId returnType, std::shared_ptr<VectorFunction> func);

    static std::shared_ptr<VectorFunction> Find(const FunctionSignaturePtr &signature)
    {
        auto res = functionMap_->find(signature);
        if (res == functionMap_->end()) {
            return nullptr;
        }
        return res->second;
    }

    static FunctionMap functionMap_;
};
}
