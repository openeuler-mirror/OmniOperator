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
#include "util/config/QueryConfig.h"

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

using VectorFunctionFactory = std::function<std::shared_ptr<VectorFunction>(const std::string &name,
    const std::vector<DataTypeId> &inputArgs, const config::QueryConfig &config)>;

using FunctionFactoryMap = std::unique_ptr<std::unordered_map<FunctionSignaturePtr, VectorFunctionFactory, Hash,
    Equals>>;

using VectorPtr = vec::BaseVector *;

class VectorFunction {
public:
    virtual ~VectorFunction() = default;

    /// Returns true if (1) supports evaluation on all constant inputs of size >
    /// 1; (2) returns flat or constant result when inputs are all flat, all
    /// constant or a mix of flat and constant; (3) guarantees that if all inputs
    /// are not null, the result is also not null.
    virtual bool supportsFlatNoNullsFastPath() const
    {
        return false;
    }

    virtual void apply(std::stack<VectorPtr> &args, const type::DataTypePtr &outputType, vec::BaseVector *&result,
        op::ExecutionContext *context) const = 0;

    /// Registers stateless VectorFunction. The same instance will be used for all
    /// expressions.
    /// Returns true iff an new function is inserted
    static bool RegisterVectorFunction(const std::string &name, const std::vector<DataTypeId> &paramsType,
        DataTypeId returnType, const std::shared_ptr<VectorFunction> &func);

    static bool RegisterVectorFunctionFactory(const std::string &name, const std::vector<DataTypeId> &paramsType,
        DataTypeId returnType, const VectorFunctionFactory &factory);

    static bool RegisterVectorFunctionFactory(std::vector<std::shared_ptr<FunctionSignature>> functionSignatures,
        const VectorFunctionFactory &factory);

    static std::shared_ptr<VectorFunction> Find(const FunctionSignaturePtr &signature,
        const config::QueryConfig &config = config::QueryConfig())
    {
        auto it = functionMap_->find(signature);
        if (it != functionMap_->end()) {
            return it->second;
        }
        auto factory = functionFactoryMap_->find(signature);
        if (factory != functionFactoryMap_->end()) {
            return factory->second(signature->GetName(), signature->GetParams(), config);
        }
        return nullptr;
    }

    static FunctionMap functionMap_;
    static FunctionFactoryMap functionFactoryMap_;
};
}
