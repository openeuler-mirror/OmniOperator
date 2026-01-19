/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#pragma once
#include <string>
#include <memory>
#include <unordered_map>
#include "../SimpleFunctionMetadata.h"
#include "../VectorFunction.h"
#include "vectorization/SimpleFunction.h"

namespace omniruntime::vectorization {


class SimpleFunctionRegistry {
public:
    static std::shared_ptr<VectorFunction> Find(const FunctionSignaturePtr &signature)
    {
        return functionMap_.find(signature)->second;
    }

    static FunctionMap functionMap_;
};

template <typename UDFHolder>
class SimpleFunctionAdapterFactoryImpl : public SimpleFunctionAdapterFactory {
public:
    explicit SimpleFunctionAdapterFactoryImpl() {}

    std::unique_ptr<VectorFunction> createVectorFunction(const std::vector<DataTypeId> &inputTypes,
        const config::QueryConfig &config, const std::vector<BaseVector *> &constantInputs) const override
    {
        return std::make_unique<SimpleFunction<UDFHolder>>(inputTypes, config, constantInputs);
    }
};

template <typename T>
static std::unique_ptr<T> CreateUdf()
{
    return std::make_unique<T>();
}

// New registration function; mostly a copy from the function above, but taking
// the inner "udf" struct directly, instead of the wrapper. We can keep both for
// a while to maintain backwards compatibility, but the idea is to remove the
// one above eventually.
template <template <class> typename Func, typename TReturn, typename... TArgs>
bool RegisterFunction(const std::string &name, std::vector<DataTypeId> paramsType,
    DataTypeId returnType)
{
    using funcClass = Func<TReturn>;
    using holderClass = FunctionHolder<funcClass, TReturn, TArgs...>;
    auto signature = std::make_shared<codegen::FunctionSignature>(name, paramsType, returnType);
    const auto factory = []() { return CreateUdf<SimpleFunctionAdapterFactoryImpl<holderClass>>(); };
    VectorFunction::simpleFunctionFactoryMap_.insert(std::make_pair(signature, factory));
    return true;
}
}
