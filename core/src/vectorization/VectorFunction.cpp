/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#include "VectorFunction.h"

namespace omniruntime::vectorization {
using namespace type;
FunctionMap VectorFunction::functionMap_ = nullptr;
FunctionFactoryMap VectorFunction::functionFactoryMap_ = nullptr;

bool VectorFunction::RegisterVectorFunction(const std::string &name, const std::vector<DataTypeId> &paramsType,
    DataTypeId returnType, const std::shared_ptr<VectorFunction> &func)
{
    auto signature = std::make_shared<FunctionSignature>(name, paramsType, returnType);
    functionMap_->insert(std::make_pair(signature, func));
    return true;
}

bool VectorFunction::RegisterVectorFunctionFactory(const std::string &name, const std::vector<DataTypeId> &paramsType,
    DataTypeId returnType, const VectorFunctionFactory &factory)
{
    auto signature = std::make_shared<FunctionSignature>(name, paramsType, returnType);
    functionFactoryMap_->insert(std::make_pair(signature, factory));
    return true;
}

bool VectorFunction::RegisterVectorFunctionFactory(std::vector<std::shared_ptr<FunctionSignature>> functionSignatures,
    const VectorFunctionFactory &factory)
{
    for (const auto &signature : functionSignatures) {
        functionFactoryMap_->insert(std::make_pair(signature, factory));
    }
    return true;
}
}
