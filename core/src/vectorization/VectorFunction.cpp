/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#include "VectorFunction.h"

namespace omniruntime::vectorization {
using namespace type;
FunctionMap VectorFunction::functionMap_ = nullptr;

bool VectorFunction::RegisterVectorFunction(const std::string &name, std::vector<DataTypeId> paramsType,
    DataTypeId returnType, std::shared_ptr<VectorFunction> func)
{
    auto signature = std::make_shared<FunctionSignature>(name, paramsType, returnType);
    functionMap_->insert(std::make_pair(signature, func));
    return true;
}
}
