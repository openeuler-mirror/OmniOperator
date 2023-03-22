/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Varchar Vector Functions Registry
 */
#ifndef OMNI_RUNTIME_FUNC_REGISTRY_VARCHAR_VECTOR_H
#define OMNI_RUNTIME_FUNC_REGISTRY_VARCHAR_VECTOR_H
#include "function.h"
#include "func_registry_base.h"

// functions called directly from codegen
const std::string WrapVarcharVectorStr = "WrapVarcharVector";

namespace omniruntime::codegen {
class VarcharVectorFunctionRegistry : public BaseFunctionRegistry {
public:
    std::vector<Function> GetFunctions() override;
};
}

#endif // OMNI_RUNTIME_FUNC_REGISTRY_VARCHAR_VECTOR_H
