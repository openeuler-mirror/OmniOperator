/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Varchar Vector Functions Registry
 */
#ifndef OMNI_RUNTIME_FUNC_REGISTRY_VARCHAR_VECTOR_H
#define OMNI_RUNTIME_FUNC_REGISTRY_VARCHAR_VECTOR_H
#include "function.h"

std::vector<omniruntime::Function> GetVarcharVectorFunctionRegistry();

// functions called directly from codegen
const std::string WrapVarcharVectorStr = "WrapVarcharVector";

#endif // OMNI_RUNTIME_FUNC_REGISTRY_VARCHAR_VECTOR_H
