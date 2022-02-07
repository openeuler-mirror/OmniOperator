/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: String Function Registry
 */
#ifndef OMNI_RUNTIME_FUNC_REGISTRY_STRING_H
#define OMNI_RUNTIME_FUNC_REGISTRY_STRING_H
#include "function.h"
#include "util/type_util.h"

std::vector<omniruntime::Function> GetStringFunctionRegistry();
std::vector<omniruntime::Function> GetStringCmpFn();

// functions called directly from codegen
const std::string mm3hashStr = "mm3hash";
const std::string strCompareExtStr = "StrCompareExt";

#endif // OMNI_RUNTIME_FUNC_REGISTRY_STRING_H
