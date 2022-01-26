/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Decimal Function Registry
 */
#ifndef OMNI_RUNTIME_FUNC_REGISTRY_DECIMAL_H
#define OMNI_RUNTIME_FUNC_REGISTRY_DECIMAL_H
#include "function.h"

std::vector<omniruntime::Function> GetDecimalFunctionRegistry();

// functions called directly from codegen
const std::string decimal128CompareExtStr = "Decimal128CompareExt";
const std::string addDec128Str = "Add_decimal128";
const std::string subDec128Str = "Sub_decimal128";
const std::string mulDec128Str = "Mul_decimal128";
const std::string divDec128Str = "Div_decimal128";

#endif // OMNI_RUNTIME_FUNC_REGISTRY_DECIMAL_H
