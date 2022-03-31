/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Decimal Function Registry
 */
#ifndef OMNI_RUNTIME_FUNC_REGISTRY_DECIMAL_H
#define OMNI_RUNTIME_FUNC_REGISTRY_DECIMAL_H
#include "function.h"
#include "func_registry_base.h"

// functions called directly from codegen
const std::string decimal128CompareStr = "compare";
const std::string addDec128Str = "add";
const std::string subDec128Str = "sub";
const std::string mulDec128Str = "mul";
const std::string divDec128Str = "div";

namespace omniruntime {
class DecimalFunctionRegistry : public BaseFunctionRegistry {
public:
    std::vector<Function> GetFunctions() override;
};
}

#endif // OMNI_RUNTIME_FUNC_REGISTRY_DECIMAL_H
