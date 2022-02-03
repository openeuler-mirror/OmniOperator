/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Decimal Function Registry
 */
#ifndef OMNI_RUNTIME_FUNC_REGISTRY_DECIMAL_H
#define OMNI_RUNTIME_FUNC_REGISTRY_DECIMAL_H
#include "function.h"
#include "func_registry_base.h"

// functions called directly from codegen
const std::string decimal128CompareStr = "Decimal128Compare";
const std::string addDec128Str = "Add_decimal128";
const std::string subDec128Str = "Sub_decimal128";
const std::string mulDec128Str = "Mul_decimal128";
const std::string divDec128Str = "Div_decimal128";

namespace omniruntime {
    class DecimalFunctionRegistry : public BaseFunctionRegistry {
    public:
        std::vector<Function> GetFunctions() override;
    };
}

#endif // OMNI_RUNTIME_FUNC_REGISTRY_DECIMAL_H
