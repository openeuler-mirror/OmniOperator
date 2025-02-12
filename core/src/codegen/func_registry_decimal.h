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
const std::string decimal64CompareStr = "Decimal64Compare";

const std::string addDec128Str = "Add_decimal128";
const std::string subDec128Str = "Sub_decimal128";
const std::string mulDec128Str = "Mul_decimal128";
const std::string divDec128Str = "Div_decimal128";
const std::string modDec128Str = "Mod_decimal128";

const std::string addDec64Str = "Add_decimal64";
const std::string subDec64Str = "Sub_decimal64";
const std::string mulDec64Str = "Mul_decimal64";
const std::string divDec64Str = "Div_decimal64";
const std::string modDec64Str = "Mod_decimal64";

constexpr const char* tryAddDecimal64FnStr = "Try_add_decimal64";
constexpr const char* tryAddDecimal128FnStr = "Try_add_decimal128";
constexpr const char* trySubDecimal64FnStr = "Try_sub_decimal64";
constexpr const char* trySubDecimal128FnStr = "Try_sub_decimal128";
constexpr const char* tryMulDecimal64FnStr = "Try_mul_decimal64";
constexpr const char* tryMulDecimal128FnStr = "Try_mul_decimal128";
constexpr const char* tryDivDecimal64FnStr = "Try_div_decimal64";
constexpr const char* tryDivDecimal128FnStr = "Try_div_decimal128";

namespace omniruntime::codegen {
class DecimalFunctionRegistry : public BaseFunctionRegistry {
public:
    std::vector<Function> GetFunctions() override;
};

class DecimalFunctionRegistryDown : public BaseFunctionRegistry {
public:
    std::vector<Function> GetFunctions() override;
};

class DecimalFunctionRegistryHalfUp : public BaseFunctionRegistry {
public:
    std::vector<Function> GetFunctions() override;
};

class DecimalFunctionRegistryNotReScale : public BaseFunctionRegistry {
public:
    std::vector<Function> GetFunctions() override;
};

class DecimalFunctionRegistryReScale : public BaseFunctionRegistry {
public:
    std::vector<Function> GetFunctions() override;
};
}

#endif // OMNI_RUNTIME_FUNC_REGISTRY_DECIMAL_H
