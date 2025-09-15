/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2024. All rights reserved.
 * Description: String Function Registry
 */
#ifndef OMNI_RUNTIME_FUNC_REGISTRY_STRING_H
#define OMNI_RUNTIME_FUNC_REGISTRY_STRING_H
#include "function.h"
#include "func_registry_base.h"
#include "util/type_util.h"

// functions called directly from codegen
const std::string strCompareStr = "compare";
const std::string strEqualStr = "strequal";

namespace omniruntime::codegen {
class StringFunctionRegistry : public BaseFunctionRegistry {
public:
    std::vector<Function> GetFunctions() override;
};

class StringFunctionRegistryNotAllowReducePrecison : public BaseFunctionRegistry {
public:
    std::vector<Function> GetFunctions() override;
};

class StringToDecimalFunctionRegistryAllowRoundUp : public BaseFunctionRegistry {
public:
    std::vector<Function> GetFunctions() override;
};

class StringToDecimalFunctionRegistry : public BaseFunctionRegistry {
public:
    std::vector<Function> GetFunctions() override;
};

class StringFunctionRegistryAllowReducePrecison : public BaseFunctionRegistry {
public:
    std::vector<Function> GetFunctions() override;
};

class StringFunctionRegistryNotReplace : public BaseFunctionRegistry {
public:
    std::vector<Function> GetFunctions() override;
};

class StringFunctionRegistryReplace : public BaseFunctionRegistry {
public:
    std::vector<Function> GetFunctions() override;
};

class StringFunctionRegistrySupportNegativeAndZeroIndex : public BaseFunctionRegistry {
public:
    std::vector<Function> GetFunctions() override;
};

class StringFunctionRegistrySupportNotNegativeAndZeroIndex : public BaseFunctionRegistry {
public:
    std::vector<Function> GetFunctions() override;
};

class StringFunctionRegistrySupportNegativeAndNotZeroIndex : public BaseFunctionRegistry {
public:
    std::vector<Function> GetFunctions() override;
};

class StringFunctionRegistrySupportNotNegativeAndNotZeroIndex : public BaseFunctionRegistry {
public:
    std::vector<Function> GetFunctions() override;
};
}

#endif // OMNI_RUNTIME_FUNC_REGISTRY_STRING_H
