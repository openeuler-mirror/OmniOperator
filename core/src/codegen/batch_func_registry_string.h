/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2024. All rights reserved.
 * Description: Batch String Function Registry
 */

#ifndef OMNI_RUNTIME_BATCH_FUNC_REGISTRY_STRING_H
#define OMNI_RUNTIME_BATCH_FUNC_REGISTRY_STRING_H
#include "function.h"
#include "func_registry_base.h"
#include "util/type_util.h"

namespace omniruntime::codegen {
class BatchStringFunctionRegistry : public BaseFunctionRegistry {
public:
    std::vector<Function> GetFunctions() override;
};

class BatchStringFunctionRegistryNotAllowReducePrecison : public BaseFunctionRegistry {
public:
    std::vector<Function> GetFunctions() override;
};

class BatchStringFunctionRegistryAllowReducePrecison : public BaseFunctionRegistry {
public:
    std::vector<Function> GetFunctions() override;
};

class BatchStringFunctionRegistryNotReplace : public BaseFunctionRegistry {
public:
    std::vector<Function> GetFunctions() override;
};

class BatchStringFunctionRegistryReplace : public BaseFunctionRegistry {
public:
    std::vector<Function> GetFunctions() override;
};

class BatchStringFunctionRegistrySupportNegativeAndZeroIndex : public BaseFunctionRegistry {
public:
    std::vector<Function> GetFunctions() override;
};

class BatchStringFunctionRegistrySupportNotNegativeAndZeroIndex : public BaseFunctionRegistry {
public:
    std::vector<Function> GetFunctions() override;
};

class BatchStringFunctionRegistrySupportNegativeAndNotZeroIndex : public BaseFunctionRegistry {
public:
    std::vector<Function> GetFunctions() override;
};

class BatchStringFunctionRegistrySupportNotNegativeAndNotZeroIndex : public BaseFunctionRegistry {
public:
    std::vector<Function> GetFunctions() override;
};

class BatchStringToDecimalFunctionRegistryAllowRoundUp : public BaseFunctionRegistry {
public:
    std::vector<Function> GetFunctions() override;
};

class BatchStringToDecimalFunctionRegistry : public BaseFunctionRegistry {
public:
    std::vector<Function> GetFunctions() override;
};
}
#endif // OMNI_RUNTIME_BATCH_FUNC_REGISTRY_STRING_H
