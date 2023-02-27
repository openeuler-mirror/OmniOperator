/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: String Function Registry
 */
#ifndef OMNI_RUNTIME_FUNC_REGISTRY_STRING_H
#define OMNI_RUNTIME_FUNC_REGISTRY_STRING_H
#include "function.h"
#include "func_registry_base.h"
#include "util/type_util.h"

// functions called directly from codegen
const std::string mm3hashStr = "mm3hash";
const std::string strCompareStr = "compare";

namespace omniruntime {
class StringFunctionRegistry : public BaseFunctionRegistry {
public:
    std::vector<Function> GetFunctions() override;
};

class StringFunctionRegistryNotAllowReducePrecison : public BaseFunctionRegistry {
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

class StringFunctionRegistryReplaceEmptyString : public BaseFunctionRegistry {
public:
    std::vector<Function> GetFunctions() override;
};

class StringFunctionRegistryReplaceInterceptFromBeyond : public BaseFunctionRegistry {
public:
    std::vector<Function> GetFunctions() override;
};
}

#endif // OMNI_RUNTIME_FUNC_REGISTRY_STRING_H
