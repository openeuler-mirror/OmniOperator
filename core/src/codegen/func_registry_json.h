/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: String Function Registry
 */
#ifndef FUNC_REGISTRY_JSON_H
#define FUNC_REGISTRY_JSON_H
#include "function.h"
#include "func_registry_base.h"
#include "util/type_util.h"

namespace omniruntime::codegen {
class JsonFunctionRegistry : public BaseFunctionRegistry {
public:
    std::vector<Function> GetFunctions() override;
};
}
#endif
