/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Context Helper Functions Registry
 */
#include "func_registry_context.h"
#include "context_helper.h"

namespace omniruntime::codegen {
using namespace omniruntime::type;

std::vector<Function> ContextFunctionRegistry::GetFunctions()
{
    std::vector<Function> contextFnRegistry = { Function(reinterpret_cast<void *>(ArenaAllocatorMalloc),
        "ArenaAllocatorMalloc", {}, { OMNI_LONG, OMNI_INT }, OMNI_CHAR) };
    return contextFnRegistry;
}
}