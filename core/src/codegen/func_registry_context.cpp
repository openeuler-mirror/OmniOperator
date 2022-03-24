/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Context Helper Functions Registry
 */
#include "func_registry_context.h"
#include "functions/context_helper.h"
using namespace omniruntime::type;
using namespace omniruntime;

std::vector<Function> ContextFunctionRegistry::GetFunctions()
{
    std::vector<Function> contextFnRegistry = { Function(reinterpret_cast<void *>(ArenaAllocatorMalloc),
        "ArenaAllocatorMalloc", {}, { OMNI_LONG, OMNI_INT }, OMNI_CHAR),
        Function(reinterpret_cast<void *>(ArenaAllocatorReset), "ArenaAllocatorReset", {}, { OMNI_LONG },
        OMNI_BOOLEAN) };
    return contextFnRegistry;
}