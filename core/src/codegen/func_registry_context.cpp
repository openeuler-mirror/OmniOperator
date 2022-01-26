/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Context Helper Functions Registry
 */
#include "func_registry_context.h"
#include "functions/context_helper.h"
using namespace omniruntime;
using namespace omniruntime::expressions;

std::vector<Function> GetContextFunctionRegistry()
{
    static std::vector<Function> contextFnRegistry = {
        Function(reinterpret_cast<void*>(ArenaAllocatorMalloc), "ArenaAllocatorMalloc", {}, {INT64D, INT32D},
                 INT8PTRD, false),
        Function(reinterpret_cast<void*>(ArenaAllocatorReset), "ArenaAllocatorReset", {}, {INT64D}, VOIDD, false)
    };
    return contextFnRegistry;
}