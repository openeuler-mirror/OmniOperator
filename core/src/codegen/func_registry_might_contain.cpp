/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 * Description: MightContain Function Registry
 */
#include "func_registry_might_contain.h"
#include "functions/mightcontain.h"

namespace omniruntime::codegen {
using namespace omniruntime::type;
using namespace omniruntime::codegen::function;

std::vector<Function> MightContainFunctionRegistry::GetFunctions()
{
    DataTypeId retTypeBoolean = OMNI_BOOLEAN;
    std::string mightContainFnStr = "might_contain";
    std::vector<Function> mightContainRegistry = { // insert native function for might contain function
        Function(reinterpret_cast<void *>(MightContain), mightContainFnStr, {}, { OMNI_LONG, OMNI_LONG },
            retTypeBoolean, INPUT_DATA)
    };

    return mightContainRegistry;
}
}