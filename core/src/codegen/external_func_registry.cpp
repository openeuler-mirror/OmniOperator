/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description:registry external function
 */
#include "external_func_registry.h"

using namespace std;
using namespace omniruntime;
using namespace omniruntime::type;

namespace omniruntime {
vector<Function> ExternalFunctionRegistry::GetFunctions()
{
    std::vector<Function> externalFunctionRegistry = {
        Function("StringLength", "length", {}, { OMNI_VARCHAR }, OMNI_INT),
        Function("Increment_int32", "increment", {}, { OMNI_INT }, OMNI_INT),
        Function("Increment_int64", "increment", {}, { OMNI_LONG }, OMNI_LONG),
    };
    return externalFunctionRegistry;
}
}