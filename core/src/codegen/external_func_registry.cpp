/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description:registry external function
 */
#include "external_func_registry.h"
#include "functions/externalfunctions.h"
#include "util/type_util.h"

using namespace std;
using namespace omniruntime;
using namespace omniruntime::type;
using namespace omniruntime::codegen;

vector<Function> ExternalFunctionRegistry::GetFunctions()
{
    std::vector<Function> externalFunctionRegistry = {
        Function(reinterpret_cast<void *>(StringLength), "length", {}, { OMNI_VARCHAR }, OMNI_INT),
        Function(reinterpret_cast<void *>(Increment<int32_t>), "Increment", {}, { OMNI_INT }, OMNI_INT),
        Function(reinterpret_cast<void *>(Increment<int64_t>), "Increment", {}, { OMNI_LONG }, OMNI_LONG),
    };
    return externalFunctionRegistry;
}