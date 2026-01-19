/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: String Function Registry
 */
#include "func_registry_json.h"
#include "functions/json_functions.h"

namespace omniruntime::codegen {
using namespace omniruntime::type;
using namespace codegen::function;

const std::string GetJsonObjectFnStr()
{
    const std::string getJsonFnStr = "GetJsonObject";
    return getJsonFnStr;
}

std::vector<Function> JsonFunctionRegistry::GetFunctions()
{
    std::vector<Function> jsonFnRegistry = { Function(reinterpret_cast<void *>(GetJsonObject), GetJsonObjectFnStr(), {},
        { OMNI_VARCHAR, OMNI_VARCHAR }, OMNI_VARCHAR, INPUT_DATA_AND_NULL_AND_RETURN_NULL, true) };
    return jsonFnRegistry;
}
}