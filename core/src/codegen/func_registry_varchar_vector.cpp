/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Varchar Vector Functions Registry
 */
#include "func_registry_varchar_vector.h"

using namespace omniruntime;
using namespace omniruntime::type;

namespace omniruntime {
std::vector<Function> VarcharVectorFunctionRegistry::GetFunctions()
{
    std::vector<DataTypeId> paramTypes = { OMNI_LONG, OMNI_INT, OMNI_VARCHAR };
    std::vector<Function> varcharVectorFnRegistry = { Function("WrapVarcharVector", "WrapVarcharVector", {}, paramTypes,
        OMNI_INT, false) };
    return varcharVectorFnRegistry;
}
}
