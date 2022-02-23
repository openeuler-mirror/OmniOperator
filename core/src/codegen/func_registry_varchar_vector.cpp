/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Varchar Vector Functions Registry
 */
#include "func_registry_varchar_vector.h"
#include "functions/varcharVectorfunctions.h"
using namespace omniruntime;
using namespace omniruntime::vec;

std::vector<Function> VarcharVectorFunctionRegistry::GetFunctions()
{
    std::vector<VecTypeId> paramTypes = { OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_INT, OMNI_VEC_TYPE_VARCHAR };
    std::vector<Function> varcharVectorFnRegistry = {
            Function(reinterpret_cast<void *>(WrapVarcharVector), "WrapVarcharVector", {}, paramTypes,
                     OMNI_VEC_TYPE_INT, false)
    };
    return varcharVectorFnRegistry;
}
