/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: Batch varchar vector Function Registry
 */

#include "batch_func_registry_varchar_vector.h"
#include "batch_functions/batch_varcharVectorfunctions.h"
using namespace omniruntime;
using namespace omniruntime::type;

std::vector<Function> BatchVarcharVectorFunctionRegistry::GetFunctions()
{
    std::vector<DataTypeId> paramTypes = { OMNI_LONG, OMNI_VARCHAR, OMNI_INT, OMNI_INT };
    std::vector<Function> batchVarcharVectorFnRegistry = { Function(reinterpret_cast<void *>(BatchWrapVarcharVector),
        "batch_WrapVarcharVector", {}, paramTypes, OMNI_INT) };
    return batchVarcharVectorFnRegistry;
}
