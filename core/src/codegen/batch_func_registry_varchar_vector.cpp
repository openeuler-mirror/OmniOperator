/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: Batch varchar vector Function Registry
 */

#include "batch_func_registry_varchar_vector.h"
#include "batch_functions/batch_varcharVectorfunctions.h"

namespace omniruntime::codegen {
using namespace omniruntime::type;
using namespace codegen::function;

std::vector<Function> BatchVarcharVectorFunctionRegistry::GetFunctions()
{
    std::vector<DataTypeId> paramTypes = { OMNI_LONG, OMNI_VARCHAR, OMNI_INT, OMNI_INT };
    std::vector<Function> batchVarcharVectorFnRegistry = { Function(reinterpret_cast<void *>(BatchWrapVarcharVector),
        "batch_WrapVarcharVector", {}, paramTypes, OMNI_INT),
        Function(reinterpret_cast<void *>(BatchNullArrayToBits), "batch_NullArrayToBits", {}, { OMNI_BOOLEAN },
            OMNI_BOOLEAN),
        Function(reinterpret_cast<void *>(BatchBitsToNullArray), "batch_BitsToNullArray", {}, { OMNI_BOOLEAN },
            OMNI_BOOLEAN) };
    return batchVarcharVectorFnRegistry;
}
}
