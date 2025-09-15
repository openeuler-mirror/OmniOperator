/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Varchar Vector Functions Registry
 */
#include "func_registry_varchar_vector.h"
#include "functions/varcharVectorfunctions.h"

namespace omniruntime::codegen {
using namespace omniruntime::type;
using namespace codegen::function;

std::vector<Function> VarcharVectorFunctionRegistry::GetFunctions()
{
    std::vector<DataTypeId> paramTypes = { OMNI_LONG, OMNI_INT, OMNI_VARCHAR };
    std::vector<Function> varcharVectorFnRegistry = { Function(reinterpret_cast<void *>(WrapVarcharVector),
        "WrapVarcharVector", {}, paramTypes, OMNI_INT),
        Function(reinterpret_cast<void *>(WrapSetBitNull), "WrapSetBitNull", {}, { OMNI_INT }, OMNI_BOOLEAN),
        Function(reinterpret_cast<void *>(WrapIsBitNull), "WrapIsBitNull", {}, { OMNI_INT }, OMNI_BOOLEAN) };
    return varcharVectorFnRegistry;
}
}
