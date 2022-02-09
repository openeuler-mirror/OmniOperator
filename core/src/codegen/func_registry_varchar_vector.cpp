/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Varchar Vector Functions Registry
 */
#include "func_registry_varchar_vector.h"
#include "functions/varcharVectorfunctions.h"
using namespace omniruntime;
using namespace omniruntime::expressions;

std::vector<Function> GetVarcharVectorFunctionRegistry()
{
    std::vector<DataType> paramTypes = { INT8PTRD, INT32D, INT8PTRD, INT32D };
    static std::vector<Function> varcharVectorFnRegistry = {
        Function(reinterpret_cast<void *>(WrapVarcharVector), "WrapVarcharVector", {}, paramTypes, VOIDD, false)
    };
    return varcharVectorFnRegistry;
}
