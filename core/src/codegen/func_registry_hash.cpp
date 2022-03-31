/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Murmur3 Hash Functions Registry
 */

#include "func_registry_hash.h"

using namespace omniruntime;
using namespace omniruntime::type;

std::vector<Function> HashFunctionRegistry::GetFunctions()
{
    DataTypeId retType = OMNI_INT;
    std::vector<Function> hashRegistry = {
        Function("Mm3Int32", "mm3hash", {}, {OMNI_INT, OMNI_INT}, retType),
        Function("Mm3Int64", "mm3hash", {}, {OMNI_LONG, OMNI_INT}, retType),
        Function("Mm3Double", "mm3hash", {}, {OMNI_DOUBLE, OMNI_INT}, retType),
        Function("Mm3String", "mm3hash", {}, {OMNI_VARCHAR, OMNI_INT}, retType)
    };
    return hashRegistry;
}
