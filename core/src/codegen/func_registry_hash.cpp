/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Murmur3 Hash Functions Registry
 */

#include "func_registry_hash.h"
#include "functions/murmur3_hash.h"
using namespace omniruntime;
using namespace omniruntime::type;
using namespace omniruntime::codegen;

std::vector<Function> HashFunctionRegistry::GetFunctions()
{
    DataTypeId retType = OMNI_INT;
    std::string mm3fnStr = "mm3hash";
    std::vector<Function> hashRegistry = { Function(reinterpret_cast<void *>(Mm3Int32), mm3fnStr, {},
        { OMNI_INT, OMNI_INT }, retType),
        Function(reinterpret_cast<void *>(Mm3Int64), mm3fnStr, {}, { OMNI_LONG, OMNI_INT }, retType),
        Function(reinterpret_cast<void *>(Mm3Double), mm3fnStr, {}, { OMNI_DOUBLE, OMNI_INT }, retType),
        Function(reinterpret_cast<void *>(Mm3String), mm3fnStr, {}, { OMNI_VARCHAR, OMNI_INT }, retType),
        Function(reinterpret_cast<void *>(Mm3Decimal64), mm3fnStr, {}, { OMNI_DECIMAL64, OMNI_INT }, retType),
        Function(reinterpret_cast<void *>(Mm3Decimal128), mm3fnStr, {}, { OMNI_DECIMAL128, OMNI_INT }, retType) };
    return hashRegistry;
}
