/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Murmur3 Hash Functions Registry
 */

#include "func_registry_hash.h"
#include "functions/murmur3_hash.h"
using namespace omniruntime;
using namespace omniruntime::expressions;

std::vector<Function> GetHashRegistry()
{
    DataType retType = INT32D;
    std::string mm3fnStr = "mm3hash";
    static std::vector<Function> hashRegistry = {
        Function(reinterpret_cast<void *>(Mm3Int32), mm3fnStr, {}, {INT32D, INT32D}, retType, true),
        Function(reinterpret_cast<void *>(Mm3Int64), mm3fnStr, {}, {INT64D, INT32D}, retType, true),
        Function(reinterpret_cast<void *>(Mm3Double), mm3fnStr, {}, {DOUBLED, INT32D}, retType, true),
        Function(reinterpret_cast<void *>(Mm3String), mm3fnStr, {}, {VARCHARD, INT32D}, retType, true)
    };
    return hashRegistry;
}
