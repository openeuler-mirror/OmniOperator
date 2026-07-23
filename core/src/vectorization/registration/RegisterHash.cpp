/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
#include <string>
#include "../functions/Crc32Function.h"
#include "../functions/HashFunctions.h"
#include "RegistrationHelpers.h"

namespace omniruntime::vectorization {
void RegisterHashFunctions(const std::string &prefix)
{
    RegisterFunction<CRC32Function, int64_t, std::string_view>(prefix + "crc32", {OMNI_VARBINARY}, OMNI_LONG);
    // hash
    RegisterMurMur3HashFunction(prefix + "mm3hash");
    // xxhash64
    RegisterXxHash64Function(prefix + "xxhash64");
    // bloom filter
    RegisterMightContainFunction(prefix + "might_contain");
}
}