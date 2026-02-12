/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
#include <string>
#include "../functions/Crc32Function.h"
#include "RegistrationHelpers.h"

namespace omniruntime::vectorization {
void RegisterHashFunctions(const std::string &prefix)
{
    RegisterFunction<CRC32Function, int64_t, std::string_view>(prefix + "crc32", {OMNI_VARBINARY}, OMNI_LONG);
}
}
