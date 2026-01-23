/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: DateTime function registration
 */

#include <string>
#include "../functions/Extract.h"
#include "../functions/ToData.h"
#include "../functions/Minute.h"
#include "RegistrationHelpers.h"

namespace omniruntime::vectorization {
void RegisterDatetimeFunctions(const std::string &prefix)
{
    RegisterExtractFunction(prefix + "extract");
    RegisterToDataFunction(prefix + "to_data");
    RegisterMinuteFunction(prefix + "minute");
}
}
