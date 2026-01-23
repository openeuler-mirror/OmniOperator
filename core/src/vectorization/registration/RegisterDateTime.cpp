/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: DateTime function registration
 */

#include <string>
#include "../functions/Minute.h"
#include "RegistrationHelpers.h"

namespace omniruntime::vectorization {
void RegisterDatetimeFunctions(const std::string &prefix)
{
    RegisterMinuteFunction(prefix + "minute");
}
}
