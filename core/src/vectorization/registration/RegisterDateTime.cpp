/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: DateTime function registration
 */

#include <string>
#include "../functions/Minute.h"
#include "../functions/Second.h"
#include "../functions/Month.h"
#include "../functions/Quarter.h"
#include "../functions/Trunc.h"
#include "RegistrationHelpers.h"

namespace omniruntime::vectorization {
void RegisterDatetimeFunctions(const std::string &prefix)
{
    RegisterMinuteFunction(prefix + "minute");
    RegisterSecondFunction(prefix + "second");
    RegisterMonthFunction(prefix + "month");
    RegisterQuarterFunction(prefix + "quarter");
    // Register as "trunc_date" to match Gluten mapping (Substrait "trunc" -> "trunc_date")
    // and codegen layer registration
    RegisterTruncFunction(prefix + "trunc_date");
}
}
