/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#include <string>
#include "../functions/String.h"
#include "../functions/SplitFunction.h"
#include "RegistrationHelpers.h"

namespace omniruntime::vectorization {
void RegisterStringFunctions(const std::string &prefix)
{
    RegisterString<StartsWithFunction>({prefix + "startswith"});
    VectorFunction::RegisterVectorFunction("split", {OMNI_VARCHAR, OMNI_VARCHAR, OMNI_INT}, OMNI_ARRAY,
        std::make_shared<SplitFunction>());
}
}
