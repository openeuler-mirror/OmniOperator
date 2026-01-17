/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#include <string>
#include "../functions/String.h"
#include "../functions/SplitFunction.h"
#include "../functions/Cast.h"
// #include "../functions/Switch.h"
#include "../functions/EqualStringFunction.h"
#include "RegistrationHelpers.h"

namespace omniruntime::vectorization {
void RegisterStringFunctions(const std::string &prefix)
{
    RegisterString<StartsWithFunction>({prefix + "startswith"});
    RegisterString<ContainsFunction>({prefix + "Contains"});
    RegisterFunction<TrimFunction, std::string, std::string_view>(prefix + "Trim", {OMNI_VARCHAR}, OMNI_VARCHAR);
    VectorFunction::RegisterVectorFunction("split", {OMNI_VARCHAR, OMNI_VARCHAR, OMNI_INT}, OMNI_ARRAY,
        std::make_shared<SplitFunction>());
    // Note: if function is now registered in RegisterConditional.cpp
    // VectorFunction::RegisterVectorFunction("equal", {OMNI_VARCHAR, OMNI_VARCHAR}, OMNI_BOOLEAN, std::make_shared<EqualStringFunction>());
}
}
