/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#include <string>
#include "../functions/String.h"
#include "../functions/SplitFunction.h"
#include "../functions/Cast.h"
#include "../functions/If.h"
#include "RegistrationHelpers.h"

namespace omniruntime::vectorization {
void RegisterStringFunctions(const std::string &prefix)
{
    RegisterString<StartsWithFunction>({prefix + "startswith"});
    VectorFunction::RegisterVectorFunction("split", {OMNI_VARCHAR, OMNI_VARCHAR, OMNI_INT}, OMNI_ARRAY,
        std::make_shared<SplitFunction>());
    //todo cast and if expr datatype need to be expanded
    VectorFunction::RegisterVectorFunction("CAST", {OMNI_VARCHAR}, OMNI_LONG, std::make_shared<CastFunction>());
    VectorFunction::RegisterVectorFunction("CAST", {OMNI_VARCHAR}, OMNI_INT, std::make_shared<CastFunction>());
    VectorFunction::RegisterVectorFunction("if", {OMNI_BOOLEAN, OMNI_VARCHAR, OMNI_VARCHAR}, OMNI_VARCHAR, std::make_shared<IfFunction>());
}
}
