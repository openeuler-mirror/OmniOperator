/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

#include <string>
#include "../functions/IsNanFunction.h"
#include "RegistrationHelpers.h"

namespace omniruntime::vectorization {
void RegisterPredicateFunctions(const std::string &prefix)
{
    auto isNanFunction = std::make_shared<IsNanFunction>();
    VectorFunction::RegisterVectorFunction(prefix + "isnan", {OMNI_FLOAT}, OMNI_BOOLEAN, isNanFunction);
    VectorFunction::RegisterVectorFunction(prefix + "isnan", {OMNI_DOUBLE}, OMNI_BOOLEAN, isNanFunction);
}
}
