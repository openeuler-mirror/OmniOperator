/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#include <string>
#include "../functions/RLike.h"
#include "RegistrationHelpers.h"

namespace omniruntime::vectorization {
void RegisterRegexpFunctions(const std::string& prefix) {
    auto rlikeFunction = std::make_shared<RLikeFunction>();
    VectorFunction::RegisterVectorFunction("RLike", {OMNI_VARCHAR, OMNI_VARCHAR}, OMNI_BOOLEAN, rlikeFunction);
}
}
