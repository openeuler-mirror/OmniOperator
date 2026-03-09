/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#include <string>
#include "../functions/RLike.h"
#include "../functions/RegexpExtractAll.h"
#include "RegistrationHelpers.h"

namespace omniruntime::vectorization {
void RegisterRegexpFunctions(const std::string& prefix) {
    auto rlikeFunction = std::make_shared<RLikeFunction>();
    VectorFunction::RegisterVectorFunction("RLike", {OMNI_VARCHAR, OMNI_VARCHAR}, OMNI_BOOLEAN, rlikeFunction);

    auto regexpExtractAllFunction = std::make_shared<RegexpExtractAllFunction>();
    VectorFunction::RegisterVectorFunction(prefix + "regexp_extract_all", {OMNI_VARCHAR, OMNI_VARCHAR}, OMNI_ARRAY,
        regexpExtractAllFunction);
    VectorFunction::RegisterVectorFunction(prefix + "regexp_extract_all", {OMNI_VARCHAR, OMNI_VARCHAR, OMNI_INT},
        OMNI_ARRAY, regexpExtractAllFunction);
    VectorFunction::RegisterVectorFunction(prefix + "regexp_extract_all", {OMNI_VARCHAR, OMNI_VARCHAR, OMNI_LONG},
        OMNI_ARRAY, regexpExtractAllFunction);
    VectorFunction::RegisterVectorFunction(prefix + "regexp_extract_all", {OMNI_CHAR, OMNI_VARCHAR}, OMNI_ARRAY,
        regexpExtractAllFunction);
    VectorFunction::RegisterVectorFunction(prefix + "regexp_extract_all", {OMNI_CHAR, OMNI_VARCHAR, OMNI_INT},
        OMNI_ARRAY, regexpExtractAllFunction);
    VectorFunction::RegisterVectorFunction(prefix + "regexp_extract_all", {OMNI_CHAR, OMNI_VARCHAR, OMNI_LONG},
        OMNI_ARRAY, regexpExtractAllFunction);
}
}