/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#include <string>
#include "../functions/RLike.h"
#include "../functions/Similar.h"
#include "../functions/RegexpExtractAll.h"
#include "../functions/RegexpReplaceFunction.h"
#include "RegistrationHelpers.h"

namespace omniruntime::vectorization {
void RegisterRegexpFunctions(const std::string& prefix) {
    auto rlikeFunction = std::make_shared<RLikeFunction>();
    VectorFunction::RegisterVectorFunction("RLike", {OMNI_VARCHAR, OMNI_VARCHAR}, OMNI_BOOLEAN, rlikeFunction);
    auto similarFunction = std::make_shared<SimilarFunction>();
    // Value column is VARCHAR while a pattern literal is CHAR; cover all VARCHAR/CHAR combos so the
    // signature lookup always hits. SimilarFunction reads both via string_view (same runtime container).
    VectorFunction::RegisterVectorFunction("similar_to", {OMNI_VARCHAR, OMNI_VARCHAR}, OMNI_BOOLEAN, similarFunction);
    VectorFunction::RegisterVectorFunction("similar_to", {OMNI_VARCHAR, OMNI_CHAR}, OMNI_BOOLEAN, similarFunction);
    VectorFunction::RegisterVectorFunction("similar_to", {OMNI_CHAR, OMNI_VARCHAR}, OMNI_BOOLEAN, similarFunction);
    VectorFunction::RegisterVectorFunction("similar_to", {OMNI_CHAR, OMNI_CHAR}, OMNI_BOOLEAN, similarFunction);
    auto regexpReplaceFunction = std::make_shared<RegexpReplaceFunction>();
    VectorFunction::RegisterVectorFunction(prefix + "regexp_replace", {OMNI_VARCHAR, OMNI_VARCHAR, OMNI_VARCHAR, OMNI_INT},
        OMNI_VARCHAR, regexpReplaceFunction);
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