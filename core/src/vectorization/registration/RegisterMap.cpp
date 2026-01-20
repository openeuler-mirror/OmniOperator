/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#include "vectorization/functions/MapSize.h"
#include "vectorization/functions/SubscriptUtil.h"
#include "RegistrationHelpers.h"
#include "vectorization/functions/Size.h"
#include "vectorization/functions/MapFromArraysFunction.h"

namespace omniruntime::vectorization {
void RegisterMapFunctions(const std::string &prefix)
{
    VectorFunction::RegisterVectorFunction("size", {OMNI_MAP, OMNI_BOOLEAN}, OMNI_INT,
        std::make_shared<MapSizeFunction>());
    VectorFunction::RegisterVectorFunction("element_at", {OMNI_MAP, OMNI_VARCHAR}, OMNI_VARCHAR,
        std::make_shared<SubscriptImpl>());
    registerSize(prefix + "size");
    VectorFunction::RegisterVectorFunction("map_from_arrays", {OMNI_ARRAY, OMNI_ARRAY}, OMNI_MAP,
        std::make_shared<MapFromArraysFunction>());
}
}
