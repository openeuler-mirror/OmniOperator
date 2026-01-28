/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#include "vectorization/functions/MapSize.h"
#include "vectorization/functions/SubscriptUtil.h"
#include "RegistrationHelpers.h"
#include "vectorization/functions/Size.h"
#include "vectorization/functions/MapFromArraysFunction.h"
#include "vectorization/functions/MapKeysAndValues.h"

namespace omniruntime::vectorization {

void RegisterMapFunctions(const std::string &prefix)
{
    VectorFunction::RegisterVectorFunction("size", {OMNI_MAP, OMNI_BOOLEAN}, OMNI_INT,
        std::make_shared<MapSizeFunction>());

    std::vector<DataTypeId> supportValueTypes = {OMNI_BOOLEAN, OMNI_BYTE, OMNI_SHORT, OMNI_INT, OMNI_LONG, OMNI_FLOAT,
        OMNI_DOUBLE, OMNI_VARCHAR,  OMNI_DATE32, OMNI_DATE64, OMNI_TIME32, OMNI_TIME64, OMNI_TIMESTAMP, OMNI_DECIMAL64, OMNI_DECIMAL128};

    for (auto &valueType : supportValueTypes) {
        VectorFunction::RegisterVectorFunction("element_at", {OMNI_MAP, OMNI_VARCHAR}, valueType,
        std::make_shared<SubscriptImpl>());
    }

    registerSize(prefix + "size");
    VectorFunction::RegisterVectorFunction("map_from_arrays", {OMNI_ARRAY, OMNI_ARRAY}, OMNI_MAP,
        std::make_shared<MapFromArraysFunction>());
    VectorFunction::RegisterVectorFunction("map_keys", {OMNI_MAP}, OMNI_ARRAY,
        std::make_shared<MapKeysAndValuesFunction<true>>());
    VectorFunction::RegisterVectorFunction("map_values", {OMNI_MAP}, OMNI_ARRAY,
        std::make_shared<MapKeysAndValuesFunction<false>>());
}
}
