/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#include "vectorization/functions/MapSize.h"
#include "RegistrationHelpers.h"

namespace omniruntime::vectorization {
    void registerMapFunctions(const std::string &prefix)
    {
        VectorFunction::RegisterVectorFunction(
                "map_size",
                {OMNI_MAP},
                OMNI_INT,
                std::make_shared<MapSizeFunction>()
        );
    }
}