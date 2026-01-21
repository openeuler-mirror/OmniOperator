/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#include "RegistrationHelpers.h"
#include "vectorization/functions/SubscriptUtil.h"
#include "vectorization/functions/Slice.h"

namespace omniruntime::vectorization {
void RegisterArrayFunctions(const std::string &prefix)
{
    VectorFunction::RegisterVectorFunction("get_array_item", {OMNI_ARRAY, OMNI_INT}, OMNI_INT,
        std::make_shared<SubscriptImpl>());
    VectorFunction::RegisterVectorFunction("get_array_item", {OMNI_ARRAY, OMNI_INT}, OMNI_VARCHAR,
        std::make_shared<SubscriptImpl>());
    VectorFunction::RegisterVectorFunction("slice", {OMNI_ARRAY, OMNI_INT, OMNI_INT}, OMNI_ARRAY,
        std::make_shared<SliceImpl>());
    VectorFunction::RegisterVectorFunction("slice", {OMNI_ARRAY, OMNI_LONG, OMNI_LONG}, OMNI_ARRAY,
        std::make_shared<SliceImpl>());
}
}
