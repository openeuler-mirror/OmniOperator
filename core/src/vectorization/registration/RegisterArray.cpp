/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#include "RegistrationHelpers.h"
#include "vectorization/functions/SubscriptUtil.h"
#include "vectorization/functions/Slice.h"
#include "vectorization/functions/SizeFunction.h"
#include "vectorization/functions/ReverseFunction.h"

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

    // Register size function for Array type (Collection Functions)
    // Note: SizeFunction handles both Array and Map types with proper legacySizeOfNull support
    VectorFunction::RegisterVectorFunction("size", {OMNI_ARRAY, OMNI_BOOLEAN}, OMNI_INT,
        std::make_shared<SizeFunction>());

    // Register reverse function for Array type
    // reverse(array<T>) -> array<T>: Returns the array with elements in reverse order
    VectorFunction::RegisterVectorFunction("reverse", {OMNI_ARRAY}, OMNI_ARRAY,
        std::make_shared<ReverseFunction>());
}
}
