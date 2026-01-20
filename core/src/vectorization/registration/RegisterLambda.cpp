/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#include "RegistrationHelpers.h"
#include "vectorization/functions/TransForm.h"

namespace omniruntime::vectorization {
    void RegisterLambdaFunctions(const std::string &prefix)
    {
        VectorFunction::RegisterVectorFunction("transform", {OMNI_ARRAY, OMNI_INT}, OMNI_ARRAY,
            std::make_shared<TransformVectorFunction>());
        VectorFunction::RegisterVectorFunction("transform", {OMNI_ARRAY, OMNI_LONG}, OMNI_ARRAY,
            std::make_shared<TransformVectorFunction>());
        VectorFunction::RegisterVectorFunction("transform", {OMNI_ARRAY, OMNI_DOUBLE}, OMNI_ARRAY,
            std::make_shared<TransformVectorFunction>());
        VectorFunction::RegisterVectorFunction("transform", {OMNI_ARRAY, OMNI_BOOLEAN}, OMNI_ARRAY,
            std::make_shared<TransformVectorFunction>());
        VectorFunction::RegisterVectorFunction("transform", {OMNI_ARRAY, OMNI_SHORT}, OMNI_ARRAY,
            std::make_shared<TransformVectorFunction>());
        VectorFunction::RegisterVectorFunction("transform", {OMNI_ARRAY, OMNI_VARCHAR}, OMNI_ARRAY,
            std::make_shared<TransformVectorFunction>());
        VectorFunction::RegisterVectorFunction("transform", {OMNI_ARRAY, OMNI_CHAR}, OMNI_ARRAY,
            std::make_shared<TransformVectorFunction>());
        VectorFunction::RegisterVectorFunction("transform", {OMNI_ARRAY, OMNI_BYTE}, OMNI_ARRAY,
            std::make_shared<TransformVectorFunction>());
        VectorFunction::RegisterVectorFunction("transform", {OMNI_ARRAY, OMNI_FLOAT}, OMNI_ARRAY,
            std::make_shared<TransformVectorFunction>());
    }
}
