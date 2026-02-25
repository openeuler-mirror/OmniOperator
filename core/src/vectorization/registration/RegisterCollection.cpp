/*
* Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: Collection functions registration
 */

#include "RegistrationHelpers.h"
#include "vectorization/functions/SizeFunction.h"
#include "vectorization/functions/CardinalityFunction.h"

namespace omniruntime::vectorization {
    using namespace omniruntime::type;

    void RegisterCollectionFunctions(const std::string &prefix)
    {
        VectorFunction::RegisterVectorFunction("size", {OMNI_ARRAY, OMNI_BOOLEAN}, OMNI_INT,
            std::make_shared<SizeFunction>());

        VectorFunction::RegisterVectorFunction("size", {OMNI_MAP, OMNI_BOOLEAN}, OMNI_INT,
            std::make_shared<SizeFunction>());

        VectorFunction::RegisterVectorFunction("size", {OMNI_ARRAY}, OMNI_INT,
            std::make_shared<SizeFunction>());
        VectorFunction::RegisterVectorFunction("size", {OMNI_MAP}, OMNI_INT,
            std::make_shared<SizeFunction>());

        VectorFunction::RegisterVectorFunction("cardinality", {OMNI_ARRAY}, OMNI_LONG,
            std::make_shared<CardinalityFunction>());
        VectorFunction::RegisterVectorFunction("cardinality", {OMNI_MAP}, OMNI_LONG,
            std::make_shared<CardinalityFunction>());
    }

} // namespace omniruntime::vectorization
