/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: Collection functions registration
 */

#include "RegistrationHelpers.h"
#include "vectorization/functions/SizeFunction.h"

namespace omniruntime::vectorization {
    using namespace omniruntime::type;

    void RegisterCollectionFunctions(const std::string &prefix)
    {
        // Register size function for arrays with boolean parameter (legacySizeOfNull)
        // Signature: size(Array<Any>, bool) -> int32_t
        // The second parameter controls NULL handling behavior:
        // - true: returns -1 for NULL input (Spark legacy behavior)
        // - false: returns NULL for NULL input (standard SQL behavior)
        VectorFunction::RegisterVectorFunction("size", {OMNI_ARRAY, OMNI_BOOLEAN}, OMNI_INT,
            std::make_shared<SizeFunction>());

        // Register size function for maps with boolean parameter (legacySizeOfNull)
        // Signature: size(Map<Any, Any>, bool) -> int32_t
        VectorFunction::RegisterVectorFunction("size", {OMNI_MAP, OMNI_BOOLEAN}, OMNI_INT,
            std::make_shared<SizeFunction>());

        // Also register without the boolean parameter for backward compatibility
        // In this case, NULL input returns NULL (standard SQL behavior)
        VectorFunction::RegisterVectorFunction("size", {OMNI_ARRAY}, OMNI_INT,
            std::make_shared<SizeFunction>());
        VectorFunction::RegisterVectorFunction("size", {OMNI_MAP}, OMNI_INT,
            std::make_shared<SizeFunction>());
    }

} // namespace omniruntime::vectorization
