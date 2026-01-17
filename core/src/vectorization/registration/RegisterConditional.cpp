/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: Registration for conditional functions
 */

#include <string>
#include "../functions/Coalesce.h"
#include "RegistrationHelpers.h"

namespace omniruntime::vectorization {
void RegisterConditionalFunctions(const std::string &prefix)
{
    auto coalesceFunction = std::make_shared<CoalesceFunction>();
    
    // Register coalesce for numeric types - 2 arguments
    VectorFunction::RegisterVectorFunction("coalesce", {OMNI_INT, OMNI_INT}, OMNI_INT, coalesceFunction);
    VectorFunction::RegisterVectorFunction("coalesce", {OMNI_LONG, OMNI_LONG}, OMNI_LONG, coalesceFunction);
    VectorFunction::RegisterVectorFunction("coalesce", {OMNI_DOUBLE, OMNI_DOUBLE}, OMNI_DOUBLE, coalesceFunction);
    VectorFunction::RegisterVectorFunction("coalesce", {OMNI_FLOAT, OMNI_FLOAT}, OMNI_FLOAT, coalesceFunction);
    VectorFunction::RegisterVectorFunction("coalesce", {OMNI_SHORT, OMNI_SHORT}, OMNI_SHORT, coalesceFunction);
    VectorFunction::RegisterVectorFunction("coalesce", {OMNI_BYTE, OMNI_BYTE}, OMNI_BYTE, coalesceFunction);
    
    // Register coalesce for string types - 2 arguments
    VectorFunction::RegisterVectorFunction("coalesce", {OMNI_VARCHAR, OMNI_VARCHAR}, OMNI_VARCHAR, coalesceFunction);
    
    // Register coalesce for boolean - 2 arguments
    VectorFunction::RegisterVectorFunction("coalesce", {OMNI_BOOLEAN, OMNI_BOOLEAN}, OMNI_BOOLEAN, coalesceFunction);
    
    // Register coalesce for numeric types - 3 arguments
    VectorFunction::RegisterVectorFunction("coalesce", {OMNI_INT, OMNI_INT, OMNI_INT}, OMNI_INT, coalesceFunction);
    VectorFunction::RegisterVectorFunction("coalesce", {OMNI_LONG, OMNI_LONG, OMNI_LONG}, OMNI_LONG, coalesceFunction);
    VectorFunction::RegisterVectorFunction("coalesce", {OMNI_DOUBLE, OMNI_DOUBLE, OMNI_DOUBLE}, OMNI_DOUBLE, coalesceFunction);
    VectorFunction::RegisterVectorFunction("coalesce", {OMNI_FLOAT, OMNI_FLOAT, OMNI_FLOAT}, OMNI_FLOAT, coalesceFunction);
    
    // Register coalesce for string types - 3 arguments
    VectorFunction::RegisterVectorFunction("coalesce", {OMNI_VARCHAR, OMNI_VARCHAR, OMNI_VARCHAR}, OMNI_VARCHAR, coalesceFunction);
    
    // Register coalesce for boolean - 3 arguments
    VectorFunction::RegisterVectorFunction("coalesce", {OMNI_BOOLEAN, OMNI_BOOLEAN, OMNI_BOOLEAN}, OMNI_BOOLEAN, coalesceFunction);
}
}
