/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: Registration for conditional functions
 */

#include <string>
#include "../functions/Coalesce.h"
#include "../functions/If.h"
#include "../functions/Nanvl.h"
#include "RegistrationHelpers.h"

namespace omniruntime::vectorization {
void RegisterConditionalFunctions(const std::string &prefix)
{
    // Register if for numeric types
    auto ifFunction = std::make_shared<IfFunction>();
    VectorFunction::RegisterVectorFunction("if", {OMNI_BOOLEAN, OMNI_BOOLEAN, OMNI_BOOLEAN}, OMNI_BOOLEAN, ifFunction);
    VectorFunction::RegisterVectorFunction("if", {OMNI_BOOLEAN, OMNI_BYTE, OMNI_BYTE}, OMNI_BYTE, ifFunction);
    VectorFunction::RegisterVectorFunction("if", {OMNI_BOOLEAN, OMNI_SHORT, OMNI_SHORT}, OMNI_SHORT, ifFunction);
    VectorFunction::RegisterVectorFunction("if", {OMNI_BOOLEAN, OMNI_INT, OMNI_INT}, OMNI_INT, ifFunction);
    VectorFunction::RegisterVectorFunction("if", {OMNI_BOOLEAN, OMNI_LONG, OMNI_LONG}, OMNI_LONG, ifFunction);
    VectorFunction::RegisterVectorFunction("if", {OMNI_BOOLEAN, OMNI_FLOAT, OMNI_FLOAT}, OMNI_FLOAT, ifFunction);
    VectorFunction::RegisterVectorFunction("if", {OMNI_BOOLEAN, OMNI_DOUBLE, OMNI_DOUBLE}, OMNI_DOUBLE, ifFunction);
    VectorFunction::RegisterVectorFunction("if", {OMNI_BOOLEAN, OMNI_VARCHAR, OMNI_VARCHAR}, OMNI_VARCHAR, ifFunction);
    VectorFunction::RegisterVectorFunction("if", {OMNI_BOOLEAN, OMNI_DATE32, OMNI_DATE32}, OMNI_DATE32, ifFunction);
    VectorFunction::RegisterVectorFunction("if", {OMNI_BOOLEAN, OMNI_DATE64, OMNI_DATE64}, OMNI_DATE64, ifFunction);
    VectorFunction::RegisterVectorFunction("if", {OMNI_BOOLEAN, OMNI_TIMESTAMP, OMNI_TIMESTAMP}, OMNI_TIMESTAMP, ifFunction);
    VectorFunction::RegisterVectorFunction("if", {OMNI_BOOLEAN, OMNI_DECIMAL64, OMNI_DECIMAL64}, OMNI_DECIMAL64, ifFunction);
    VectorFunction::RegisterVectorFunction("if", {OMNI_BOOLEAN, OMNI_DECIMAL128, OMNI_DECIMAL128}, OMNI_DECIMAL128, ifFunction);
    VectorFunction::RegisterVectorFunction("if", {OMNI_BOOLEAN, OMNI_VARBINARY, OMNI_VARBINARY}, OMNI_VARBINARY, ifFunction);
    VectorFunction::RegisterVectorFunction("if", {OMNI_BOOLEAN, OMNI_ARRAY, OMNI_ARRAY}, OMNI_ARRAY, ifFunction);
    VectorFunction::RegisterVectorFunction("if", {OMNI_BOOLEAN, OMNI_MAP, OMNI_MAP}, OMNI_MAP, ifFunction);
    VectorFunction::RegisterVectorFunction("if", {OMNI_BOOLEAN, OMNI_ROW, OMNI_ROW}, OMNI_ROW, ifFunction);

    // Register coalesce - 2 arguments
    auto coalesceFunction = std::make_shared<CoalesceFunction>();
    VectorFunction::RegisterVectorFunction("coalesce", {OMNI_BOOLEAN, OMNI_BOOLEAN}, OMNI_BOOLEAN, coalesceFunction);
    VectorFunction::RegisterVectorFunction("coalesce", {OMNI_BYTE, OMNI_BYTE}, OMNI_BYTE, coalesceFunction);
    VectorFunction::RegisterVectorFunction("coalesce", {OMNI_SHORT, OMNI_SHORT}, OMNI_SHORT, coalesceFunction);
    VectorFunction::RegisterVectorFunction("coalesce", {OMNI_INT, OMNI_INT}, OMNI_INT, coalesceFunction);
    VectorFunction::RegisterVectorFunction("coalesce", {OMNI_LONG, OMNI_LONG}, OMNI_LONG, coalesceFunction);
    VectorFunction::RegisterVectorFunction("coalesce", {OMNI_FLOAT, OMNI_FLOAT}, OMNI_FLOAT, coalesceFunction);
    VectorFunction::RegisterVectorFunction("coalesce", {OMNI_DOUBLE, OMNI_DOUBLE}, OMNI_DOUBLE, coalesceFunction);
    VectorFunction::RegisterVectorFunction("coalesce", {OMNI_VARCHAR, OMNI_VARCHAR}, OMNI_VARCHAR, coalesceFunction);
    VectorFunction::RegisterVectorFunction("coalesce", {OMNI_DATE32, OMNI_DATE32}, OMNI_DATE32, coalesceFunction);
    VectorFunction::RegisterVectorFunction("coalesce", {OMNI_DATE64, OMNI_DATE64}, OMNI_DATE64, coalesceFunction);
    VectorFunction::RegisterVectorFunction("coalesce", {OMNI_TIMESTAMP, OMNI_TIMESTAMP}, OMNI_TIMESTAMP, coalesceFunction);
    VectorFunction::RegisterVectorFunction("coalesce", {OMNI_DECIMAL64, OMNI_DECIMAL64}, OMNI_DECIMAL64, coalesceFunction);

    // Register nanvl - conditional function for NaN handling (float and double only)
    auto nanvlFunction = std::make_shared<NanvlFunction>();
    VectorFunction::RegisterVectorFunction("nanvl", {OMNI_FLOAT, OMNI_FLOAT}, OMNI_FLOAT, nanvlFunction);
    VectorFunction::RegisterVectorFunction("nanvl", {OMNI_DOUBLE, OMNI_DOUBLE}, OMNI_DOUBLE, nanvlFunction);
}
}
