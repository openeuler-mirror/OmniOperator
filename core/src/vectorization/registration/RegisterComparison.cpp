/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#include "vectorization/functions/Comparisons.h"
#include "vectorization/functions/In.h"
#include "vectorization/functions/LeastGreatest.h"
#include "vectorization/functions/ComplexTypeEqualFunction.h"

namespace omniruntime::vectorization {
void RegisterCompareFunctions(const std::string &prefix)
{
    VectorFunction::RegisterVectorFunctionFactory(ComparisonSignatures("equal"), makeEqualTo);
    VectorFunction::RegisterVectorFunctionFactory(ComparisonSignatures("greaterThan"), makeGreaterThan);
    VectorFunction::RegisterVectorFunctionFactory(ComparisonSignatures("greaterThanEqual"), makeGreaterThanOrEqual);
    VectorFunction::RegisterVectorFunctionFactory(ComparisonSignatures("lessThan"), makeLessThan);
    VectorFunction::RegisterVectorFunctionFactory(ComparisonSignatures("lessThanEqual"), makeLessThanOrEqual);
    registerEqualNullSafeFunction(prefix);
    
    // Register complex type equal functions for ARRAY and ROW types
    auto complexEqualFunction = std::make_shared<ComplexTypeEqualFunction>();
    VectorFunction::RegisterVectorFunction("equal", {OMNI_ARRAY, OMNI_ARRAY}, OMNI_BOOLEAN, complexEqualFunction);
    VectorFunction::RegisterVectorFunction("equal", {OMNI_ROW, OMNI_ROW}, OMNI_BOOLEAN, complexEqualFunction);
    
    // Register Greatest and Least functions for all supported types
    VectorFunction::RegisterVectorFunctionFactory(GreatestSignatures(), makeGreatest);
    VectorFunction::RegisterVectorFunctionFactory(LeastSignatures(), makeLeast);
    
    registerIn(prefix);
}
}
