/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#include "vectorization/functions/Comparisons.h"
#include "vectorization/functions/In.h"

namespace omniruntime::vectorization {
void RegisterCompareFunctions(const std::string &prefix)
{
    VectorFunction::RegisterVectorFunctionFactory(ComparisonSignatures("equal"), makeEqualTo);
    VectorFunction::RegisterVectorFunctionFactory(ComparisonSignatures("greaterThan"), makeGreaterThan);
    VectorFunction::RegisterVectorFunctionFactory(ComparisonSignatures("greaterThanEqual"), makeGreaterThanOrEqual);
    VectorFunction::RegisterVectorFunctionFactory(ComparisonSignatures("lessThan"), makeLessThan);
    VectorFunction::RegisterVectorFunctionFactory(ComparisonSignatures("lessThanEqual"), makeLessThanOrEqual);
    registerIn(prefix);
}
}
