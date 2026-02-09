/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: ArrayMinMax function implementation for finding max/min value in array
 */

#pragma once

#include <vector>
#include <stack>
#include <cmath>
#include <limits>
#include <type_traits>
#include "vectorization/VectorFunction.h"
#include "vector/vector.h"
#include "vector/array_vector.h"
#include "util/debug.h"

namespace omniruntime::vectorization {
using namespace omniruntime::type;
using namespace omniruntime::vec;
using namespace omniruntime::op;

/// ArrayMaxFunction - Returns the maximum value in an array
/// array_max(array<T>) -> T
/// Returns null if the array is empty or contains only null elements.
/// For floating point types, NaN is considered greater than any non-NaN value.
class ArrayMaxFunction : public VectorFunction {
public:
    explicit ArrayMaxFunction() {}

    void Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, BaseVector *&result,
        ExecutionContext *context) const override;

private:
    /// Process array max calculation for different element types
    template <typename T>
    void ProcessArrayMax(ArrayVector *arrayVec, BaseVector *result, int32_t rowSize) const;
};

/// ArrayMinFunction - Returns the minimum value in an array
/// array_min(array<T>) -> T
/// Returns null if the array is empty or contains only null elements.
/// For floating point types, NaN handling follows IEEE 754 semantics.
class ArrayMinFunction : public VectorFunction {
public:
    explicit ArrayMinFunction() {}

    void Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, BaseVector *&result,
        ExecutionContext *context) const override;

private:
    /// Process array min calculation for different element types
    template <typename T>
    void ProcessArrayMin(ArrayVector *arrayVec, BaseVector *result, int32_t rowSize) const;
};

} // namespace omniruntime::vectorization
