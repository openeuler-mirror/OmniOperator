/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: ArraysOverlap function implementation for checking overlap between two arrays
 */

#pragma once

#include <vector>
#include <stack>
#include <unordered_set>
#include <cmath>
#include <type_traits>
#include "vectorization/VectorFunction.h"
#include "vector/vector.h"
#include "vector/array_vector.h"
#include "util/debug.h"

namespace omniruntime::vectorization {
using namespace omniruntime::type;
using namespace omniruntime::vec;
using namespace omniruntime::op;

/// ArraysOverlapFunction - Tests if two arrays have any non-null elements in common
/// arrays_overlap(array<T>, array<T>) -> boolean
/// Returns true if the two arrays have at least one non-null element in common.
/// Returns null if there are no non-null elements in common but either array contains null.
/// Returns false if neither array has common elements and neither contains null.
/// For REAL and DOUBLE, NaNs (Not-a-Number) are considered equal.
class ArraysOverlapFunction : public VectorFunction {
public:
    explicit ArraysOverlapFunction() {}

    void Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, BaseVector *&result,
        ExecutionContext *context) const override;

private:
    template <typename T>
    void ProcessArraysOverlap(ArrayVector *leftArrayVec, ArrayVector *rightArrayVec,
        BaseVector *result, int32_t rowSize) const;

    void ProcessArraysOverlapVarchar(ArrayVector *leftArrayVec, ArrayVector *rightArrayVec,
        BaseVector *result, int32_t rowSize) const;
};

} // namespace omniruntime::vectorization
