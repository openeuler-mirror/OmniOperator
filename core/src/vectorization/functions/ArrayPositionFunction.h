/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: ArrayPosition function implementation for finding element position in array
 */

#pragma once

#include <vector>
#include <stack>
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

/// ArrayPositionFunction - Returns the 1-based position of an element in an array
/// array_position(array<T>, T) -> bigint
/// Returns the (1-based) index of the first occurrence of the given value in the array.
/// Returns 0 if the value is not found.
/// Returns NULL if the array is NULL or the search value is NULL.
/// NaN values are considered equal to each other (Spark semantics).
class ArrayPositionFunction : public VectorFunction {
public:
    explicit ArrayPositionFunction() {}

    void Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, BaseVector *&result,
        ExecutionContext *context) const override;

private:
    template <typename T>
    void ProcessArrayPosition(ArrayVector *arrayVec, BaseVector *searchVec,
        Vector<int64_t> *resultVec, int32_t rowSize) const;
};

} // namespace omniruntime::vectorization
