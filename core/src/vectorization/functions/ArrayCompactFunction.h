/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: ArrayCompact function implementation for removing null elements from array
 */

#pragma once

#include <vector>
#include <stack>
#include "vectorization/VectorFunction.h"
#include "vector/vector.h"
#include "vector/array_vector.h"
#include "util/debug.h"

namespace omniruntime::vectorization {
using namespace omniruntime::type;
using namespace omniruntime::vec;
using namespace omniruntime::op;

/// ArrayCompactFunction - Removes all null elements from an array
/// array_compact(array<T>) -> array<T>
/// Returns a new array with all null elements removed.
/// If input array is NULL, returns NULL.
/// If input array is empty, returns empty array.
/// If input array has no nulls, returns the original array content.
class ArrayCompactFunction : public VectorFunction {
public:
    explicit ArrayCompactFunction() {}

    void Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, BaseVector *&result,
        ExecutionContext *context) const override;
};

} // namespace omniruntime::vectorization
