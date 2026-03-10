/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: ArrayIntersect function implementation
 */

#pragma once
#include "vectorization/VectorFunction.h"
#include "vector/array_vector.h"
#include "vector/vector.h"
#include "vector/vector_helper.h"

namespace omniruntime::vectorization {
using namespace omniruntime::type;
using namespace omniruntime::vec;
using namespace omniruntime::op;

/// array_intersect function implementation for arrays.
/// array_intersect(array(T), array(T)) -> array(T)
/// Returns an array of elements in the intersection of two arrays, without duplicates.
/// NULL elements are included if present in both arrays (at most one NULL in result).
/// The order of elements in the result follows the left-hand side array.
/// Supports: BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, BOOLEAN,
/// VARCHAR, VARBINARY, DECIMAL64, DECIMAL128, DATE32, TIMESTAMP.
/// Float/double NaN values are treated as equal (NaN == NaN).
class ArrayIntersectImpl : public VectorFunction {
public:
    explicit ArrayIntersectImpl() {}

    void Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, BaseVector *&result,
        ExecutionContext *context) const override;
};
} // namespace omniruntime::vectorization
