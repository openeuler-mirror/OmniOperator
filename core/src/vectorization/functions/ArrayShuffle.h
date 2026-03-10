/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: ArrayShuffle function implementation
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

/// shuffle function implementation for arrays.
/// shuffle(array(T)) -> array(T)
/// shuffle(array(T), seed) -> array(T)
/// Returns the array with elements in random order.
/// When a seed is provided (Spark behavior), the shuffle is deterministic
/// with the given seed value. When no seed is provided, a random seed is used.
/// If the input array is null, returns null. Null elements within the array
/// are preserved in the shuffled output.
/// Supports all element types: BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, BOOLEAN,
/// VARCHAR, CHAR, VARBINARY, DECIMAL64, DECIMAL128, DATE32, TIMESTAMP.
class ArrayShuffleImpl : public VectorFunction {
public:
    explicit ArrayShuffleImpl() {}

    void Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, BaseVector *&result,
        ExecutionContext *context) const override;
};
} // namespace omniruntime::vectorization
