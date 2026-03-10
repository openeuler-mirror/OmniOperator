/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: ArrayZip function implementation
 */

#pragma once
#include "vectorization/VectorFunction.h"
#include "vector/array_vector.h"
#include "vector/row_vector.h"
#include "vector/vector.h"
#include "vector/vector_helper.h"

namespace omniruntime::vectorization {
using namespace omniruntime::type;
using namespace omniruntime::vec;
using namespace omniruntime::op;

/// arrays_zip function implementation for arrays.
/// arrays_zip(array(T1), array(T2), ...) -> array(row(T1, T2, ...))
/// Merges the given arrays, element-wise, into a single array of structs.
/// The N-th struct contains all N-th values of input arrays.
/// If one of the arrays is shorter than others, null values are appended to pad it.
/// If any entire input array row is null, the output row is null.
/// Supports: BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, BOOLEAN,
/// VARCHAR, CHAR, VARBINARY, DECIMAL64, DECIMAL128, DATE32, TIMESTAMP.
/// Arity: 2 to 7 input arrays (same as velox).
class ArrayZipImpl : public VectorFunction {
public:
    explicit ArrayZipImpl(int32_t numArrays) : numArrays_(numArrays) {}

    void Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, BaseVector *&result,
        ExecutionContext *context) const override;

private:
    int32_t numArrays_;
};
} // namespace omniruntime::vectorization
