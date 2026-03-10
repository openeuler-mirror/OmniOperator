/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: ArrayJoin function implementation
 */

#pragma once
#include "vectorization/VectorFunction.h"
#include "vectorization/SelectivityVector.h"
#include "vector/array_vector.h"
#include "vector/vector.h"
#include "vector/vector_helper.h"
#include "type/decimal128.h"
#include "type/date32.h"
#include "type/TimestampConversion.h"

namespace omniruntime::vectorization {
using namespace omniruntime::type;
using namespace omniruntime::vec;
using namespace omniruntime::op;

/// array_join function implementation for arrays.
/// array_join(array(T), delimiter) -> varchar
/// array_join(array(T), delimiter, nullReplacement) -> varchar
/// Concatenates the elements of the given array using the delimiter
/// and an optional string to replace nulls.
/// Supports element types: BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, BOOLEAN,
/// VARCHAR, VARBINARY, DECIMAL64, DECIMAL128, DATE32, TIMESTAMP.
class ArrayJoinImpl : public VectorFunction {
public:
    explicit ArrayJoinImpl() {}

    void Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, BaseVector *&result,
        ExecutionContext *context) const override;

private:
    std::string ElementToString(BaseVector *elementVector, int64_t index) const;
    std::string GetStringValue(BaseVector *vec, int32_t row) const;
};
} // namespace omniruntime::vectorization
