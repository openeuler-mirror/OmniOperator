/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: Reverse function implementation for both String and Array types
 */

#pragma once
#include <vector>
#include <string_view>
#include "vectorization/VectorFunction.h"
#include "vector/vector.h"
#include "vector/array_vector.h"
#include "util/debug.h"

namespace omniruntime::vectorization {
using namespace omniruntime::type;
using namespace omniruntime::vec;
using namespace omniruntime::op;

/**
 * ReverseFunction - Unified reverse function for both String and Array types
 *
 * reverse(varchar) -> varchar
 * Takes any string as input and returns a string with characters in reverse order.
 *
 * reverse(array<T>) -> array<T>
 * Takes any array as input and returns the array with elements in reverse order.
 *
 * NULL handling: If input is NULL, output is also NULL.
 */
class ReverseFunction final : public VectorFunction {
public:
    explicit ReverseFunction() {}

    void Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, BaseVector *&result,
        ExecutionContext *context) const override;

private:
    // ========== String Type Processing ==========
    // Helper: Get string value from vector with different encodings
    std::string_view GetStringValueFromVector(BaseVector *vec, int32_t row) const;

    // Helper: Set string value to vector
    void SetStringValueToVector(BaseVector *vec, int32_t row, std::string_view &value) const;

    // Main implementation for string REVERSE operation
    void ApplyReverseString(BaseVector *argVector, BaseVector *&result,
        const DataTypePtr &outputType, ExecutionContext *context) const;

    // Reverse a string character by character (UTF-8 aware)
    std::string ReverseString(const std::string_view &input) const;

    // ========== Array Type Processing ==========
    // Main implementation for array REVERSE operation
    void ApplyReverseArray(BaseVector *argVector, BaseVector *&result,
        const DataTypePtr &outputType, ExecutionContext *context) const;
};

} // namespace omniruntime::vectorization
