/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: Concat function implementation
 */

#pragma once
#include <vector>
#include <string_view>
#include "vectorization/VectorFunction.h"
#include "vector/vector.h"
#include "util/debug.h"

namespace omniruntime::vectorization {
    using namespace omniruntime::type;
    using namespace omniruntime::vec;
    using namespace omniruntime::op;

    class ConcatFunction final : public VectorFunction {
    public:
        explicit ConcatFunction() {}

        void Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, BaseVector *&result,
            ExecutionContext *context) const override;

    private:
        // Helper: Get string value from vector with different encodings
        std::string_view GetStringValueFromVector(BaseVector *vec, int32_t row) const;

        // Helper: Set string value to vector
        void SetStringValueToVector(BaseVector *vec, int32_t row, std::string_view &value) const;

        // Main implementation for CONCAT operation
        void ApplyConcat(const std::vector<BaseVector *> &argVectors, BaseVector *&result,
            const DataTypePtr &outputType, ExecutionContext *context) const;
    };

    class ConcatWsFunction final : public VectorFunction {
    public:
        explicit ConcatWsFunction() {}

        explicit ConcatWsFunction(const std::vector<DataTypePtr> &inputDataTypes) : inputDataTypes_(inputDataTypes) {}

        void Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, BaseVector *&result,
            ExecutionContext *context) const override;

    private:
        void ApplyConcatWs(const std::vector<BaseVector *> &argVectors, BaseVector *&result,
                           const DataTypePtr &outputType, ExecutionContext *context) const;

        void ConcatWsRow(const std::vector<BaseVector *> &argVectors,
                         Vector<LargeStringContainer<std::string_view>> *&resultVector, int32_t row) const;

        std::vector<DataTypePtr> inputDataTypes_;
    };

} // namespace omniruntime::vectorization
