/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: ArrayConstructor function implementation
 */

#pragma once

#include <vector>
#include <stack>
#include <string_view>
#include "vectorization/VectorFunction.h"
#include "vector/vector.h"
#include "vector/array_vector.h"
#include "vector/vector_helper.h"
#include "util/debug.h"
#include "util/type_util.h"

namespace omniruntime::vectorization {
using namespace omniruntime::type;
using namespace omniruntime::vec;
using namespace omniruntime::op;

/// ArrayConstructorFunction - Constructs an array from a variable number of arguments
/// array(T, T, ..., T) -> array<T>
/// Takes any number of arguments of the same type and constructs an array.
/// NULL arguments are preserved as NULL elements within the resulting array.
/// An empty argument list produces an empty array.
class ArrayConstructorFunction : public VectorFunction {
public:
    explicit ArrayConstructorFunction() {}

    void Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, BaseVector *&result,
        ExecutionContext *context) const override;

private:
    template <typename T>
    void ConstructArray(const std::vector<BaseVector *> &argVectors, BaseVector *&result,
        int32_t rowSize, DataTypeId elementTypeId) const;

    void ConstructArrayVarchar(const std::vector<BaseVector *> &argVectors, BaseVector *&result,
        int32_t rowSize) const;

    void ConstructArrayNested(const std::vector<BaseVector *> &argVectors, BaseVector *&result,
        int32_t rowSize) const;

    void ConstructEmptyArray(BaseVector *&result, int32_t rowSize) const;

    template <typename T>
    static T GetValue(BaseVector *vec, int32_t row);

    static std::string_view GetStringValue(BaseVector *vec, int32_t row);

    static bool IsValueNull(BaseVector *vec, int32_t row);
};

std::shared_ptr<VectorFunction> makeArrayConstructor(const std::string &name,
    const std::vector<DataTypeId> &inputArgs, const config::QueryConfig &);

std::vector<std::shared_ptr<codegen::FunctionSignature>> ArrayConstructorSignatures();

} // namespace omniruntime::vectorization
