/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: NamedStruct function for constructing ROW vectors
 */

#pragma once
#include "vectorization/VectorFunction.h"
#include "vector/row_vector.h"
#include "type/data_type.h"
#include "vector/vector_helper.h"
#include <vector>

namespace omniruntime::vectorization {
using namespace omniruntime::type;
using namespace omniruntime::vec;
using namespace omniruntime::op;

class NamedStructFunction : public VectorFunction {
public:
    explicit NamedStructFunction() {}
    explicit NamedStructFunction(const std::vector<DataTypePtr> &inputDataTypes) : inputDataTypes_(inputDataTypes) {}

    void Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, BaseVector *&result,
               ExecutionContext *context) const override;

private:
    template <typename T>
    T GetValueFromVector(BaseVector *vec, int32_t row) const;
    std::string_view GetStringValueFromVector(BaseVector *vec, int32_t row) const;
    template <typename T>
    void SetValueToVector(BaseVector *vec, int32_t row, const T &value) const;
    void SetStringValueToVector(BaseVector *vec, int32_t row, std::string_view &value) const;
    void CopyFieldAtRow(BaseVector *srcVec, BaseVector *dstVec, int32_t dstRow, DataTypeId typeId, int32_t srcRow,
                       int64_t *mapRunningOffset = nullptr) const;
    std::vector<DataTypePtr> inputDataTypes_;
};
}
