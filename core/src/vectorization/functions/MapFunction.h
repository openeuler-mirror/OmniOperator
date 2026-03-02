/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: MapFunction for constructing MAP vectors from key-value pairs
 */

#pragma once
#include "vectorization/VectorFunction.h"
#include "vector/map_vector.h"
#include "vector/array_vector.h"
#include "vector/row_vector.h"
#include "vector/vector_helper.h"
#include "type/data_type.h"
#include "util/debug.h"
#include <vector>

namespace omniruntime::vectorization {
using namespace omniruntime::type;
using namespace omniruntime::vec;
using namespace omniruntime::op;

class MapFunction : public VectorFunction {
public:
    void Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, BaseVector *&result,
               ExecutionContext *context) const override;

private:
    template <typename T>
    void CopyValue(BaseVector *src, int32_t srcRow, BaseVector *dst, int32_t dstRow) const;
    void CopyStringValue(BaseVector *src, int32_t srcRow, BaseVector *dst, int32_t dstRow) const;
    void CopyElement(BaseVector *src, int32_t srcRow, BaseVector *dst, int32_t dstRow,
                     DataTypeId typeId, int64_t *runningOffset = nullptr) const;

    template <typename T>
    T ReadValue(BaseVector *vec, int32_t row) const;
    std::string_view ReadStringValue(BaseVector *vec, int32_t row) const;

    template <typename T>
    bool ValuesEqual(BaseVector *vec1, int32_t row1, BaseVector *vec2, int32_t row2) const;
    bool StringValuesEqual(BaseVector *vec1, int32_t row1, BaseVector *vec2, int32_t row2) const;
    bool ElementsEqual(BaseVector *vec1, int32_t row1, BaseVector *vec2, int32_t row2,
                       DataTypeId typeId) const;
};
}
