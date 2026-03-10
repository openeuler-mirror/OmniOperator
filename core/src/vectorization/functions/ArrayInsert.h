/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: ArrayInsert function implementation
 */

#pragma once
#include "vectorization/VectorFunction.h"
#include "vectorization/SelectivityVector.h"
#include "vector/array_vector.h"
#include "vector/vector.h"
#include "vector/vector_helper.h"

namespace omniruntime::vectorization {
using namespace omniruntime::type;
using namespace omniruntime::vec;
using namespace omniruntime::op;

/// array_insert function implementation for arrays.
/// array_insert(array(E), pos, E, legacyNegativeIndex) -> array(E)
/// Places new element into index pos of the input array.
/// If pos is positive, inserts at that 1-based position; pads with nulls if pos exceeds array size.
/// If pos is negative, inserts counting from the end of the array.
/// The legacyNegativeIndex boolean controls negative index semantics:
///   false: -1 refers to the last position
///   true:  -1 refers to the second-to-last position
class ArrayInsertImpl : public VectorFunction {
public:
    explicit ArrayInsertImpl() {}

    void Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, BaseVector *&result,
        ExecutionContext *context) const override;

private:
    static constexpr int32_t kMaxNumberOfElements = 10000;

    int32_t GetPosValue(BaseVector *posArg, int32_t row) const;
    bool IsVectorNullAtRow(BaseVector *vec, int32_t row) const;
    void CopyItemToElement(BaseVector *itemVector, int32_t row, BaseVector *resultElem,
        int64_t targetIdx) const;
    void CopyConstItemToElement(BaseVector *constItem, BaseVector *resultElem,
        int64_t targetIdx) const;
};
} // namespace omniruntime::vectorization
