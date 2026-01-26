/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: Slice function implementation
 */

#pragma once
#include "vectorization/VectorFunction.h"
#include "vectorization/SelectivityVector.h"

namespace omniruntime::vectorization {
using namespace omniruntime::type;
using namespace omniruntime::vec;
using namespace omniruntime::op;

/// Slice function implementation for arrays.
/// slice(array, start, length) returns a new array containing elements
/// from position start (1-based) with the specified length.
class SliceImpl : public VectorFunction {
public:
    explicit SliceImpl() {}

    void Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, BaseVector *&result,
        ExecutionContext *context) const override;

private:
    template <typename I>
    BaseVector *applySliceTyped(const SelectivityVector &rows, BaseVector *arrayArg, BaseVector *startArg,
        BaseVector *lengthArg, ExecutionContext *context) const;
};
}