/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: Cardinality function for Array and Map types
 */

#pragma once
#include <stack>
#include "vectorization/VectorFunction.h"
#include "vector/vector.h"
#include "util/debug.h"

namespace omniruntime::vectorization {
using namespace omniruntime::type;
using namespace omniruntime::vec;
using namespace omniruntime::op;

class CardinalityFunction : public VectorFunction {
public:
    explicit CardinalityFunction() {}

    void Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, BaseVector *&result,
        ExecutionContext *context) const override;

private:
    void ProcessArrayCardinality(BaseVector *arrayArg, Vector<int64_t> *resultVec, int32_t rowSize) const;
    void ProcessMapCardinality(BaseVector *mapArg, Vector<int64_t> *resultVec, int32_t rowSize) const;
};

} // namespace omniruntime::vectorization
