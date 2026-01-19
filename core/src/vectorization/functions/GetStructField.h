/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#pragma once
#include "../VectorFunction.h"
#include "../VectorReaders.h"

namespace omniruntime::vectorization {
class GetStructFieldFunction final : public VectorFunction {
public:
    explicit GetStructFieldFunction(const int32_t ordinal) : ordinal_(ordinal) {}

    void Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, BaseVector *&result,
        op::ExecutionContext *context) const override {}

private:
    // The position to select subfield from the struct.
    const int32_t ordinal_;
};
}
