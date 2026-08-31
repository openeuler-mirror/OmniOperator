/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: concat SV-in / SV-out (materialized StringView output).
 *   Unlike substr/trim SV-out, concat produces new content — results are copied into the output
 *   vector's own string buffer via SetValue (inline <=12B, arena copy otherwise).
 */

#pragma once

#include <stack>
#include <string_view>
#include <vector>

#include "vectorization/VectorFunction.h"
#include "vector/vector.h"

namespace omniruntime::vectorization {
using namespace omniruntime::type;
using namespace omniruntime::vec;
using namespace omniruntime::op;

class ConcatStringViewFunction final : public VectorFunction {
public:
    explicit ConcatStringViewFunction() {}

    void Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, BaseVector *&result,
        ExecutionContext *context) const override;

private:
    static std::string_view GetStringValueFromVector(BaseVector *vec, int32_t row);

    static void ApplyConcat(const std::vector<BaseVector *> &argVectors, BaseVector *&result,
        ExecutionContext *context);
};

} // namespace omniruntime::vectorization
