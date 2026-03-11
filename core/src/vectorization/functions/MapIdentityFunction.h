/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.
 * Description: map(x) identity for map type - pass-through for first_value/last_value result.
 */
#pragma once
#include "vectorization/VectorFunction.h"

namespace omniruntime::vectorization {

class MapIdentityFunction final : public VectorFunction {
public:
    void Apply(std::stack<vec::BaseVector *> &args, const type::DataTypePtr &outputType,
        vec::BaseVector *&result, op::ExecutionContext *context) const override;
};

} // namespace omniruntime::vectorization
