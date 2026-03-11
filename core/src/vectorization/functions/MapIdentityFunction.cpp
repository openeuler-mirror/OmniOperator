/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.
 * Description: map(x) identity for map type - pass-through.
 */
#include "MapIdentityFunction.h"
#include "util/debug.h"

namespace omniruntime::vectorization {

void MapIdentityFunction::Apply(std::stack<vec::BaseVector *> &args, const type::DataTypePtr &outputType,
    vec::BaseVector *&result, op::ExecutionContext *context) const
{
    (void)outputType;
    (void)context;
    if (args.empty()) {
        OMNI_THROW("MapIdentityFunction Error:", "map() requires 1 argument");
    }
    vec::BaseVector *arg = args.top();
    args.pop();
    if (arg == nullptr) {
        OMNI_THROW("MapIdentityFunction Error:", "Input vector is null");
    }
    result = arg;
}

} // namespace omniruntime::vectorization
