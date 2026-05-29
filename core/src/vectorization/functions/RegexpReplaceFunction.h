/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: RegexpReplace function implementation
 */

#pragma once
#include <vector>
#include <string_view>
#include <stack>

#include "vectorization/VectorFunction.h"
#include "vector/array_vector.h"
#include "type/data_operations.h"
#include "util/debug.h"
#include "util/type_util.h"
#include "vector/vector_helper.h"

namespace omniruntime::vectorization {
    using namespace omniruntime::type;
    using namespace omniruntime::vec;
    using namespace omniruntime::op;

class RegexpReplaceFunction : public VectorFunction {
public:
    explicit RegexpReplaceFunction() {}

    void Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, BaseVector *&result,
        ExecutionContext *context) const override;

private:
    void ApplyRegexpReplace(BaseVector *strVec, BaseVector *patternVec, BaseVector *replacementVec,
        BaseVector *positionVec, BaseVector *&result, const DataTypePtr &outputType, ExecutionContext *context) const;

    std::string RegexpReplace(const std::string_view &str, const std::string_view &pattern,
        const std::string_view &replacement, int32_t position) const;
};
}