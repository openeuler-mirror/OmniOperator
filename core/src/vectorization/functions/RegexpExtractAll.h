/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: RegexpExtractAll function implementation - returns array of all regex matches
 */

#pragma once
#include <vector>
#include <string_view>

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

class RegexpExtractAllFunction : public VectorFunction {
public:
    explicit RegexpExtractAllFunction() {}

    void Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, BaseVector *&result,
               ExecutionContext *context) const override;

private:
    std::string_view GetStringValueFromVector(BaseVector *vec, int32_t row) const;
    int32_t GetIntValueFromVector(BaseVector *vec, int32_t row) const;
    int64_t GetLongValueFromVector(BaseVector *vec, int32_t row) const;

    void ApplyRegexpExtractAll(BaseVector *strVec, BaseVector *patternVec, BaseVector *groupIdxVec,
                               BaseVector *&result, const DataTypePtr &outputType) const;

    void ExtractAllMatches(const std::string_view &str, const std::string_view &pattern, int32_t groupIdx,
                           std::vector<std::string> &outMatches) const;
};
}
