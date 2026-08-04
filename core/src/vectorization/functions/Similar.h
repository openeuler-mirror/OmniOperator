/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Description: SIMILAR TO function implementation (SQL regex full-match, vectorized)
 */

#pragma once
#include <vector>
#include <string_view>
#include <string>
#include <codecvt>

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

// Vectorized SIMILAR TO: full-match a string against an SQL SIMILAR pattern.
// Translates SQL SIMILAR syntax to a POSIX regex (re2) and full-matches each row.
class SimilarFunction : public VectorFunction {
public:
    explicit SimilarFunction() {}

    void Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, BaseVector *&result,
               ExecutionContext *context) const override;

private:
    std::string_view GetStringValueFromVector(BaseVector *vec, int32_t row) const;
    void SetBooleanValueToVector(BaseVector *vec, int32_t row, bool value) const;
    void ApplySimilar(BaseVector *strVec, BaseVector *patternVec, BaseVector *&result,
                      const DataTypePtr &outputType) const;
    // Translate SQL SIMILAR pattern to POSIX regex (no ESCAPE; escapeChar=0).
    std::string SimilarPatternToRegex(const std::string &sqlPattern) const;
    // Full-match str against SQL SIMILAR pattern (thread-local compiled regex cache).
    bool MatchSimilar(const std::string_view &str, const std::string_view &pattern) const;
};
}
