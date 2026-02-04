/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: LIKE function implementation.
 * LIKE(string, pattern) -> boolean. Pattern: % = zero or more chars, _ = one char, \ = escape.
 */

#pragma once
#include <vector>
#include <string_view>
#include <string>

#include "vectorization/VectorFunction.h"
#include "vector/vector.h"
#include "type/data_operations.h"
#include "util/debug.h"
#include "vector/vector_helper.h"

namespace omniruntime::vectorization {
using namespace omniruntime::type;
using namespace omniruntime::vec;
using namespace omniruntime::op;

class LikeFunction : public VectorFunction {
public:
    explicit LikeFunction() = default;

    void Apply(std::stack<BaseVector*>& args, const DataTypePtr& outputType, BaseVector*& result,
               ExecutionContext* context) const override;

private:
    /// Get string value from vector (flat / const / dictionary)
    static std::string_view GetStringValueFromVector(BaseVector* vec, int32_t row);

    /// Set boolean value to result vector
    static void SetBooleanValueToVector(BaseVector* vec, int32_t row, bool value);

    /// Main implementation: match str against LIKE pattern (% _ escape \)
    void ApplyLike(BaseVector* strVec, BaseVector* patternVec, BaseVector*& result,
                   const DataTypePtr& outputType) const;

    /// Convert LIKE pattern to regex string; then match str (full match) via std::regex
    bool MatchLike(const std::string_view& str, const std::string_view& pattern) const;
};

}  // namespace omniruntime::vectorization
