/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: Size function implementation for Array and Map types
 */

#pragma once
#include <vector>
#include <stack>
#include "vectorization/VectorFunction.h"
#include "vector/vector.h"
#include "util/debug.h"

namespace omniruntime::vectorization {
using namespace omniruntime::type;
using namespace omniruntime::vec;
using namespace omniruntime::op;

/// SizeFunction - Unified size function for both Array and Map types
/// size(array | map, legacySizeOfNull) -> int32_t
/// Returns the number of elements in an array or the number of key-value pairs in a map.
/// - If input is NULL and legacySizeOfNull=true: returns -1
/// - If input is NULL and legacySizeOfNull=false: returns NULL
/// - If input is empty array [] or empty map {}: returns 0
class SizeFunction : public VectorFunction {
public:
    explicit SizeFunction() {}

    void Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, BaseVector *&result,
        ExecutionContext *context) const override;

private:
    /// Process array size calculation
    void ProcessArraySize(BaseVector *arrayArg, Vector<int32_t> *resultVec, int32_t rowSize,
        bool legacySizeOfNull) const;

    /// Process map size calculation
    void ProcessMapSize(BaseVector *mapArg, Vector<int32_t> *resultVec, int32_t rowSize,
        bool legacySizeOfNull) const;
};

} // namespace omniruntime::vectorization
