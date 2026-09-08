/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: StringView "SV-out" (zero-copy sub-view) functions.
 *   substr / trim / ltrim / rtrim whose result is a contiguous sub-view of the input string.
 *   These emit a Vector<StringView> that SHARES the input column's string buffer (via the
 *   Vector<StringView>(size, source) shared-buffer constructor) and stores each result with
 *   SetNoCopy — so a >12B result points into the input arena with no allocation and no byte copy.
 *   The shared_ptr on the string buffer keeps the input bytes alive for the output's whole
 *   lifetime, even after the input vector is freed. Same model as Vector<StringView>::CopyPositions
 *   / Slice and the ORC dictionary reader (nextAsStringView).
 *
 *   Only a flat/sliced StringView string column is supported as the buffer source; other encodings
 *   throw (documented limitation — the common scan/projection path is flat/sliced).
 */
#pragma once

#include <stack>
#include "vectorization/VectorFunction.h"
#include "vector/vector.h"

namespace omniruntime::vectorization {
using namespace omniruntime::type;
using namespace omniruntime::vec;
using namespace omniruntime::op;

/// substr(SV string, INT start [, INT length]) -> StringView. Handles both the 2-arg and 3-arg
/// forms (distinguished at runtime by the number of arguments on the stack).
class SubstrStringViewFunction final : public VectorFunction {
public:
    explicit SubstrStringViewFunction() {}

    void Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, BaseVector *&result,
        ExecutionContext *context) const override;
};

/// trim / ltrim / rtrim (SV string) -> StringView. `kind` selects which whitespace-strip window.
class TrimStringViewFunction final : public VectorFunction {
public:
    enum class Kind { Trim, LTrim, RTrim };

    explicit TrimStringViewFunction(Kind kind) : kind_(kind) {}

    void Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, BaseVector *&result,
        ExecutionContext *context) const override;

private:
    Kind kind_;
};

} // namespace omniruntime::vectorization
