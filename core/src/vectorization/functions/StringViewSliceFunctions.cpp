/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: StringView "SV-out" (zero-copy sub-view) functions — implementation.
 */

#include "StringViewSliceFunctions.h"

#include <limits>
#include "vectorization/functions/String.h" // SubstrComputeRange / {Trim,LTrim,RTrim}ComputeRange
#include "vector/vector.h"
#include "util/debug.h"

namespace omniruntime::vectorization {
using namespace omniruntime::type;
using namespace omniruntime::vec;
using namespace omniruntime::op;

namespace {
// Reads an INT argument value for a row (const vector -> single value; flat -> per-row).
ALWAYS_INLINE int32_t ReadInt(BaseVector *vec, int32_t row)
{
    if (vec->GetEncoding() == OMNI_ENCODING_CONST) {
        return static_cast<ConstVector<int32_t> *>(vec)->GetConstValue();
    }
    return static_cast<Vector<int32_t> *>(vec)->GetValue(row);
}

// Null index for an argument (const args carry their value/null at slot 0).
ALWAYS_INLINE int32_t NullIdx(BaseVector *vec, int32_t row)
{
    return (vec->GetEncoding() == OMNI_ENCODING_CONST) ? 0 : row;
}

// The output shares this vector's string buffer, so it must be a real Vector<StringView>
// (flat or sliced). Const/dictionary SV inputs are not supported here (documented limitation);
// they would need a copy path. This matches the common scan/projection input (flat or Slice).
ALWAYS_INLINE Vector<StringView> *AsStringViewColumn(BaseVector *vec)
{
    if (vec->GetTypeId() != OMNI_STRING_VIEW || vec->GetEncoding() == OMNI_DICTIONARY ||
        vec->GetEncoding() == OMNI_ENCODING_CONST) {
        OMNI_THROW("StringView SV-out Error", "SV-out substr/trim requires a flat StringView string column");
    }
    return static_cast<Vector<StringView> *>(vec);
}
} // namespace

void SubstrStringViewFunction::Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType,
    BaseVector *&result, ExecutionContext *context) const
{
    // Stack order (ExprEval pushes children left-to-right, top = last pushed):
    //   3-arg substr(string, start, length): top=length, next=start, next=string
    //   2-arg substr(string, start):         top=start,  next=string
    const bool threeArg = args.size() >= 3;
    BaseVector *lengthVec = nullptr;
    if (threeArg) {
        lengthVec = args.top();
        args.pop();
    }
    BaseVector *startVec = args.top();
    args.pop();
    BaseVector *stringVec = args.top();
    args.pop();

    const int32_t size = context->GetResultRowSize();
    Vector<StringView> *src = AsStringViewColumn(stringVec);
    // Shares src's string buffer (refcounted) so SetNoCopy sub-views stay valid after src is freed.
    auto *out = new Vector<StringView>(size, *src);

    for (int32_t row = 0; row < size; ++row) {
        if (src->IsNull(row) || startVec->IsNull(NullIdx(startVec, row)) ||
            (lengthVec != nullptr && lengthVec->IsNull(NullIdx(lengthVec, row)))) {
            out->SetNull(row);
            continue;
        }
        const StringView &sv = src->GetValueRef(row);
        std::string_view input(sv.data(), sv.size());
        int32_t start = ReadInt(startVec, row);
        int32_t length = threeArg ? ReadInt(lengthVec, row) : std::numeric_limits<int32_t>::max();
        size_t off;
        size_t len;
        SubstrComputeRange(input, start, length, off, len);
        out->SetNoCopy(row, StringView(sv.data() + off, static_cast<int32_t>(len)));
    }

    result = out;
    delete stringVec;
    delete startVec;
    if (lengthVec != nullptr) {
        delete lengthVec;
    }
}

void TrimStringViewFunction::Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType,
    BaseVector *&result, ExecutionContext *context) const
{
    BaseVector *stringVec = args.top();
    args.pop();

    const int32_t size = context->GetResultRowSize();
    Vector<StringView> *src = AsStringViewColumn(stringVec);
    auto *out = new Vector<StringView>(size, *src);

    for (int32_t row = 0; row < size; ++row) {
        if (src->IsNull(row)) {
            out->SetNull(row);
            continue;
        }
        const StringView &sv = src->GetValueRef(row);
        std::string_view s(sv.data(), sv.size());
        size_t off;
        size_t len;
        switch (kind_) {
            case Kind::Trim:
                TrimComputeRange(s, off, len);
                break;
            case Kind::LTrim:
                LTrimComputeRange(s, off, len);
                break;
            case Kind::RTrim:
                RTrimComputeRange(s, off, len);
                break;
        }
        out->SetNoCopy(row, StringView(sv.data() + off, static_cast<int32_t>(len)));
    }

    result = out;
    delete stringVec;
}

} // namespace omniruntime::vectorization
