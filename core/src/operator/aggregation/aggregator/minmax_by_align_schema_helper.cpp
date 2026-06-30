/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
#include "minmax_by_align_schema_helper.h"

namespace omniruntime::op {

using omniruntime::vec::BaseVector;
using omniruntime::vec::NullsBuffer;
using omniruntime::vec::NullsHelper;
using omniruntime::vec::Vector;
using omniruntime::vec::VectorBatch;
using omniruntime::vec::VectorHelper;
using omniruntime::vec::unsafe::UnsafeVector;

std::shared_ptr<NullsHelper> MinMaxByAlignMergeSortKeySkip(BaseVector *sortKeyVec, Vector<bool> *filterVec,
    bool filterIsActive, int32_t rowCount)
{
    auto buf = std::make_shared<NullsBuffer>(rowCount);
    auto merged = std::make_shared<NullsHelper>(buf);
    bool *fp = filterIsActive ? UnsafeVector::GetRawValues(filterVec) : nullptr;
    for (int32_t i = 0; i < rowCount; ++i) {
        const bool skip = sortKeyVec->IsNull(i) || (filterIsActive && !fp[i]);
        merged->SetNull(i, skip);
    }
    if (!buf->HasNull()) {
        return nullptr;
    }
    return merged;
}

void MinMaxByAlignAppendPartialSlices(VectorBatch *result, VectorBatch *input, const std::vector<int32_t> &channels,
    int32_t rowCount)
{
    for (int32_t c = 0; c < 2; ++c) {
        BaseVector *col = input->Get(channels[static_cast<size_t>(c)]);
        result->Append(VectorHelper::SliceVector(col, 0, rowCount));
    }
}

} // namespace omniruntime::op
