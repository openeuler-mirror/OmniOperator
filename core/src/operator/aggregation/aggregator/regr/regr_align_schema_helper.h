/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: Align aggregate schema (skip partial) helpers for regr_* aggregators — per-row partial layout
 *              matches ExtractPartialRow / merge batch column layouts.
 */
#ifndef OMNI_RUNTIME_REGR_ALIGN_SCHEMA_HELPER_H
#define OMNI_RUNTIME_REGR_ALIGN_SCHEMA_HELPER_H

#include "vector/vector_batch.h"

#include <cstdint>
#include <memory>
#include <vector>

namespace omniruntime::vec {
class NullsHelper;
}

namespace omniruntime::op {

/** Merge per-row skip: y|x null (yxNull) OR (when filter is active) !filter[i]. */
std::shared_ptr<vec::NullsHelper> RegrAlignMergeYxNullsWithFilter(const std::shared_ptr<vec::NullsHelper> &yxNull,
    vec::Vector<bool> *filterVec, bool filterIsActive, int32_t rowCount);

void RegrAlignAppendEmptyVarPop3(vec::VectorBatch *result);
void RegrAlignAppendEmptyCov4(vec::VectorBatch *result);
void RegrAlignAppendEmptyPearson6(vec::VectorBatch *result);
void RegrAlignAppendEmptySlope7(vec::VectorBatch *result);

void RegrAlignAppendVarPop3AllNullRows(vec::VectorBatch *result, int32_t rowCount);
void RegrAlignAppendCov4AllNullRows(vec::VectorBatch *result, int32_t rowCount);
void RegrAlignAppendPearson6AllNullRows(vec::VectorBatch *result, int32_t rowCount);
void RegrAlignAppendSlope7AllNullRows(vec::VectorBatch *result, int32_t rowCount);

void RegrAlignAppendVarPop3RawFromX(vec::VectorBatch *result, vec::BaseVector *xVec,
    const std::shared_ptr<vec::NullsHelper> &rowSkip, int32_t rowCount);
void RegrAlignAppendVarPop3RawFromY(vec::VectorBatch *result, vec::BaseVector *yVec,
    const std::shared_ptr<vec::NullsHelper> &rowSkip, int32_t rowCount);

void RegrAlignAppendCov4Raw(vec::VectorBatch *result, vec::BaseVector *yVec, vec::BaseVector *xVec,
    const std::shared_ptr<vec::NullsHelper> &rowSkip, int32_t rowCount);

/** Standard Spark Pearson 6-col layout (count + means + ck + m2), same as regr_count partial. */
void RegrAlignAppendPearson6RawStd(vec::VectorBatch *result, vec::BaseVector *yVec, vec::BaseVector *xVec,
    const std::shared_ptr<vec::NullsHelper> &rowSkip, int32_t rowCount);

/** Same intermediate as regr_r2 ExtractPartialRow (swapped mean / m2 columns for Spark merge). */
void RegrAlignAppendPearson6RawR2Layout(vec::VectorBatch *result, vec::BaseVector *yVec, vec::BaseVector *xVec,
    const std::shared_ptr<vec::NullsHelper> &rowSkip, int32_t rowCount);

void RegrAlignAppendSlope7Raw(vec::VectorBatch *result, vec::BaseVector *yVec, vec::BaseVector *xVec,
    const std::shared_ptr<vec::NullsHelper> &rowSkip, int32_t rowCount);

void RegrAlignAppendPartialSlices(vec::VectorBatch *result, vec::VectorBatch *input,
    const std::vector<int32_t> &channels, int32_t numCols, int32_t rowCount);

void RegrAlignAppendPartialColumnsWithSkip(vec::VectorBatch *result, vec::VectorBatch *input,
    const std::vector<int32_t> &channels, int32_t numCols, int32_t rowCount,
    const std::shared_ptr<vec::NullsHelper> &rowSkip, bool firstColIsLong);

/** regr_replacement / RegrReplacement partial: Spark-compatible DOUBLE n + avg DOUBLE + m2 DOUBLE. */
void RegrAlignAppendEmptyReplacementPartial3(vec::VectorBatch *result);
void RegrAlignAppendReplacementPartial3AllNullRows(vec::VectorBatch *result, int32_t rowCount);
void RegrAlignAppendReplacementPartial3RawDouble(vec::VectorBatch *result, vec::BaseVector *valVec,
    const std::shared_ptr<vec::NullsHelper> &rowSkip, int32_t rowCount);

} // namespace omniruntime::op

#endif // OMNI_RUNTIME_REGR_ALIGN_SCHEMA_HELPER_H
