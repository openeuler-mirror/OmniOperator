/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 * Description: Hash Aggregation Source File
 */

#include "group_aggregation_sort.h"

using namespace omniruntime::op;

void AggregationSort::SetSpillVectorBatch(vec::VectorBatch *spillVecBatch, uint64_t rowOffset)
{
    auto keyVector = reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(spillVecBatch->Get(0));
    auto kvPtr = kvVec.data() + rowOffset;
    auto rowCount = spillVecBatch->GetRowCount();
    groupStates.resize(rowCount);
    for (int32_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        auto &kv = kvPtr[rowIndex];
        std::string_view keyStr(kv.keyAddr, kv.keyLen);
        keyVector->SetValue(rowIndex, keyStr);
        groupStates[rowIndex] = kv.value;
    }

    const size_t aggNum = aggregators.size();
    if (aggNum > 0) {
        std::vector<BaseVector *> oneAggOutputVecs;
        auto aggOutputStartIndex = 1;
        for (size_t aggIndex = 0; aggIndex < aggNum; ++aggIndex) {
            int32_t oneAggOutputCols = aggVectorCounts[aggIndex];
            oneAggOutputVecs.resize(oneAggOutputCols);
            for (auto j = 0; j < oneAggOutputCols; j++) {
                oneAggOutputVecs[j] = spillVecBatch->Get(aggOutputStartIndex + j);
            }
            aggOutputStartIndex += oneAggOutputCols;

            aggregators[aggIndex]->ExtractValuesForSpill(groupStates, oneAggOutputVecs);
        }
    }
}