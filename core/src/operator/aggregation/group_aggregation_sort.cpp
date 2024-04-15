/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 * Description: Hash Aggregation Source File
 */

#include "group_aggregation_sort.h"
using namespace omniruntime::op;
void AggregationSort::SetSpillVectorBatch(vec::VectorBatch *spillVecBatch, uint64_t rowOffset)
{
    auto rowCount = spillVecBatch->GetRowCount();
    auto keyVector = reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(spillVecBatch->Get(0));
    for (int64_t i = 0; i < rowCount; i++) {
        std::string_view keyStr(kvVec[i + rowOffset].keyAddr, kvVec[i + rowOffset].keyLen);
        keyVector->SetValue(i, keyStr);
    }
    const size_t aggNum = aggregators.size();
    if (aggNum > 0) {
        auto aggOutputStartIndex = 1;
        for (size_t aggIndex = 0; aggIndex < aggNum; ++aggIndex) {
            auto currentAggType = (*aggregators[aggIndex])->GetType();
            int32_t oneAggOutputCols = 1;
            if (currentAggType == FunctionType::OMNI_AGGREGATION_TYPE_SUM ||
                currentAggType == FunctionType::OMNI_AGGREGATION_TYPE_AVG ||
                currentAggType == FunctionType::OMNI_AGGREGATION_TYPE_FIRST_INCLUDENULL ||
                currentAggType == FunctionType::OMNI_AGGREGATION_TYPE_FIRST_IGNORENULL) {
                // Among these types, there will be two columns of arrays to store spill data.
                oneAggOutputCols = 2;
            }
            std::vector<BaseVector *> adaptAggVectors(oneAggOutputCols);
            for (auto j = 0; j < oneAggOutputCols; j++) {
                adaptAggVectors[j] = spillVecBatch->Get(aggOutputStartIndex + j);
            }
            aggOutputStartIndex += oneAggOutputCols;
            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                auto state = (reinterpret_cast<AggregateState *>(kvVec[rowIndex + rowOffset].valAddr))[aggIndex];
                (*aggregators[aggIndex])->ExtractSpillValues(state, adaptAggVectors, rowIndex);
            }
        }
    }
}