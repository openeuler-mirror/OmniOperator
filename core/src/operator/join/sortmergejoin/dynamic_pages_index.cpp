/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: dynamic pages index implementations
 */
#include "dynamic_pages_index.h"
#include "../../pages_index.h"
#include "../../../vector/vector_helper.h"

using namespace omniruntime::vec;

// function implements for class PagesIndex
DynamicPagesIndex::DynamicPagesIndex(const omniruntime::vec::VecTypes &types)
    : vecTypes(types.Get().data()),
      vecTypeIds(types.GetIds()),
      typesCount(types.GetSize()),
      positionCount(0),
      finishAddData(false)
{}

int32_t DynamicPagesIndex::AddVecBatches(const std::vector<VectorBatch *> &vecBatches)
{
    if (finishAddData) {
        return 0;
    }
    int32_t vecBatchCount = vecBatches.size();
    int32_t vecBatchLastIndex = this->vecBatchFreeFlagDeque.size();
    int32_t columnCount = this->typesCount;

    for (int32_t vecBatchIdx = 0; vecBatchIdx < vecBatchCount; ++vecBatchIdx) {
        VectorBatch *vecBatch = vecBatches[vecBatchIdx];
        int32_t rowCount = vecBatch->GetRowCount();
        // no more vector batch will add
        if (rowCount == 0) {
            this->finishAddData = true;
            this->vecBatchFreeFlagDeque.push_back(false);
            this->vectorBatchDeque.push_back(vecBatch);
            return 0;
        }

        this->positionCount += rowCount;
        this->vecBatchFreeFlagDeque.push_back(false);
        this->vectorBatchDeque.push_back(vecBatch);

        // generate value address.
        for (int32_t rowIdx = 0; rowIdx < rowCount; rowIdx++) {
            int64_t valueAddress = EncodeSyntheticAddress(vecBatchIdx + vecBatchLastIndex, rowIdx);
            this->valueAddressesDeque.push_back(valueAddress);
        }

        std::deque<Vector *> vectorBatchColumns;
        for (int32_t colIdx = 0; colIdx < columnCount; ++colIdx) {
            vectorBatchColumns.push_back(vecBatch->GetVector(colIdx));
        }
        this->columnsDeque.push_back(vectorBatchColumns);
    }
    return 0;
}

void DynamicPagesIndex::FreeBeforeVecBatch(int32_t vecBatchIdx)
{
    if (vecBatchIdx >= this->vecBatchFreeFlagDeque.size() || vecBatchIdx - 1 <= lastFreedVecBatchIdx) {
        return;
    }
    for (int batchIdx = lastFreedVecBatchIdx + 1; batchIdx < vecBatchIdx; batchIdx++) {
        this->vecBatchFreeFlagDeque[batchIdx] = true;
        VectorHelper::FreeVecBatch(this->vectorBatchDeque[batchIdx]);
    }
    lastFreedVecBatchIdx = vecBatchIdx - 1;
}

void DynamicPagesIndex::FreeAllRemainingVecBatch()
{
    for (int idx = lastFreedVecBatchIdx + 1; idx < vectorBatchDeque.size(); idx++) {
        this->vecBatchFreeFlagDeque[idx] = true;
        VectorHelper::FreeVecBatch(this->vectorBatchDeque[idx]);
    }
    lastFreedVecBatchIdx = vectorBatchDeque.size() - 1;
}

DynamicPagesIndex::~DynamicPagesIndex() {}
