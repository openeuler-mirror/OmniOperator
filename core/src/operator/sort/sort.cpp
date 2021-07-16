/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2021. All rights reserved.
 */
#include "sort.h"
#include "../../util/type_util.h"
#include "../../util/debug.h"
#include "../../memory/memory_pool.h"
#include "../../vector/vector_common.h"
#include "../status.h"
#include <iostream>
#include <algorithm>

using namespace std;
namespace omniruntime {
namespace op {
int32_t GetMaxRowCount(const int32_t *sourceTypes, const int32_t *outputCols, int32_t outputColsCount)
{
    int32_t rowSize = 0;
    int type;
    for (int32_t i = 0; i < outputColsCount; i++) {
        type = sourceTypes[outputCols[i]];
        switch (type) {
            case OMNI_VEC_TYPE_INT:
                rowSize = rowSize + sizeof(int32_t);
                break;
            case OMNI_VEC_TYPE_LONG:
                rowSize = rowSize + sizeof(int64_t);
                break;
            case OMNI_VEC_TYPE_DOUBLE:
                rowSize = rowSize + sizeof(double);
                break;
            default:
                break;
        }
    }

    int32_t maxRowCount = (MAX_VEC_BATCH_SIZE_IN_BYTES + rowSize - 1) / rowSize;
    return maxRowCount;
}

int32_t GetPageCount(int32_t positionCount, int32_t maxRowCount)
{
    return ((positionCount + maxRowCount - 1) / maxRowCount);
}

SortOperatorFactory::SortOperatorFactory(int32_t *sourceTypes, int32_t sourceTypeCount, int32_t *outputCols,
    int32_t outputColCount, int32_t *sortCols, int32_t *sortAscendings, int32_t *sortNullFirsts, int32_t sortColCount)
{
    this->sourceTypes.insert(this->sourceTypes.end(), sourceTypes, sourceTypes + sourceTypeCount);
    this->outputCols.insert(this->outputCols.end(), outputCols, outputCols + outputColCount);
    this->sortCols.insert(this->sortCols.end(), sortCols, sortCols + sortColCount);
    this->sortAscendings.insert(this->sortAscendings.end(), sortAscendings, sortAscendings + sortColCount);
    this->sortNullFirsts.insert(this->sortNullFirsts.end(), sortNullFirsts, sortNullFirsts + sortColCount);
}

SortOperatorFactory::~SortOperatorFactory() {}

SortOperatorFactory *SortOperatorFactory::CreateSortOperatorFactory(int32_t *sourceTypes, int32_t sourceTypeCount,
    int32_t *outputCols, int32_t outputColCount, int32_t *sortCols, int32_t *sortAscendings, int32_t *sortNullFirsts,
    int32_t sortColCount)
{
    auto pOperatorFactory = std::make_unique<SortOperatorFactory>(sourceTypes, sourceTypeCount, outputCols,
        outputColCount, sortCols, sortAscendings, sortNullFirsts, sortColCount);
    return pOperatorFactory.release();
}

Operator *SortOperatorFactory::CreateOperator()
{
    auto pSortOperator =
        std::make_unique<SortOperator>(sourceTypes, outputCols, sortCols, sortAscendings, sortNullFirsts);
    return pSortOperator.release();
}

// function implements for class Sort
SortOperator::SortOperator(std::vector<int32_t> &sourceTypes, std::vector<int32_t> &outputCols,
    std::vector<int32_t> &sortCols, std::vector<int32_t> &sortAscendings, std::vector<int32_t> &sortNullFirsts)
{
    this->sourceTypes = sourceTypes;
    this->outputCols = outputCols;
    this->sortCols = sortCols;
    this->sortAscendings = sortAscendings;
    this->sortNullFirsts = sortNullFirsts;
    this->pagesIndex = std::make_unique<PagesIndex>(sourceTypes.data(), sourceTypes.size());
}

SortOperator::~SortOperator() {}

int32_t SortOperator::AddInput(VectorBatch *vecBatch)
{
    inputVecBatches.push_back(vecBatch);
    return 0;
}

// return error code
int32_t SortOperator::GetOutput(vector<VectorBatch *> &outputPages)
{
    pagesIndex->AddVecBatches(inputVecBatches);

    int32_t positionCount = pagesIndex->GetPositionCount();
    if (positionCount <= 0) {
        return 0;
    }

    int32_t sortColCount = sortCols.size();
    int32_t outputColsCount = outputCols.size();

    // first, sort
    int32_t to = positionCount;
    int32_t from = 0;
    int32_t sortColTypes[sortColCount];
    for (int32_t i = 0; i < sortColCount; i++) {
        sortColTypes[i] = sourceTypes[sortCols[i]];
    }

    auto quickSortStart = START();
    pagesIndex->Sort(sortCols.data(), sortColTypes, sortAscendings.data(), sortNullFirsts.data(), sortColCount, from,
        to);
    OP_DEBUG_LOG("quick sort elapsed time : %ld ms.", END(quickSortStart));

    // next, get output
    int32_t maxRowCount = GetMaxRowCount(sourceTypes.data(), outputCols.data(), outputColsCount);
    int32_t vecBatchCount = GetPageCount(positionCount, maxRowCount);
    outputPages.reserve(vecBatchCount);
    int32_t outputTypes[outputColsCount];
    for (int32_t i = 0; i < outputColsCount; i++) {
        outputTypes[i] = sourceTypes[outputCols[i]];
    }

    VectorBatch *vecBatch = nullptr;
    int32_t position = 0;
    int32_t rowCount = 0;
    for (int32_t i = 0; i < vecBatchCount; i++) {
        rowCount = min(maxRowCount, positionCount - position);
        auto start = START();
        OP_DEBUG_LOG("alloc columns elapsed time: %ld ms.", END(start));
        vecBatch = std::make_unique<VectorBatch>(outputColsCount).release();
        pagesIndex->GetOutput(outputCols.data(), outputColsCount, vecBatch, sourceTypes.data(), position, rowCount);
        OP_DEBUG_LOG("get result elapsed time: %ld ms.", END(start));
        position += rowCount;
        outputPages.push_back(vecBatch);
    }
    SetStatus(OMNI_STATUS_FINISHED);
    return 0;
}
} // end of namespace op
} // end of namespace omniruntime