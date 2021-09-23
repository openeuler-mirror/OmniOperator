/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: sort implementations
 */
#include "sort.h"
#include "../../util/type_util.h"
#include "../../util/debug.h"
#include "../../vector/vector_common.h"
#include "../../vector/vector_helper.h"
#include "../util/operator_util.h"

using namespace std;
namespace omniruntime {
namespace op {
using namespace omniruntime::vec;

SortOperatorFactory::SortOperatorFactory(const VecTypes &sourceTypes, int32_t *outputCols, int32_t outputColCount,
    int32_t *sortCols, int32_t *sortAscendings, int32_t *sortNullFirsts, int32_t sortColCount)
{
    this->sourceTypes = std::make_unique<VecTypes>(sourceTypes);
    this->outputCols.insert(this->outputCols.end(), outputCols, outputCols + outputColCount);
    this->sortCols.insert(this->sortCols.end(), sortCols, sortCols + sortColCount);
    this->sortAscendings.insert(this->sortAscendings.end(), sortAscendings, sortAscendings + sortColCount);
    this->sortNullFirsts.insert(this->sortNullFirsts.end(), sortNullFirsts, sortNullFirsts + sortColCount);
}

SortOperatorFactory::~SortOperatorFactory() {}

SortOperatorFactory *SortOperatorFactory::CreateSortOperatorFactory(const VecTypes &vecTypes, int32_t *outputCols,
    int32_t outputColCount, int32_t *sortCols, int32_t *sortAscendings, int32_t *sortNullFirsts, int32_t sortColCount)
{
    auto pOperatorFactory = std::make_unique<SortOperatorFactory>(vecTypes, outputCols, outputColCount, sortCols,
        sortAscendings, sortNullFirsts, sortColCount);
    return pOperatorFactory.release();
}

Operator *SortOperatorFactory::CreateOperator()
{
    auto pSortOperator =
        std::make_unique<SortOperator>(*(sourceTypes.get()), outputCols, sortCols, sortAscendings, sortNullFirsts);
    return pSortOperator.release();
}

// function implements for class Sort
SortOperator::SortOperator(const VecTypes &vecTypes, std::vector<int32_t> &outputCols, std::vector<int32_t> &sortCols,
    std::vector<int32_t> &sortAscendings, std::vector<int32_t> &sortNullFirsts)
    : sourceTypes(vecTypes)
{
    this->outputCols = outputCols;
    this->sortCols = sortCols;
    this->sortAscendings = sortAscendings;
    this->sortNullFirsts = sortNullFirsts;
    this->pagesIndex = std::make_unique<PagesIndex>(vecTypes);
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
        sortColTypes[i] = sourceTypes.GetIds()[sortCols[i]];
    }

    auto quickSortStart = START();
    pagesIndex->Sort(sortCols.data(), sortColTypes, sortAscendings.data(), sortNullFirsts.data(), sortColCount, from,
        to);
    OP_DEBUG_LOG("quick sort elapsed time : %ld ms.", END(quickSortStart));

    // next, get output
    int32_t maxRowCount = OperatorUtil::GetMaxRowCount(sourceTypes.Get(), outputCols.data(), outputColsCount);
    int32_t vecBatchCount = OperatorUtil::GetVecBatchCount(positionCount, maxRowCount);
    outputPages.reserve(vecBatchCount);

    VectorBatch *vecBatch = nullptr;
    int32_t position = 0;
    int32_t rowCount = 0;
    for (int32_t i = 0; i < vecBatchCount; i++) {
        rowCount = min(maxRowCount, positionCount - position);
        auto start = START();
        OP_DEBUG_LOG("alloc columns elapsed time: %ld ms.", END(start));
        vecBatch = std::make_unique<VectorBatch>(outputColsCount).release();
        pagesIndex->GetOutput(outputCols.data(), outputColsCount, vecBatch, sourceTypes.GetIds(), position, rowCount,
            this->vecAllocator);
        OP_DEBUG_LOG("get result elapsed time: %ld ms.", END(start));
        position += rowCount;
        outputPages.push_back(vecBatch);
    }
    SetStatus(OMNI_STATUS_FINISHED);
    return 0;
}
} // end of namespace op
} // end of namespace omniruntime