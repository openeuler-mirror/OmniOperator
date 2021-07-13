#include "sort.h"
#include "../../util/type_infer.h"
#include "../../util/debug.h"
#include "../../memory/memory_pool.h"
#include "../../vector/vector_common.h"
#include "../status.h"
#include "../../jit/annotation.h"
#include "../optimization.h"
#include <iostream>
#include <algorithm>
#include <cstring>

using namespace std;
namespace omniruntime {
namespace op {
int32_t getMaxRowCount(int32_t *sourceTypes, int32_t *outputCols, int32_t outputColsCount)
{
    int32_t rowSize = 0;
    int type;
    for (int32_t i = 0; i < outputColsCount; i++) {
        type = sourceTypes[outputCols[i]];
        switch (type)
        {
        case 1:
            rowSize = rowSize + sizeof(int32_t);
            break;
        case 2:
            rowSize = rowSize + sizeof(int64_t);
            break;
        case 3:
            rowSize = rowSize + sizeof(double);
            break;
        default:
            break;
        }
    }

    int32_t maxRowCount = (MAX_VEC_BATCH_SIZE_IN_BYTES + rowSize - 1) / rowSize;
    return maxRowCount;
}

int32_t getPageCount(int32_t positionCount, int32_t maxRowCount)
{
    return ((positionCount + maxRowCount - 1) / maxRowCount);
}

SortOperatorFactory::SortOperatorFactory(
    int32_t *sourceTypes,
    int32_t sourceTypeCount,
    int32_t *outputCols,
    int32_t outputColCount,
    int32_t *sortCols,
    int32_t *sortAscendings,
    int32_t *sortNullFirsts,
    int32_t sortColCount)
{
    int32_t intByteLen = sizeof(int32_t);

    this->sourceTypes = new int32_t[sourceTypeCount];
    memcpy(this->sourceTypes, sourceTypes, sourceTypeCount * intByteLen);
    this->sourceTypeCount = sourceTypeCount;

    this->outputCols = new int32_t[outputColCount];
    memcpy(this->outputCols, outputCols, outputColCount * intByteLen);
    this->outputColCount = outputColCount;

    int32_t sortColByteLen = sortColCount * intByteLen;
    this->sortCols = new int32_t[sortColCount];
    memcpy(this->sortCols, sortCols, sortColByteLen);

    this->sortAscendings = new int32_t[sortColCount];
    memcpy(this->sortAscendings, sortAscendings, sortColByteLen);

    this->sortNullFirsts = new int32_t[sortColCount];
    memcpy(this->sortNullFirsts, sortNullFirsts, sortColByteLen);

    this->sortColCount = sortColCount;
}

SortOperatorFactory::~SortOperatorFactory()
{
    delete[] sourceTypes;
    delete[] outputCols;
    delete[] sortCols;
    delete[] sortAscendings;
    delete[] sortNullFirsts;
}

SortOperatorFactory * SortOperatorFactory::createSortOperatorFactory(
    int32_t *sourceTypes,
    int32_t sourceTypeCount,
    int32_t *outputCols,
    int32_t outputColCount,
    int32_t *sortCols,
    int32_t *sortAscendings,
    int32_t *sortNullFirsts,
    int32_t sortColCount)
{
    SortOperatorFactory *operatorFactory = new SortOperatorFactory(
        sourceTypes,
        sourceTypeCount,
        outputCols,
        outputColCount,
        sortCols,
        sortAscendings,
        sortNullFirsts,
        sortColCount);
    return operatorFactory;
}

Operator * SortOperatorFactory::createOperator()
{
    SortOperator *sortOperator = new SortOperator(
        sourceTypes,
        sourceTypeCount,
        outputCols,
        outputColCount,
        sortCols,
        sortAscendings,
        sortNullFirsts,
        sortColCount);
    return sortOperator;
}

// function implements for class Sort
SortOperator::SortOperator(
    int32_t *sourceTypes,
    int32_t typesCount,
    int32_t *outputCols,
    int32_t outputColsCount,
    int32_t *sortCols,
    int32_t *sortAscendings,
    int32_t *sortNullFirsts,
    int32_t sortColCount)
{
    this->sourceTypes = sourceTypes;
    this->typesCount = typesCount;
    this->outputCols = outputCols;
    this->outputColsCount = outputColsCount;
    this->sortCols = sortCols;
    this->sortAscendings = sortAscendings;
    this->sortNullFirsts = sortNullFirsts;
    this->sortColCount = sortColCount;
    this->pagesIndex = new PagesIndex(sourceTypes, typesCount);
}

SortOperator::~SortOperator()
{
    delete pagesIndex;
}

int32_t SortOperator::addInput(VectorBatch *vecBatch)
{
    inputVecBatches.push_back(vecBatch);
    return 0;
}

// return error code
int32_t SortOperator::getOutput(vector<VectorBatch *>& outputPages)
{
    pagesIndex->addVecBatches(inputVecBatches);

    int32_t positionCount = pagesIndex->getPositionCount();
    if (positionCount <= 0) {
        return 0;
    }

    // first, sort
    int32_t to = positionCount;
    int32_t from = 0;
    int32_t sortColTypes[sortColCount];
    for (int32_t i = 0; i < sortColCount; i++) {
        sortColTypes[i] = sourceTypes[sortCols[i]];
    }

    auto quickSortStart = START();
    pagesIndex->sort(
        sortCols,
        sortColTypes,
        sortAscendings,
        sortNullFirsts,
        sortColCount,
        from,
        to);   
    OP_DEBUG_LOG("quick sort elapsed time : %ld ms.", END(quickSortStart));

    // next, get output
    int32_t maxRowCount = getMaxRowCount(sourceTypes, outputCols, outputColsCount);
    int32_t vecBatchCount = getPageCount(positionCount, maxRowCount);
    outputPages.reserve(vecBatchCount);

    VectorBatch *vecBatch = nullptr;
    int32_t outputTypes[outputColsCount];
    for (int colIdx = 0; colIdx < outputColsCount; ++colIdx) {
        outputTypes[colIdx] = sourceTypes[outputCols[colIdx]];
    }
    int32_t position = 0;
    int32_t rowCount = 0;
    for (int32_t i = 0; i < vecBatchCount; i++) {
        rowCount = min(maxRowCount, positionCount - position);
        auto start = START();
        OP_DEBUG_LOG("alloc columns elapsed time: %ld ms.", END(start));
        vecBatch = new VectorBatch(outputTypes, outputColsCount, rowCount);
        pagesIndex->getOutput(outputCols, outputColsCount, vecBatch, sourceTypes, position, rowCount);
        OP_DEBUG_LOG("get result elapsed time: %ld ms.", END(start));
        position += rowCount;
        outputPages.push_back(vecBatch);
    }
    setStatus(OMNI_STATUS_FINISHED);
    return 0;
}

} // end of namespace op
} // end of namespace omniruntime