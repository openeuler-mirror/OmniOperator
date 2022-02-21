/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: window implementations
 */

#include "window.h"
#include "../sort/sort.h"
#include "../status.h"
#include "../util/operator_util.h"
#include "../util/function_type.h"

using namespace std;
namespace omniruntime {
namespace op {
using namespace omniruntime::vec;
WindowOperatorFactory::WindowOperatorFactory(const VecTypes &sourceTypes, int32_t *outputCols, int32_t outputColsCount,
    int32_t *windowFunctionTypes, int32_t windowFunctionCount, int32_t *partitionCols, int32_t partitionCount,
    int32_t *preGroupedCols, int32_t preGroupedCount, int32_t *sortCols, int32_t *sortAscendings,
    int32_t *sortNullFirsts, int32_t sortColCount, int32_t preSortedChannelPrefix, int32_t expectedPositions,
    const VecTypes &allTypes, int32_t *argumentChannels, int32_t argumentChannelsCount)
{
    this->outputColsCount = outputColsCount;
    this->windowFunctionCount = windowFunctionCount;
    this->partitionCount = partitionCount;
    this->preGroupedCount = preGroupedCount;
    this->sortColCount = sortColCount;
    this->preSortedChannelPrefix = preSortedChannelPrefix;
    this->expectedPositions = expectedPositions;
    this->argumentChannelsCount = argumentChannelsCount;

    this->sourceTypes = std::make_unique<VecTypes>(sourceTypes);
    this->outputCols.insert(this->outputCols.begin(), outputCols, outputCols + outputColsCount);
    this->windowFunctionTypes.insert(this->windowFunctionTypes.begin(), windowFunctionTypes,
        windowFunctionTypes + windowFunctionCount);
    this->partitionCols.insert(this->partitionCols.begin(), partitionCols, partitionCols + partitionCount);
    this->preGroupedCols.insert(this->preGroupedCols.begin(), preGroupedCols, preGroupedCols + preGroupedCount);
    this->sortCols.insert(this->sortCols.begin(), sortCols, sortCols + sortColCount);
    this->sortAscendings.insert(this->sortAscendings.begin(), sortAscendings, sortAscendings + sortColCount);
    this->sortNullFirsts.insert(this->sortNullFirsts.begin(), sortNullFirsts, sortNullFirsts + sortColCount);
    this->allTypes = std::make_unique<VecTypes>(allTypes);
    this->argumentChannels.insert(this->argumentChannels.begin(), argumentChannels,
        argumentChannels + argumentChannelsCount);
}

OmniStatus WindowOperatorFactory::Init()
{
    return OMNI_STATUS_NORMAL;
}

WindowOperatorFactory::~WindowOperatorFactory() = default;

WindowOperatorFactory *WindowOperatorFactory::CreateWindowOperatorFactory(const VecTypes &sourceTypes,
    int32_t *outputCols, int32_t outputColsCount, int32_t *windowFunctionTypes, int32_t windowFunctionCount,
    int32_t *partitionCols, int32_t partitionCount, int32_t *preGroupedCols, int32_t preGroupedCount, int32_t *sortCols,
    int32_t *sortAscendings, int32_t *sortNullFirsts, int32_t sortColCount, int32_t preSortedChannelPrefix,
    int32_t expectedPositions, const VecTypes &allTypes, int32_t *argumentChannels, int32_t argumentChannelsCount)
{
    auto operatorFactory = make_unique<WindowOperatorFactory>(sourceTypes, outputCols, outputColsCount,
        windowFunctionTypes, windowFunctionCount, partitionCols, partitionCount, preGroupedCols, preGroupedCount,
        sortCols, sortAscendings, sortNullFirsts, sortColCount, preSortedChannelPrefix, expectedPositions, allTypes,
        argumentChannels, argumentChannelsCount);
    operatorFactory->Init();
    return operatorFactory.release();
}

Operator *WindowOperatorFactory::CreateOperator()
{
    auto windowOperator = make_unique<WindowOperator>(*(sourceTypes), outputCols, outputColsCount, windowFunctionTypes,
        windowFunctionCount, partitionCols, partitionCount, preGroupedCols, preGroupedCount, sortCols, sortAscendings,
        sortNullFirsts, sortColCount, preSortedChannelPrefix, expectedPositions, *(allTypes), argumentChannels,
        argumentChannelsCount);
    windowOperator->Init();
    return windowOperator.release();
}

WindowOperator::WindowOperator(const vec::VecTypes &sourceTypes, std::vector<int32_t> &outputCols,
    int32_t outputColsCount, std::vector<int32_t> &windowFunctionTypes, int32_t windowFunctionCount,
    std::vector<int32_t> &partitionCols, int32_t partitionCount, std::vector<int32_t> &preGroupedCols,
    int32_t preGroupedCount, std::vector<int32_t> &sortCols, std::vector<int32_t> &sortAscendings,
    std::vector<int32_t> &sortNullFirsts, int32_t sortColCount, int32_t preSortedChannelPrefix,
    int32_t expectedPositions, const vec::VecTypes &allTypes, std::vector<int32_t> &argumentChannels,
    int32_t argumentChannelsCount)
    : sourceTypes(sourceTypes), allTypes(allTypes)
{
    this->typesCount = sourceTypes.GetSize();
    this->outputCols = outputCols;
    this->outputColsCount = outputColsCount;
    this->windowFunctionTypes = windowFunctionTypes;
    this->windowFunctionCount = windowFunctionCount;
    this->partitionCols = partitionCols;
    this->partitionCount = partitionCount;
    this->preGroupedCols = preGroupedCols;
    this->preGroupedCount = preGroupedCount;
    this->originSortCols = sortCols;
    this->originSortColCount = sortColCount;
    for (int32_t i = 0; i < partitionCount; i++) {
        this->sortCols.push_back(partitionCols[i]);
        this->sortAscendings.push_back(true);
        this->sortNullFirsts.push_back(false);
    }
    for (int32_t i = partitionCount; i < partitionCount + sortColCount; i++) {
        this->sortCols.push_back(sortCols[i - partitionCount]);
        this->sortAscendings.push_back(sortAscendings[i - partitionCount]);
        this->sortNullFirsts.push_back(sortNullFirsts[i - partitionCount]);
    }
    this->sortColCount = sortColCount + partitionCount;
    this->preSortedChannelPrefix = preSortedChannelPrefix;
    this->expectedPositions = expectedPositions;

    this->argumentChannels = argumentChannels;
    this->argumentChannelsCount = argumentChannelsCount;
    pendingInput = nullptr;
    partition = nullptr;
}

OmniStatus WindowOperator::Init()
{
    OmniStatus ret = OMNI_STATUS_NORMAL;
    pagesIndex = std::move(make_unique<PagesIndex>(sourceTypes));
    for (int32_t i = 0; i < windowFunctionCount; i++) {
        auto type = windowFunctionTypes[i];
        switch (type) {
            case OMNI_WINDOW_TYPE_ROW_NUMBER:
                windowFunctions.push_back(std::move(make_unique<RowNumberFunction>()));
                break;
            case OMNI_WINDOW_TYPE_RANK:
                windowFunctions.push_back(std::move(make_unique<RankFunction>()));
                break;
            // for aggregate function we use AggregateType
            case OMNI_AGGREGATION_TYPE_SUM:
            case OMNI_AGGREGATION_TYPE_COUNT_COLUMN:
            case OMNI_AGGREGATION_TYPE_AVG:
            case OMNI_AGGREGATION_TYPE_MAX:
            case OMNI_AGGREGATION_TYPE_MIN:
                windowFunctions.push_back(std::move(make_unique<AggregateWindowFunction>(argumentChannels[i], type,
                    sourceTypes.Get()[argumentChannels[i]], allTypes.Get()[sourceTypes.GetSize() + i])));
                break;
            default:
                ret = OMNI_STATUS_ERROR;
                break;
        }
    }
    return ret;
}

WindowOperator::~WindowOperator()
{
    delete pendingInput;
}

int32_t WindowOperator::AddInput(VectorBatch *vecBatch)
{
    inputVecBatches.push_back(vecBatch);
    return 0;
}

int32_t WindowOperator::GetOutput(vector<VectorBatch *> &outputPages)
{
    Initialization();
    int32_t positionCount = pagesIndex->GetPositionCount();
    int finalOutputColsCount = 0;
    if (positionCount <= 0) {
        return 0;
    }
    FinishPagesIndex();

    // first, build the final output col number according to the outputCols and additional cols created by the window
    int32_t allCount = allTypes.GetSize();
    int finalOutputCols[allCount];
    for (int32_t i = 0; i < outputColsCount; i++) {
        finalOutputCols[finalOutputColsCount] = outputCols[i];
        finalOutputColsCount++;
    }
    for (int32_t i = typesCount; i < allCount; i++) {
        finalOutputCols[finalOutputColsCount] = i;
        finalOutputColsCount++;
    }

    // next, get output
    int32_t maxRowCount = OperatorUtil::GetMaxRowCount(allTypes.Get(), finalOutputCols, finalOutputColsCount);
    int32_t outputPageCount = OperatorUtil::GetVecBatchCount(positionCount, maxRowCount);
    outputPages.reserve(outputPageCount);

    std::vector<VecType> finalOutputTypes;
    finalOutputTypes.reserve(finalOutputColsCount);
    for (int colIdx = 0; colIdx < finalOutputColsCount; ++colIdx) {
        finalOutputTypes.push_back(allTypes.Get()[finalOutputCols[colIdx]]);
    }

    VectorBatch *vecBatch = nullptr;
    int32_t position = 0;
    int32_t rowCount = 0;
    for (int32_t i = 0; i < outputPageCount; i++) {
        ProcessData(positionCount, finalOutputColsCount, maxRowCount, finalOutputTypes, position, vecBatch, rowCount);
        position += rowCount;
        outputPages.push_back(vecBatch);
    }

    VectorHelper::FreeVecBatches(this->inputVecBatches);
    SetStatus(OMNI_STATUS_FINISHED);
    return 0;
}

void WindowOperator::ProcessData(int32_t positionCount, int finalOutputColsCount, int32_t maxRowCount,
    std::vector<vec::VecType> &outputTypes, int32_t position, VectorBatch *&vecBatch, int32_t &rowCount)
{
    rowCount = min(maxRowCount, positionCount - position);
    vecBatch = std::make_unique<VectorBatch>(finalOutputColsCount, rowCount).release();

    // the data of input columns will create vectors in pageIndex GetOutput, we need to create the vector for the window
    // result
    InitResultVectors(outputTypes, vecBatch, rowCount, outputColsCount, finalOutputColsCount);

    // build the output data with original input vecBatch, input data are not changed in window operator
    // we add extra columns of window result to the output vecBatch in window partition
    pagesIndex->GetOutput(outputCols.data(), outputColsCount, vecBatch, sourceTypes.GetIds(), position, rowCount,
        GetVecAllocator());
    for (int32_t j = 0; j < rowCount; j++) {
        if (partition == nullptr || !partition->HasNext()) {
            int32_t partitionStart = partition == nullptr ? 0 : partition->GetPartitionEnd();
            if (partitionStart >= pagesIndex->GetPositionCount()) {
                partition = nullptr;
                break;
            }
            int32_t partitionEnd = FindGroupEnd(pagesIndex.get(), unGroupedPartitionHashStrategy.get(), partitionStart);
            partition = make_unique<WindowPartition>(pagesIndex.get(), partitionStart, partitionEnd, outputCols.data(),
                outputColsCount, windowFunctions, peerGroupHashStrategy.get());
        }
        partition->ProcessNextRow(vecBatch, j);
    }
}

void WindowOperator::InitResultVectors(const std::vector<VecType> &outputTypes, VectorBatch *&vecBatch,
    const int32_t &rowCount, const int32_t outputColsCount, const int finalOutputColsCount) const
{
    for (int colIndex = outputColsCount; colIndex < finalOutputColsCount; ++colIndex) {
        auto type = outputTypes[colIndex];
        switch (type.GetId()) {
            case OMNI_VEC_TYPE_BOOLEAN:
                vecBatch->SetVector(colIndex, new BooleanVector(vecAllocator, rowCount));
                break;
            case OMNI_VEC_TYPE_INT:
            case OMNI_VEC_TYPE_DATE32: {
                vecBatch->SetVector(colIndex, new IntVector(vecAllocator, rowCount));
                break;
            }
            case OMNI_VEC_TYPE_LONG:
            case OMNI_VEC_TYPE_DECIMAL64: {
                vecBatch->SetVector(colIndex, new LongVector(vecAllocator, rowCount));
                break;
            }
            case OMNI_VEC_TYPE_DOUBLE: {
                vecBatch->SetVector(colIndex, new DoubleVector(vecAllocator, rowCount));
                break;
            }
            case OMNI_VEC_TYPE_SHORT: {
                vecBatch->SetVector(colIndex, new IntVector(vecAllocator, rowCount));
                break;
            }
            case OMNI_VEC_TYPE_VARCHAR:
            case OMNI_VEC_TYPE_CHAR: {
                int32_t width = (static_cast<const VarcharVecType *>(&type))->GetWidth();
                vecBatch->SetVector(colIndex, new VarcharVector(vecAllocator, rowCount * width, rowCount));
                break;
            }
            case OMNI_VEC_TYPE_DECIMAL128: {
                vecBatch->SetVector(colIndex, new Decimal128Vector(vecAllocator, rowCount));
                break;
            }
            default: {
                break;
            }
        }
    }
}

void WindowOperator::Initialization()
{
    pagesIndex->AddVecBatches(inputVecBatches);

    // right now we assume the pregroup and presort are null
    preGroupedPartitionHashStrategy = make_unique<PagesHashStrategy>(pagesIndex->GetColumns(), pagesIndex->GetTypes(),
        pagesIndex->GetTypesCount(), preGroupedCols.data(), preGroupedCount);
    unGroupedPartitionHashStrategy = make_unique<PagesHashStrategy>(pagesIndex->GetColumns(), pagesIndex->GetTypes(),
        pagesIndex->GetTypesCount(), partitionCols.data(), partitionCount);
    preSortedPartitionHashStrategy = make_unique<PagesHashStrategy>(pagesIndex->GetColumns(), pagesIndex->GetTypes(),
        pagesIndex->GetTypesCount(), preGroupedCols.data(), preGroupedCount);
    peerGroupHashStrategy = make_unique<PagesHashStrategy>(pagesIndex->GetColumns(), pagesIndex->GetTypes(),
        pagesIndex->GetTypesCount(), originSortCols.data(), originSortColCount);
}

void WindowOperator::FinishPagesIndex()
{
    SortPagesIndexIfNecessary();
}

void WindowOperator::SortPagesIndexIfNecessary()
{
    if (pagesIndex->GetPositionCount() > 1 && sortColCount != 0) {
        int32_t sortColTypes[sortColCount];
        for (int32_t i = 0; i < sortColCount; i++) {
            sortColTypes[i] = sourceTypes.GetIds()[sortCols[i]];
        }

        int32_t startPosition = 0;
        auto positionCount = pagesIndex->GetPositionCount();
        while (startPosition < positionCount) {
            int32_t endPosition = FindGroupEnd(pagesIndex.get(), preSortedPartitionHashStrategy.get(), startPosition);
            pagesIndex->Sort(sortCols.data(), sortColTypes, sortAscendings.data(), sortNullFirsts.data(), sortColCount,
                startPosition, endPosition);
            startPosition = endPosition;
        }
    }
}

int32_t FindGroupEnd(PagesIndex *pagesIndex, PagesHashStrategy *pagesHashStrategy, int32_t startPosition)
{
    int32_t left = startPosition;
    int32_t right = pagesIndex->GetPositionCount();

    while (left + 1 < right) {
        int32_t middle = left + (right - left) / MID_SEARCH_FACTOR;

        if (PositionEqualsPosition(pagesIndex, pagesHashStrategy, startPosition, middle)) {
            left = middle;
        } else {
            right = middle;
        }
    }

    return right;
}
}
}