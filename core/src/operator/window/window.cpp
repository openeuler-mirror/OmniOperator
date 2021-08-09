/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: window implementations
 */
#include "window.h"
#include "../sort/sort.h"
#include "../status.h"

using namespace std;
namespace omniruntime {
namespace op {
    using namespace omniruntime::vec;
    WindowOperatorFactory::WindowOperatorFactory(int32_t *sourceTypes, int32_t typesCount, int32_t *outputCols,
    int32_t outputColsCount, int32_t *windowFunctionTypes, int32_t windowFunctionCount, int32_t *partitionCols,
    int32_t partitionCount, int32_t *preGroupedCols, int32_t preGroupedCount, int32_t *sortCols,
    int32_t *sortAscendings, int32_t *sortNullFirsts, int32_t sortColCount, int32_t preSortedChannelPrefix,
    int32_t expectedPositions, int32_t *allTypes, int32_t allCount, int32_t *argumentChannels,
    int32_t argumentChannelsCount)
{
    this->typesCount = typesCount;
    this->outputColsCount = outputColsCount;
    this->windowFunctionCount = windowFunctionCount;
    this->partitionCount = partitionCount;
    this->preGroupedCount = preGroupedCount;
    this->sortColCount = sortColCount;
    this->preSortedChannelPrefix = preSortedChannelPrefix;
    this->expectedPositions = expectedPositions;
    this->allCount = allCount;
    this->argumentChannelsCount = argumentChannelsCount;

    this->sourceTypes.insert(this->sourceTypes.begin(), sourceTypes, sourceTypes + typesCount);
    this->outputCols.insert(this->outputCols.begin(), outputCols, outputCols + outputColsCount);
    this->windowFunctionTypes.insert(this->windowFunctionTypes.begin(), windowFunctionTypes,
        windowFunctionTypes + windowFunctionCount);
    this->partitionCols.insert(this->partitionCols.begin(), partitionCols, partitionCols + partitionCount);
    this->preGroupedCols.insert(this->preGroupedCols.begin(), preGroupedCols, preGroupedCols + preGroupedCount);
    this->sortCols.insert(this->sortCols.begin(), sortCols, sortCols + sortColCount);
    this->sortAscendings.insert(this->sortAscendings.begin(), sortAscendings, sortAscendings + sortColCount);
    this->sortNullFirsts.insert(this->sortNullFirsts.begin(), sortNullFirsts, sortNullFirsts + sortColCount);
    this->allTypes.insert(this->allTypes.begin(), allTypes, allTypes + allCount);
    this->argumentChannels.insert(this->argumentChannels.begin(), argumentChannels,
        argumentChannels + argumentChannelsCount);
}

OmniStatus WindowOperatorFactory::Init()
{
    return OMNI_STATUS_NORMAL;
}

WindowOperatorFactory::~WindowOperatorFactory() {}

WindowOperatorFactory *WindowOperatorFactory::CreateWindowOperatorFactory(int32_t *sourceTypes, int32_t typesCount,
    int32_t *outputCols, int32_t outputColsCount, int32_t *windowFunctionTypes, int32_t windowFunctionCount,
    int32_t *partitionCols, int32_t partitionCount, int32_t *preGroupedCols, int32_t preGroupedCount, int32_t *sortCols,
    int32_t *sortAscendings, int32_t *sortNullFirsts, int32_t sortColCount, int32_t preSortedChannelPrefix,
    int32_t expectedPositions, int32_t *allTypes, int32_t allCount, int32_t *argumentChannels,
    int32_t argumentChannelsCount)
{
    auto operatorFactory = make_unique<WindowOperatorFactory>(sourceTypes, typesCount, outputCols, outputColsCount,
        windowFunctionTypes, windowFunctionCount, partitionCols, partitionCount, preGroupedCols, preGroupedCount,
        sortCols, sortAscendings, sortNullFirsts, sortColCount, preSortedChannelPrefix, expectedPositions, allTypes,
        allCount, argumentChannels, argumentChannelsCount);
    operatorFactory->Init();
    return operatorFactory.release();
}

Operator *WindowOperatorFactory::CreateOperator()
{
    auto windowOperator = make_unique<WindowOperator>(sourceTypes, typesCount, outputCols, outputColsCount,
        windowFunctionTypes, windowFunctionCount, partitionCols, partitionCount, preGroupedCols, preGroupedCount,
        sortCols, sortAscendings, sortNullFirsts, sortColCount, preSortedChannelPrefix, expectedPositions, allTypes,
        allCount, argumentChannels, argumentChannelsCount);
    windowOperator->Init();
    return windowOperator.release();
}

WindowOperator::WindowOperator(vector<int32_t> &sourceTypes, int32_t typesCount, vector<int32_t> &outputCols,
    int32_t outputColsCount, vector<int32_t> &windowFunctionTypes, int32_t windowFunctionCount,
    vector<int32_t> &partitionCols, int32_t partitionCount, vector<int32_t> &preGroupedCols, int32_t preGroupedCount,
    vector<int32_t> &sortCols, vector<int32_t> &sortAscendings, vector<int32_t> &sortNullFirsts, int32_t sortColCount,
    int32_t preSortedChannelPrefix, int32_t expectedPositions, vector<int32_t> &allTypes, int32_t allCount,
    vector<int32_t> &argumentChannels, int32_t argumentChannelsCount)
{
    WindowOperator::sourceTypes = sourceTypes;
    WindowOperator::typesCount = typesCount;
    WindowOperator::outputCols = outputCols;
    WindowOperator::outputColsCount = outputColsCount;
    WindowOperator::windowFunctionTypes = windowFunctionTypes;
    WindowOperator::windowFunctionCount = windowFunctionCount;
    WindowOperator::partitionCols = partitionCols;
    WindowOperator::partitionCount = partitionCount;
    WindowOperator::preGroupedCols = preGroupedCols;
    WindowOperator::preGroupedCount = preGroupedCount;
    originSortCols = sortCols;
    originSortColCount = sortColCount;
    for (int32_t i = 0; i < partitionCount; i++) {
        WindowOperator::sortCols.push_back(partitionCols[i]);
        WindowOperator::sortAscendings.push_back(true);
        WindowOperator::sortNullFirsts.push_back(false);
    }
    for (int32_t i = partitionCount; i < partitionCount + sortColCount; i++) {
        WindowOperator::sortCols.push_back(sortCols[i - partitionCount]);
        WindowOperator::sortAscendings.push_back(sortAscendings[i - partitionCount]);
        WindowOperator::sortNullFirsts.push_back(sortNullFirsts[i - partitionCount]);
    }
    WindowOperator::sortColCount = sortColCount + partitionCount;
    WindowOperator::preSortedChannelPrefix = preSortedChannelPrefix;
    WindowOperator::expectedPositions = expectedPositions;
    WindowOperator::allTypes = allTypes;
    WindowOperator::allCount = allCount;

    WindowOperator::argumentChannels = argumentChannels;
    WindowOperator::argumentChannelsCount = argumentChannelsCount;
    pendingInput = nullptr;
    partition = nullptr;
}

OmniStatus WindowOperator::Init()
{
    OmniStatus ret = OMNI_STATUS_NORMAL;
    pagesIndex = std::move(make_unique<PagesIndex>(sourceTypes.data(), typesCount));
    for (int32_t i = 0; i < windowFunctionCount; i++) {
        switch (windowFunctionTypes[i]) {
            case WIN_ROW_NUMBER:
                windowFunctions.push_back(std::move(make_unique<RowNumberFunction>()));
                break;
            case WIN_RANK:
                windowFunctions.push_back(std::move(make_unique<RankFunction>()));
                break;
            case WIN_SUM:
            case WIN_COUNT:
            case WIN_AVG:
            case WIN_MAX:
            case WIN_MIN:
                windowFunctions.push_back(std::move(make_unique<AggregateWindowFunction>(argumentChannels[i],
                    windowFunctionTypes[i], sourceTypes[argumentChannels[i]])));
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
    int32_t maxRowCount = GetMaxRowCount(allTypes.data(), finalOutputCols, finalOutputColsCount);
    int32_t outputPageCount = GetPageCount(positionCount, maxRowCount);
    outputPages.reserve(outputPageCount);

    int outputTypes[finalOutputColsCount];
    for (int colIdx = 0; colIdx < finalOutputColsCount; ++colIdx) {
        outputTypes[colIdx] = allTypes[finalOutputCols[colIdx]];
    }

    VectorBatch *vecBatch = nullptr;
    int32_t position = 0;
    int32_t rowCount = 0;
    for (int32_t i = 0; i < outputPageCount; i++) {
        ProcessData(positionCount, finalOutputColsCount, maxRowCount, outputTypes, position, vecBatch, rowCount);
        position += rowCount;
        outputPages.push_back(vecBatch);
    }
    SetStatus(OMNI_STATUS_FINISHED);
    return 0;
}

void WindowOperator::ProcessData(int32_t positionCount, int finalOutputColsCount, int32_t maxRowCount, int *outputTypes,
    int32_t position, VectorBatch *&vecBatch, int32_t &rowCount)
{
    rowCount = min(maxRowCount, positionCount - position);
    vecBatch = std::make_unique<VectorBatch>(finalOutputColsCount, rowCount).release();
    vecBatch->NewVectors(outputTypes);
    pagesIndex->GetOutput(outputCols.data(), outputColsCount, vecBatch, sourceTypes.data(), position, rowCount);
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
        partition->ProcessNextRow(vecBatch, j, allTypes.data(), typesCount);
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
            sortColTypes[i] = sourceTypes[sortCols[i]];
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