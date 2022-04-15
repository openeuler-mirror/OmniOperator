/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: window implementations
 */

#include "window.h"
#include "../sort/sort.h"

using namespace std;
using namespace omniruntime::vec;
namespace omniruntime {
namespace op {
const int MID_SEARCH_FACTOR = 2;
WindowOperatorFactory::WindowOperatorFactory(const DataTypes &sourceTypes, int32_t *outputCols, int32_t outputColsCount,
    int32_t *windowFunctionTypes, int32_t windowFunctionCount, int32_t *partitionCols, int32_t partitionCount,
    int32_t *preGroupedCols, int32_t preGroupedCount, int32_t *sortCols, int32_t *sortAscendings,
    int32_t *sortNullFirsts, int32_t sortColCount, int32_t preSortedChannelPrefix, int32_t expectedPositions,
    const DataTypes &allTypes, int32_t *argumentChannels, int32_t argumentChannelsCount)
    : sourceTypes(std::make_unique<DataTypes>(sourceTypes)),
      outputColsCount(outputColsCount),
      windowFunctionCount(windowFunctionCount),
      partitionCount(partitionCount),
      preGroupedCount(preGroupedCount),
      sortColCount(sortColCount),
      preSortedChannelPrefix(preSortedChannelPrefix),
      expectedPositions(expectedPositions),
      allTypes(std::make_unique<DataTypes>(allTypes)),
      argumentChannelsCount(argumentChannelsCount)
{
    this->outputCols.insert(this->outputCols.begin(), outputCols, outputCols + outputColsCount);
    this->windowFunctionTypes.insert(this->windowFunctionTypes.begin(), windowFunctionTypes,
        windowFunctionTypes + windowFunctionCount);
    this->partitionCols.insert(this->partitionCols.begin(), partitionCols, partitionCols + partitionCount);
    this->preGroupedCols.insert(this->preGroupedCols.begin(), preGroupedCols, preGroupedCols + preGroupedCount);
    this->sortCols.insert(this->sortCols.begin(), sortCols, sortCols + sortColCount);
    this->sortAscendings.insert(this->sortAscendings.begin(), sortAscendings, sortAscendings + sortColCount);
    this->sortNullFirsts.insert(this->sortNullFirsts.begin(), sortNullFirsts, sortNullFirsts + sortColCount);
    this->argumentChannels.insert(this->argumentChannels.begin(), argumentChannels,
        argumentChannels + argumentChannelsCount);
}

OmniStatus WindowOperatorFactory::Init()
{
    return OMNI_STATUS_NORMAL;
}

WindowOperatorFactory::~WindowOperatorFactory() = default;

WindowOperatorFactory *WindowOperatorFactory::CreateWindowOperatorFactory(const DataTypes &sourceTypesField,
    int32_t *outputColsField, int32_t outputColsCountField, int32_t *windowFunctionTypesField,
    int32_t windowFunctionCountField, int32_t *partitionColsField, int32_t partitionCountField,
    int32_t *preGroupedColsField, int32_t preGroupedCountField, int32_t *sortColsField, int32_t *sortAscendingsField,
    int32_t *sortNullFirstsField, int32_t sortColCountField, int32_t preSortedChannelPrefixField,
    int32_t expectedPositionsField, const DataTypes &allTypesField, int32_t *argumentChannelsField,
    int32_t argumentChannelsCountField)
{
    auto operatorFactory =
        new WindowOperatorFactory(sourceTypesField, outputColsField, outputColsCountField, windowFunctionTypesField,
        windowFunctionCountField, partitionColsField, partitionCountField, preGroupedColsField, preGroupedCountField,
        sortColsField, sortAscendingsField, sortNullFirstsField, sortColCountField, preSortedChannelPrefixField,
        expectedPositionsField, allTypesField, argumentChannelsField, argumentChannelsCountField);
    operatorFactory->Init();
    return operatorFactory;
}

Operator *WindowOperatorFactory::CreateOperator()
{
    auto windowOperator =
        new WindowOperator(*(sourceTypes), outputCols, outputColsCount, windowFunctionTypes, windowFunctionCount,
        partitionCols, partitionCount, preGroupedCols, preGroupedCount, sortCols, sortAscendings, sortNullFirsts,
        sortColCount, preSortedChannelPrefix, expectedPositions, *(allTypes), argumentChannels, argumentChannelsCount);
    windowOperator->Init();
    return windowOperator;
}

WindowOperator::WindowOperator(const type::DataTypes &sourceTypes, std::vector<int32_t> &outputCols,
    int32_t outputColsCount, std::vector<int32_t> &windowFunctionTypes, int32_t windowFunctionCount,
    std::vector<int32_t> &partitionCols, int32_t partitionCount, std::vector<int32_t> &preGroupedCols,
    int32_t preGroupedCount, std::vector<int32_t> &sortCols, std::vector<int32_t> &sortAscendings,
    std::vector<int32_t> &sortNullFirsts, int32_t sortColCount, int32_t preSortedChannelPrefix,
    int32_t expectedPositions, const type::DataTypes &allTypes, std::vector<int32_t> &argumentChannels,
    int32_t argumentChannelsCount)
    : sourceTypes(sourceTypes),
      typesCount(sourceTypes.GetSize()),
      outputCols(outputCols),
      outputColsCount(outputColsCount),
      windowFunctionTypes(windowFunctionTypes),
      windowFunctionCount(windowFunctionCount),
      partitionCols(partitionCols),
      partitionCount(partitionCount),
      preGroupedCols(preGroupedCols),
      preGroupedCount(preGroupedCount),
      originSortCols(sortCols),
      originSortColCount(sortColCount),
      sortColCount(sortColCount + partitionCount),
      preSortedChannelPrefix(preSortedChannelPrefix),
      expectedPositions(expectedPositions),
      allTypes(allTypes),
      pendingInput(nullptr),
      partition(nullptr),
      argumentChannels(argumentChannels),
      argumentChannelsCount(argumentChannelsCount)
{
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
                    sourceTypes.Get()[argumentChannels[i]], allTypes.Get()[sourceTypes.GetSize() + i], vecAllocator)));
                break;
            case OMNI_AGGREGATION_TYPE_COUNT_ALL:
                windowFunctions.push_back(std::move(make_unique<AggregateWindowFunction>(argumentChannels[i], type,
                    DataType(OMNI_NONE), allTypes.Get()[sourceTypes.GetSize() + i], vecAllocator)));
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

    std::vector<DataType> finalOutputTypes;
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
    std::vector<type::DataType> &outputTypes, int32_t position, VectorBatch *&vecBatch, int32_t &rowCount)
{
    rowCount = min(maxRowCount, positionCount - position);
    vecBatch = new VectorBatch(finalOutputColsCount, rowCount);

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

void WindowOperator::InitResultVectors(const std::vector<DataType> &outputTypesField, VectorBatch *&vecBatchField,
    const int32_t &rowCountField, const int32_t outputColsCountField, const int finalOutputColsCountField) const
{
    for (int colIndex = outputColsCountField; colIndex < finalOutputColsCountField; ++colIndex) {
        auto type = outputTypesField[colIndex];
        switch (type.GetId()) {
            case OMNI_BOOLEAN:
                vecBatchField->SetVector(colIndex, new BooleanVector(vecAllocator, rowCountField));
                break;
            case OMNI_INT:
            case OMNI_DATE32: {
                vecBatchField->SetVector(colIndex, new IntVector(vecAllocator, rowCountField));
                break;
            }
            case OMNI_LONG:
            case OMNI_DECIMAL64: {
                vecBatchField->SetVector(colIndex, new LongVector(vecAllocator, rowCountField));
                break;
            }
            case OMNI_DOUBLE: {
                vecBatchField->SetVector(colIndex, new DoubleVector(vecAllocator, rowCountField));
                break;
            }
            case OMNI_SHORT: {
                vecBatchField->SetVector(colIndex, new IntVector(vecAllocator, rowCountField));
                break;
            }
            case OMNI_VARCHAR:
            case OMNI_CHAR: {
                uint32_t width = (static_cast<const VarcharDataType *>(&type))->GetWidth();
                vecBatchField->SetVector(colIndex,
                    new VarcharVector(vecAllocator, rowCountField * static_cast<int32_t>(width), rowCountField));
                break;
            }
            case OMNI_DECIMAL128: {
                vecBatchField->SetVector(colIndex, new Decimal128Vector(vecAllocator, rowCountField));
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