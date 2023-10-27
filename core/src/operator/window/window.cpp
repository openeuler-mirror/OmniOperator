/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: window implementations
 */

#include "window.h"

using namespace std;
using namespace omniruntime::vec;
namespace omniruntime {
namespace op {
const int MID_SEARCH_FACTOR = 2;
WindowOperatorFactory::WindowOperatorFactory(const DataTypes &sourceTypes, int32_t *outputCols, int32_t outputColsCount,
    int32_t *windowFunctionTypes, int32_t windowFunctionCount, int32_t *partitionCols, int32_t partitionCount,
    int32_t *preGroupedCols, int32_t preGroupedCount, int32_t *sortCols, int32_t *sortAscendings,
    int32_t *sortNullFirsts, int32_t sortColCount, int32_t preSortedChannelPrefix, int32_t expectedPositions,
    const DataTypes &allTypes, int32_t *argumentChannels, int32_t argumentChannelsCount, int32_t *windowFrameTypesField,
    int32_t *windowFrameStartTypesField, int32_t *windowFrameStartChannelsField, int32_t *windowFrameEndTypesField,
    int32_t *windowFrameEndChannelsField, bool isOverflowAsNullField)
    : sourceTypes(sourceTypes),
      outputColsCount(outputColsCount),
      windowFunctionCount(windowFunctionCount),
      partitionCount(partitionCount),
      preGroupedCount(preGroupedCount),
      sortColCount(sortColCount),
      preSortedChannelPrefix(preSortedChannelPrefix),
      expectedPositions(expectedPositions),
      allTypes(allTypes),
      argumentChannelsCount(argumentChannelsCount),
      isOverflowAsNull(isOverflowAsNullField)
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
    this->windowFrameTypes.insert(this->windowFrameTypes.begin(), windowFrameTypesField,
        windowFrameTypesField + windowFunctionCount); // each windowFunction has one windowFrame
    this->windowFrameStartTypes.insert(this->windowFrameStartTypes.begin(), windowFrameStartTypesField,
        windowFrameStartTypesField + windowFunctionCount);
    this->windowFrameStartChannels.insert(this->windowFrameStartChannels.begin(), windowFrameStartChannelsField,
        windowFrameStartChannelsField + windowFunctionCount);
    this->windowFrameEndTypes.insert(this->windowFrameEndTypes.begin(), windowFrameEndTypesField,
        windowFrameEndTypesField + windowFunctionCount);
    this->windowFrameEndChannels.insert(this->windowFrameEndChannels.begin(), windowFrameEndChannelsField,
        windowFrameEndChannelsField + windowFunctionCount);
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
    int32_t argumentChannelsCountField, int32_t *windowFrameTypesField, int32_t *windowFrameStartTypesField,
    int32_t *windowFrameStartChannelsField, int32_t *windowFrameEndTypesField, int32_t *windowFrameEndChannelsField,
    bool isOverflowAsNullField)
{
    auto operatorFactory = new WindowOperatorFactory(sourceTypesField, outputColsField, outputColsCountField,
        windowFunctionTypesField, windowFunctionCountField, partitionColsField, partitionCountField,
        preGroupedColsField, preGroupedCountField, sortColsField, sortAscendingsField, sortNullFirstsField,
        sortColCountField, preSortedChannelPrefixField, expectedPositionsField, allTypesField, argumentChannelsField,
        argumentChannelsCountField, windowFrameTypesField, windowFrameStartTypesField, windowFrameStartChannelsField,
        windowFrameEndTypesField, windowFrameEndChannelsField, isOverflowAsNullField);
    operatorFactory->Init();
    return operatorFactory;
}

Operator *WindowOperatorFactory::CreateOperator()
{
    auto windowOperator = new WindowOperator(sourceTypes, outputCols, outputColsCount, windowFunctionTypes,
        windowFunctionCount, partitionCols, partitionCount, preGroupedCols, preGroupedCount, sortCols, sortAscendings,
        sortNullFirsts, sortColCount, preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels,
        argumentChannelsCount, windowFrameTypes, windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes,
        windowFrameEndChannels, isOverflowAsNull);
    windowOperator->Init();
    return windowOperator;
}

WindowOperator::WindowOperator(const type::DataTypes &sourceTypes, std::vector<int32_t> &outputCols,
    int32_t outputColsCount, std::vector<int32_t> &windowFunctionTypes, int32_t windowFunctionCount,
    std::vector<int32_t> &partitionCols, int32_t partitionCount, std::vector<int32_t> &preGroupedCols,
    int32_t preGroupedCount, std::vector<int32_t> &sortCols, std::vector<int32_t> &sortAscendings,
    std::vector<int32_t> &sortNullFirsts, int32_t sortColCount, int32_t preSortedChannelPrefix,
    int32_t expectedPositions, const type::DataTypes &allTypes, std::vector<int32_t> &argumentChannels,
    int32_t argumentChannelsCount, const std::vector<int32_t> &windowFrameTypes,
    const std::vector<int32_t> &windowFrameStartTypes, const std::vector<int32_t> &windowFrameStartChannels,
    const std::vector<int32_t> &windowFrameEndTypes, const std::vector<int32_t> &windowFrameEndChannels,
    bool isOverflowAsNull)
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
      argumentChannelsCount(argumentChannelsCount),
      windowFrameTypes(windowFrameTypes),
      windowFrameStartTypes(windowFrameStartTypes),
      windowFrameStartChannels(windowFrameStartChannels),
      windowFrameEndTypes(windowFrameEndTypes),
      windowFrameEndChannels(windowFrameEndChannels),
      isOverflowAsNull(isOverflowAsNull)
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
        auto windowFrame = std::make_unique<WindowFrameInfo>(static_cast<FrameType>(windowFrameTypes[i]),
            static_cast<FrameBoundType>(windowFrameStartTypes[i]), windowFrameStartChannels[i],
            static_cast<FrameBoundType>(windowFrameEndTypes[i]), windowFrameEndChannels[i]);
        switch (type) {
            case OMNI_WINDOW_TYPE_ROW_NUMBER:
                windowFunctions.push_back(std::move(make_unique<RowNumberFunction>(std::move(windowFrame),
                    NoneDataType::Instance(), allTypes.GetType(sourceTypes.GetSize() + i))));
                break;
            case OMNI_WINDOW_TYPE_RANK:
                windowFunctions.push_back(std::move(make_unique<RankFunction>(std::move(windowFrame),
                    NoneDataType::Instance(), allTypes.GetType(sourceTypes.GetSize() + i))));
                break;
            // for aggregate function we use AggregateType
            case OMNI_AGGREGATION_TYPE_SUM:
            case OMNI_AGGREGATION_TYPE_COUNT_COLUMN:
            case OMNI_AGGREGATION_TYPE_AVG:
            case OMNI_AGGREGATION_TYPE_MAX:
            case OMNI_AGGREGATION_TYPE_MIN:
            case OMNI_AGGREGATION_TYPE_FIRST_INCLUDENULL:
            case OMNI_AGGREGATION_TYPE_FIRST_IGNORENULL:
                windowFunctions.push_back(std::move(make_unique<AggregateWindowFunction>(argumentChannels[i], type,
                    sourceTypes.GetType(argumentChannels[i]), allTypes.GetType(sourceTypes.GetSize() + i),
                    std::move(windowFrame), isOverflowAsNull)));
                break;
            case OMNI_AGGREGATION_TYPE_COUNT_ALL:
                windowFunctions.push_back(
                    std::move(make_unique<AggregateWindowFunction>(argumentChannels[i], type, NoneDataType::Instance(),
                    allTypes.GetType(sourceTypes.GetSize() + i), std::move(windowFrame), isOverflowAsNull)));
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
    pagesIndex->AddVecBatch(vecBatch);
    return 0;
}

void WindowOperator::PrepareOutput()
{
    Initialization();
    totalRowCount = pagesIndex->GetRowCount();
    if (totalRowCount == 0) {
        return;
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
    maxRowCount = OperatorUtil::GetMaxRowCount(allTypes.Get(), finalOutputCols, finalOutputColsCount);

    outputTypes.reserve(finalOutputColsCount);
    for (int colIdx = 0; colIdx < finalOutputColsCount; ++colIdx) {
        outputTypes.push_back(allTypes.GetType(finalOutputCols[colIdx]));
    }
    inputVecBatchForAgg = new VectorBatch(totalRowCount);
    inputVecBatchForAgg->ResizeVectorCount(1);
    hasPrepare = true;
}

int32_t WindowOperator::GetOutput(VectorBatch **outputVecBatch)
{
    if (!hasPrepare) {
        PrepareOutput();
    }

    // check whether the output is completed
    if (totalRowCount == 0 || totalRowCount == rowCountOutputted) {
        totalRowCount = 0;
        rowCountOutputted = 0;
        hasPrepare = false;
        delete inputVecBatchForAgg;
        inputVecBatchForAgg = nullptr;
        pagesIndex->Clear();
        SetStatus(OMNI_STATUS_FINISHED);
        return 0;
    }

    /*
     * First, new a vectorBatch dedicated to calling the agg interface with a single column;
     * Second, the vector in the vectorBatch is obtained from the AggregateWindowFunction::Accumulate with
     * argumentChannels.
     * This is mainly to avoid using location information to copy data and construct a continuous vectorBatch when
     * calling agg interface.
     */
    int32_t rowCount = min(maxRowCount, totalRowCount - rowCountOutputted);
    auto output = new VectorBatch(rowCount);
    try {
        ProcessData(output, rowCount);
    } catch (const OmniException &e) {
        // in ProcessData, WindowFunction may be throw exception:
        // when spark sum/avg decimal overflow, it will throw exception when
        // OverflowConfigId==OVERFLOW_CONFIG_EXCEPTION
        totalRowCount = 0;
        rowCountOutputted = 0;
        hasPrepare = false;
        delete inputVecBatchForAgg;
        inputVecBatchForAgg = nullptr;
        pagesIndex->Clear();
        throw e;
    }

    *outputVecBatch = output;
    rowCountOutputted += rowCount;

    if (totalRowCount == rowCountOutputted) {
        totalRowCount = 0;
        rowCountOutputted = 0;
        hasPrepare = false;
        delete inputVecBatchForAgg;
        inputVecBatchForAgg = nullptr;
        pagesIndex->Clear();
        SetStatus(OMNI_STATUS_FINISHED);
    }

    return 0;
}

void WindowOperator::ProcessData(VectorBatch *&outputVecBatch, int32_t rowCount)
{
    // build the output data with original input vecBatch, input data are not changed in window operator
    // we add extra columns of window result to the output vecBatch in window partition
    pagesIndex->GetOutput(outputCols.data(), outputColsCount, outputVecBatch, sourceTypes.GetIds(), rowCountOutputted,
        rowCount);

    // the data of input columns will create vectors in pageIndex GetOutput, we need to create the vector for the window
    // result
    InitResultVectors(outputVecBatch, rowCount);
    for (int32_t j = 0; j < rowCount; j++) {
        if (partition == nullptr || !partition->HasNext()) {
            int32_t partitionStart = partition == nullptr ? 0 : partition->GetPartitionEnd();
            if (partitionStart >= pagesIndex->GetRowCount()) {
                partition = nullptr;
                break;
            }
            int32_t partitionEnd = FindGroupEnd(pagesIndex.get(), unGroupedPartitionHashStrategy.get(), partitionStart);
            partition = make_unique<WindowPartition>(sourceTypes, pagesIndex.get(), partitionStart, partitionEnd,
                outputCols.data(), outputColsCount, windowFunctions, peerGroupHashStrategy.get());
        }
        partition->ProcessNextRow(inputVecBatchForAgg, outputVecBatch, j);
    }
}

void WindowOperator::InitResultVectors(VectorBatch *&vecBatchField, const int32_t &rowCountField) const
{
    for (int colIndex = outputColsCount; colIndex < finalOutputColsCount; ++colIndex) {
        auto &type = outputTypes[colIndex];
        switch (type->GetId()) {
            case OMNI_BOOLEAN:
                vecBatchField->Append(std::make_unique<Vector<bool>>(rowCountField).release());
                break;
            case OMNI_INT:
            case OMNI_DATE32: {
                vecBatchField->Append(std::make_unique<Vector<int32_t>>(rowCountField).release());
                break;
            }
            case OMNI_LONG:
            case OMNI_DECIMAL64: {
                vecBatchField->Append(std::make_unique<Vector<int64_t>>(rowCountField).release());
                break;
            }
            case OMNI_DOUBLE: {
                vecBatchField->Append(std::make_unique<Vector<double>>(rowCountField).release());
                break;
            }
            case OMNI_SHORT: {
                vecBatchField->Append(std::make_unique<Vector<int16_t>>(rowCountField).release());
                break;
            }
            case OMNI_VARCHAR:
            case OMNI_CHAR: {
                vecBatchField->Append(
                    std::make_unique<Vector<LargeStringContainer<std::string_view>>>(rowCountField).release());
                break;
            }
            case OMNI_DECIMAL128: {
                vecBatchField->Append(std::make_unique<Vector<Decimal128>>(rowCountField).release());
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
    pagesIndex->Prepare();

    // right now we assume the pregroup and presort are null
    preGroupedPartitionHashStrategy = make_unique<PagesHashStrategy>(pagesIndex->GetColumns(), pagesIndex->GetTypes(),
        preGroupedCols.data(), preGroupedCount);
    unGroupedPartitionHashStrategy = make_unique<PagesHashStrategy>(pagesIndex->GetColumns(), pagesIndex->GetTypes(),
        partitionCols.data(), partitionCount);
    preSortedPartitionHashStrategy = make_unique<PagesHashStrategy>(pagesIndex->GetColumns(), pagesIndex->GetTypes(),
        preGroupedCols.data(), preGroupedCount);
    peerGroupHashStrategy = make_unique<PagesHashStrategy>(pagesIndex->GetColumns(), pagesIndex->GetTypes(),
        originSortCols.data(), originSortColCount);
}

void WindowOperator::FinishPagesIndex()
{
    SortPagesIndexIfNecessary();
}

void WindowOperator::SortPagesIndexIfNecessary()
{
    if (pagesIndex->GetRowCount() > 1 && sortColCount != 0) {
        int32_t startPosition = 0;
        auto positionCount = pagesIndex->GetRowCount();
        while (startPosition < positionCount) {
            int32_t endPosition = FindGroupEnd(pagesIndex.get(), preSortedPartitionHashStrategy.get(), startPosition);
            pagesIndex->Sort(sortCols.data(), sortAscendings.data(), sortNullFirsts.data(), sortColCount, startPosition,
                endPosition);
            startPosition = endPosition;
        }
    }
}

int32_t FindGroupEnd(PagesIndex *pagesIndex, PagesHashStrategy *pagesHashStrategy, int32_t startPosition)
{
    int32_t left = startPosition;
    int32_t right = pagesIndex->GetRowCount();

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