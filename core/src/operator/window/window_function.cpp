/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: window operator implementations
 */

#include "window_function.h"
#include "operator/util/operator_util.h"
#include "operator/aggregation/aggregator/aggregator_factory.h"

using namespace omniruntime::op;
using namespace omniruntime::vec;
using namespace std;

namespace omniruntime {
namespace op {
WindowIndex::WindowIndex(PagesIndex *pagesIndex, int32_t start, int32_t end)
    : pagesIndex(pagesIndex), start(start), size(end - start)
{}

WindowIndex::~WindowIndex() = default;

RankingWindowFunction::RankingWindowFunction(std::unique_ptr<WindowFrameInfo> frame, DataTypePtr inputType,
    DataTypePtr outputType)
    : WindowFunction(std::move(frame), std::move(inputType), std::move(outputType)),
      windowIndex(nullptr),
      currentPeerGroupStart(0),
      currentPosition(0)
{}

RankingWindowFunction::~RankingWindowFunction() = default;

RankFunction::RankFunction(std::unique_ptr<WindowFrameInfo> frame, DataTypePtr inputType, DataTypePtr outputType)
    : RankingWindowFunction(std::move(frame), std::move(inputType), std::move(outputType)), rank(0), count(1)
{}

RankFunction::~RankFunction() = default;

void RankingWindowFunction::ProcessRow(VectorBatch *vectorBatch, BaseVector *column, int32_t index,
    int32_t peerGroupStart, int32_t peerGroupEnd, int32_t frameStart, int32_t frameEnd)
{
    bool newPeerGroup = false;
    if (peerGroupStart != currentPeerGroupStart) {
        currentPeerGroupStart = peerGroupStart;
        newPeerGroup = true;
    }
    int peerGroupCount = (peerGroupEnd - peerGroupStart) + 1;
    RankingProcessRow(column, index, newPeerGroup, peerGroupCount, currentPosition);
    currentPosition++;
}

void RankingWindowFunction::Reset(WindowIndex *pWindowIndex)
{
    this->windowIndex = pWindowIndex;
    this->currentPeerGroupStart = -1;
    this->currentPosition = 0;
    Reset();
}

void RankFunction::Reset()
{
    rank = 0;
    count = 1;
}

void RankFunction::RankingProcessRow(BaseVector *column, int32_t index, bool newPeerGroup, int32_t peerGroupCount,
    int32_t currentPositionIndex)
{
    if (newPeerGroup) {
        rank += count;
        count = 1;
    } else {
        count++;
    }
    VectorHelper::SetValue(column, index, &rank);
}

void RowNumberFunction::RankingProcessRow(BaseVector *column, int32_t index, bool newPeerGroup, int32_t peerGroupCount,
    int32_t currentPositionIndex)
{
    int64_t value = currentPositionIndex + 1;
    VectorHelper::SetValue(column, index, &value);
}

PercentRankFunction::PercentRankFunction(std::unique_ptr<WindowFrameInfo> frame, DataTypePtr inputType,
    DataTypePtr outputType)
    : RankingWindowFunction(std::move(frame), std::move(inputType), std::move(outputType)), rank(0), count(1),
      numPartitionRows(1)
{}

PercentRankFunction::~PercentRankFunction() = default;

void PercentRankFunction::Reset()
{
    rank = 0;
    count = 1;
    if (windowIndex != nullptr) {
        numPartitionRows = windowIndex->GetSize();
    } else {
        numPartitionRows = 1;
    }
}

void PercentRankFunction::RankingProcessRow(BaseVector *column, int32_t index, bool newPeerGroup, int32_t peerGroupCount,
    int32_t currentPositionIndex)
{
    if (newPeerGroup) {
        rank += count;
        count = 1;
    } else {
        count++;
    }
    
    double percentRankValue;
    if (numPartitionRows == 1) {
        percentRankValue = 0.0;
    } else {
        percentRankValue = static_cast<double>(rank - 1) / static_cast<double>(numPartitionRows - 1);
    }
    VectorHelper::SetValue(column, index, &percentRankValue);
}

CumeDistFunction::CumeDistFunction(std::unique_ptr<WindowFrameInfo> frame, DataTypePtr inputType,
    DataTypePtr outputType)
    : RankingWindowFunction(std::move(frame), std::move(inputType), std::move(outputType)), runningTotal(0),
      numPartitionRows(1)
{}

CumeDistFunction::~CumeDistFunction() = default;

void CumeDistFunction::Reset()
{
    runningTotal = 0;
    if (windowIndex != nullptr) {
        numPartitionRows = windowIndex->GetSize();
    } else {
        numPartitionRows = 1;
    }
}

void CumeDistFunction::RankingProcessRow(BaseVector *column, int32_t index, bool newPeerGroup, int32_t peerGroupCount,
    int32_t currentPositionIndex)
{
    // When we encounter a new peer group, update runningTotal
    // runningTotal represents the cumulative count of rows up to and including the current peer group
    if (newPeerGroup) {
        runningTotal += peerGroupCount;
    }
    
    double cumeDistValue;
    if (numPartitionRows == 0) {
        cumeDistValue = 0.0;
    } else {
        cumeDistValue = static_cast<double>(runningTotal) / static_cast<double>(numPartitionRows);
    }
    VectorHelper::SetValue(column, index, &cumeDistValue);
}

AggregateWindowFunction::~AggregateWindowFunction() = default;

AggregateWindowFunction::AggregateWindowFunction(int32_t argumentChannel, int32_t aggregationType,
    DataTypePtr inputType, DataTypePtr outputType, std::unique_ptr<WindowFrameInfo> frame,
    ExecutionContext *executionContext, bool isOverflowAsNull)
    : WindowFunction(std::move(frame), std::move(inputType), std::move(outputType)),
      windowIndex(nullptr),
      currentStart(0),
      currentEnd(0),
      executionContext(executionContext),
      isOverflowAsNull(isOverflowAsNull)
{
    this->argumentChannels.push_back(argumentChannel);
    this->aggregatorFactory = CreateAggregatorFactory(static_cast<FunctionType>(aggregationType));
}

void AggregateWindowFunction::Reset(WindowIndex *pWindowIndex)
{
    this->windowIndex = pWindowIndex;
    ResetAccumulator();
}

/*
 * for aggregation function, we will build vector based on the window partition and pass it to the aggregator
 */
void AggregateWindowFunction::ProcessRow(VectorBatch *inputVecBatchForAgg, BaseVector *column, int32_t index,
    int32_t peerGroupStart, int32_t peerGroupEnd, int32_t frameStart, int32_t frameEnd)
{
    if (frameStart < 0) {
        ResetAccumulator();
    } else if ((frameStart == currentStart) && (frameEnd >= currentEnd)) {
        // same or expanding frame
        Accumulate(inputVecBatchForAgg, currentEnd + 1, frameEnd);
        currentEnd = frameEnd;
    } else {
        // different frame
        ResetAccumulator();
        Accumulate(inputVecBatchForAgg, frameStart, frameEnd);
        currentStart = frameStart;
        currentEnd = frameEnd;
    }
    EvaluateFinal(aggregator, column, index);
}

void AggregateWindowFunction::ResetAccumulator()
{
    if (currentStart >= 0) {
        // aggregator inputVector is generated by runtime and only 1 columnar, so input channel fixed as 0
        std::vector<int32_t> winChannels = { 0 };
        aggregator = aggregatorFactory->CreateAggregator(*DataTypes::GenerateDataTypes(inputType),
            *DataTypes::GenerateDataTypes(outputType), winChannels, true, false, isOverflowAsNull);
        aggregator->SetExecutionContext(executionContext);
        auto aggStateSize = aggregator->GetStateSize();
        aggregateState = executionContext->GetArena()->Allocate(aggStateSize);
        aggregator->SetStateOffset(0);
        aggregator->InitState(aggregateState);
        currentStart = -1;
        currentEnd = -1;
    }
}

void AggregateWindowFunction::EvaluateFinal(unique_ptr<omniruntime::op::Aggregator> &pAggregator, BaseVector *pColumn,
    int32_t index) const
{
    std::vector<BaseVector *> extractVectors;
    extractVectors.push_back(pColumn);
    pAggregator->ExtractValues(aggregateState, extractVectors, index);
}

void AggregateWindowFunction::Accumulate(omniruntime::vec::VectorBatch *inputVecBatchForAgg, int32_t start, int32_t end)
{
    if (start > end) {
        return;
    }

    auto rowCount = end - start + 1;
    if (aggregator->GetType() == OMNI_AGGREGATION_TYPE_COUNT_ALL) {
        auto vector = std::make_unique<ConstVector<int64_t>>(0, OMNI_LONG, rowCount);
        inputVecBatchForAgg->SetVector(0, vector.get());
        aggregator->ProcessGroup(aggregateState, inputVecBatchForAgg, start, rowCount);
        return;
    }

    int32_t argumentChannel = argumentChannels[0];
    BaseVector **vectors = windowIndex->GetPagesIndex()->GetColumns()[argumentChannel];
    uint64_t *valueAddresses = windowIndex->GetPagesIndex()->GetValueAddresses();
    int32_t windowStart = windowIndex->GetStart();
    for (int32_t position = start; position <= end; ++position) {
        uint32_t vectorIndex = UINT32_MAX;
        uint64_t sliceAddress = valueAddresses[position + windowStart];
        if (DecodeSliceIndex(sliceAddress) != vectorIndex) {
            vectorIndex = DecodeSliceIndex(sliceAddress);
            auto vector = vectors[vectorIndex];
            inputVecBatchForAgg->SetVector(0, vector);
        }
        uint32_t vectorPosition = DecodePosition(sliceAddress);
        aggregator->ProcessGroup(aggregateState, inputVecBatchForAgg, static_cast<int32_t>(vectorPosition), 1);
    }
}

void CountAllWindowFunction::Accumulate(BaseVector *column, int32_t index, int32_t start, int32_t end)
{
    int64_t rowCount = end - start + 1;
    VectorHelper::SetValue(column, index, &rowCount);
}

CountAllWindowFunction::CountAllWindowFunction(std::unique_ptr<WindowFrameInfo> frame, DataTypePtr inputType, DataTypePtr outputType)
        : WindowFunction(std::move(frame), std::move(inputType), std::move(outputType)),
          currentStart(0),
          currentEnd(0)
{}

void CountAllWindowFunction::ResetAccumulator()
{
    if (currentStart >= 0) {
        currentStart = -1;
        currentEnd = -1;
    }
}

void CountAllWindowFunction::Reset(WindowIndex *pWindowIndex)
{
    ResetAccumulator();
}

void CountAllWindowFunction::ProcessRow(VectorBatch *inputVecBatchForAgg, BaseVector *column, int32_t index, int32_t peerGroupStart,
                int32_t peerGroupEnd, int32_t frameStart, int32_t frameEnd)
{

    if (frameStart < 0) {
        ResetAccumulator();
    } else if ((frameStart == currentStart) && (frameEnd >= currentEnd)) {
        // same or expanding frame
        Accumulate(column, index, frameStart, frameEnd);
        currentEnd = frameEnd;
    } else {
        // different frame
        ResetAccumulator();
        Accumulate(column, index, frameStart, frameEnd);
        currentStart = frameStart;
        currentEnd = frameEnd;
    }
}

// NthValueFunction implementation
NthValueFunction::NthValueFunction(std::unique_ptr<WindowFrameInfo> frame, DataTypePtr inputType,
    DataTypePtr outputType, int32_t valueChannel, int64_t offset)
    : WindowFunction(std::move(frame), std::move(inputType), std::move(outputType)),
      valueChannel_(valueChannel),
      offset_(offset),
      windowIndex_(nullptr)
{}

void NthValueFunction::Reset(WindowIndex *pWindowIndex)
{
    windowIndex_ = pWindowIndex;
}

void NthValueFunction::ProcessRow(VectorBatch *inputVecBatchForAgg, BaseVector *column, int32_t index,
    int32_t peerGroupStart, int32_t peerGroupEnd, int32_t frameStart, int32_t frameEnd)
{
    if (frameStart < 0 || frameEnd < 0) {
        column->SetNull(index);
        return;
    }

    int32_t targetRow = frameStart + static_cast<int32_t>(offset_) - 1;
    if (targetRow >= frameStart && targetRow <= frameEnd) {
        int32_t absoluteRow = windowIndex_->GetStart() + targetRow;
        CopyValueFromPartition(column, index, absoluteRow);
    } else {
        column->SetNull(index);
    }
}

void NthValueFunction::CopyValueFromPartition(BaseVector *outputColumn, int32_t outputIndex, int32_t sourceRow)
{
    if (windowIndex_ == nullptr || windowIndex_->GetPagesIndex() == nullptr) {
        outputColumn->SetNull(outputIndex);
        return;
    }

    PagesIndex *pagesIndex = windowIndex_->GetPagesIndex();
    uint64_t *valueAddresses = pagesIndex->GetValueAddresses();
    BaseVector **vectors = pagesIndex->GetColumns()[valueChannel_];

    uint64_t sliceAddress = valueAddresses[sourceRow];
    uint32_t vectorIndex = DecodeSliceIndex(sliceAddress);
    uint32_t vectorPosition = DecodePosition(sliceAddress);

    BaseVector *sourceVector = vectors[vectorIndex];

    if (sourceVector->IsNull(vectorPosition)) {
        outputColumn->SetNull(outputIndex);
    } else {
        vec::VectorHelper::CopyValue(sourceVector, vectorPosition, outputColumn, outputIndex);
    }
}

// DenseRankFunction implementation
DenseRankFunction::DenseRankFunction(std::unique_ptr<WindowFrameInfo> frame, DataTypePtr inputType,
    DataTypePtr outputType)
    : RankingWindowFunction(std::move(frame), std::move(inputType), std::move(outputType)), rank(0)
{}

DenseRankFunction::~DenseRankFunction() = default;

void DenseRankFunction::Reset()
{
    rank = 0;
}

void DenseRankFunction::RankingProcessRow(BaseVector *column, int32_t index, bool newPeerGroup,
    int32_t peerGroupCount, int32_t currentPositionIndex)
{
    if (newPeerGroup) {
        rank++;
    }
    VectorHelper::SetValue(column, index, &rank);
}

// NtileFunction implementation
NtileFunction::NtileFunction(std::unique_ptr<WindowFrameInfo> frame, DataTypePtr inputType,
    DataTypePtr outputType, int32_t numBuckets)
    : RankingWindowFunction(std::move(frame), std::move(inputType), std::move(outputType)),
      numBuckets_(numBuckets),
      numPartitionRows_(0),
      rowsPerBucket_(0),
      bucketsWithExtraRow_(0),
      extraBucketsBoundary_(0)
{}

void NtileFunction::Reset()
{
    if (windowIndex != nullptr) {
        numPartitionRows_ = windowIndex->GetSize();
    } else {
        numPartitionRows_ = 0;
    }

    if (numBuckets_ > 0 && numPartitionRows_ > 0) {
        if (numBuckets_ >= numPartitionRows_) {
            rowsPerBucket_ = 1;
            bucketsWithExtraRow_ = 0;
            extraBucketsBoundary_ = numPartitionRows_;
        } else {
            rowsPerBucket_ = numPartitionRows_ / numBuckets_;
            bucketsWithExtraRow_ = numPartitionRows_ % numBuckets_;
            extraBucketsBoundary_ = bucketsWithExtraRow_ * (rowsPerBucket_ + 1);
        }
    }
}

void NtileFunction::RankingProcessRow(BaseVector *column, int32_t index, bool newPeerGroup,
    int32_t peerGroupCount, int32_t currentPositionIndex)
{
    if (numBuckets_ <= 0 || numPartitionRows_ <= 0) {
        column->SetNull(index);
        return;
    }

    int64_t bucketValue;
    if (numBuckets_ >= numPartitionRows_) {
        bucketValue = currentPositionIndex + 1;
    } else if (currentPositionIndex < extraBucketsBoundary_) {
        bucketValue = currentPositionIndex / (rowsPerBucket_ + 1) + 1;
    } else {
        bucketValue = (currentPositionIndex - bucketsWithExtraRow_) / rowsPerBucket_ + 1;
    }
    VectorHelper::SetValue(column, index, &bucketValue);
}

}
}