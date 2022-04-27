/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: window operator implementations
 */

#include "window_function.h"

using namespace omniruntime::op;
using namespace omniruntime::vec;
using namespace std;

namespace omniruntime {
namespace op {
WindowIndex::WindowIndex(PagesIndex *pagesIndex, int32_t start, int32_t end)
    : pagesIndex(pagesIndex), start(start), size(end - start)
{}

WindowIndex::~WindowIndex() = default;

RankingWindowFunction::RankingWindowFunction() : windowIndex(nullptr), currentPeerGroupStart(0), currentPosition(0) {}

RankingWindowFunction::~RankingWindowFunction() = default;

RankFunction::RankFunction() : rank(0), count(1) {}

RankFunction::~RankFunction() = default;

void RankingWindowFunction::ProcessRow(Vector *column, int32_t index, int32_t peerGroupStart, int32_t peerGroupEnd,
    int32_t frameStart, int32_t frameEnd)
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

void RankFunction::RankingProcessRow(Vector *column, int32_t index, bool newPeerGroup, int32_t peerGroupCount,
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

void RowNumberFunction::RankingProcessRow(Vector *column, int32_t index, bool newPeerGroup, int32_t peerGroupCount,
    int32_t currentPositionIndex)
{
    int64_t value = currentPositionIndex + 1;
    VectorHelper::SetValue(column, index, &value);
}

AggregateWindowFunction::~AggregateWindowFunction() = default;

AggregateWindowFunction::AggregateWindowFunction(int32_t argumentChannels, int32_t aggregationType,
    const DataType &inputType, const DataType &outputType, VectorAllocator *allocator)
    : windowIndex(nullptr),
      argumentChannels(argumentChannels),
      currentStart(0),
      currentEnd(0),
      inputType(inputType),
      outputType(outputType),
      allocator(allocator)
{
    this->aggregatorFactory = omniruntime::op::CreateAggregatorFactory(static_cast<FunctionType>(aggregationType));
}

void AggregateWindowFunction::Reset(WindowIndex *pWindowIndex)
{
    this->windowIndex = pWindowIndex;
    ResetAccumulator();
}

/*
 * for aggregation function, we will build vector based on the window partition and pass it to the aggregator
 */
void AggregateWindowFunction::ProcessRow(Vector *column, int32_t index, int32_t peerGroupStart, int32_t peerGroupEnd,
    int32_t frameStart, int32_t frameEnd)
{
    if (frameStart < 0) {
        ResetAccumulator();
    } else if ((frameStart == currentStart) && (frameEnd >= currentEnd)) {
        // same or expanding frame
        Accumulate(column->GetAllocator(), column->GetEncoding(), currentEnd + 1, frameEnd);
        currentEnd = frameEnd;
    } else {
        // different frame
        ResetAccumulator();
        Accumulate(column->GetAllocator(), column->GetEncoding(), frameStart, frameEnd);
        currentStart = frameStart;
        currentEnd = frameEnd;
    }
    EvaluateFinal(aggregator, column, index);
}

void AggregateWindowFunction::ResetAccumulator()
{
    if (currentStart >= 0) {
        aggregator = aggregatorFactory->CreateAggregator(inputType, outputType, 0);
        aggregator->SetExecutionContextAllocator(allocator);
        aggregateState = std::make_unique<omniruntime::op::AggregateState>();
        currentStart = -1;
        currentEnd = -1;
    }
}

void AggregateWindowFunction::EvaluateFinal(unique_ptr<omniruntime::op::Aggregator> &pAggregator, Vector *pColumn,
    int32_t index) const
{
    pAggregator->ExtractValue(aggregateState.operator*(), pColumn, index);
}

void AggregateWindowFunction::Accumulate(VectorAllocator *vecAllocator, VectorEncoding vectorEncoding, int32_t start,
    int32_t end)
{
    if (start > end) {
        return;
    }
    Vector ***vectors = windowIndex->GetPagesIndex()->GetColumns();
    int rowCount = end - start + 1;
    uint32_t width = (inputType.GetId() == OMNI_VARCHAR || inputType.GetId() == OMNI_CHAR) ?
        static_cast<const VarcharDataType &>(inputType).GetWidth() :
        0;
    // this is important to package data into an extra vector and use it to do the aggregation
    // the vector is used for aggregation in window operation
    auto resultVectorBatch = new VectorBatch(1, rowCount);
    if (aggregator->GetType() == OMNI_AGGREGATION_TYPE_COUNT_ALL) {
        resultVectorBatch->SetVector(0, new LongVector(vecAllocator, rowCount));
    } else {
        resultVectorBatch->SetVector(0, VectorHelper::CreateVector(vecAllocator, vectorEncoding,
            static_cast<int32_t>(inputType.GetId()), rowCount * static_cast<int32_t>(width), rowCount));
    }
    for (int32_t resultVectorPosition = start; resultVectorPosition <= end; ++resultVectorPosition) {
        int64_t sliceAddress =
            windowIndex->GetPagesIndex()->GetValueAddresses()[resultVectorPosition + windowIndex->GetStart()];

        // actually the data to the aggregation function are from the sorted data with SortPagesIndexIfNecessary()
        // function since the implementation of SortPagesIndexIfNecessary will never return dictionary block here we add
        // the ExpandVectorAndIndex to ensure we send the right data to aggregation
        AccumulateData(resultVectorBatch, resultVectorPosition - start, vectors, sliceAddress);
    }
    // after using the vector, we should release it
    VectorHelper::FreeVecBatch(resultVectorBatch);
}

void AggregateWindowFunction::AccumulateData(VectorBatch *resultVectorBatch, int32_t resultVectorPosition,
    Vector ***inputVectors, int64_t inputAddress)
{
    uint32_t vectorIndex = DecodeSliceIndex(inputAddress);
    uint32_t vectorPosition = DecodePosition(inputAddress);
    Vector *originalVector = nullptr;
    int32_t originalVectorPosition;
    if (argumentChannels == -1) {
        originalVectorPosition = static_cast<int32_t>(vectorPosition);
        aggregator->ProcessGroup(aggregateState.operator*(), resultVectorBatch, resultVectorPosition);
        return;
    } else {
        Vector *vector = inputVectors[argumentChannels][vectorIndex];
        originalVector =
            VectorHelper::ExpandVectorAndIndex(vector, static_cast<int32_t>(vectorPosition), originalVectorPosition);
    }

    auto resultVector = resultVectorBatch->GetVector(0);
    if (originalVector->IsValueNull(originalVectorPosition)) {
        resultVector->SetValueNull(resultVectorPosition);
    } else {
        AssignValueForVector(originalVector, originalVectorPosition, resultVector, resultVectorPosition);
        aggregator->ProcessGroup(aggregateState.operator*(), resultVectorBatch, resultVectorPosition);
    }
}

void AggregateWindowFunction::AssignValueForVector(Vector *originalVector, int32_t originalVectorPosition,
    Vector *resultVector, int32_t resultVectorPosition)
{
    switch (originalVector->GetTypeId()) {
        case OMNI_INT:
        case OMNI_DATE32: {
            SetValue<IntVector>(originalVector, originalVectorPosition, resultVector, resultVectorPosition);
            break;
        }
        case OMNI_LONG:
        case OMNI_DECIMAL64: {
            SetValue<LongVector>(originalVector, originalVectorPosition, resultVector, resultVectorPosition);
            break;
        }
        case OMNI_DOUBLE: {
            SetValue<DoubleVector>(originalVector, originalVectorPosition, resultVector, resultVectorPosition);
            break;
        }
        case OMNI_BOOLEAN: {
            SetValue<BooleanVector>(originalVector, originalVectorPosition, resultVector, resultVectorPosition);
            break;
        }
        case OMNI_VARCHAR:
        case OMNI_CHAR: {
            uint8_t *actual = nullptr;
            int32_t length = static_cast<VarcharVector *>(originalVector)->GetValue(originalVectorPosition, &actual);
            static_cast<VarcharVector *>(resultVector)->SetValue(resultVectorPosition, actual, length);
            break;
        }
        case OMNI_DECIMAL128: {
            SetValue<Decimal128Vector>(originalVector, originalVectorPosition, resultVector, resultVectorPosition);
            break;
        }
        default:
            break;
    }
}

template <typename T>
void AggregateWindowFunction::SetValue(Vector *inputVector, int32_t inputPosition, Vector *outputVector,
    int32_t outputPosition)
{
    auto value = static_cast<T *>(inputVector)->GetValue(inputPosition);
    static_cast<T *>(outputVector)->SetValue(outputPosition, value);
}
}
}
