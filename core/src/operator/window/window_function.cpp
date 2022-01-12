/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: window operator implementations
 */

#include "window_function.h"
#include "../../vector/vector_common.h"
#include "../../vector/vector_helper.h"
#include "../util/operator_util.h"

using namespace omniruntime::vec;
using namespace std;

WindowIndex::WindowIndex(PagesIndex *pagesIndex, int32_t start, int32_t end)
{
    this->pagesIndex = pagesIndex;
    this->start = start;
    this->size = (end - start);
};

WindowIndex::~WindowIndex() = default;

RankingWindowFunction::RankingWindowFunction()
{
    this->windowIndex = nullptr;
    this->currentPeerGroupStart = 0;
    this->currentPosition = 0;
}

RankingWindowFunction::~RankingWindowFunction() = default;

RankFunction::RankFunction()
{
    this->rank = 0;
    this->count = 1;
}

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
    const VecType &inputType, const VecType &outputType)
    : inputType(inputType), outputType(outputType)
{
    this->windowIndex = nullptr;
    this->argumentChannels = argumentChannels;
    this->aggregatorFactory =
        omniruntime::op::CreateAggregatorFactory(static_cast<omniruntime::op::AggregateType>(aggregationType));
    this->currentStart = 0;
    this->currentEnd = 0;
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
    // the vector is used for aggregation in window operation
    Vector *resultVector = nullptr;
    if (frameStart < 0) {
        ResetAccumulator();
    } else if ((frameStart == currentStart) && (frameEnd >= currentEnd)) {
        // same or expanding frame
        Accumulate(&resultVector, column->GetAllocator(), currentEnd + 1, frameEnd);
        currentEnd = frameEnd;
    } else {
        // different frame
        ResetAccumulator();
        Accumulate(&resultVector, column->GetAllocator(), frameStart, frameEnd);
        currentStart = frameStart;
        currentEnd = frameEnd;
    }
    EvaluateFinal(aggregator, column, index);

    // after the EvaluateFinal, we should release the vector
    if (resultVector != nullptr) {
        delete resultVector;
    }
}

void AggregateWindowFunction::ResetAccumulator()
{
    if (currentStart >= 0) {
        aggregator = aggregatorFactory->CreateAggregator(inputType.GetId(), outputType.GetId());
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

void AggregateWindowFunction::Accumulate(Vector **resultVector, VectorAllocator *vecAllocator, int32_t start,
    int32_t end)
{
    if (start > end) {
        return;
    }
    Vector ***vectorBatch = windowIndex->GetPagesIndex()->GetColumns();
    int rowCount = end - start + 1;
    uint32_t width = (inputType.GetId() == OMNI_VEC_TYPE_VARCHAR || inputType.GetId() == OMNI_VEC_TYPE_CHAR) ?
        static_cast<const VarcharVecType &>(inputType).GetWidth() :
        0;

    // this is important to package data into an extra vector and use it to do the aggregation
    *resultVector = VectorHelper::CreateVector(vecAllocator, inputType.GetId(), rowCount * width, rowCount);
    for (int32_t resultVectorPosition = start; resultVectorPosition <= end; ++resultVectorPosition) {
        int64_t sliceAddress =
            windowIndex->GetPagesIndex()->GetValueAddresses()[resultVectorPosition + windowIndex->GetStart()];
        int32_t vectorIndex = DecodeSliceIndex(sliceAddress);
        int32_t vectorPosition = DecodePosition(sliceAddress);

        // actually the data to the aggregation function are from the sorted data with SortPagesIndexIfNecessary()
        // function since the implementation of SortPagesIndexIfNecessary will never return dictionary block here we add
        // the ExpandVectorAndIndex to ensure we send the right data to aggregation
        Vector *vector = vectorBatch[argumentChannels][vectorIndex];
        int32_t originalVectorPosition;
        Vector *originalVector = VectorHelper::ExpandVectorAndIndex(vector, vectorPosition, originalVectorPosition);
        AccumulateData(start, *resultVector, resultVectorPosition, originalVectorPosition, originalVector);
    }
}

void AggregateWindowFunction::AccumulateData(int32_t start, omniruntime::vec::Vector *resultVector,
    int32_t resultVectorPosition, int32_t originalVectorPosition, omniruntime::vec::Vector *originalVector)
{
    if (originalVector->IsValueNull(originalVectorPosition)) {
        resultVector->SetValueNull(resultVectorPosition - start);
    } else {
        switch (originalVector->GetTypeId()) {
            case OMNI_VEC_TYPE_INT:
            case OMNI_VEC_TYPE_DATE32: {
                int32_t actual = static_cast<IntVector *>(originalVector)->GetValue(originalVectorPosition);
                static_cast<IntVector *>(resultVector)->SetValue(resultVectorPosition - start, actual);
                break;
            }
            case OMNI_VEC_TYPE_LONG:
            case OMNI_VEC_TYPE_DECIMAL64: {
                int64_t actual = static_cast<LongVector *>(originalVector)->GetValue(originalVectorPosition);
                static_cast<LongVector *>(resultVector)->SetValue(resultVectorPosition - start, actual);
                break;
            }
            case OMNI_VEC_TYPE_DOUBLE: {
                double actual = static_cast<DoubleVector *>(originalVector)->GetValue(originalVectorPosition);
                static_cast<DoubleVector *>(resultVector)->SetValue(resultVectorPosition - start, actual);
                break;
            }
            case OMNI_VEC_TYPE_BOOLEAN: {
                bool actual = static_cast<BooleanVector *>(originalVector)->GetValue(originalVectorPosition);
                static_cast<BooleanVector *>(resultVector)->SetValue(resultVectorPosition - start, actual);
                break;
            }
            case OMNI_VEC_TYPE_VARCHAR:
            case OMNI_VEC_TYPE_CHAR: {
                uint8_t *actual = nullptr;
                int32_t length =
                    static_cast<VarcharVector *>(originalVector)->GetValue(originalVectorPosition, &actual);
                static_cast<VarcharVector *>(resultVector)->SetValue(resultVectorPosition - start, actual, length);
                break;
            }
            case OMNI_VEC_TYPE_DECIMAL128: {
                Decimal128 actual = static_cast<Decimal128Vector *>(originalVector)->GetValue(originalVectorPosition);
                static_cast<Decimal128Vector *>(resultVector)->SetValue(resultVectorPosition - start, actual);
                break;
            }
            default:
                break;
        }

        aggregator->ProcessGroup(aggregateState.operator*(), resultVector, resultVectorPosition - start);
    }
}
