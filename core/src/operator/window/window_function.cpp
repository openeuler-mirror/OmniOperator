/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: window operator implementations
 */

#include "window_function.h"
#include "../../vector/vector_common.h"
#include "../../vector/vector_helper.h"

using namespace omniruntime::vec;
using namespace std;

WindowIndex::WindowIndex(PagesIndex *pagesIndex, int32_t start, int32_t end)
{
    this->pagesIndex = pagesIndex;
    this->start = start;
    this->size = end - start;
};

WindowIndex::~WindowIndex() {}

RankingWindowFunction::RankingWindowFunction()
{
    this->windowIndex = nullptr;
    this->currentPeerGroupStart = 0;
    this->currentPosition = 0;
}

RankingWindowFunction::~RankingWindowFunction() {}

RankFunction::RankFunction()
{
    this->rank = 0;
    this->count = 1;
}

RankFunction::~RankFunction() {}

void RankingWindowFunction::ProcessRow(Vector *column, int32_t index, int32_t peerGroupStart, int32_t peerGroupEnd,
    int32_t frameStart, int32_t frameEnd)
{
    bool newPeerGroup = false;
    if (peerGroupStart != currentPeerGroupStart) {
        currentPeerGroupStart = peerGroupStart;
        newPeerGroup = true;
    }
    int peerGroupCount = (peerGroupEnd - peerGroupStart) + 1;
    ProcessRow(column, index, newPeerGroup, peerGroupCount, currentPosition);
    currentPosition++;
}

void RankingWindowFunction::Reset(WindowIndex *windowIndex)
{
    this->windowIndex = windowIndex;
    this->currentPeerGroupStart = -1;
    this->currentPosition = 0;
    Reset();
}

void RankFunction::Reset()
{
    rank = 0;
    count = 1;
}

void RankFunction::ProcessRow(Vector *column, int32_t index, bool newPeerGroup, int32_t peerGroupCount,
    int32_t currentPosition)
{
    if (newPeerGroup) {
        rank += count;
        count = 1;
    } else {
        count++;
    }
    VectorHelper::SetValue(column, index, &rank);
}

void RowNumberFunction::ProcessRow(Vector *column, int32_t index, bool newPeerGroup, int32_t peerGroupCount,
    int32_t currentPosition)
{
    int64_t value = currentPosition + 1;
    VectorHelper::SetValue(column, index, &value);
}

AggregateWindowFunction::~AggregateWindowFunction() {}

AggregateWindowFunction::AggregateWindowFunction(int32_t argumentChannels, int32_t aggregationType, int32_t dataType)
{
    this->argumentChannels = argumentChannels;
    this->aggregationType = aggregationType;
    this->currentStart = 0;
    this->currentEnd = 0;
    this->dataType = dataType;
}

void AggregateWindowFunction::Reset(WindowIndex *windowIndex)
{
    this->windowIndex = windowIndex;
    ResetAccumulator();
}

void AggregateWindowFunction::ProcessRow(Vector *column, int32_t index, int32_t peerGroupStart, int32_t peerGroupEnd,
    int32_t frameStart, int32_t frameEnd)
{
    if (frameStart < 0) {
        ResetAccumulator();
    } else if ((frameStart == currentStart) && (frameEnd >= currentEnd)) {
        // same or expanding frame
        Accumulate(currentEnd + 1, frameEnd);
        currentEnd = frameEnd;
    } else {
        // different frame
        ResetAccumulator();
        Accumulate(frameStart, frameEnd);
        currentStart = frameStart;
        currentEnd = frameEnd;
    }
    EvaluateFinal(aggregator, column, index);
}
omniruntime::op::Aggregator *CreateAccumulator(int32_t aggregationType, int32_t dataType)
{
    switch (aggregationType) {
        case WIN_SUM:
            return make_unique<omniruntime::op::SumAggregator>(dataType).release();
        case WIN_COUNT:
            return make_unique<omniruntime::op::CountAggregator>(dataType).release();
        case WIN_AVG:
            return make_unique<omniruntime::op::AverageAggregator>(dataType).release();
        case WIN_MAX:
            return make_unique<omniruntime::op::MaxAggregator>(dataType).release();
        case WIN_MIN:
            return make_unique<omniruntime::op::MinAggregator>(dataType).release();
        default:
            return nullptr;
    }
}

void AggregateWindowFunction::ResetAccumulator()
{
    if (currentStart >= 0) {
        aggregator = CreateAccumulator(aggregationType, dataType);
        currentStart = -1;
        currentEnd = -1;
    }
}

void AggregateWindowFunction::EvaluateFinal(omniruntime::op::Aggregator *pAggregator, Vector *pColumn,
    int32_t index) const
{
    auto state = pAggregator->GetNonGroupState();
    switch (aggregationType) {
        case WIN_SUM:
        case WIN_MAX:
        case WIN_MIN:
            VectorHelper::SetValue(pColumn, index, state.val);
            break;
        case WIN_COUNT:
            VectorHelper::SetValue(pColumn, index, (void *)(&state.count));
            break;
        case WIN_AVG:
            VectorHelper::SetValue(pColumn, index, state.avgVal);
            break;
        default:
            break;
    }
}

void AggregateWindowFunction::Accumulate(int32_t start, int32_t end)
{
    if (start > end) {
        return;
    }
    Vector ***leftColumns = windowIndex->GetPagesIndex()->GetColumns();
    int rowCount = end - start + 1;
    Vector *vector = InitVector(rowCount);
    for (int32_t position = start; position <= end; ++position) {
        int64_t leftValueAddress =
            windowIndex->GetPagesIndex()->GetValueAddresses()[position + windowIndex->GetStart()];
        int32_t leftColumnIndex = DecodeSliceIndex(leftValueAddress);
        int32_t leftColumnPosition = DecodePosition(leftValueAddress);
        Vector *tempColumn = leftColumns[argumentChannels][leftColumnIndex];
        if (!tempColumn->IsValueNull(leftColumnPosition)) {
            switch (tempColumn->GetType().GetId()) {
                case OMNI_VEC_TYPE_INT: {
                    int32_t actual = ((IntVector *)tempColumn)->GetValue(leftColumnPosition);
                    ((IntVector *)vector)->SetValue(position - start, actual);
                    break;
                }
                case OMNI_VEC_TYPE_LONG: {
                    int64_t actual = ((LongVector *)tempColumn)->GetValue(leftColumnPosition);
                    ((LongVector *)vector)->SetValue(position - start, actual);
                    break;
                }
                case OMNI_VEC_TYPE_DOUBLE: {
                    double actual = ((DoubleVector *)tempColumn)->GetValue(leftColumnPosition);
                    ((DoubleVector *)vector)->SetValue(position - start, actual);
                    break;
                }
                default:
                    break;
            }
            aggregator->ProcessNonGroup(vector, dataType, position - start);
        }
    }
}

Vector *AggregateWindowFunction::InitVector(int rowCount)
{
    switch (dataType) {
        case OMNI_VEC_TYPE_INT: {
            return make_unique<IntVector>(nullptr, rowCount).release();
        }
        case OMNI_VEC_TYPE_LONG: {
            return make_unique<LongVector>(nullptr, rowCount).release();
        }
        case OMNI_VEC_TYPE_DOUBLE: {
            return make_unique<LongVector>(nullptr, rowCount).release();
        }
        default:
            break;
    }
    return nullptr;
}
