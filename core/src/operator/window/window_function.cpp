#include <cstring>
#include "window_function.h"
#include "../../util/debug.h"
#include "../../vector/vector_common.h"
#include "../../vector/vector_helper.h"

WindowIndex::WindowIndex(PagesIndex *pagesIndex, int32_t start, int32_t end)
{
    this->pagesIndex = pagesIndex;
    this->start = start;
    this->size = end - start;
};

WindowIndex::~WindowIndex() {}

RankingWindowFunction::RankingWindowFunction()
{
    this->currentPeerGroupStart = 0;
    this->currentPosition = 0;
}

RankingWindowFunction::~RankingWindowFunction() {}

RankFunction::RankFunction()
{
    this->rank = 0;
    this->count = 1;
}

void RankingWindowFunction::processRow(Vector *column, int32_t index, int32_t peerGroupStart, int32_t peerGroupEnd,
    int32_t frameStart, int32_t frameEnd)
{
    bool newPeerGroup = false;
    if (peerGroupStart != currentPeerGroupStart) {
        currentPeerGroupStart = peerGroupStart;
        newPeerGroup = true;
    }
    int peerGroupCount = (peerGroupEnd - peerGroupStart) + 1;
    processRow(column, index, newPeerGroup, peerGroupCount, currentPosition);
    currentPosition++;
}

void RankingWindowFunction::reset(WindowIndex *windowIndex)
{
    this->windowIndex = windowIndex;
    this->currentPeerGroupStart = -1;
    this->currentPosition = 0;
    reset();
}

void RankFunction::reset()
{
    rank = 0;
    count = 1;
}

void RankFunction::processRow(Vector *column, int32_t index, bool newPeerGroup, int32_t peerGroupCount,
    int32_t currentPosition)
{
    if (newPeerGroup) {
        rank += count;
        count = 1;
    } else {
        count++;
    }
    VectorHelper::setValue(column, index, &rank);
}

void RowNumberFunction::processRow(Vector *column, int32_t index, bool newPeerGroup, int32_t peerGroupCount,
    int32_t currentPosition)
{
    int64_t value = currentPosition + 1;
    VectorHelper::setValue(column, index, &value);
}

AggregateWindowFunction::~AggregateWindowFunction() {}

AggregateWindowFunction::AggregateWindowFunction(int32_t argumentChannels, int32_t aggregationType, int32_t dataType)
{
    this->windowIndex = nullptr;
    this->argumentChannels = argumentChannels;
    this->aggregationType = aggregationType;
    this->currentStart = 0;
    this->currentEnd = 0;
    this->dataType = dataType;
    this->aggregator = nullptr;
}

void AggregateWindowFunction::reset(WindowIndex *windowIndex)
{
    this->windowIndex = windowIndex;
    resetAccumulator();
}

void AggregateWindowFunction::processRow(Vector *column, int32_t index, int32_t peerGroupStart, int32_t peerGroupEnd,
    int32_t frameStart, int32_t frameEnd)
{
    if (frameStart < 0) {
        resetAccumulator();
    } else if ((frameStart == currentStart) && (frameEnd >= currentEnd)) {
        // same or expanding frame
        accumulate(currentEnd + 1, frameEnd);
        currentEnd = frameEnd;
    } else {
        // different frame
        resetAccumulator();
        accumulate(frameStart, frameEnd);
        currentStart = frameStart;
        currentEnd = frameEnd;
    }
    evaluateFinal(aggregator, column, index);
}
omniruntime::op::Aggregator *createAccumulator(int32_t aggregationType, int32_t dataType)
{
    switch (aggregationType) {
        case WIN_SUM:
            return new omniruntime::op::SumAggregator(dataType);
        case WIN_COUNT:
            return new omniruntime::op::CountAggregator(dataType);
        case WIN_AVG:
            return new omniruntime::op::AverageAggregator(dataType);
        case WIN_MAX:
            return new omniruntime::op::MaxAggregator(dataType);
        case WIN_MIN:
            return new omniruntime::op::MinAggregator(dataType);
        default:
            return nullptr;
    }
}

void AggregateWindowFunction::resetAccumulator()
{
    if (currentStart >= 0) {
        aggregator = createAccumulator(aggregationType, dataType);
        currentStart = -1;
        currentEnd = -1;
    }
}

void AggregateWindowFunction::evaluateFinal(omniruntime::op::Aggregator *pAggregator, Vector *pColumn, int32_t index)
{
    auto state = pAggregator->GetNonGroupState();
    switch (aggregationType) {
        case WIN_SUM:
        case WIN_MAX:
        case WIN_MIN:
            VectorHelper::setValue(pColumn, index, state.val);
            break;
        case WIN_COUNT:
            VectorHelper::setValue(pColumn, index, (void *)(&state.count));
            break;
        case WIN_AVG:
            VectorHelper::setValue(pColumn, index, state.avgVal);
            break;
        default:
            break;
    }
}

void AggregateWindowFunction::accumulate(int32_t start, int32_t end)
{
    if (start > end) {
        return;
    }
    Vector ***leftColumns = windowIndex->getPagesIndex()->getColumns();
    Vector *vector = 0;
    int rowCount = end - start + 1;
    switch (dataType) {
        case OMNI_VEC_TYPE_INT: {
            vector = new IntVector(nullptr, rowCount);
            break;
        }
        case OMNI_VEC_TYPE_LONG: {
            vector = new LongVector(nullptr, rowCount);
            break;
        }
        case OMNI_VEC_TYPE_DOUBLE: {
            vector = new DoubleVector(nullptr, rowCount);
            break;
        }
        default:
            break;
    }
    for (int32_t position = start; position <= end; ++position) {
        int64_t leftValueAddress = windowIndex->getPagesIndex()->getValueAddresses()[position + windowIndex->getStart()];
        int32_t leftColumnIndex = decodeSliceIndex(leftValueAddress);
        int32_t leftColumnPosition = decodePosition(leftValueAddress);
        Vector *tempColumn = leftColumns[argumentChannels][leftColumnIndex];
        if (!tempColumn->isValueNull(leftColumnPosition)) {
            switch (tempColumn->getType()) {
                case OMNI_VEC_TYPE_INT: {
                    int32_t actual = ((IntVector *)tempColumn)->getValue(leftColumnPosition);
                    ((IntVector *)vector)->setValue(position - start, actual);
                    break;
                }
                case OMNI_VEC_TYPE_LONG: {
                    int64_t actual = ((LongVector *)tempColumn)->getValue(leftColumnPosition);
                    ((LongVector *)vector)->setValue(position - start, actual);
                    break;
                }
                case OMNI_VEC_TYPE_DOUBLE: {
                    double actual = ((DoubleVector *)tempColumn)->getValue(leftColumnPosition);
                    ((DoubleVector *)vector)->setValue(position - start, actual);
                    break;
                }
                default:
                    break;
            }
            aggregator->ProcessNonGroup(vector, dataType, position - start);
        }
    }
};