#include "window_function.h"

WindowIndex::WindowIndex(PagesIndex *pagesIndex, int32_t start, int32_t size)
{
    this->pagesIndex = pagesIndex;
    this->start = start;
    this->size = size;
};

WindowIndex::~WindowIndex() {}

RankingWindowFunction::~RankingWindowFunction() {}

void RankingWindowFunction::processRow(Column *column, int32_t index, int32_t peerGroupStart, int32_t peerGroupEnd,
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

void RankFunction::processRow(Column *column, int32_t index, bool newPeerGroup, int32_t peerGroupCount,
    int32_t currentPosition)
{
    if (newPeerGroup) {
        rank += count;
        count = 1;
    } else {
        count++;
    }
    column->setValue(index, &rank);
}

void RowNumberFunction::processRow(Column *column, int32_t index, bool newPeerGroup, int32_t peerGroupCount,
    int32_t currentPosition)
{
    int64_t value = currentPosition + 1;
    column->setValue(index, &value);
}

AggregateWindowFunction::~AggregateWindowFunction()
{
    delete[] argumentChannels;
}

AggregateWindowFunction::AggregateWindowFunction(int32_t *argumentChannels, int32_t argumentChannelsCount)
{
    this->argumentChannels = new int32_t[argumentChannelsCount];
}

void AggregateWindowFunction::reset(WindowIndex *windowIndex)
{
    this->windowIndex = windowIndex;
}

void AggregateWindowFunction::processRow(Column *column, int32_t index, int32_t peerGroupStart, int32_t peerGroupEnd,
    int32_t frameStart, int32_t frameEnd)
{
    Column ***leftColumns = windowIndex->getPagesIndex()->getColumns();
    int64_t leftValueAddress = windowIndex->getPagesIndex()->getValueAddresses()[0];
    int32_t leftColumnIndex = decodeSliceIndex(leftValueAddress);
    int32_t leftColumnPosition = decodePosition(leftValueAddress);

    Column *tempColumn = leftColumns[argumentChannels[0]][leftColumnIndex];

    if (!tempColumn->isNull(leftColumnPosition)) {
        column->setValue(index, tempColumn->getValue(leftColumnPosition));
    }
};