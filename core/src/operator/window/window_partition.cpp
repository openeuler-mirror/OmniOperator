#include "window_partition.h"
#include "window_function.h"

using namespace std;

WindowPartition::WindowPartition(PagesIndex *pagesIndex, int32_t partitionStart, int32_t partitionEnd,
    int32_t *outputChannels, int32_t outputChannelsCount, vector<WindowFunction *> &windowFunctions,
    PagesHashStrategy *peerGroupHashStrategy)
{
    this->pagesIndex = pagesIndex;
    this->partitionStart = partitionStart;
    this->partitionEnd = partitionEnd;
    this->outputChannels = outputChannels;
    this->outputChannelsCount = outputChannelsCount;
    this->peerGroupHashStrategy = peerGroupHashStrategy;
    this->currentPosition = partitionStart;
    this->peerGroupStart = 0;
    this->peerGroupEnd = 0;
    this->windowFunctions = windowFunctions;
    this->windowIndex = new WindowIndex(pagesIndex, partitionStart, partitionEnd);
    for (auto i = 0; i < this->windowFunctions.size(); ++i) {
        windowFunctions[i]->reset(windowIndex);
    }
    updatePeerGroup();
}

WindowPartition::~WindowPartition()
{
    delete windowIndex;
}

void WindowPartition::processNextRow(VectorBatch *vecBatch, int32_t index, int32_t *sourceTypes, int32_t typesCount)
{
    int32_t channel = outputChannelsCount;

    // check for new peer group
    if (currentPosition == peerGroupEnd) {
        updatePeerGroup();
    }

    for (int32_t i = 0; i < windowFunctions.size(); i++) {
        Range *range = getFrameRange();
        windowFunctions[i]->processRow(vecBatch->getVector(channel), index, peerGroupStart - partitionStart,
            peerGroupEnd - partitionStart - 1, range->getStart(), range->getEnd());
        channel++;
        delete range;
    }

    currentPosition++;
}

bool positionEqualsPosition(PagesIndex *pagesIndex, PagesHashStrategy *partitionHashStrategy, int leftPosition,
    int rightPosition)
{
    int64_t leftValueAddress = pagesIndex->getValueAddresses()[leftPosition];
    int32_t leftColumnIndex = decodeSliceIndex(leftValueAddress);
    int32_t leftColumnPosition = decodePosition(leftValueAddress);

    int64_t rightValueAddress = pagesIndex->getValueAddresses()[rightPosition];
    int32_t rightColumnIndex = decodeSliceIndex(rightValueAddress);
    int32_t rightColumnPosition = decodePosition(rightValueAddress);


    return partitionHashStrategy->positionEqualsPosition(leftColumnIndex, leftColumnPosition, rightColumnIndex,
        rightColumnPosition);
}

void WindowPartition::updatePeerGroup()
{
    peerGroupStart = currentPosition;
    peerGroupEnd = peerGroupStart + 1;
    while ((peerGroupEnd < partitionEnd) &&
        positionEqualsPosition(pagesIndex, peerGroupHashStrategy, peerGroupStart, peerGroupEnd)) {
        peerGroupEnd++;
    }
}