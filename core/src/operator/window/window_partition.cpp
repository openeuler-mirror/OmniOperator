/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: window partiton implementations
 */
#include "window_partition.h"
#include <memory>
#include "window_function.h"

using namespace std;

WindowPartition::WindowPartition(PagesIndex *pagesIndex, int32_t partitionStart, int32_t partitionEnd,
    int32_t *outputChannels, int32_t outputChannelsCount, vector<unique_ptr<WindowFunction>> &windowFunctions,
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
    for (auto i = 0; i < windowFunctions.size(); ++i) {
        this->windowFunctions.push_back(windowFunctions[i].get());
    }
    this->windowIndex = make_unique<WindowIndex>(pagesIndex, partitionStart, partitionEnd).release();
    for (auto i = 0; i < windowFunctions.size(); ++i) {
        windowFunctions[i]->Reset(windowIndex);
    }
    UpdatePeerGroup();
}

WindowPartition::~WindowPartition()
{
    delete windowIndex;
}

void WindowPartition::ProcessNextRow(VectorBatch *vecBatch, int32_t index, int32_t *sourceTypes, int32_t typesCount)
{
    int32_t channel = outputChannelsCount;

    // check for new peer group
    if (currentPosition == peerGroupEnd) {
        UpdatePeerGroup();
    }

    for (int32_t i = 0; i < windowFunctions.size(); i++) {
        Range *range = GetFrameRange();
        windowFunctions[i]->ProcessRow(vecBatch->GetVector(channel), index, peerGroupStart - partitionStart,
            peerGroupEnd - partitionStart - 1, range->GetStart(), range->GetEnd());
        channel++;
        delete range;
    }

    currentPosition++;
}

bool PositionEqualsPosition(PagesIndex *pagesIndex, PagesHashStrategy *partitionHashStrategy, int32_t leftPosition,
    int32_t rightPosition)
{
    int64_t leftValueAddress = pagesIndex->GetValueAddresses()[leftPosition];
    int32_t leftColumnIndex = DecodeSliceIndex(leftValueAddress);
    int32_t leftColumnPosition = DecodePosition(leftValueAddress);

    int64_t rightValueAddress = pagesIndex->GetValueAddresses()[rightPosition];
    int32_t rightColumnIndex = DecodeSliceIndex(rightValueAddress);
    int32_t rightColumnPosition = DecodePosition(rightValueAddress);


    return partitionHashStrategy->PositionEqualsPosition(leftColumnIndex, leftColumnPosition, rightColumnIndex,
        rightColumnPosition);
}

void WindowPartition::UpdatePeerGroup()
{
    peerGroupStart = currentPosition;
    peerGroupEnd = peerGroupStart + 1;
    while ((peerGroupEnd < partitionEnd) &&
        PositionEqualsPosition(pagesIndex, peerGroupHashStrategy, peerGroupStart, peerGroupEnd)) {
        peerGroupEnd++;
    }
}