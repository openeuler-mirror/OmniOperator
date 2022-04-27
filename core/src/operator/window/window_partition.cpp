/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: window partiton implementations
 */
#include "window_partition.h"
#include <memory>
#include "window_function.h"

using namespace omniruntime::op;
using namespace omniruntime::vec;
using namespace std;

namespace omniruntime {
namespace op {
WindowPartition::WindowPartition(PagesIndex *pagesIndex, int32_t partitionStart, int32_t partitionEnd,
    int32_t *outputChannels, int32_t outputChannelsCount, vector<unique_ptr<WindowFunction>> &windowFunctions,
    PagesHashStrategy *peerGroupHashStrategy)
    : pagesIndex(pagesIndex),
      partitionStart(partitionStart),
      partitionEnd(partitionEnd),
      outputChannels(outputChannels),
      outputChannelsCount(outputChannelsCount),
      peerGroupHashStrategy(peerGroupHashStrategy),
      currentPosition(partitionStart),
      peerGroupStart(0),
      peerGroupEnd(0)
{
    for (auto &windowFunction : windowFunctions) {
        this->windowFunctions.push_back(windowFunction.get());
    }
    this->windowIndex = make_unique<WindowIndex>(pagesIndex, partitionStart, partitionEnd);
    for (auto &windowFunction : windowFunctions) {
        windowFunction->Reset(windowIndex.get());
    }
    UpdatePeerGroup();
}

WindowPartition::~WindowPartition() = default;

void WindowPartition::ProcessNextRow(VectorBatch *vecBatch, int32_t index)
{
    int32_t channel = outputChannelsCount;

    // check for new peer group
    if (currentPosition == peerGroupEnd) {
        UpdatePeerGroup();
    }

    for (auto &windowFunction : windowFunctions) {
        unique_ptr<Range> range = GetFrameRange();
        windowFunction->ProcessRow(vecBatch->GetVector(channel), index, peerGroupStart - partitionStart,
            peerGroupEnd - partitionStart - 1, range->GetStart(), range->GetEnd());
        channel++;
    }

    currentPosition++;
}

bool PositionEqualsPosition(PagesIndex *pagesIndex, PagesHashStrategy *partitionHashStrategy, int32_t leftPosition,
    int32_t rightPosition)
{
    uint64_t leftValueAddress = pagesIndex->GetValueAddresses()[leftPosition];
    int32_t leftColumnIndex = static_cast<int32_t>(DecodeSliceIndex(leftValueAddress));
    int32_t leftColumnPosition = static_cast<int32_t>(DecodePosition(leftValueAddress));

    uint64_t rightValueAddress = pagesIndex->GetValueAddresses()[rightPosition];
    int32_t rightColumnIndex = static_cast<int32_t>(DecodeSliceIndex(rightValueAddress));
    int32_t rightColumnPosition = static_cast<int32_t>(DecodePosition(rightValueAddress));

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
}
}