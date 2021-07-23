/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: window partiton implementations
 */
#ifndef __WINDOW_PARTITION_H__
#define __WINDOW_PARTITION_H__

#include <vector>
#include "../pages_index.h"
#include "window_function.h"
#include "../pages_hash_strategy.h"

class Range {
public:
    Range(int32_t start, int32_t end)
    {
        this->start = start;
        this->end = end;
    }

    ~Range() {}

    int32_t GetStart() const
    {
        return start;
    }

    int32_t GetEnd() const
    {
        return end;
    }

private:
    int32_t start;
    int32_t end;
};

class WindowPartition {
public:
    WindowPartition(PagesIndex *pagesIndex, int32_t partitionStart, int32_t partitionEnd, int32_t *outputChannels,
        int32_t outputChannelsCount, std::vector<std::unique_ptr<WindowFunction>> &windowFunctions,
        PagesHashStrategy *peerGroupHashStrategy);

    ~WindowPartition();

    int32_t GetPartitionEnd() const
    {
        return partitionEnd;
    }

    bool HasNext() const
    {
        return currentPosition < partitionEnd;
    }

    void ProcessNextRow(VectorBatch *vecBatch, int32_t index, int32_t *sourceTypes, int32_t typesCount);

    void UpdatePeerGroup();

    Range *GetFrameRange()
    {
        int32_t rowPosition = currentPosition - partitionStart;
        int32_t endPosition = partitionEnd - partitionStart - 1;
        return std::make_unique<Range>(0, peerGroupEnd - partitionStart - 1).release();
    }

private:
    PagesIndex *pagesIndex;
    int32_t partitionStart;
    int32_t partitionEnd;
    int32_t *outputChannels;
    int32_t outputChannelsCount;
    PagesHashStrategy *peerGroupHashStrategy;
    int32_t currentPosition;
    int32_t peerGroupStart;
    int32_t peerGroupEnd;
    std::vector<WindowFunction *> windowFunctions;
    WindowIndex *windowIndex;
};

bool PositionEqualsPosition(PagesIndex *pagesIndex, PagesHashStrategy *partitionHashStrategy, int32_t leftPosition,
    int32_t rightPosition);

#endif