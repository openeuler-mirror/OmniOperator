#ifndef __WINDOW_PARTITION_H__
#define __WINDOW_PARTITION_H__

#include "../operator_factory.h"
#include "../../vector/table.h"
#include "../pages_index.h"
#include "window_function.h"
#include "../pages_hash_strategy.h"
#include <vector>

using namespace std;

class Range {
private:
    int32_t start;
    int32_t end;

public:
    Range(int32_t start, int32_t end)
    {
        this->start = start;
        this->end = end;
    }

    ~Range() {}

    int32_t getStart()
    {
        return start;
    }

    int32_t getEnd()
    {
        return end;
    }
};

class WindowPartition {
public:
    WindowPartition(PagesIndex *pagesIndex, int32_t partitionStart, int32_t partitionEnd, int32_t *outputChannels,
        int32_t outputChannelsCount, vector<WindowFunction *> &windowFunctions,
        PagesHashStrategy *peerGroupHashStrategy);

    ~WindowPartition();

    int32_t getPartitionEnd()
    {
        return partitionEnd;
    }

    bool hasNext()
    {
        return currentPosition < partitionEnd;
    }

    void processNextRow(Table *table, int32_t index, int32_t *sourceTypes, int32_t typesCount);

    void updatePeerGroup();

    Range *getFrameRange()
    {
        int32_t rowPosition = currentPosition - partitionStart;
        int32_t endPosition = partitionEnd - partitionStart - 1;
        return new Range(0, peerGroupEnd - partitionStart - 1);
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
    vector<WindowFunction *> windowFunctions;
    WindowIndex *windowIndex;
};

bool positionEqualsPosition(PagesIndex *pagesIndex, PagesHashStrategy *partitionHashStrategy, int leftPosition,
    int rightPosition);

#endif