/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: window partiton implementations
 */
#ifndef __WINDOW_PARTITION_H__
#define __WINDOW_PARTITION_H__

#include <vector>
#include "type/data_types.h"
#include "operator/pages_index.h"
#include "window_function.h"
#include "operator/pages_hash_strategy.h"

namespace omniruntime {
namespace op {
class Range {
public:
    Range(int32_t start, int32_t end) : start(start), end(end) {}

    ~Range() = default;

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
    WindowPartition(const type::DataTypes &sourceTypes, PagesIndex *pagesIndex, int32_t partitionStart,
        int32_t partitionEnd, int32_t *outputChannels, int32_t outputChannelsCount,
        std::vector<std::unique_ptr<WindowFunction>> &windowFunctions, PagesHashStrategy *peerGroupHashStrategy);

    ~WindowPartition();

    int32_t GetPartitionEnd() const
    {
        return partitionEnd;
    }

    bool HasNext() const
    {
        return currentPosition < partitionEnd;
    }

    void ProcessNextRow(VectorBatch *inputVecBatchForAgg, VectorBatch *outputVecBatch, int32_t index);

    void UpdatePeerGroup();

private:
    std::unique_ptr<Range> GetFrameRange(WindowFrameInfo *frameInfo);

    bool IsEmptyFrame(WindowFrameInfo *frameInfo, int32_t rowPosition, int32_t endPosition);

    int32_t GetFrameStart(WindowFrameInfo *frameInfo, int32_t rowPosition, int32_t endPosition);

    int32_t GetFrameEnd(WindowFrameInfo *frameInfo, int32_t rowPosition, int32_t endPosition);

    static int32_t Preceding(int32_t rowPosition, int64_t value);

    static int32_t Following(int32_t rowPosition, int32_t endPosition, int64_t value);

    int64_t GetStartValue(WindowFrameInfo *frameInfo);

    int64_t GetEndValue(WindowFrameInfo *frameInfo);

    int64_t GetFrameValue(int32_t channel, std::string &valueTypeName);

private:
    type::DataTypes sourceTypes;
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
    std::shared_ptr<WindowIndex> windowIndex;
};

bool PositionEqualsPosition(PagesIndex *pagesIndex, PagesHashStrategy *partitionHashStrategy, int32_t leftPosition,
    int32_t rightPosition);
}
}
#endif