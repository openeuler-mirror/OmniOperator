/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: window operator implementations
 */

#ifndef __WINDOW_FUNCTION_H__
#define __WINDOW_FUNCTION_H__

#include <memory>
#include "vector/vector.h"
#include "operator/pages_index.h"
#include "operator/aggregation/aggregator/aggregator.h"
#include "operator/aggregation/aggregator/aggregator_factory.h"

namespace omniruntime {
namespace op {
class WindowIndex {
public:
    WindowIndex(omniruntime::op::PagesIndex *pagesIndex, int32_t start, int32_t size);
    ~WindowIndex();

    omniruntime::op::PagesIndex *GetPagesIndex() const
    {
        return pagesIndex;
    }

    int32_t GetStart() const
    {
        return start;
    }

    int32_t GetSize() const
    {
        return size;
    }

private:
    omniruntime::op::PagesIndex *pagesIndex;
    int32_t start;
    int32_t size;
};

using FrameType = enum FrameType {
    OMNI_FRAME_TYPE_RANGE = 0,
    OMNI_FRAME_TYPE_ROWS = 1
};

using FrameBoundType = enum FrameBoundType {
    OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING = 0,
    OMNI_FRAME_BOUND_PRECEDING = 1,
    OMNI_FRAME_BOUND_CURRENT_ROW = 2,
    OMNI_FRAME_BOUND_FOLLOWING = 3,
    OMNI_FRAME_BOUND_UNBOUNDED_FOLLOWING = 4
};

class WindowFrameInfo {
public:
    WindowFrameInfo()
        : type(OMNI_FRAME_TYPE_RANGE),
          startType(OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING),
          startChannel(INVALID_BOUND_CHANNEL),
          endType(OMNI_FRAME_BOUND_CURRENT_ROW),
          endChannel(INVALID_BOUND_CHANNEL)
    {}

    WindowFrameInfo(FrameType framType, FrameBoundType frameStartType, int32_t frameStartCol,
        FrameBoundType frameEndType, int32_t frameEndCol)
        : type(framType),
          startType(frameStartType),
          startChannel(frameStartCol),
          endType(frameEndType),
          endChannel(frameEndCol)
    {}

    ~WindowFrameInfo() {}

    FrameType getType()
    {
        return type;
    }

    FrameBoundType getStartType()
    {
        return startType;
    }

    int32_t getStartChannel()
    {
        return startChannel;
    }

    FrameBoundType getEndType()
    {
        return endType;
    }

    int32_t getEndChannel()
    {
        return endChannel;
    }

public:
    static const int32_t INVALID_BOUND_CHANNEL = -1;

private:
    FrameType type;
    FrameBoundType startType;
    int32_t startChannel;
    FrameBoundType endType;
    int32_t endChannel;
};

class WindowFunction {
public:
    WindowFunction()
    {
        frameInfo = std::make_unique<WindowFrameInfo>();
    }

    WindowFunction(std::unique_ptr<WindowFrameInfo> frame) : frameInfo(std::move(frame)) {}

    virtual ~WindowFunction() {}

    virtual void Reset(WindowIndex *windowIndex) {};
    virtual void ProcessRow(omniruntime::vec::Vector *column, int32_t index, int32_t peerGroupStart,
        int32_t peerGroupEnd, int32_t frameStart, int32_t frameEnd) {};

    WindowFrameInfo *GetWindowFrameInfo()
    {
        return frameInfo.get();
    };

private:
    std::unique_ptr<WindowFrameInfo> frameInfo;
};

class RankingWindowFunction : public WindowFunction {
public:
    void Reset(WindowIndex *pWindowIndex) override;
    void ProcessRow(omniruntime::vec::Vector *column, int32_t index, int32_t peerGroupStart, int32_t peerGroupEnd,
        int32_t frameStart, int32_t frameEnd) override;
    virtual void Reset() {};
    virtual void RankingProcessRow(omniruntime::vec::Vector *column, int32_t index, bool newPeerGroup,
        int32_t peerGroupCount, int32_t currentPositionIndex) {};
    RankingWindowFunction(std::unique_ptr<WindowFrameInfo> frame);
    ~RankingWindowFunction() override;

protected:
    WindowIndex *windowIndex;

private:
    int32_t currentPeerGroupStart;
    int32_t currentPosition;
};

class RankFunction : public RankingWindowFunction {
public:
    RankFunction(std::unique_ptr<WindowFrameInfo> frame);
    ~RankFunction() override;
    void Reset() override;
    void RankingProcessRow(omniruntime::vec::Vector *column, int32_t index, bool newPeerGroup, int32_t peerGroupCount,
        int32_t currentPositionIndex) override;

private:
    long rank;
    long count;
};

class RowNumberFunction : public RankingWindowFunction {
public:
    RowNumberFunction(std::unique_ptr<WindowFrameInfo> frame) : RankingWindowFunction(std::move(frame)) {};
    ~RowNumberFunction() override = default;
    void RankingProcessRow(omniruntime::vec::Vector *column, int32_t index, bool newPeerGroup, int32_t peerGroupCount,
        int32_t currentPositionIndex) override;
};

class AggregateWindowFunction : public WindowFunction {
public:
    AggregateWindowFunction(int32_t argumentChannels, int32_t aggregationType, omniruntime::type::DataTypePtr inputType,
        omniruntime::type::DataTypePtr outputType, omniruntime::vec::VectorAllocator *allocator,
        std::unique_ptr<WindowFrameInfo> frame);
    ~AggregateWindowFunction() override;
    void Reset(WindowIndex *pWindowIndex) override;
    void ProcessRow(omniruntime::vec::Vector *column, int32_t index, int32_t peerGroupStart, int32_t peerGroupEnd,
        int32_t frameStart, int32_t frameEnd) override;
    void ResetAccumulator();

private:
    WindowIndex *windowIndex;
    int32_t argumentChannels;
    std::unique_ptr<omniruntime::op::AggregatorFactory> aggregatorFactory;
    int32_t currentStart;
    int32_t currentEnd;
    const omniruntime::type::DataTypePtr inputType;
    const omniruntime::type::DataTypePtr outputType;
    std::unique_ptr<omniruntime::op::Aggregator> aggregator;
    std::unique_ptr<omniruntime::op::AggregateState> aggregateState;
    omniruntime::vec::VectorAllocator *allocator;

    void EvaluateFinal(std::unique_ptr<omniruntime::op::Aggregator> &pAggregator, omniruntime::vec::Vector *pColumn,
        int32_t index) const;

    void Accumulate(omniruntime::vec::VectorAllocator *vecAllocator, omniruntime::vec::VectorEncoding vectorEncoding,
        int32_t start, int32_t end);

    void AccumulateData(omniruntime::vec::VectorBatch *resultVectorBatch, int32_t resultVectorPosition,
        omniruntime::vec::Vector ***inputVectors, int64_t inputAddress);

    void AssignValueForVector(omniruntime::vec::Vector *originalVector, int32_t originalVectorPosition,
        omniruntime::vec::Vector *resultVector, int32_t resultVectorPosition);

    template <typename T>
    void SetValue(omniruntime::vec::Vector *inputVector, int32_t inputPosition, omniruntime::vec::Vector *outputVector,
        int32_t outputPosition);
};
}
}
#endif