/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: window operator implementations
 */

#ifndef __WINDOW_FUNCTION_H__
#define __WINDOW_FUNCTION_H__

#include <memory>
#include "vector/vector.h"
#include "operator/pages_index.h"
#include "operator/aggregation/aggregator/only_aggregator_factory.h"
#include "window_frame.h"

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

class WindowFunction {
public:
    WindowFunction()
    {
        frameInfo = std::make_unique<WindowFrameInfo>();
    }

    WindowFunction(std::unique_ptr<WindowFrameInfo> frame, DataTypePtr inputType, DataTypePtr outputType)
        : inputType(std::move(inputType)), outputType(std::move(outputType)), frameInfo(std::move(frame))
    {}

    virtual ~WindowFunction() = default;

    virtual void Reset(WindowIndex *windowIndex){};
    virtual void ProcessRow(VectorBatch *inputVecBatchForAgg, BaseVector *column, int32_t index, int32_t peerGroupStart,
        int32_t peerGroupEnd, int32_t frameStart, int32_t frameEnd){};

    WindowFrameInfo *GetWindowFrameInfo()
    {
        return frameInfo.get();
    };

protected:
    const DataTypePtr inputType;
    const DataTypePtr outputType;

private:
    std::unique_ptr<WindowFrameInfo> frameInfo;
};

class RankingWindowFunction : public WindowFunction {
public:
    void Reset(WindowIndex *pWindowIndex) override;
    void ProcessRow(VectorBatch *vectorBatch, BaseVector *column, int32_t index, int32_t peerGroupStart,
        int32_t peerGroupEnd, int32_t frameStart, int32_t frameEnd) override;
    virtual void Reset(){};
    virtual void RankingProcessRow(BaseVector *column, int32_t index, bool newPeerGroup, int32_t peerGroupCount,
        int32_t currentPositionIndex){};
    RankingWindowFunction(std::unique_ptr<WindowFrameInfo> frame, DataTypePtr inputType, DataTypePtr outputType);
    ~RankingWindowFunction() override;

protected:
    WindowIndex *windowIndex;

private:
    int32_t currentPeerGroupStart;
    int32_t currentPosition;
};

class RankFunction : public RankingWindowFunction {
public:
    RankFunction(std::unique_ptr<WindowFrameInfo> frame, DataTypePtr inputType, DataTypePtr outputType);
    ~RankFunction() override;
    void Reset() override;
    void RankingProcessRow(BaseVector *column, int32_t index, bool newPeerGroup, int32_t peerGroupCount,
        int32_t currentPositionIndex) override;

private:
    long rank;
    long count;
};

class RowNumberFunction : public RankingWindowFunction {
public:
    RowNumberFunction(std::unique_ptr<WindowFrameInfo> frame, DataTypePtr inputType, DataTypePtr outputType)
        : RankingWindowFunction(std::move(frame), std::move(inputType), std::move(outputType)){};
    ~RowNumberFunction() override = default;
    void RankingProcessRow(BaseVector *column, int32_t index, bool newPeerGroup, int32_t peerGroupCount,
        int32_t currentPositionIndex) override;
};

class AggregateWindowFunction : public WindowFunction {
public:
    AggregateWindowFunction(int32_t argumentChannel, int32_t aggregationType, DataTypePtr inputType,
        DataTypePtr outputType, std::unique_ptr<WindowFrameInfo> frame, ExecutionContext *executionContext,
        bool isOverflowAsNull = false);
    ~AggregateWindowFunction() override;
    void Reset(WindowIndex *pWindowIndex) override;
    void ProcessRow(VectorBatch *inputVecBatchForAgg, BaseVector *column, int32_t index, int32_t peerGroupStart,
        int32_t peerGroupEnd, int32_t frameStart, int32_t frameEnd) override;
    void ResetAccumulator();

private:
    WindowIndex *windowIndex;
    std::vector<int32_t> argumentChannels;
    std::unique_ptr<omniruntime::op::AggregatorFactory> aggregatorFactory;
    int32_t currentStart;
    int32_t currentEnd;
    std::unique_ptr<Aggregator> aggregator;
    AggregateState *aggregateState;
    ExecutionContext *executionContext;
    bool isOverflowAsNull;

    void EvaluateFinal(std::unique_ptr<Aggregator> &pAggregator, BaseVector *pColumn, int32_t index) const;

    void Accumulate(omniruntime::vec::VectorBatch *inputVecBatchForAgg, int32_t start, int32_t end);
};
}
}
#endif