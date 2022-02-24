/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: window operator implementations
 */

#ifndef __WINDOW_FUNCTION_H__
#define __WINDOW_FUNCTION_H__

#include "../../vector/vector.h"
#include "../pages_index.h"
#include "operator/aggregation/aggregator/aggregator.h"
#include "operator/aggregation/aggregator/aggregator_factory.h"

#include <memory>


using namespace omniruntime::vec;

class WindowIndex {
public:
    WindowIndex(PagesIndex *pagesIndex, int32_t start, int32_t size);
    ~WindowIndex();

    PagesIndex *GetPagesIndex() const
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
    PagesIndex *pagesIndex;
    int32_t start;
    int32_t size;
};

class WindowFunction {
public:
    WindowFunction() = default;
    ~WindowFunction() = default;
    virtual void Reset(WindowIndex *windowIndex) {};
    virtual void ProcessRow(Vector *column, int32_t index, int32_t peerGroupStart, int32_t peerGroupEnd,
        int32_t frameStart, int32_t frameEnd) {};
};

class RankingWindowFunction : public WindowFunction {
public:
    void Reset(WindowIndex *pWindowIndex) override;
    void ProcessRow(Vector *column, int32_t index, int32_t peerGroupStart, int32_t peerGroupEnd, int32_t frameStart,
        int32_t frameEnd) override;
    virtual void Reset() {};
    virtual void RankingProcessRow(Vector *column, int32_t index, bool newPeerGroup, int32_t peerGroupCount,
        int32_t currentPositionIndex) {};
    RankingWindowFunction();
    ~RankingWindowFunction();

protected:
    WindowIndex *windowIndex;

private:
    int32_t currentPeerGroupStart;
    int32_t currentPosition;
};

class RankFunction : public RankingWindowFunction {
public:
    RankFunction();
    ~RankFunction();
    void Reset() override;
    void RankingProcessRow(Vector *column, int32_t index, bool newPeerGroup, int32_t peerGroupCount,
        int32_t currentPositionIndex) override;

private:
    long rank;
    long count;
};

class RowNumberFunction : public RankingWindowFunction {
public:
    RowNumberFunction() = default;
    ~RowNumberFunction() = default;
    void RankingProcessRow(Vector *column, int32_t index, bool newPeerGroup, int32_t peerGroupCount,
        int32_t currentPositionIndex) override;
};

class AggregateWindowFunction : public WindowFunction {
public:
    AggregateWindowFunction(int32_t argumentChannels, int32_t aggregationType, const VecType &inputType,
        const VecType &outputType);
    ~AggregateWindowFunction();
    void Reset(WindowIndex *pWindowIndex) override;
    void ProcessRow(Vector *column, int32_t index, int32_t peerGroupStart, int32_t peerGroupEnd, int32_t frameStart,
        int32_t frameEnd) override;
    void ResetAccumulator();

private:
    WindowIndex *windowIndex;
    int32_t argumentChannels;
    std::unique_ptr<omniruntime::op::AggregatorFactory> aggregatorFactory;
    int32_t currentStart;
    int32_t currentEnd;
    const VecType &inputType;
    const VecType &outputType;
    std::unique_ptr<omniruntime::op::Aggregator> aggregator;
    std::unique_ptr<omniruntime::op::AggregateState> aggregateState;

    void EvaluateFinal(std::unique_ptr<omniruntime::op::Aggregator> &pAggregator, Vector *pColumn, int32_t index) const;
    void Accumulate(VectorAllocator *vecAllocator, int32_t start, int32_t end);

    void AccumulateData(int32_t start, VectorBatch *resultVectorBatch, int32_t resultVectorPosition,
        int32_t originalVectorPosition, Vector *originalVector);
};
#endif