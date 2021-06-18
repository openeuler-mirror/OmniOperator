#ifndef __WINDOW_FUNCTION_H__
#define __WINDOW_FUNCTION_H__

#include "../../vector/table.h"
#include "../pages_index.h"
#include "../aggregation/aggregator.h"
#include <vector>

typedef enum WindowFunctionType {
    WIN_ROW_NUMBER = 0,
    WIN_RANK,
    WIN_SUM,
    WIN_COUNT,
    WIN_AVG,
    WIN_MAX,
    WIN_MIN
} WindowFunctionType;

class WindowIndex {
public:
    WindowIndex(PagesIndex *pagesIndex, int32_t start, int32_t size);
    ~WindowIndex();

    PagesIndex *getPagesIndex()
    {
        return pagesIndex;
    }

private:
    PagesIndex *pagesIndex;
    int32_t start;
    int32_t size;
};

class WindowFunction {
public:
    WindowFunction() {};
    ~WindowFunction() {};
    virtual void reset(WindowIndex *windowIndex) {};
    virtual void processRow(Column *column, int32_t index, int32_t peerGroupStart, int32_t peerGroupEnd,
        int32_t frameStart, int32_t frameEnd) {};
};

class RankingWindowFunction : public WindowFunction {
public:
    void reset(WindowIndex *windowIndex) override;
    void processRow(Column *column, int32_t index, int32_t peerGroupStart, int32_t peerGroupEnd, int32_t frameStart,
        int32_t frameEnd) override;
    virtual void reset() {};
    virtual void processRow(Column *column, int32_t index, bool newPeerGroup, int32_t peerGroupCount,
        int32_t currentPosition) {};
    RankingWindowFunction() {};
    ~RankingWindowFunction();

protected:
    WindowIndex *windowIndex;

private:
    int32_t currentPeerGroupStart;
    int32_t currentPosition;
};

class RankFunction : public RankingWindowFunction {
public:
    RankFunction() {};
    ~RankFunction() {};
    void reset() override;
    void processRow(Column *column, int32_t index, bool newPeerGroup, int32_t peerGroupCount,
        int32_t currentPosition) override;

private:
    long rank;
    long count;
};

class RowNumberFunction : public RankingWindowFunction {
public:
    RowNumberFunction() {};
    ~RowNumberFunction() {};
    void processRow(Column *column, int32_t index, bool newPeerGroup, int32_t peerGroupCount,
        int32_t currentPosition) override;
};

class AggregateWindowFunction : public WindowFunction {
public:
    AggregateWindowFunction(int32_t argumentChannels, int32_t aggregationType,
        int32_t dataType);
    ~AggregateWindowFunction();
    void reset(WindowIndex *windowIndex) override;
    void processRow(Column *column, int32_t index, int32_t peerGroupStart, int32_t peerGroupEnd, int32_t frameStart,
        int32_t frameEnd) override;
    void resetAccumulator();

private:
    WindowIndex *windowIndex;
    int32_t argumentChannels;
    int32_t aggregationType;
    int32_t currentStart;
    int32_t currentEnd;
    int32_t dataType;
    omniruntime::op::Aggregator *aggregator;

    void evaluateFinal(omniruntime::op::Aggregator *pAggregator, Column *pColumn, int32_t index);
    void accumulate(int32_t start, int32_t end);
};
#endif