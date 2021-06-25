#ifndef __WINDOW_H__
#define __WINDOW_H__

#include "../operator_factory.h"
#include "../pages_index.h"
#include "window_partition.h"

#include <vector>

using namespace std;
namespace omniruntime {
namespace op {
class WindowOperatorFactory : public OperatorFactory {
public:
    WindowOperatorFactory(int32_t *sourceTypes, int32_t typesCount, int32_t *outputCols, int32_t outputColsCount,
        int32_t *windowFunctionTypes, int32_t windowFunctionCount, int32_t *partitionCols, int32_t partitionCount,
        int32_t *preGroupedCols, int32_t preGroupedCount, int32_t *sortCols, int32_t *sortAscendings,
        int32_t *sortNullFirsts, int32_t sortColCount, int32_t preSortedChannelPrefix, int32_t expectedPositions,
        int32_t *allTypes, int32_t allCount, int32_t *argumentChannels, int32_t argumentChannelsCount);

    ~WindowOperatorFactory();

    static WindowOperatorFactory *createWindowOperatorFactory(int32_t *sourceTypes, int32_t typesCount,
        int32_t *outputCols, int32_t outputColsCount, int32_t *windowFunctionTypes, int32_t windowFunctionCount,
        int32_t *partitionCols, int32_t partitionCount, int32_t *preGroupedCols, int32_t preGroupedCount,
        int32_t *sortCols, int32_t *sortAscendings, int32_t *sortNullFirsts, int32_t sortColCount,
        int32_t preSortedChannelPrefix, int32_t expectedPositions, int32_t *allTypes, int32_t allCount,
        int32_t *argumentChannels, int32_t argumentChannelsCount);

    Operator *createOperator();

private:
    int32_t *sourceTypes;
    int32_t typesCount;
    int32_t *outputCols;
    int32_t outputColsCount;
    int32_t *windowFunctionTypes;
    int32_t windowFunctionCount;
    int32_t *partitionCols;
    int32_t partitionCount;
    int32_t *preGroupedCols;
    int32_t preGroupedCount;
    int32_t *sortCols;
    int32_t *sortAscendings;
    int32_t *sortNullFirsts;
    int32_t sortColCount;
    int32_t preSortedChannelPrefix;
    int32_t expectedPositions;
    int32_t *allTypes;
    int32_t allCount;
    int32_t *argumentChannels;
    int32_t argumentChannelsCount;
};

class WindowOperator : public Operator {
public:
    WindowOperator(int32_t *sourceTypes, int32_t typesCount, int32_t *outputCols, int32_t outputColsCount,
        int32_t *windowFunctionTypes, int32_t windowFunctionCount, int32_t *partitionCols, int32_t partitionCount,
        int32_t *preGroupedCols, int32_t preGroupedCount, int32_t *sortCols, int32_t *sortAscendings,
        int32_t *sortNullFirsts, int32_t sortColCount, int32_t preSortedChannelPrefix, int32_t expectedPositions,
        int32_t *allTypes, int32_t allCount, int32_t *argumentChannels, int32_t argumentChannelsCount);

    ~WindowOperator();

    int32_t addInput(VectorBatch *vecBatch) override;
    int32_t getOutput(std::vector<VectorBatch *> &outputPages) override;
    int32_t *getSourceTypes() override
    {
        return sourceTypes;
    }
    bool processPendingInput();
    VectorBatch *updatePagesIndex(VectorBatch *vecBatch);
    void sortPagesIndexIfNecessary();
    void finishPagesIndex();
    PagesIndex *getPagesIndex()
    {
        return pagesIndex;
    }
    VectorBatch *getPendingInput()
    {
        return pendingInput;
    }

private:
    int32_t *sourceTypes;
    int32_t typesCount;
    int32_t *outputCols;
    int32_t outputColsCount;
    int32_t *windowFunctionTypes;
    int32_t windowFunctionCount;
    int32_t *partitionCols;
    int32_t partitionCount;
    int32_t *preGroupedCols;
    int32_t preGroupedCount;
    int32_t *originSortCols;
    int32_t originSortColCount;
    int32_t *sortCols;
    int32_t *sortAscendings;
    int32_t *sortNullFirsts;
    int32_t sortColCount;
    int32_t preSortedChannelPrefix;
    int32_t expectedPositions;
    int32_t *allTypes;
    int32_t allCount;
    PagesIndex *pagesIndex;
    VectorBatch *pendingInput;
    PagesHashStrategy *preGroupedPartitionHashStrategy = nullptr;
    PagesHashStrategy *unGroupedPartitionHashStrategy = nullptr;
    PagesHashStrategy *preSortedPartitionHashStrategy = nullptr;
    PagesHashStrategy *peerGroupHashStrategy = nullptr;
    WindowPartition *partition;
    vector<WindowFunction *> windowFunctions;
    vector<VectorBatch *> inputVecBatches;
};

int32_t findGroupEnd(PagesIndex *pagesIndex, PagesHashStrategy *pagesHashStrategy, int32_t startPosition);
}
}
#endif