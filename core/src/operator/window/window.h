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

    Operator *CreateOperator();

    int32_t *GetSourceTypes() const
    {
        return sourceTypes;
    }

    int32_t getTypesCount() const
    {
        return typesCount;
    }

    int32_t *GetOutputCols() const
    {
        return outputCols;
    }

    int32_t GetOutputColsCount() const
    {
        return outputColsCount;
    }

    int32_t *getWindowFunctionTypes() const
    {
        return windowFunctionTypes;
    }

    int32_t getWindowFunctionCount() const
    {
        return windowFunctionCount;
    }

    int32_t *getPartitionCols() const
    {
        return partitionCols;
    }

    int32_t getPartitionCount() const
    {
        return partitionCount;
    }

    int32_t *getPreGroupedCols() const
    {
        return preGroupedCols;
    }

    int32_t getPreGroupedCount() const
    {
        return preGroupedCount;
    }

    int32_t *getSortCols() const
    {
        return sortCols;
    }

    int32_t *getSortAscendings() const
    {
        return sortAscendings;
    }

    int32_t *getSortNullFirsts() const
    {
        return sortNullFirsts;
    }

    int32_t getSortColCount() const
    {
        return sortColCount;
    }

    int32_t getPreSortedChannelPrefix() const
    {
        return preSortedChannelPrefix;
    }

    int32_t getExpectedPositions() const
    {
        return expectedPositions;
    }

    int32_t *getAllTypes() const
    {
        return allTypes;
    }

    int32_t getAllCount() const
    {
        return allCount;
    }

    int32_t *getArgumentChannels() const
    {
        return argumentChannels;
    }

    int32_t getArgumentChannelsCount() const
    {
        return argumentChannelsCount;
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

    int32_t AddInput(VectorBatch *vecBatch) override;
    int32_t GetOutput(std::vector<VectorBatch *> &outputPages) override;
    int32_t *GetSourceTypes() override
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