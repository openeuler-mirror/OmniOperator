#include "window.h"
#include "../sort/sort.h"
#include <cstring>

using namespace std;
namespace omniruntime {
namespace op {
WindowOperatorFactory::WindowOperatorFactory(int32_t *sourceTypes, int32_t typesCount, int32_t *outputCols,
    int32_t outputColsCount, int32_t *windowFunctionTypes, int32_t windowFunctionCount, int32_t *partitionCols,
    int32_t partitionCount, int32_t *preGroupedCols, int32_t preGroupedCount, int32_t *sortCols,
    int32_t *sortAscendings, int32_t *sortNullFirsts, int32_t sortColCount, int32_t preSortedChannelPrefix,
    int32_t expectedPositions, int32_t *allTypes, int32_t allCount, int32_t *argumentChannels,
    int32_t argumentChannelsCount)
{
    int32_t intByteLen = sizeof(int32_t);
    this->sourceTypes = new int32_t[typesCount];
    memcpy(this->sourceTypes, sourceTypes, typesCount * intByteLen);
    this->typesCount = typesCount;

    this->outputCols = new int32_t[outputColsCount];
    memcpy(this->outputCols, outputCols, outputColsCount * intByteLen);
    this->outputColsCount = outputColsCount;

    this->windowFunctionTypes = new int32_t[windowFunctionCount];
    memcpy(this->windowFunctionTypes, windowFunctionTypes, windowFunctionCount * intByteLen);
    this->windowFunctionCount = windowFunctionCount;

    this->partitionCols = new int32_t[partitionCount];
    memcpy(this->partitionCols, partitionCols, partitionCount * intByteLen);
    this->partitionCount = partitionCount;

    this->preGroupedCols = new int32_t[preGroupedCount];
    memcpy(this->preGroupedCols, preGroupedCols, preGroupedCount * intByteLen);
    this->preGroupedCount = preGroupedCount;

    this->sortCols = new int32_t[sortColCount];
    memcpy(this->sortCols, sortCols, sortColCount * intByteLen);
    this->sortAscendings = new int32_t[sortColCount];
    memcpy(this->sortAscendings, sortAscendings, sortColCount * intByteLen);
    this->sortNullFirsts = new int32_t[sortColCount];
    memcpy(this->sortNullFirsts, sortNullFirsts, sortColCount * intByteLen);
    this->sortColCount = sortColCount;

    this->preSortedChannelPrefix = preSortedChannelPrefix;
    this->expectedPositions = expectedPositions;

    this->allTypes = new int32_t[allCount];
    memcpy(this->allTypes, allTypes, allCount * intByteLen);
    this->allCount = allCount;

    this->argumentChannels = new int32_t[argumentChannelsCount];
    memcpy(this->argumentChannels, argumentChannels, argumentChannelsCount * intByteLen);
    this->argumentChannelsCount = argumentChannelsCount;
}

WindowOperatorFactory::~WindowOperatorFactory()
{
    delete[] sourceTypes;
    delete[] outputCols;
    delete[] windowFunctionTypes;
    delete[] partitionCols;
    delete[] preGroupedCols;
    delete[] sortCols;
    delete[] sortAscendings;
    delete[] sortNullFirsts;
    delete[] allTypes;
    delete[] argumentChannels;
}

WindowOperatorFactory *WindowOperatorFactory::createWindowOperatorFactory(int32_t *sourceTypes, int32_t typesCount,
    int32_t *outputCols, int32_t outputColsCount, int32_t *windowFunctionTypes, int32_t windowFunctionCount,
    int32_t *partitionCols, int32_t partitionCount, int32_t *preGroupedCols, int32_t preGroupedCount, int32_t *sortCols,
    int32_t *sortAscendings, int32_t *sortNullFirsts, int32_t sortColCount, int32_t preSortedChannelPrefix,
    int32_t expectedPositions, int32_t *allTypes, int32_t allCount, int32_t *argumentChannels,
    int32_t argumentChannelsCount)
{
    WindowOperatorFactory *operatorFactory = new WindowOperatorFactory(sourceTypes, typesCount, outputCols,
        outputColsCount, windowFunctionTypes, windowFunctionCount, partitionCols, partitionCount, preGroupedCols,
        preGroupedCount, sortCols, sortAscendings, sortNullFirsts, sortColCount, preSortedChannelPrefix,
        expectedPositions, allTypes, allCount, argumentChannels, argumentChannelsCount);
    return operatorFactory;
}

Operator *WindowOperatorFactory::createOperator()
{
    WindowOperator *windowOperator = new WindowOperator(sourceTypes, typesCount, outputCols, outputColsCount,
        windowFunctionTypes, windowFunctionCount, partitionCols, partitionCount, preGroupedCols, preGroupedCount,
        sortCols, sortAscendings, sortNullFirsts, sortColCount, preSortedChannelPrefix, expectedPositions, allTypes,
        allCount, argumentChannels, argumentChannelsCount);
    return windowOperator;
}

WindowOperator::WindowOperator(int32_t *sourceTypes, int32_t typesCount, int32_t *outputCols, int32_t outputColsCount,
    int32_t *windowFunctionTypes, int32_t windowFunctionCount, int32_t *partitionCols, int32_t partitionCount,
    int32_t *preGroupedCols, int32_t preGroupedCount, int32_t *sortCols, int32_t *sortAscendings,
    int32_t *sortNullFirsts, int32_t sortColCount, int32_t preSortedChannelPrefix, int32_t expectedPositions,
    int32_t *allTypes, int32_t allCount, int32_t *argumentChannels, int32_t argumentChannelsCount)
{
    this->sourceTypes = sourceTypes;
    this->typesCount = typesCount;
    this->outputCols = outputCols;
    this->outputColsCount = outputColsCount;
    this->windowFunctionTypes = windowFunctionTypes;
    this->windowFunctionCount = windowFunctionCount;
    this->partitionCols = partitionCols;
    this->partitionCount = partitionCount;
    this->preGroupedCols = preGroupedCols;
    this->preGroupedCount = preGroupedCount;

    this->originSortCols = sortCols;
    this->originSortColCount = sortColCount;
    this->sortCols = new int32_t[sortColCount + partitionCount];
    this->sortAscendings = new int32_t[sortColCount + partitionCount];
    this->sortNullFirsts = new int32_t[sortColCount + partitionCount];

    for (int32_t i = 0; i < partitionCount; i++) {
        this->sortCols[i] = partitionCols[i];
        this->sortAscendings[i] = true;
        this->sortNullFirsts[i] = false;
    }
    for (int32_t i = partitionCount; i < partitionCount + sortColCount; i++) {
        this->sortCols[i] = sortCols[i - partitionCount];
        this->sortAscendings[i] = sortAscendings[i - partitionCount];
        this->sortNullFirsts[i] = sortNullFirsts[i - partitionCount];
    }

    this->sortColCount = sortColCount + partitionCount;

    this->preSortedChannelPrefix = preSortedChannelPrefix;
    this->expectedPositions = expectedPositions;
    this->allTypes = allTypes;
    this->allCount = allCount;

    this->pagesIndex = new PagesIndex(sourceTypes, typesCount);
    this->pendingInput = nullptr;
    this->partition = nullptr;

    for (int32_t i = 0; i < windowFunctionCount; i++) {
        switch (windowFunctionTypes[i]) {
            case WIN_ROW_NUMBER:
                windowFunctions.push_back(new RowNumberFunction());
                break;
            case WIN_RANK:
                windowFunctions.push_back(new RankFunction());
                break;
            case WIN_SUM:
            case WIN_COUNT:
            case WIN_AVG:
            case WIN_MAX:
            case WIN_MIN:
                windowFunctions.push_back(new AggregateWindowFunction(argumentChannels[i], windowFunctionTypes[i],
                    sourceTypes[argumentChannels[i]]));
                break;
            default:
                break;
        }
    }
}

WindowOperator::~WindowOperator()
{
    delete[] sortCols;
    delete[] sortAscendings;
    delete[] sortNullFirsts;
    delete pagesIndex;
    delete preGroupedPartitionHashStrategy;
    delete unGroupedPartitionHashStrategy;
    delete preSortedPartitionHashStrategy;
    delete peerGroupHashStrategy;
    delete pendingInput;
    delete partition;
    for (auto w : windowFunctions) {
        delete w;
    }
}

int32_t WindowOperator::addInput(Table **datas, int32_t *rowCounts, int32_t pageCount)
{
    if (pageCount <= 0) {
        return 0;
    }
    pagesIndex->addTables(datas, rowCounts, pageCount);
    // right now we assume the pregroup and presort are null
    preGroupedPartitionHashStrategy = new PagesHashStrategy(pagesIndex->getColumns(), pageCount,
        pagesIndex->getTypes(), pagesIndex->getTypesCount(), preGroupedCols, preGroupedCount);
    unGroupedPartitionHashStrategy = new PagesHashStrategy(pagesIndex->getColumns(), pageCount,
        pagesIndex->getTypes(), pagesIndex->getTypesCount(), partitionCols, partitionCount);
    preSortedPartitionHashStrategy = new PagesHashStrategy(pagesIndex->getColumns(), pageCount,
        pagesIndex->getTypes(), pagesIndex->getTypesCount(), preGroupedCols, preGroupedCount);
    peerGroupHashStrategy = new PagesHashStrategy(pagesIndex->getColumns(), pageCount,pagesIndex->getTypes(), pagesIndex->getTypesCount(),
        originSortCols, originSortColCount);
    return 0;
}

int32_t WindowOperator::getOutput(vector<Table *> &outputTables)
{
    int32_t positionCount = pagesIndex->getPositionCount();
    int32_t finalOutputColsCount = 0;
    if (positionCount <= 0) {
        return 0;
    }

    finishPagesIndex();

    int32_t finalOutputCols[allCount];
    for (int32_t i = 0; i < outputColsCount; i++) {
        finalOutputCols[finalOutputColsCount] = outputCols[i];
        finalOutputColsCount++;
    }

    for (int32_t i = typesCount; i < allCount; i++) {
        finalOutputCols[finalOutputColsCount] = i;
        finalOutputColsCount++;
    }

    // next, get output
    int32_t maxRowCount = getMaxRowCount(allTypes, finalOutputCols, finalOutputColsCount);
    int32_t tableCount = getTableCount(positionCount, maxRowCount);
    outputTables.reserve(tableCount);

    Table *table = NULL;
    int32_t position = 0;
    int32_t rowCount = 0;
    for (int32_t i = 0; i < tableCount; i++) {
        rowCount = min(maxRowCount, positionCount - position);
        table = new Table(rowCount, finalOutputColsCount);
        allocColumns((int64_t)table, allTypes, finalOutputCols, finalOutputColsCount, rowCount);
        pagesIndex->getOutput(outputCols, outputColsCount, (int64_t)table, sourceTypes, position, rowCount);
        for (int32_t j = 0; j < rowCount; j++) {
            if (partition == nullptr || !partition->hasNext()) {
                int partitionStart = partition == nullptr ? 0 : partition->getPartitionEnd();
                if (partitionStart >= pagesIndex->getPositionCount()) {
                    partition = nullptr;
                    // pagesIndex.clear();
                    break;
                }
                int partitionEnd = findGroupEnd(pagesIndex, unGroupedPartitionHashStrategy, partitionStart);
                partition = new WindowPartition(pagesIndex, partitionStart, partitionEnd, outputCols, outputColsCount,
                    windowFunctions, peerGroupHashStrategy);
            }
            partition->processNextRow(table, j, allTypes, typesCount);
        }

        position += rowCount;
        outputTables.push_back(table);
    }
    status = 2;
    return 0;
}

void WindowOperator::finishPagesIndex()
{
    sortPagesIndexIfNecessary();
}

void WindowOperator::sortPagesIndexIfNecessary()
{
    if (pagesIndex->getPositionCount() > 1 && sortColCount != 0) {
        int32_t sortColTypes[sortColCount];
        for (int32_t i = 0; i < sortColCount; i++) {
            sortColTypes[i] = sourceTypes[sortCols[i]];
        }

        int32_t startPosition = 0;
        auto positionCount = pagesIndex->getPositionCount();
        while (startPosition < positionCount) {
            int32_t endPosition = findGroupEnd(pagesIndex, preSortedPartitionHashStrategy, startPosition);
            pagesIndex->sort(sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, startPosition,
                endPosition);
            startPosition = endPosition;
        }
    }
}

int32_t findGroupEnd(PagesIndex *pagesIndex, PagesHashStrategy *pagesHashStrategy, int32_t startPosition)
{
    int32_t left = startPosition;
    int32_t right = pagesIndex->getPositionCount();

    while (left + 1 < right) {
        int32_t middle = left + (right - left) / 2;

        if (positionEqualsPosition(pagesIndex, pagesHashStrategy, startPosition, middle)) {
            left = middle;
        } else {
            right = middle;
        }
    }

    return right;
}
}
}