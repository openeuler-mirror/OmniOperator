#include "sort.h"
#include "../../util/type_infer.h"
#include "../../util/debug.h"
#include "../../memory/memory_pool.h"
#include <iostream>
#include <algorithm>
#include <cstring>

using namespace std;

int32_t DEFAULT_MAX_PAGE_SIZE_IN_BYTES = 1 * 1024 * 1024;

int32_t getColTypeIdx(ColumnType type)
{
    if (type == INT32) {
        return 1;
    }
    else if (type == INT64) {
        return 2;
    }
    else if (type == DOUBLE) {
        return 3;
    }
    else {
        return 1;
    }
}

int32_t getMaxRowCount(int32_t *sourceTypes, int32_t *outputCols, int32_t outputColsCount)
{
    int32_t rowSize = 0;
    int type;
    for (int32_t i = 0; i < outputColsCount; i++) {
        type = sourceTypes[outputCols[i]];
        switch (type)
        {
        case 1:
            rowSize = rowSize + sizeof(int32_t);
            break;
        case 2:
            rowSize = rowSize + sizeof(int64_t);
            break;
        case 3:
            rowSize = rowSize + sizeof(double);
            break;
        default:
            break;
        }
    }

    int32_t maxRowCount = (DEFAULT_MAX_PAGE_SIZE_IN_BYTES + rowSize - 1) / rowSize;
    return maxRowCount;
}

int32_t getTableCount(int32_t positionCount, int32_t maxRowCount)
{
    return ((positionCount + maxRowCount - 1) / maxRowCount);
}

void allocColumns(int64_t outputTableAddr, int32_t *sourceTypes, int32_t *outputCols, int32_t outputColCount, int32_t positionCount)
{
    Table *outputTable = (Table *)outputTableAddr;
    int32_t outputCol;
    int32_t columnTypeIdx;
    void *data = NULL;
    Column *column = NULL;
    
    for (int32_t i = 0; i < outputColCount; i++) {
        outputCol = outputCols[i];
        columnTypeIdx = sourceTypes[outputCol];
        switch (columnTypeIdx)
        {
        case 1:
            data = omni_allocate(positionCount * sizeof(int32_t));
            column = new Column(data, INT32, positionCount);
            outputTable->setColumn(column, INT32);
            break;
        case 2:
            data = omni_allocate(positionCount * sizeof(int64_t));
            column = new Column(data, INT64, positionCount);
            outputTable->setColumn(column, INT64);
            break;
        case 3:
            data = omni_allocate(positionCount * sizeof(double));
            column = new Column(data, DOUBLE, positionCount);
            outputTable->setColumn(column, DOUBLE);
            break;    
        default:
            printf("unsupported type.");
            break;
        }
    }
}

SortOperatorFactory::SortOperatorFactory(
    int32_t *sourceTypes,
    int32_t sourceTypeCount,
    int32_t *outputCols,
    int32_t outputColCount,
    int32_t *sortCols,
    int32_t *sortAscendings,
    int32_t *sortNullFirsts,
    int32_t sortColCount)
{
    int32_t intByteLen = sizeof(int32_t);

    this->sourceTypes = new int32_t[sourceTypeCount];
    memcpy(this->sourceTypes, sourceTypes, sourceTypeCount * intByteLen);
    this->sourceTypeCount = sourceTypeCount;

    this->outputCols = new int32_t[outputColCount];
    memcpy(this->outputCols, outputCols, outputColCount * intByteLen);
    this->outputColCount = outputColCount;

    int32_t sortColByteLen = sortColCount * intByteLen;
    this->sortCols = new int32_t[sortColCount];
    memcpy(this->sortCols, sortCols, sortColByteLen);

    this->sortAscendings = new int32_t[sortColCount];
    memcpy(this->sortAscendings, sortAscendings, sortColByteLen);

    this->sortNullFirsts = new int32_t[sortColCount];
    memcpy(this->sortNullFirsts, sortNullFirsts, sortColByteLen);

    this->sortColCount = sortColCount;
}

SortOperatorFactory::~SortOperatorFactory()
{
    delete[] sourceTypes;
    delete[] outputCols;
    delete[] sortCols;
    delete[] sortAscendings;
    delete[] sortNullFirsts;
}

SortOperatorFactory * SortOperatorFactory::createOperatorFactory(
    int32_t *sourceTypes,
    int32_t sourceTypeCount,
    int32_t *outputCols,
    int32_t outputColCount,
    int32_t *sortCols,
    int32_t *sortAscendings,
    int32_t *sortNullFirsts,
    int32_t sortColCount)
{
    SortOperatorFactory *operatorFactory = new SortOperatorFactory(
        sourceTypes,
        sourceTypeCount,
        outputCols,
        outputColCount,
        sortCols,
        sortAscendings,
        sortNullFirsts,
        sortColCount);
    return operatorFactory;
}

omni::Operator * SortOperatorFactory::createOperator()
{
    SortOperator *sortOperator = new SortOperator(
        sourceTypes,
        sourceTypeCount,
        outputCols,
        outputColCount,
        sortCols,
        sortAscendings,
        sortNullFirsts,
        sortColCount);
    return sortOperator;
}

// function implements for class Sort
SortOperator::SortOperator(
    int32_t *sourceTypes,
    int32_t typesCount,
    int32_t *outputCols,
    int32_t outputColsCount,
    int32_t *sortCols,
    int32_t *sortAscendings,
    int32_t *sortNullFirsts,
    int32_t sortColCount)
{
    this->sourceTypes = sourceTypes;
    this->typesCount = typesCount;
    this->outputCols = outputCols;
    this->outputColsCount = outputColsCount;
    this->sortCols = sortCols;
    this->sortAscendings = sortAscendings;
    this->sortNullFirsts = sortNullFirsts;
    this->sortColCount = sortColCount;
    this->pagesIndex = new PagesIndex(sourceTypes, typesCount);
}

SortOperator::~SortOperator()
{
    delete pagesIndex;
}

int32_t SortOperator::addInput(Table **datas, int32_t *rowCounts, int32_t pageCount)
{
    if (pageCount <= 0) {
        return 0;
    }

    pagesIndex->addTables(datas, rowCounts, pageCount);
    return 0;
}

// return error code
int32_t SortOperator::getOutput(vector<Table *>& outputTables)
{
    int32_t positionCount = pagesIndex->getPositionCount();
    if (positionCount <= 0) {
        return 0;
    }

    // first, sort
    int32_t to = positionCount;
    int32_t from = 0;
    int32_t sortColTypes[sortColCount];
    for (int32_t i = 0; i < sortColCount; i++) {
        sortColTypes[i] = sourceTypes[sortCols[i]];
    }

    auto quickSortStart = START();
    pagesIndex->sort(
        sortCols,
        sortColTypes,
        sortAscendings,
        sortNullFirsts,
        sortColCount,
        from,
        to);   
    OP_DEBUG_LOG("quick sort elapsed time : %ld ms.", END(quickSortStart));

    // next, get output
    int32_t maxRowCount = getMaxRowCount(sourceTypes, outputCols, outputColsCount);
    int32_t tableCount = getTableCount(positionCount, maxRowCount);
    outputTables.reserve(tableCount);

    Table *table = NULL;
    int32_t position = 0;
    int32_t rowCount = 0;
    for (int32_t i = 0; i < tableCount; i++) {
        rowCount = min(maxRowCount, positionCount - position);
        table = new Table(rowCount, outputColsCount);

        auto start = START();
        allocColumns((int64_t)table, sourceTypes, outputCols, outputColsCount, rowCount);
        OP_DEBUG_LOG("alloc columns elapsed time: %ld ms.", END(start));
        pagesIndex->getOutput(outputCols, outputColsCount, (int64_t)table, sourceTypes, position, rowCount);
        OP_DEBUG_LOG("get result elapsed time: %ld ms.", END(start));

        position += rowCount;
        outputTables.push_back(table);
    }
    status = 2;
    return 0;
}

void freeInputTable(Table **inputTables, int32_t inputTableCount)
{
    for (int32_t tableIdx = 0; tableIdx < inputTableCount; tableIdx++) {
        delete inputTables[tableIdx];
    }
    delete inputTables;
}

void freeOutputTable(vector<Table *>& outputTables)
{
    int32_t tablesCount = outputTables.size();
    for (int32_t tableIdx = 0; tableIdx < tablesCount; tableIdx++) {
        delete outputTables[tableIdx];
    }
}

void freeDataInColumn(Table **tables, int32_t tableCount)
{
    Table *table;
    int32_t columnCount = 0;

    for (int32_t tableIdx = 0; tableIdx < tableCount; tableIdx++) {
        table = tables[tableIdx];
        columnCount = table->getColumnNumber();
        for (int32_t colIdx = 0; colIdx < columnCount; colIdx++) {
            free(table->getColumn(colIdx)->getData());
        }
    }
}
