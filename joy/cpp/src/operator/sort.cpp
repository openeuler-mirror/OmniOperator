#include "sort.h"
#include "../util/type_infer.h"
#include "../util/debug.h"
#include "../memory_pool/memory_pool.h"
#include <iostream>
#include <algorithm>

void quickSort(long sortAddress, int *sortCols, int *sortAscendings, int *sortNullFirsts, int sortColCount, uint32_t from, uint32_t to);

uint64_t encodeSyntheticAddress(uint32_t sliceIndex, uint32_t sliceOffset)
{
    return (((uint64_t)sliceIndex) << 32) | sliceOffset;
}

uint32_t decodeSliceIndex(uint64_t sliceAddress)
{
    return ((uint32_t)(sliceAddress >> 32));
}

uint32_t decodePosition(uint64_t sliceAddress)
{
    return ((uint32_t)(sliceAddress));
}

ColumnType getColumnType(int colTypeIdx)
{
    if (colTypeIdx == 1) {
        return INT32;
    }
    else if (colTypeIdx == 2) {
        return INT64;
    }
    else if (colTypeIdx == 3) {
        return DOUBLE;
    }
    else {
        return INT32;
    }
}

int getColTypeIdx(ColumnType type)
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

void allocColumns(long outputTableAddr, int *sourceTypes, int *outputCols, int outputColCount, uint32_t positionCount)
{
    Table *outputTable = (Table *)outputTableAddr;
    MemoryPool *pool = getMemoryPool();
    int outputCol;
    int columnTypeIdx;
    uint8_t *data = NULL;
    
    for (int i = 0; i < outputColCount; i++) {
        outputCol = outputCols[i];
        columnTypeIdx = sourceTypes[outputCol];
        switch (columnTypeIdx)
        {
        case 1:
            pool->allocate(positionCount * sizeof(int32_t), &data);
            break;
        case 2:
            pool->allocate(positionCount * sizeof(int64_t), &data);
            break;
        case 3:
            pool->allocate(positionCount * sizeof(double), &data);
            break;    
        default:
            break;
        }
        ColumnType columnType = getColumnType(columnTypeIdx);
        Column *column = new Column(data, columnType, positionCount);
        outputTable->setColumn(column, columnType);
    }
}

// function implements for class Sort
Sort::Sort(int *sourceTypes, 
    int typesCount, 
    int *outputCols, 
    int outputColsCount, 
    int *sortCols, 
    int *sortAscendings, 
    int *sortNullFirsts, 
    int sortColCount)
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

Sort::~Sort()
{
    delete pagesIndex;
}

void Sort::preloop(Table *table)
{
}

void Sort::inloop(Table *table, uint32_t rowIdx)
{
}

void Sort::postloop(Table *table)
{
}

void Sort::process(Table *table, uint32_t rowIdx)
{
}

void swap(long *valueAddresses, uint32_t a, uint32_t b)
{
    long temp = valueAddresses[a];
    valueAddresses[a] = valueAddresses[b];
    valueAddresses[b] = temp;
}

void vectorSwap(long *valueAddresses, uint32_t from, uint32_t l, uint32_t s)
{
    for (uint32_t i = 0; i < s; i++, from++, l++)
    {
        swap(valueAddresses, from, l);
    }
}

// function implements for class PagesIndex
PagesIndex::PagesIndex(int *types, int typeCount)
{
    this->types = types;
    this->typeCount = typeCount;
    for (uint32_t i = 0; i < typeCount; i++) {
        vector<Column *> column;
        this->columns.push_back(column);
    }
    this->positionCount = 0;
    this->valueAddresses.reserve(100000000);
}

void PagesIndex::addTable(Table *table, int32_t colCount, uint32_t positionCount)
{
    if (positionCount == 0)
    {
        return;
    }

    this->positionCount += positionCount;
    int tableIndex = (colCount > 0) ? this->columns[0].size() : 0;
    for (uint32_t i = 0; i < colCount; i++)
    {
        Column *column = table->getColumn(i);
        columns[i].push_back(column);
    }

    long sliceAddress = 0;
    for (uint32_t position = 0; position < positionCount; position++)
    {
        sliceAddress = encodeSyntheticAddress(tableIndex, position);
        valueAddresses.push_back(sliceAddress);
    }
}

// return 0 when left and right both are nulls
// return 2 when left and right both are not nulls
int compareNull(int *leftNulls, uint32_t leftPosition, int *rightNulls, uint32_t rightPosition, int nullsFirst)
{
    int leftIsNull = leftNulls[leftPosition];
    int rightIsNull = rightNulls[rightPosition];

    if (leftIsNull && rightIsNull)
    {
        return 0;
    }
    if (leftIsNull) {
        return nullsFirst ? -1 : 1;
    }
    if (rightIsNull) {
        return nullsFirst ? -1 : 1;
    }
    return 2;
}

int compareIntValue(int32_t *leftData, uint32_t leftPosition, int32_t *rightData, uint32_t rightPosition) 
{
    return leftData[leftPosition] - rightData[rightPosition];        
}

int compareInt64Value(int64_t *leftData, uint32_t leftPosition, int64_t *rightData, uint32_t rightPosition)
{
    int64_t leftValue = leftData[leftPosition];
    int64_t rightValue = rightData[rightPosition];
    if (leftValue > rightValue) {
        return 1;
    }
    else if (leftValue < rightValue) {
        return -1;
    }
    else {
        return 0;
    }
}

int compareDoubleValue(double *leftData, uint32_t leftPosition, double *rightData, uint32_t rightPosition)
{
    double leftValue = leftData[leftPosition];
    double rightValue = rightData[rightPosition];

    if (leftValue > rightValue) {
        return 1;
    }
    else if (leftValue < rightValue) {
        return -1;
    }
    else {
        return 0;
    }
}

int compareTo(
    long pagesIndexAddr,
    int *sortCols,
    int *sortColTypes, 
    int *sortAscendings, 
    int *sortNullFirsts, 
    int sortColCount, 
    uint32_t leftPosition, 
    uint32_t rightPosition)
{
    PagesIndex *pagesIndex = (PagesIndex *)pagesIndexAddr;
    vector<long>& valueAddresses = pagesIndex->getValueAddresses();
    vector<vector<Column *>>& columns = pagesIndex->getColumns();

    uint64_t leftValueAddress = valueAddresses[leftPosition];
    uint32_t leftColumnIndex = decodeSliceIndex(leftValueAddress);
    uint32_t leftColumnPosition = decodePosition(leftValueAddress);

    uint64_t rightValueAddress = valueAddresses[rightPosition];
    uint32_t rightColumnIndex = decodeSliceIndex(rightValueAddress);
    uint32_t rightColumnPosition = decodePosition(rightValueAddress);

    bool isSameColumn = false;
    if (leftColumnIndex == rightColumnIndex) {
        isSameColumn = true;
    }

    int compare = 0;
    for (int i = 0; i < sortColCount; i++) {
        int sortCol = sortCols[i];
        Column *leftColumn = columns[sortCol][leftColumnIndex];
        void *leftData = leftColumn->getData();
        int *leftNulls = leftColumn->getNulls();
        int colType = sortColTypes[i];
        Column *rightColumn;
        void *rightData;
        int *rightNulls;

        if (isSameColumn) {
            rightColumn = leftColumn;
            rightData = leftData;
            rightNulls = leftNulls;
        }
        else {
            rightColumn = columns[sortCol][rightColumnIndex];
            rightData = rightColumn->getData();
            rightNulls = rightColumn->getNulls();
        }

        // compare = compareNull(leftNulls, leftColumnPosition, rightNulls, rightColumnPosition, sortNullFirsts[i]);
        // if (compare != 2) {
        //     break;
        // }

        switch (colType)
        {
        case 1:
            compare = compareIntValue((int32_t *)leftData, leftColumnPosition, (int32_t *)rightData, rightColumnPosition);
            break;
        case 2:
            compare = compareInt64Value((int64_t *)leftData, leftColumnPosition, (int64_t *)rightData, rightColumnPosition);
            break;
        case 3:
            compare = compareDoubleValue((double *)leftData, leftColumnPosition, (double *)rightData, rightColumnPosition);
            break;
        default:
            break;
        }

        if (sortAscendings[i] == 0) {
            compare = -compare;
        }

        if (compare != 0) {
            break;
        }
    }

    return compare;
}

uint32_t median3( 
    long pagesIndexAddr,
    int *sortCols,
    int *sortColTypes, 
    int *sortAscendings, 
    int *sortNullFirsts, 
    int sortColCount, 
    uint32_t a, 
    uint32_t b, 
    uint32_t c)
{
    int ab = compareTo(pagesIndexAddr, sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, a, b);
    int ac = compareTo(pagesIndexAddr, sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, a, c);
    int bc = compareTo(pagesIndexAddr, sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, b, c);
    return (ab < 0 ? (bc < 0 ? b : ac < 0 ? c
                                          : a)
                   : (bc > 0 ? b : ac > 0 ? c
                                          : a));
}

void quickSort(long pagesIndexAddr,
               int *sortCols,
               int *sortColTypes, 
               int *sortAscendings, 
               int *sortNullFirsts, 
               int sortColCount, 
               uint32_t from, 
               uint32_t to)
{
    PagesIndex *pagesIndex = (PagesIndex *)pagesIndexAddr;
    vector<long>& valueAddresses = pagesIndex->getValueAddresses();
    uint32_t len = to - from;
    if (len < 7)
    {
        for (uint32_t i = from; i < to; i++)
        {
            for (uint32_t j = i; j > from && (compareTo(pagesIndexAddr, sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, j - 1, j)) > 0; j--)
            {
                swap(&valueAddresses[0], j, j - 1);
            }
        }
        return;
    }
    uint32_t m = from + len / 2;
    if (len > 7)
    {
        uint32_t l = from;
        uint32_t n = to - 1;
        if (len > 40)
        {
            uint32_t s = len / 8;
            l = median3(pagesIndexAddr, sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, l, l + s, l + 2 * s);
            m = median3(pagesIndexAddr, sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, m - s, m, m + s);
            n = median3(pagesIndexAddr, sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, n - 2 * s, n - s, n);
        }
        m = median3(pagesIndexAddr, sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, l, m, n);
    }

    uint32_t a = from;
    uint32_t b = a;
    uint32_t c = to - 1;
    uint32_t d = c;
    int comparison = 0;
    while (true)
    {
        while (b <= c && ((comparison = compareTo(pagesIndexAddr, sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, b, m)) <= 0))
        {
            if (comparison == 0)
            {
                if (a == m)
                {
                    m = b;
                }
                else if (b == m)
                {
                    m = a;
                }
                swap(&valueAddresses[0], a++, b);
            }
            b++;
        }
        while (c >= b && ((comparison = compareTo(pagesIndexAddr, sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, c, m)) >= 0))
        {
            if (comparison == 0)
            {
                if (c == m)
                {
                    m = d;
                }
                else if (d == m)
                {
                    m = c;
                }
                swap(&valueAddresses[0], c, d--);
            }
            c--;
        }
        if (b > c)
        {
            break;
        }
        if (b == m)
        {
            m = d;
        }
        else if (c == m)
        {
            m = c;
        }
        swap(&valueAddresses[0], b++, c--);
    }

    // Swap partition elements back to middle
    uint32_t s;
    uint32_t n = to;
    s = min(a - from, b - a);
    vectorSwap(&valueAddresses[0], from, b - s, s);
    s = min(d - c, n - d - 1);
    vectorSwap(&valueAddresses[0], b, n - s, s);

    // Recursively sort non-partition-elements
    if ((s = b - a) > 1)
    {
        quickSort(pagesIndexAddr, sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, from, from + s);
    }
    if ((s = d - c) > 1)
    {
        quickSort(pagesIndexAddr, sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, n - s, n);
    }
}

void setIntColumnValues(long *valueAddresses, uint32_t positionCount, vector<Column*>& inputTable, int *outputData) {
    int preTableIndex = -1;
    long valueAddress = 0;
    Column *inputColumn = NULL;
    int *inputData = NULL;
    int tableIndex = 0;
    int position = 0;

    for (uint32_t i = 0; i < positionCount; i++) {
        valueAddress = valueAddresses[i];
        tableIndex = decodeSliceIndex(valueAddress);
        position = decodePosition(valueAddress);
        if (preTableIndex != tableIndex) {
            inputColumn = inputTable[tableIndex];
            inputData = (int *)(inputColumn->getData());
            preTableIndex = tableIndex;
        }

        outputData[i] = inputData[position];
    }
}

void setInt64ColumnValues(long *valueAddresses, uint32_t positionCount, vector<Column*>& inputTable, int64_t *outputData) {
    int preTableIndex = -1;
    long valueAddress = 0;
    Column *inputColumn = NULL;
    int64_t *inputData = NULL;
    int tableIndex = 0;
    int position = 0;

    for (uint32_t i = 0; i < positionCount; i++) {
        valueAddress = valueAddresses[i];
        tableIndex = decodeSliceIndex(valueAddress);
        position = decodePosition(valueAddress);
        if (preTableIndex != tableIndex) {
            inputColumn = inputTable[tableIndex];
            inputData = (int64_t *)(inputColumn->getData());
            preTableIndex = tableIndex;
        }

        outputData[i] = inputData[position];
    }
}

void setDoubleColumnValues(long *valueAddresses, uint32_t positionCount, vector<Column*>& inputTable, double *outputData) {
    int preTableIndex = -1;
    long valueAddress = 0;
    Column *inputColumn = NULL;
    double *inputData = NULL;
    int tableIndex = 0;
    int position = 0;

    for (uint32_t i = 0; i < positionCount; i++) {
        valueAddress = valueAddresses[i];
        tableIndex = decodeSliceIndex(valueAddress);
        position = decodePosition(valueAddress);
        if (preTableIndex != tableIndex) {
            inputColumn = inputTable[tableIndex];
            inputData = (double *)(inputColumn->getData());
            preTableIndex = tableIndex;
        }

        outputData[i] = inputData[position];
    }
}

void getResult(long pagesIndexAddr, int *outputCols, int outputColsCount, long outputTableAddr, int *sourceTypes, uint32_t positionCount)
{
    PagesIndex *pagesIndex = (PagesIndex *)pagesIndexAddr;
    vector<vector<Column *>>& inputTables = pagesIndex->getColumns();
    Table *outputTable = (Table *)outputTableAddr;
    vector<long>& valueAddresses = pagesIndex->getValueAddresses();

    Column *outputColumn = NULL;
    int outputCol = 0;
    void *outputData = NULL;
    int colType = 0;

    for (uint32_t j = 0; j < outputColsCount; j++) {
        outputColumn = outputTable->getColumn(j);
        outputCol = outputCols[j];
        outputData = outputColumn->getData();
        colType = sourceTypes[outputCol];
        vector<Column *>& inputTable = inputTables[outputCol];

        switch (colType)
        {
        case 1:
            setIntColumnValues(&valueAddresses[0], positionCount, inputTable, (int *)outputData);
            break;
        case 2:
            setInt64ColumnValues(&valueAddresses[0], positionCount, inputTable, (int64_t *)outputData);
            break;
        case 3:
            setDoubleColumnValues(&valueAddresses[0], positionCount, inputTable, (double *)outputData);
            break;    
        default:
            break;
        }
    }
}
