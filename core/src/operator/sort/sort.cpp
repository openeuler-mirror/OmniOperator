#include "sort.h"
#include "../../util/type_infer.h"
#include "../../util/debug.h"
#include "../../memory/memory_pool.h"
#include <iostream>
#include <algorithm>

void quickSort(int64_t sortAddress, int32_t *sortCols, int32_t *sortAscendings, int32_t *sortNullFirsts, int32_t sortColCount, int32_t from, int32_t to);

int64_t encodeSyntheticAddress(int32_t sliceIndex, int32_t sliceOffset)
{
    return (((int64_t)sliceIndex) << 32) | sliceOffset;
}

int32_t decodeSliceIndex(int64_t sliceAddress)
{
    return ((int32_t)(sliceAddress >> 32));
}

int32_t decodePosition(int64_t sliceAddress)
{
    return ((int32_t)(sliceAddress));
}

ColumnType getColumnType(int32_t colTypeIdx)
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
            printf("unsupported type\n");
            break;
        }
    }
}

// function implements for class Sort
Sort::Sort(int32_t *sourceTypes,
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

void swap(int64_t *valueAddresses, int32_t a, int32_t b)
{
    int64_t temp = valueAddresses[a];
    valueAddresses[a] = valueAddresses[b];
    valueAddresses[b] = temp;
}

void vectorSwap(int64_t *valueAddresses, int32_t from, int32_t l, int32_t s)
{
    for (int32_t i = 0; i < s; i++, from++, l++)
    {
        swap(valueAddresses, from, l);
    }
}

// function implements for class PagesIndex
PagesIndex::PagesIndex(int32_t *sourceTypes, int32_t typesCount)
{
    this->types = sourceTypes;
    this->typesCount = typesCount;
}

void PagesIndex::addTables(int64_t *datas, int64_t *nulls, int32_t pageCount, int32_t *rowCounts, int32_t totalRowCount)
{
    Column *column;
    ColumnType type;
    void *data;
    int32_t *null;
    int32_t rowNum;
    int32_t start;
    int32_t columnIdx;
    int32_t valueAddrIdx = 0;
    int64_t valueAddress = 0;

    this->tableCount = pageCount;
    this->positionCount = totalRowCount;
    
    if (pageCount == 0) {
        return;
    }
    
    valueAddresses = (int64_t *)malloc(totalRowCount * sizeof(int64_t));
    columns = (Column ***)malloc(sizeof(Column **) * typesCount);
    for (int32_t i = 0; i < typesCount; i++) {
        columns[i] = (Column **)malloc(sizeof(Column *) * tableCount);
    }

    for (int32_t tableIdx = 0; tableIdx < pageCount; tableIdx++) {
        rowNum = (int32_t)(rowCounts[tableIdx]);
        start = tableIdx * typesCount;

        for (int32_t colIdx = 0; colIdx < typesCount; colIdx++) {
            columnIdx = start + colIdx;
            type = getColumnType(types[colIdx]);
            data = (void *)(datas[columnIdx]);
            null = (int32_t *)(nulls[columnIdx]);
            column = new Column(data, type, rowNum, null);
            columns[colIdx][tableIdx] = column;
        }

        for (int32_t rowIdx = 0; rowIdx < rowNum; rowIdx++) {
            valueAddress = encodeSyntheticAddress(tableIdx, rowIdx);
            valueAddresses[valueAddrIdx] = valueAddress;
            valueAddrIdx++;
        }
    }
}

PagesIndex::~PagesIndex()
{
    for (int32_t colIdx = 0; colIdx < typesCount; colIdx++) {
        for (int32_t tableIdx = 0; tableIdx < tableCount; tableIdx++) {
            delete columns[colIdx][tableIdx];
        }
        free(columns[colIdx]);
    }
    free(columns);
    free(valueAddresses);
}

int64_t createSort(
    int32_t *sourceTypes,
    int32_t typeCount,
    int32_t *outputCols,
    int32_t outputColCount,
    int32_t *sortCols,
    int32_t *sortAscendings,
    int32_t *sortNullFirsts,
    int32_t sortColCount)
{
    Sort *sort = new Sort(sourceTypes, typeCount, outputCols, outputColCount, sortCols, sortAscendings, sortNullFirsts, sortColCount);
    return (int64_t)sort;
}

// return 0 when left and right both are nulls
// return 2 when left and right both are not nulls
int32_t compareNull(int32_t *leftNulls, int32_t leftPosition, int32_t *rightNulls, int32_t rightPosition, int32_t nullsFirst)
{
    int32_t leftIsNull = leftNulls[leftPosition];
    int32_t rightIsNull = rightNulls[rightPosition];

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

int32_t compareIntValue(int32_t *leftData, int32_t leftPosition, int32_t *rightData, int32_t rightPosition)
{
    return leftData[leftPosition] - rightData[rightPosition];
}

int32_t compareInt64Value(int64_t *leftData, int32_t leftPosition, int64_t *rightData, int32_t rightPosition)
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

int32_t compareDoubleValue(double *leftData, int32_t leftPosition, double *rightData, int32_t rightPosition)
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

int32_t compareTo(
    int64_t pagesIndexAddr,
    int32_t *sortCols,
    int32_t *sortColTypes,
    int32_t *sortAscendings,
    int32_t *sortNullFirsts,
    int32_t sortColCount,
    int32_t leftPosition,
    int32_t rightPosition)
{
    PagesIndex *pagesIndex = (PagesIndex *)pagesIndexAddr;
    int64_t *valueAddresses = pagesIndex->getValueAddresses();
    Column *** columns = pagesIndex->getColumns();

    int64_t leftValueAddress = valueAddresses[leftPosition];
    int32_t leftColumnIndex = decodeSliceIndex(leftValueAddress);
    int32_t leftColumnPosition = decodePosition(leftValueAddress);

    int64_t rightValueAddress = valueAddresses[rightPosition];
    int32_t rightColumnIndex = decodeSliceIndex(rightValueAddress);
    int32_t rightColumnPosition = decodePosition(rightValueAddress);

    bool isSameColumn = false;
    if (leftColumnIndex == rightColumnIndex) {
        isSameColumn = true;
    }

    int compare = 0;
    for (int32_t i = 0; i < sortColCount; i++) {
        int32_t sortCol = sortCols[i];
        Column *leftColumn = columns[sortCol][leftColumnIndex];
        void *leftData = leftColumn->getData();
        int32_t *leftNulls = leftColumn->getNulls();
        int32_t colType = sortColTypes[i];
        Column *rightColumn;
        void *rightData;
        int32_t *rightNulls;

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

int32_t median3(
    int64_t pagesIndexAddr,
    int32_t *sortCols,
    int32_t *sortColTypes,
    int32_t *sortAscendings,
    int32_t *sortNullFirsts,
    int32_t sortColCount,
    int32_t a,
    int32_t b,
    int32_t c)
{
    int32_t ab = compareTo(pagesIndexAddr, sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, a, b);
    int32_t ac = compareTo(pagesIndexAddr, sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, a, c);
    int32_t bc = compareTo(pagesIndexAddr, sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, b, c);
    return (ab < 0 ? (bc < 0 ? b : ac < 0 ? c
                                          : a)
                   : (bc > 0 ? b : ac > 0 ? c
                                          : a));
}

void quickSort(int64_t pagesIndexAddr,
               int32_t *sortCols,
               int32_t *sortColTypes,
               int32_t *sortAscendings,
               int32_t *sortNullFirsts,
               int32_t sortColCount,
               int32_t from,
               int32_t to)
{
    PagesIndex *pagesIndex = (PagesIndex *)pagesIndexAddr;
    int64_t *valueAddresses = pagesIndex->getValueAddresses();
    int32_t positionCount = pagesIndex->getPositionCount();
    int32_t len = to - from;
    if (len < 7)
    {
        for (int32_t i = from; i < to; i++)
        {
            for (int32_t j = i; j > from && (compareTo(pagesIndexAddr, sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, j - 1, j)) > 0; j--)
            {
                swap(valueAddresses, j, j - 1);
            }
        }
        return;
    }
    int32_t m = from + len / 2;
    if (len > 7)
    {
        int32_t l = from;
        int32_t n = to - 1;
        if (len > 40)
        {
            int32_t s = len / 8;
            l = median3(pagesIndexAddr, sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, l, l + s, l + 2 * s);
            m = median3(pagesIndexAddr, sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, m - s, m, m + s);
            n = median3(pagesIndexAddr, sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, n - 2 * s, n - s, n);
        }
        m = median3(pagesIndexAddr, sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, l, m, n);
    }

    int32_t a = from;
    int32_t b = a;
    int32_t c = to - 1;
    int32_t d = c;
    int32_t comparison = 0;
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
                swap(valueAddresses, a++, b);
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
                swap(valueAddresses, c, d--);
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
        swap(valueAddresses, b++, c--);
    }

    // Swap partition elements back to middle
    int32_t s;
    int32_t n = to;
    s = min(a - from, b - a);
    vectorSwap(valueAddresses, from, b - s, s);
    s = min(d - c, n - d - 1);
    vectorSwap(valueAddresses, b, n - s, s);

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

void setInt32ColumnValues(int64_t *valueAddresses, int32_t offset, int32_t length, Column **inputTable, int32_t *outputData) {
    int32_t preTableIndex = -1;
    int64_t valueAddress = 0;
    Column *inputColumn = NULL;
    int32_t *inputData = NULL;
    int32_t tableIndex = 0;
    int32_t position = 0;

    int32_t start = offset;
    int32_t end = offset + length;
    int32_t outputIndex = 0;
    for (int32_t i = start; i < end; i++) {
        valueAddress = valueAddresses[i];
        tableIndex = decodeSliceIndex(valueAddress);
        position = decodePosition(valueAddress);
        if (preTableIndex != tableIndex) {
            inputColumn = inputTable[tableIndex];
            inputData = (int32_t *)(inputColumn->getData());
            preTableIndex = tableIndex;
        }

        outputData[outputIndex] = inputData[position];
        outputIndex++;
    }
}

void setInt64ColumnValues(int64_t *valueAddresses, int32_t offset, int32_t length, Column **inputTable, int64_t *outputData) {
    int32_t preTableIndex = -1;
    int64_t valueAddress = 0;
    Column *inputColumn = NULL;
    int64_t *inputData = NULL;
    int32_t tableIndex = 0;
    int32_t position = 0;

    int32_t start = offset;
    int32_t end = offset + length;
    int32_t outputIndex = 0;
    for (int32_t i = start; i < end; i++) {
        valueAddress = valueAddresses[i];
        tableIndex = decodeSliceIndex(valueAddress);
        position = decodePosition(valueAddress);

        if (preTableIndex != tableIndex) {
            inputColumn = inputTable[tableIndex];
            inputData = (int64_t *)(inputColumn->getData());
            preTableIndex = tableIndex;
        }

        outputData[outputIndex] = inputData[position];
        outputIndex++;
    }
}

void setDoubleColumnValues(int64_t *valueAddresses, int32_t offset, int32_t length, Column **inputTable, double *outputData) {
    int32_t preTableIndex = -1;
    int64_t valueAddress = 0;
    Column *inputColumn = NULL;
    double *inputData = NULL;
    int32_t tableIndex = 0;
    int32_t position = 0;

    int32_t start = offset;
    int32_t end = offset + length;
    int32_t outputIndex = 0;
    for (int32_t i = start; i < end; i++) {
        valueAddress = valueAddresses[i];
        tableIndex = decodeSliceIndex(valueAddress);
        position = decodePosition(valueAddress);
        if (preTableIndex != tableIndex) {
            inputColumn = inputTable[tableIndex];
            inputData = (double *)(inputColumn->getData());
            preTableIndex = tableIndex;
        }

        outputData[outputIndex] = inputData[position];
        outputIndex++;
    }
}

void getResult(int64_t pagesIndexAddr, int32_t *outputCols, int32_t outputColsCount, int64_t outputTableAddr, int32_t *sourceTypes, int32_t offset, int32_t length)
{
    PagesIndex *pagesIndex = (PagesIndex *)pagesIndexAddr;
    Column ***inputTables = pagesIndex->getColumns();
    Table *outputTable = (Table *)outputTableAddr;
    int64_t *valueAddresses = pagesIndex->getValueAddresses();

    Column *outputColumn = NULL;
    int32_t outputCol = 0;
    void *outputData = NULL;
    int colType = 0;

    for (int32_t j = 0; j < outputColsCount; j++) {
        outputColumn = outputTable->getColumn(j);
        outputCol = outputCols[j];
        outputData = outputColumn->getData();
        colType = sourceTypes[outputCol];
        Column **inputTable = inputTables[outputCol];

        switch (colType)
        {
        case 1:
            setInt32ColumnValues(valueAddresses, offset, length, inputTable, (int32_t *)outputData);
            break;
        case 2:
            setInt64ColumnValues(valueAddresses, offset, length, inputTable, (int64_t *)outputData);
            break;
        case 3:
            setDoubleColumnValues(valueAddresses, offset, length, inputTable, (double *)outputData);
            break;    
        default:
            break;
        }
    }
}
