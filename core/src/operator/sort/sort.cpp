#include "sort.h"
#include "../../util/type_infer.h"
#include "../../util/debug.h"
#include "../../memory/memory_pool.h"
#include <iostream>
#include <algorithm>
#include <cstring>

using namespace std;

int32_t DEFAULT_MAX_PAGE_SIZE_IN_BYTES = 1 * 1024 * 1024;

void quickSort(int64_t operatorAddress, int32_t *sortCols, int32_t *sortAscendings, int32_t *sortNullFirsts, int32_t sortColCount, int32_t from, int32_t to);
void setInt32ColumnValues(int64_t *valueAddresses, int32_t offset, int32_t length, Column **inputTable, int32_t *outputData);
void setInt64ColumnValues(int64_t *valueAddresses, int32_t offset, int32_t length, Column **inputTable, int64_t *outputData);
void setDoubleColumnValues(int64_t *valueAddresses, int32_t offset, int32_t length, Column **inputTable, double *outputData);

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
            printf("unsupported type\n");
            break;
        }
    }
}

NativeOmniSortOperatorFactory::NativeOmniSortOperatorFactory(
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

NativeOmniSortOperatorFactory::~NativeOmniSortOperatorFactory()
{
    delete[] sourceTypes;
    delete[] outputCols;
    delete[] sortCols;
    delete[] sortAscendings;
    delete[] sortNullFirsts;
}

NativeOmniSortOperatorFactory * NativeOmniSortOperatorFactory::createNativeOmniSortOperatorFactory(
    int32_t *sourceTypes,
    int32_t sourceTypeCount,
    int32_t *outputCols,
    int32_t outputColCount,
    int32_t *sortCols,
    int32_t *sortAscendings,
    int32_t *sortNullFirsts,
    int32_t sortColCount)
{
    NativeOmniSortOperatorFactory *operatorFactory = new NativeOmniSortOperatorFactory(
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

NativeOmniOperator * NativeOmniSortOperatorFactory::createOmniOperator()
{
    NativeOmniSortOperator *sortOperator = new NativeOmniSortOperator(
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
NativeOmniSortOperator::NativeOmniSortOperator(
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

NativeOmniSortOperator::~NativeOmniSortOperator()
{
    delete pagesIndex;
}

int32_t NativeOmniSortOperator::addInput(Table **datas, int32_t *rowCounts, int32_t pageCount)
{
    if (pageCount <= 0) {
        return 0;
    }

    pagesIndex->addTables(datas, rowCounts, pageCount);
    return 0;
}

// return error code
int32_t NativeOmniSortOperator::getOutput(vector<Table *>& outputTables)
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
    quickSort(
        (int64_t)pagesIndex,
        sortCols,
        sortColTypes,
        sortAscendings,
        sortNullFirsts,
        sortColCount,
        from,
        to);   
    PRINT_IMPL("quick sort elapsed time : %ld ms\n", END(quickSortStart));

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
        PRINT_IMPL("alloc columns elapsed time: %ld ms\n", END(start));
        pagesIndex->getOutput(outputCols, outputColsCount, (int64_t)table, sourceTypes, position, rowCount);
        PRINT_IMPL("get result elapsed time: %ld ms\n", END(start));

        position += rowCount;
        outputTables.push_back(table);
    }
    return 0;
}

// function implements for class PagesIndex
PagesIndex::PagesIndex(int32_t *types, int32_t typesCount)
{
    this->types = types;
    this->typesCount = typesCount;
    this->tablesCount = 0;
    this->positionCount = 0;
}

// return error number
int32_t PagesIndex::addTables(Table **datas, int32_t *rowCounts, int32_t tableCount)
{
    int32_t rowCount = 0;
    Table *data = NULL;
    int64_t valueAddress = 0;
    int32_t valueAddrIdx = 0;

    for (int32_t tableIdx = 0; tableIdx < tableCount; tableIdx++) {
        this->positionCount += rowCounts[tableIdx];
    }
    this->valueAddresses = (int64_t *)malloc(this->positionCount * sizeof(int64_t));
    this->columns = (Column ***)malloc(this->typesCount * sizeof(Column **));
    for (int32_t i = 0; i < this->typesCount; i++) {
        this->columns[i] = (Column **)malloc(tableCount * sizeof(Column *));
    }

    for (int32_t tableIdx = 0; tableIdx < tableCount; tableIdx++) {
        data = datas[tableIdx];
        for (int32_t colIdx = 0; colIdx < this->typesCount; colIdx++) {
            this->columns[colIdx][tableIdx] = data->getColumn(colIdx);
        }

        rowCount = rowCounts[tableIdx];
        for (int32_t rowIdx = 0; rowIdx < rowCount; rowIdx++) {
            valueAddress = encodeSyntheticAddress(tableIdx, rowIdx);
            this->valueAddresses[valueAddrIdx] = valueAddress;
            valueAddrIdx++;
        }

    }

    return 0;
}

void PagesIndex::getOutput(int32_t *outputCols, int32_t outputColsCount, int64_t outputTableAddr, int32_t *sourceTypes, int32_t offset, int32_t length)
{
    Column ***inputTables = this->columns;
    Table *outputTable = (Table *)outputTableAddr;
    int64_t *valueAddresses = this->valueAddresses;

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

PagesIndex::~PagesIndex()
{
    for (int32_t colIdx = 0; colIdx < typesCount; colIdx++) {
        free(columns[colIdx]);
    }
    free(columns);
    free(valueAddresses);
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
    Column ***columns = pagesIndex->getColumns();

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

void setInt32ColumnValues(int64_t *valueAddresses, int32_t offset, int32_t length, Column **inputTable, int32_t *outputData)
{
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

void setInt64ColumnValues(int64_t *valueAddresses, int32_t offset, int32_t length, Column **inputTable, int64_t *outputData)
{
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

void setDoubleColumnValues(int64_t *valueAddresses, int32_t offset, int32_t length, Column **inputTable, double *outputData)
{
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
