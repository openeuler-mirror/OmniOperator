#include "pages_index.h"
#include "optimization.h"
#include "../jit/annotation.h"
#include "../vector/int_vector.h"
#include "../vector/long_vector.h"
#include "../vector/double_vector.h"
#include "../vector/vector_helper.h"
#include "util/operator_util.h"

#include <algorithm>

void quickSort(int64_t pagesIndexAddr,
               int32_t *sortCols,
               int32_t *sortColTypes,
               int32_t *sortAscendings,
               int32_t *sortNullFirsts,
               int32_t sortColCount,
               int32_t from,
               int32_t to);
void setInt32ColumnValues(int64_t *valueAddresses, int32_t offset, int32_t length, Vector ** inputVecBatch, int32_t *outputData);
void setInt64ColumnValues(int64_t *valueAddresses, int32_t offset, int32_t length, Vector ** inputVecBatch, int64_t *outputData);
void setDoubleColumnValues(int64_t *valueAddresses, int32_t offset, int32_t length, Vector ** inputVecBatch, double *outputData);

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

// function implements for class PagesIndex
PagesIndex::PagesIndex(int32_t *types, int32_t typesCount)
{
    this->types = types;
    this->typesCount = typesCount;
    this->columns = nullptr;
    this->valueAddresses = nullptr;
    this->positionCount = 0;
}

// return error number
int32_t PagesIndex::addVecBatches(std::vector<VectorBatch *> &vecBatches)
{
    int32_t vecBatchCount = vecBatches.size();
    int32_t columnCount = this->typesCount;

    for (int vecBatchIdx = 0; vecBatchIdx < vecBatchCount; ++vecBatchIdx) {
        this->positionCount += vecBatches[vecBatchIdx]->GetRowCount();
    }
    this->valueAddresses = new int64_t[this->positionCount];

    this->columns = new Vector **[columnCount];
    for (int colIdx = 0; colIdx < columnCount; ++colIdx) {
        this->columns[colIdx] = new Vector *[vecBatchCount];
    }

    int32_t valueIndex = 0;
    for (int32_t vecBatchIdx = 0; vecBatchIdx < vecBatchCount; ++vecBatchIdx) {
        VectorBatch *vecBatch = vecBatches[vecBatchIdx];
        int32_t rowCount = vecBatch->GetRowCount();
        // generate value address.
        for (int32_t rowIdx = 0; rowIdx < rowCount; rowIdx++) {
            int64_t valueAddress = encodeSyntheticAddress(vecBatchIdx, rowIdx);
            this->valueAddresses[valueIndex++] = valueAddress;
        }

        // put vectors to a collector.
        for (int32_t colIdx = 0; colIdx < columnCount; ++colIdx) {
            this->columns[colIdx][vecBatchIdx] = vecBatch->GetVector(colIdx);
        }
    }
    return 0;
}

void PagesIndex::sort(
    int32_t *sortCols,
    int32_t *sortColTypes,
    int32_t *sortAscendings,
    int32_t *sortNullFirsts,
    int32_t sortColCount,
    int32_t from,
    int32_t to)
{
    quickSort(
        (int64_t)this,
        sortCols,
        sortColTypes,
        sortAscendings,
        sortNullFirsts,
        sortColCount,
        from,
        to);
}

SPECIALIZE(OMNIJIT_PAGE_INDEX_GET_OUTPUT)
void PagesIndex::getOutput(int32_t *outputCols, int32_t outputColsCount, VectorBatch* outputVecBatch, int32_t *sourceTypes, int32_t offset, int32_t length)
{
    Vector ***inputVecBatches = this->columns;
    int64_t *valueAddresses = this->valueAddresses;

    Vector *outputColumn = nullptr;
    int32_t outputCol = 0;
    void *outputData = nullptr;
    int colType = 0;

    for (int32_t j = 0; j < outputColsCount; j++) {
        outputColumn = outputVecBatch->GetVector(j);
        outputCol = outputCols[j];
        outputData = outputColumn->GetValues();
        colType = sourceTypes[outputCol];
        Vector **inputVecBatch = inputVecBatches[outputCol];

        switch (colType)
        {
        case 1:
            setInt32ColumnValues(valueAddresses, offset, length, inputVecBatch, (int32_t *)outputData);
            break;
        case 2:
            setInt64ColumnValues(valueAddresses, offset, length, inputVecBatch, (int64_t *)outputData);
            break;
        case 3:
            setDoubleColumnValues(valueAddresses, offset, length, inputVecBatch, (double *)outputData);
            break;
        default:
            break;
        }
    }
}

PagesIndex::~PagesIndex()
{
    if (this->columns != nullptr){
        for (int32_t colIdx = 0; colIdx < this->typesCount; ++colIdx) {
            delete[] this->columns[colIdx];
        }
        delete[] this->columns;
    }

    if (this->valueAddresses != nullptr) {
        delete[] this->valueAddresses;
    }
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

SPECIALIZE(OMNIJIT_PAGE_INDEX_COMPARE_TO)
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
    Vector ***columns = pagesIndex->getColumns();

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
        Vector *leftColumn = columns[sortCol][leftColumnIndex];
        void *leftData = leftColumn->GetValues();
        void *leftNulls = leftColumn->GetValueNulls();
        int32_t colType = sortColTypes[i];
        Vector *rightColumn;
        void *rightData;
        void *rightNulls;

        if (isSameColumn) {
            rightColumn = leftColumn;
            rightData = leftData;
            rightNulls = leftNulls;
        }
        else {
            rightColumn = columns[sortCol][rightColumnIndex];
            rightData = rightColumn->GetValues();
            rightNulls = rightColumn->GetValueNulls();
        }

        // compare = compareNull(leftNulls, leftColumnPosition, rightNulls, rightColumnPosition, sortNullFirsts[i]);
        // if (compare != 2) {
        //     break;
        // }

        compare= OperatorUtil::compareVectorAtPosition((VecType)colType, leftColumn,leftColumnPosition,rightColumn,rightColumnPosition);

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
    s = std::min(a - from, b - a);
    vectorSwap(valueAddresses, from, b - s, s);
    s = std::min(d - c, n - d - 1);
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

void setInt32ColumnValues(int64_t *valueAddresses, int32_t offset, int32_t length, Vector **inputVecBatch, int32_t *outputData)
{
    int32_t preTableIndex = -1;
    int64_t valueAddress = 0;
    Vector *inputColumn = nullptr;
    int32_t *inputData = nullptr;
    int32_t pageIndex = 0;
    int32_t position = 0;

    int32_t start = offset;
    int32_t end = offset + length;
    int32_t outputIndex = 0;
    for (int32_t i = start; i < end; i++) {
        valueAddress = valueAddresses[i];
        pageIndex = decodeSliceIndex(valueAddress);
        position = decodePosition(valueAddress);
        if (preTableIndex != pageIndex) {
            inputColumn = inputVecBatch[pageIndex];
            inputData = (int32_t *)(inputColumn->GetValues());
            preTableIndex = pageIndex;
        }

        outputData[outputIndex] = inputData[position];
        outputIndex++;
    }
}

void setInt64ColumnValues(int64_t *valueAddresses, int32_t offset, int32_t length, Vector **inputVecBatch, int64_t *outputData)
{
    int32_t preTableIndex = -1;
    int64_t valueAddress = 0;
    Vector *inputColumn = nullptr;
    int64_t *inputData = nullptr;
    int32_t pageIndex = 0;
    int32_t position = 0;

    int32_t start = offset;
    int32_t end = offset + length;
    int32_t outputIndex = 0;
    for (int32_t i = start; i < end; i++) {
        valueAddress = valueAddresses[i];
        pageIndex = decodeSliceIndex(valueAddress);
        position = decodePosition(valueAddress);

        if (preTableIndex != pageIndex) {
            inputColumn = inputVecBatch[pageIndex];
            inputData = (int64_t *)(inputColumn->GetValues());
            preTableIndex = pageIndex;
        }

        outputData[outputIndex] = inputData[position];
        outputIndex++;
    }
}

void setDoubleColumnValues(int64_t *valueAddresses, int32_t offset, int32_t length, Vector **inputVecBatch, double *outputData)
{
    int32_t preTableIndex = -1;
    int64_t valueAddress = 0;
    Vector *inputColumn = nullptr;
    double *inputData = nullptr;
    int32_t pageIndex = 0;
    int32_t position = 0;

    int32_t start = offset;
    int32_t end = offset + length;
    int32_t outputIndex = 0;
    for (int32_t i = start; i < end; i++) {
        valueAddress = valueAddresses[i];
        pageIndex = decodeSliceIndex(valueAddress);
        position = decodePosition(valueAddress);
        if (preTableIndex != pageIndex) {
            inputColumn = inputVecBatch[pageIndex];
            inputData = (double *)(inputColumn->GetValues());
            preTableIndex = pageIndex;
        }

        outputData[outputIndex] = inputData[position];
        outputIndex++;
    }
}
