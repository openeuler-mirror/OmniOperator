/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: pages index implementations
 */
#include "pages_index.h"
#include "optimization.h"
#include "../jit/annotation.h"
#include "../vector/vector_helper.h"
#include "util/operator_util.h"

#include <algorithm>

using namespace omniruntime::vec;

const int32_t QUICK_SORT_SMALL_LEN = 7;
const int32_t QUICK_SORT_BIG_LEN = 40;
const int32_t QUICK_SORT_STEP_SIZE = 8;
const int32_t QUICK_SORT_MIDDLE = 2;

void QuickSort(const int32_t *sortCols, const int32_t *sortColTypes, const int32_t *sortAscendings,
    const int32_t *sortNullFirsts, int32_t sortColCount, int64_t *valueAddresses, Vector ***columns, int32_t from,
    int32_t to);
IntVector *ConstructInt32Vector(int64_t *valueAddresses, int32_t offset, int32_t length, Vector **inputVecBatch);
LongVector *ConstructInt64Vector(int64_t *valueAddresses, int32_t offset, int32_t length, Vector **inputVecBatch);
DoubleVector *ConstructDoubleVector(int64_t *valueAddresses, int32_t offset, int32_t length, Vector **inputVecBatch);
VarcharVector *ConstructVarcharVector(int64_t *valueAddresses, int32_t offset, int32_t length, Vector **inputVecBatch,
    uint32_t width);

int32_t GetMedianPosition(const int32_t *sortCols, const int32_t *sortColTypes, const int32_t *sortAscendings,
    const int32_t *sortNullFirsts, int32_t sortColCount, int64_t *valueAddresses, Vector ***columns, int32_t from,
    int32_t to, int32_t len);

// function implements for class PagesIndex
PagesIndex::PagesIndex(const omniruntime::vec::VecTypes &types)
    : vecTypes(types.Get().data()),
      vecTypeIds(types.GetIds()),
      typesCount(types.GetSize()),
      columns(nullptr),
      valueAddresses(nullptr),
      positionCount(0)
{}

// return error number
int32_t PagesIndex::AddVecBatches(std::vector<VectorBatch *> &vecBatches)
{
    int32_t vecBatchCount = vecBatches.size();
    int32_t columnCount = this->typesCount;

    for (int vecBatchIdx = 0; vecBatchIdx < vecBatchCount; ++vecBatchIdx) {
        this->positionCount += vecBatches[vecBatchIdx]->GetRowCount();
    }
    this->valueAddresses = std::make_unique<int64_t[]>(this->positionCount).release();
    this->columns = std::make_unique<Vector **[]>(columnCount).release();
    for (int colIdx = 0; colIdx < columnCount; ++colIdx) {
        this->columns[colIdx] = std::make_unique<Vector *[]>(vecBatchCount).release();
    }

    int32_t valueIndex = 0;
    for (int32_t vecBatchIdx = 0; vecBatchIdx < vecBatchCount; ++vecBatchIdx) {
        VectorBatch *vecBatch = vecBatches[vecBatchIdx];
        int32_t rowCount = vecBatch->GetRowCount();
        // generate value address.
        for (int32_t rowIdx = 0; rowIdx < rowCount; rowIdx++) {
            int64_t valueAddress = EncodeSyntheticAddress(vecBatchIdx, rowIdx);
            this->valueAddresses[valueIndex++] = valueAddress;
        }

        // put vectors to a collector.
        for (int32_t colIdx = 0; colIdx < columnCount; ++colIdx) {
            this->columns[colIdx][vecBatchIdx] = vecBatch->GetVector(colIdx);
        }
    }
    return 0;
}

void PagesIndex::Sort(const int32_t *sortCols, const int32_t *sortColTypes, const int32_t *sortAscendings,
    const int32_t *sortNullFirsts, int32_t sortColCount, int32_t from, int32_t to) const
{
    QuickSort(sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, valueAddresses, columns, from, to);
}

SPECIALIZE(OMNIJIT_PAGE_INDEX_GET_OUTPUT)
void PagesIndex::GetOutput(int32_t *outputCols, int32_t outputColsCount, VectorBatch *outputVecBatch,
    const int32_t *sourceTypes, int32_t offset, int32_t length) const
{
    Vector ***inputVecBatches = this->columns;
    int64_t *valueAddresses = this->valueAddresses;

    int32_t outputCol = 0;
    int colTypeId = 0;
    for (int32_t j = 0; j < outputColsCount; j++) {
        outputCol = outputCols[j];
        colTypeId = sourceTypes[outputCol];
        Vector **inputVecBatch = inputVecBatches[outputCol];

        switch (colTypeId) {
            case OMNI_VEC_TYPE_INT: {
                IntVector *intVector = ConstructInt32Vector(valueAddresses, offset, length, inputVecBatch);
                outputVecBatch->SetVector(j, intVector);
                break;
            }
            case OMNI_VEC_TYPE_LONG: {
                LongVector *longVector = ConstructInt64Vector(valueAddresses, offset, length, inputVecBatch);
                outputVecBatch->SetVector(j, longVector);
                break;
            }
            case OMNI_VEC_TYPE_DOUBLE: {
                DoubleVector *doubleVector = ConstructDoubleVector(valueAddresses, offset, length, inputVecBatch);
                outputVecBatch->SetVector(j, doubleVector);
                break;
            }
            case OMNI_VEC_TYPE_VARCHAR: {
                auto vecType = (VarcharVecType &)vecTypes[outputCol];
                VarcharVector *varcharVector =
                    ConstructVarcharVector(valueAddresses, offset, length, inputVecBatch, vecType.GetWidth());
                outputVecBatch->SetVector(j, varcharVector);
                break;
            }
            default:
                break;
        }
    }
}

PagesIndex::~PagesIndex()
{
    if (this->columns != nullptr) {
        for (int32_t colIdx = 0; colIdx < this->typesCount; ++colIdx) {
            delete[] this->columns[colIdx];
        }
        delete[] this->columns;
    }

    if (this->valueAddresses != nullptr) {
        delete[] this->valueAddresses;
    }
}

inline void Swap(int64_t *valueAddresses, int32_t a, int32_t b)
{
    int64_t temp = valueAddresses[a];
    valueAddresses[a] = valueAddresses[b];
    valueAddresses[b] = temp;
}

inline void VectorSwap(int64_t *valueAddresses, int32_t from, int32_t l, int32_t s)
{
    for (int32_t i = 0; i < s; i++, from++, l++) {
        Swap(valueAddresses, from, l);
    }
}

// return 0 when left and right both are nulls
// return 2 when left and right both are not nulls
int32_t CompareNull(const int32_t *leftNulls, int32_t leftPosition, const int32_t *rightNulls, int32_t rightPosition,
    int32_t nullsFirst)
{
    int32_t leftIsNull = leftNulls[leftPosition];
    int32_t rightIsNull = rightNulls[rightPosition];

    if (leftIsNull && rightIsNull) {
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
int32_t CompareTo(const int32_t *sortCols, const int32_t *sortColTypes, const int32_t *sortAscendings,
    const int32_t *sortNullFirsts, int32_t sortColCount, const int64_t *valueAddresses, Vector ***columns,
    int32_t leftPosition, int32_t rightPosition)
{
    int64_t leftValueAddress = valueAddresses[leftPosition];
    int32_t leftColumnIndex = DecodeSliceIndex(leftValueAddress);
    int32_t leftColumnPosition = DecodePosition(leftValueAddress);

    int64_t rightValueAddress = valueAddresses[rightPosition];
    int32_t rightColumnIndex = DecodeSliceIndex(rightValueAddress);
    int32_t rightColumnPosition = DecodePosition(rightValueAddress);

    bool isSameColumn = false;
    if (leftColumnIndex == rightColumnIndex) {
        isSameColumn = true;
    }

    int compare = 0;
    for (int32_t i = 0; i < sortColCount; i++) {
        int32_t sortCol = sortCols[i];
        Vector *leftColumn = columns[sortCol][leftColumnIndex];
        void *leftNulls = leftColumn->GetValueNulls();
        int32_t colTypeId = sortColTypes[i];
        Vector *rightColumn = nullptr;
        void *rightNulls = nullptr;

        if (isSameColumn) {
            rightColumn = leftColumn;
            rightNulls = leftNulls;
        } else {
            rightColumn = columns[sortCol][rightColumnIndex];
            rightNulls = rightColumn->GetValueNulls();
        }

        // compare = compareNull(leftNulls, leftColumnPosition, rightNulls, rightColumnPosition, sortNullFirsts[i]);
        // if (compare != 2) {
        //     break;
        // }

        compare = OperatorUtil::CompareVectorAtPosition(colTypeId, leftColumn, leftColumnPosition, rightColumn,
            rightColumnPosition);

        if (sortAscendings[i] == 0) {
            compare = -compare;
        }

        if (compare != 0) {
            break;
        }
    }

    return compare;
}

int32_t Median3(const int32_t *sortCols, const int32_t *sortColTypes, const int32_t *sortAscendings,
    const int32_t *sortNullFirsts, int32_t sortColCount, int64_t *valueAddresses, Vector ***columns, int32_t a,
    int32_t b, int32_t c)
{
    int32_t ab =
        CompareTo(sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, valueAddresses, columns, a, b);
    int32_t ac =
        CompareTo(sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, valueAddresses, columns, a, c);
    int32_t bc =
        CompareTo(sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, valueAddresses, columns, b, c);
    return (ab < 0 ? (bc < 0 ? b : ac < 0 ? c : a) : (bc > 0 ? b : ac > 0 ? c : a));
}

void QuickSortSmall(const int32_t *sortCols, const int32_t *sortColTypes, const int32_t *sortAscendings,
    const int32_t *sortNullFirsts, int32_t sortColCount, int64_t *valueAddresses, Vector ***columns, int32_t from,
    int32_t to)
{
    for (int32_t i = from; i < to; i++) {
        for (int32_t j = i; j > from && (CompareTo(sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount,
            valueAddresses, columns, j - 1, j)) > 0;
            j--) {
            Swap(valueAddresses, j, j - 1);
        }
    }
}

void LeftAdvance(const int32_t *sortCols, const int32_t *sortColTypes, const int32_t *sortAscendings,
    const int32_t *sortNullFirsts, int32_t sortColCount, int64_t *valueAddresses, Vector ***columns, int32_t c,
    int32_t &m, int32_t &a, int32_t &b)
{
    int32_t comparison = 0;
    while (b <= c && ((comparison = CompareTo(sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount,
        valueAddresses, columns, b, m)) <= 0)) {
        if (comparison == 0) {
            if (a == m) {
                m = b;
            } else if (b == m) {
                m = a;
            }
            Swap(valueAddresses, a++, b);
        }
        b++;
    }
}

void RightAdvance(const int32_t *sortCols, const int32_t *sortColTypes, const int32_t *sortAscendings,
    const int32_t *sortNullFirsts, int32_t sortColCount, int64_t *valueAddresses, Vector ***columns, int32_t b,
    int32_t &m, int32_t &c, int32_t &d)
{
    int32_t comparison = 0;
    while (c >= b && ((comparison = CompareTo(sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount,
        valueAddresses, columns, c, m)) >= 0)) {
        if (comparison == 0) {
            if (c == m) {
                m = d;
            } else if (d == m) {
                m = c;
            }
            Swap(valueAddresses, c, d--);
        }
        c--;
    }
}

int32_t GetMedianPosition(const int32_t *sortCols, const int32_t *sortColTypes, const int32_t *sortAscendings,
    const int32_t *sortNullFirsts, int32_t sortColCount, int64_t *valueAddresses, Vector ***columns, int32_t from,
    int32_t to, int32_t len)
{
    int32_t m = from + len / QUICK_SORT_MIDDLE;
    if (len > QUICK_SORT_SMALL_LEN) {
        int32_t l = from;
        int32_t n = to - 1;
        if (len > QUICK_SORT_BIG_LEN) {
            int32_t s = len / QUICK_SORT_STEP_SIZE;
            l = Median3(sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, valueAddresses, columns,
                l, l + s, l + QUICK_SORT_MIDDLE * s);
            m = Median3(sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, valueAddresses, columns,
                m - s, m, m + s);
            n = Median3(sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, valueAddresses, columns,
                n - QUICK_SORT_MIDDLE * s, n - s, n);
        }
        m = Median3(sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, valueAddresses, columns, l, m,
            n);
    }
    return m;
}

void QuickSort(const int32_t *sortCols, const int32_t *sortColTypes, const int32_t *sortAscendings,
    const int32_t *sortNullFirsts, int32_t sortColCount, int64_t *valueAddresses, Vector ***columns, int32_t from,
    int32_t to)
{
    int32_t len = to - from;
    if (len < QUICK_SORT_SMALL_LEN) {
        QuickSortSmall(sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, valueAddresses, columns,
            from, to);
        return;
    }

    int32_t m = GetMedianPosition(sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, valueAddresses,
        columns, from, to, len);

    int32_t a = from;
    int32_t b = a;
    int32_t c = to - 1;
    int32_t d = c;
    while (true) {
        LeftAdvance(sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, valueAddresses, columns, c, m,
            a, b);
        RightAdvance(sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, valueAddresses, columns, b,
            m, c, d);
        if (b > c) {
            break;
        }
        if (b == m) {
            m = d;
        } else if (c == m) {
            m = c;
        }
        Swap(valueAddresses, b++, c--);
    }

    // Swap partition elements back to middle
    int32_t s;
    int32_t n = to;
    s = std::min(a - from, b - a);
    VectorSwap(valueAddresses, from, b - s, s);
    s = std::min(d - c, n - d - 1);
    VectorSwap(valueAddresses, b, n - s, s);

    // Recursively sort non-partition-elements
    if ((s = b - a) > 1) {
        QuickSort(sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, valueAddresses, columns, from,
            from + s);
    }
    if ((s = d - c) > 1) {
        QuickSort(sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, valueAddresses, columns, n - s,
            n);
    }
}

IntVector *ConstructInt32Vector(int64_t *valueAddresses, int32_t offset, int32_t length, Vector **inputVecBatch)
{
    int32_t preTableIndex = -1;
    int64_t valueAddress = 0;
    Vector *inputColumn = nullptr;
    int32_t *inputValues = nullptr;
    int32_t pageIndex = 0;
    int32_t position = 0;

    IntVector *intVector = std::make_unique<IntVector>(nullptr, length).release();
    int32_t *outputValues = static_cast<int32_t *>(intVector->GetValues());

    int32_t start = offset;
    int32_t end = offset + length;
    int32_t outputIndex = 0;
    for (int32_t i = start; i < end; i++) {
        valueAddress = valueAddresses[i];
        pageIndex = DecodeSliceIndex(valueAddress);
        position = DecodePosition(valueAddress);
        if (preTableIndex != pageIndex) {
            inputColumn = inputVecBatch[pageIndex];
            inputValues = static_cast<int32_t *>(inputColumn->GetValues());
            preTableIndex = pageIndex;
        }
        outputValues[outputIndex++] = inputValues[position];
    }
    return intVector;
}

LongVector *ConstructInt64Vector(int64_t *valueAddresses, int32_t offset, int32_t length, Vector **inputVecBatch)
{
    int32_t preTableIndex = -1;
    int64_t valueAddress = 0;
    Vector *inputColumn = nullptr;
    int64_t *inputValues = nullptr;
    int32_t pageIndex = 0;
    int32_t position = 0;

    LongVector *longVector = std::make_unique<LongVector>(nullptr, length).release();
    int64_t *outputValues = static_cast<int64_t *>(longVector->GetValues());

    int32_t start = offset;
    int32_t end = offset + length;
    int32_t outputIndex = 0;
    for (int32_t i = start; i < end; i++) {
        valueAddress = valueAddresses[i];
        pageIndex = DecodeSliceIndex(valueAddress);
        position = DecodePosition(valueAddress);
        if (preTableIndex != pageIndex) {
            inputColumn = inputVecBatch[pageIndex];
            inputValues = static_cast<int64_t *>(inputColumn->GetValues());
            preTableIndex = pageIndex;
        }
        outputValues[outputIndex++] = inputValues[position];
    }
    return longVector;
}

DoubleVector *ConstructDoubleVector(int64_t *valueAddresses, int32_t offset, int32_t length, Vector **inputVecBatch)
{
    int32_t preTableIndex = -1;
    int64_t valueAddress = 0;
    Vector *inputColumn = nullptr;
    double *inputValues = nullptr;
    int32_t pageIndex = 0;
    int32_t position = 0;

    DoubleVector *doubleVector = std::make_unique<DoubleVector>(nullptr, length).release();
    double *outputValues = static_cast<double *>(doubleVector->GetValues());

    int32_t start = offset;
    int32_t end = offset + length;
    int32_t outputIndex = 0;
    for (int32_t i = start; i < end; i++) {
        valueAddress = valueAddresses[i];
        pageIndex = DecodeSliceIndex(valueAddress);
        position = DecodePosition(valueAddress);
        if (preTableIndex != pageIndex) {
            inputColumn = inputVecBatch[pageIndex];
            inputValues = static_cast<double *>(inputColumn->GetValues());
            preTableIndex = pageIndex;
        }
        outputValues[outputIndex++] = inputValues[position];
    }
    return doubleVector;
}

VarcharVector *ConstructVarcharVector(int64_t *valueAddresses, int32_t offset, int32_t length, Vector **inputVecBatch,
    uint32_t width)
{
    int32_t preTableIndex = -1;
    int64_t valueAddress = 0;
    VarcharVector *inputColumn = nullptr;
    int32_t pageIndex = 0;
    int32_t position = 0;

    VarcharVector *varcharVector =
        std::make_unique<VarcharVector>(static_cast<VectorAllocator *>(nullptr), length * width, length).release();

    int32_t start = offset;
    int32_t end = offset + length;
    int32_t outputIndex = 0;
    for (int32_t i = start; i < end; i++) {
        valueAddress = valueAddresses[i];
        pageIndex = DecodeSliceIndex(valueAddress);
        position = DecodePosition(valueAddress);
        if (preTableIndex != pageIndex) {
            inputColumn = static_cast<VarcharVector *>(inputVecBatch[pageIndex]);
            preTableIndex = pageIndex;
        }
        uint8_t *value = nullptr;
        int32_t valueLength = inputColumn->GetValue(position, &value);
        varcharVector->SetValue(outputIndex++, value, valueLength);
    }
    return varcharVector;
}
