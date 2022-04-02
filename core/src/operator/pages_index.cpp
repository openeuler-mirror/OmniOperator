/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: pages index implementations
 */
#include "pages_index.h"
#include <algorithm>
#include "optimization.h"
#include "jit/annotation.h"

using namespace omniruntime::vec;

namespace omniruntime {
namespace op {
const int32_t QUICK_SORT_SMALL_LEN = 7;
const int32_t QUICK_SORT_BIG_LEN = 40;
const int32_t QUICK_SORT_STEP_SIZE = 8;
const int32_t QUICK_SORT_MIDDLE = 2;

void ColumnarSort(const int32_t *sortCols, const int32_t *sortColTypes, const int32_t *sortAscendings,
    const int32_t *sortNullFirsts, int32_t sortColCount, uint64_t *valueAddresses, Vector ***columns, int32_t from,
    int32_t to, int32_t currentCol);

void QuickSort(const int32_t *sortCols, const int32_t *sortColTypes, const int32_t *sortAscendings,
    const int32_t *sortNullFirsts, int32_t sortColCount, uint64_t *valueAddresses, Vector ***columns, int32_t from,
    int32_t to);

template <typename T>
T *ConstructVector(uint64_t *valueAddresses, int32_t offset, int32_t length, Vector **inputVecBatch,
    VectorAllocator *vecAllocator);

VarcharVector *ConstructVarcharVector(uint64_t *valueAddresses, int32_t offset, int32_t length, Vector **inputVecBatch,
    uint32_t width, VectorAllocator *vecAllocator);

int32_t GetMedianPosition(const int32_t *sortCols, const int32_t *sortColTypes, const int32_t *sortAscendings,
    const int32_t *sortNullFirsts, int32_t sortColCount, uint64_t *valueAddresses, Vector ***columns, int32_t from,
    int32_t to, int32_t len);

// function implements for class PagesIndex
PagesIndex::PagesIndex(const omniruntime::type::DataTypes &types)
    : dataTypes(types.Get().data()),
      dataTypeIds(types.GetIds()),
      typesCount(types.GetSize()),
      columns(nullptr),
      valueAddresses(nullptr),
      positionCount(0)
{}


inline void Swap(uint64_t *valueAddresses, int32_t a, int32_t b)
{
    auto temp = valueAddresses[a];
    valueAddresses[a] = valueAddresses[b];
    valueAddresses[b] = temp;
}

inline void VectorSwap(uint64_t *valueAddresses, int32_t from, int32_t l, int32_t s)
{
    for (int32_t i = 0; i < s; i++, from++, l++) {
        Swap(valueAddresses, from, l);
    }
}

// return error number
int32_t PagesIndex::AddVecBatches(std::vector<VectorBatch *> &vecBatches)
{
    int32_t vecBatchCount = vecBatches.size();
    int32_t columnCount = this->typesCount;

    for (int vecBatchIdx = 0; vecBatchIdx < vecBatchCount; ++vecBatchIdx) {
        this->positionCount += vecBatches[vecBatchIdx]->GetRowCount();
    }
    this->valueAddresses = new uint64_t[this->positionCount];
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

template <typename V>
int32_t CompareTo(const int32_t sortAscendings, const int32_t sortNullFirsts, const uint64_t *valueAddresses,
    Vector **columns, int32_t leftPosition, int32_t rightPosition)
{
    return Compare(sortAscendings, sortNullFirsts, valueAddresses, columns, leftPosition, rightPosition,
        OperatorUtil::CompareTemplate<V>);
}

int32_t CompareToDouble(const int32_t sortAscendings, const int32_t sortNullFirsts, const uint64_t *valueAddresses,
    Vector **columns, int32_t leftPosition, int32_t rightPosition)
{
    return Compare(sortAscendings, sortNullFirsts, valueAddresses, columns, leftPosition, rightPosition,
        OperatorUtil::CompareDouble);
}

int32_t CompareToVarChar(const int32_t sortAscendings, const int32_t sortNullFirsts, const uint64_t *valueAddresses,
    Vector **columns, int32_t leftPosition, int32_t rightPosition)
{
    return Compare(sortAscendings, sortNullFirsts, valueAddresses, columns, leftPosition, rightPosition,
        OperatorUtil::CompareVarchar);
}

int32_t CompareToDec128(const int32_t sortAscendings, const int32_t sortNullFirsts, const uint64_t *valueAddresses,
    Vector **columns, int32_t leftPosition, int32_t rightPosition)
{
    return Compare(sortAscendings, sortNullFirsts, valueAddresses, columns, leftPosition, rightPosition,
        OperatorUtil::CompareDecimal128);
}

template <typename V>
void QuickSortColumnSmall(const int32_t sortAscendings, const int32_t sortNullFirsts, uint64_t *valueAddresses,
    Vector **columns, int32_t from, int32_t to)
{
    for (int32_t i = from; i < to; i++) {
        for (int32_t j = i;
            j > from && (CompareTo<V>(sortAscendings, sortNullFirsts, valueAddresses, columns, j - 1, j)) > 0; j--) {
            Swap(valueAddresses, j, j - 1);
        }
    }
}

void QuickSortColumnDoubleSmall(const int32_t sortAscendings, const int32_t sortNullFirsts, uint64_t *valueAddresses,
    Vector **columns, int32_t from, int32_t to)
{
    for (int32_t i = from; i < to; i++) {
        for (int32_t j = i;
            j > from && (CompareToDouble(sortAscendings, sortNullFirsts, valueAddresses, columns, j - 1, j)) > 0; j--) {
            Swap(valueAddresses, j, j - 1);
        }
    }
}

void QuickSortColumnVarCharSmall(const int32_t sortAscendings, const int32_t sortNullFirsts, uint64_t *valueAddresses,
    Vector **columns, int32_t from, int32_t to)
{
    for (int32_t i = from; i < to; i++) {
        for (int32_t j = i;
            j > from && (CompareToVarChar(sortAscendings, sortNullFirsts, valueAddresses, columns, j - 1, j)) > 0;
            j--) {
            Swap(valueAddresses, j, j - 1);
        }
    }
}

void QuickSortColumnDec128Small(const int32_t sortAscendings, const int32_t sortNullFirsts, uint64_t *valueAddresses,
    Vector **columns, int32_t from, int32_t to)
{
    for (int32_t i = from; i < to; i++) {
        for (int32_t j = i;
            j > from && (CompareToDec128(sortAscendings, sortNullFirsts, valueAddresses, columns, j - 1, j)) > 0; j--) {
            Swap(valueAddresses, j, j - 1);
        }
    }
}

template <typename V>
int32_t Median3(const int32_t sortAscendings, const int32_t sortNullFirsts, uint64_t *valueAddresses, Vector **columns,
    int32_t a, int32_t b, int32_t c)
{
    int32_t ab = CompareTo<V>(sortAscendings, sortNullFirsts, valueAddresses, columns, a, b);
    int32_t ac = CompareTo<V>(sortAscendings, sortNullFirsts, valueAddresses, columns, a, c);
    int32_t bc = CompareTo<V>(sortAscendings, sortNullFirsts, valueAddresses, columns, b, c);
    return ((ab < 0) ? (bc < 0 ? b : ac < 0 ? c : a) : (bc > 0 ? b : ac > 0 ? c : a));
}

int32_t Median3Double(const int32_t sortAscendings, const int32_t sortNullFirsts, uint64_t *valueAddresses,
    Vector **columns, int32_t a, int32_t b, int32_t c)
{
    int32_t ab = CompareToDouble(sortAscendings, sortNullFirsts, valueAddresses, columns, a, b);
    int32_t ac = CompareToDouble(sortAscendings, sortNullFirsts, valueAddresses, columns, a, c);
    int32_t bc = CompareToDouble(sortAscendings, sortNullFirsts, valueAddresses, columns, b, c);
    return ((ab < 0) ? (bc < 0 ? b : ac < 0 ? c : a) : (bc > 0 ? b : ac > 0 ? c : a));
}

int32_t Median3VarChar(const int32_t sortAscendings, const int32_t sortNullFirsts, uint64_t *valueAddresses,
    Vector **columns, int32_t a, int32_t b, int32_t c)
{
    int32_t ab = CompareToVarChar(sortAscendings, sortNullFirsts, valueAddresses, columns, a, b);
    int32_t ac = CompareToVarChar(sortAscendings, sortNullFirsts, valueAddresses, columns, a, c);
    int32_t bc = CompareToVarChar(sortAscendings, sortNullFirsts, valueAddresses, columns, b, c);
    return (ab < 0 ? (bc < 0 ? b : ac < 0 ? c : a) : (bc > 0 ? b : ac > 0 ? c : a));
}

int32_t Median3Dec128(const int32_t sortAscendings, const int32_t sortNullFirsts, uint64_t *valueAddresses,
    Vector **columns, int32_t a, int32_t b, int32_t c)
{
    int32_t ab = CompareToDec128(sortAscendings, sortNullFirsts, valueAddresses, columns, a, b);
    int32_t ac = CompareToDec128(sortAscendings, sortNullFirsts, valueAddresses, columns, a, c);
    int32_t bc = CompareToDec128(sortAscendings, sortNullFirsts, valueAddresses, columns, b, c);
    return ((ab < 0) ? (bc < 0 ? b : ac < 0 ? c : a) : (bc > 0 ? b : ac > 0 ? c : a));
}

template <typename V>
void LeftAdvance(const int32_t sortAscendings, const int32_t sortNullFirsts, uint64_t *valueAddresses, Vector **columns,
    int32_t c, int32_t &m, int32_t &a, int32_t &b)
{
    int32_t comparison = 0;
    while (b <= c &&
        ((comparison = CompareTo<V>(sortAscendings, sortNullFirsts, valueAddresses, columns, b, m)) <= 0)) {
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

template <typename V>
void RightAdvance(const int32_t sortAscendings, const int32_t sortNullFirsts, uint64_t *valueAddresses,
    Vector **columns, int32_t b, int32_t &m, int32_t &c, int32_t &d)
{
    int32_t comparison = 0;
    while (c >= b &&
        ((comparison = CompareTo<V>(sortAscendings, sortNullFirsts, valueAddresses, columns, c, m)) >= 0)) {
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

template <typename V>
int32_t GetMedianPosition(const int32_t sortAscendings, const int32_t sortNullFirsts, uint64_t *valueAddresses,
    Vector **columns, int32_t from, int32_t to, int32_t len)
{
    int32_t m = from + len / QUICK_SORT_MIDDLE;
    if (len > QUICK_SORT_SMALL_LEN) {
        int32_t l = from;
        int32_t n = to - 1;
        if (len > QUICK_SORT_BIG_LEN) {
            int32_t s = len / QUICK_SORT_STEP_SIZE;
            l = Median3<V>(sortAscendings, sortNullFirsts, valueAddresses, columns, l, l + s,
                l + QUICK_SORT_MIDDLE * s);
            m = Median3<V>(sortAscendings, sortNullFirsts, valueAddresses, columns, m - s, m, m + s);
            n = Median3<V>(sortAscendings, sortNullFirsts, valueAddresses, columns, n - QUICK_SORT_MIDDLE * s, n - s,
                n);
        }
        m = Median3<V>(sortAscendings, sortNullFirsts, valueAddresses, columns, l, m, n);
    }
    return m;
}

template <typename V>
void QuickSortColumn(const int32_t sortAscendings, const int32_t sortNullFirsts, uint64_t *valueAddresses,
    Vector **columns, int32_t from, int32_t to)
{
    int32_t len = to - from;
    if (len < QUICK_SORT_SMALL_LEN) {
        QuickSortColumnSmall<V>(sortAscendings, sortNullFirsts, valueAddresses, columns, from, to);
        return;
    }

    int32_t m = GetMedianPosition<V>(sortAscendings, sortNullFirsts, valueAddresses, columns, from, to, len);

    int32_t a = from;
    int32_t b = a;
    int32_t c = to - 1;
    int32_t d = c;
    while (true) {
        LeftAdvance<V>(sortAscendings, sortNullFirsts, valueAddresses, columns, c, m, a, b);
        RightAdvance<V>(sortAscendings, sortNullFirsts, valueAddresses, columns, b, m, c, d);
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
        QuickSortColumn<V>(sortAscendings, sortNullFirsts, valueAddresses, columns, from, from + s);
    }
    if ((s = d - c) > 1) {
        QuickSortColumn<V>(sortAscendings, sortNullFirsts, valueAddresses, columns, n - s, n);
    }
}

void LeftAdvanceDouble(const int32_t sortAscendings, const int32_t sortNullFirsts, uint64_t *valueAddresses,
    Vector **columns, int32_t c, int32_t &m, int32_t &a, int32_t &b)
{
    int32_t comparison = 0;
    while (b <= c &&
        ((comparison = CompareToDouble(sortAscendings, sortNullFirsts, valueAddresses, columns, b, m)) <= 0)) {
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

void RightAdvanceDouble(const int32_t sortAscendings, const int32_t sortNullFirsts, uint64_t *valueAddresses,
    Vector **columns, int32_t b, int32_t &m, int32_t &c, int32_t &d)
{
    int32_t comparison = 0;
    while (c >= b &&
        ((comparison = CompareToDouble(sortAscendings, sortNullFirsts, valueAddresses, columns, c, m)) >= 0)) {
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

int32_t GetMedianPositionDouble(const int32_t sortAscendings, const int32_t sortNullFirsts, uint64_t *valueAddresses,
    Vector **columns, int32_t from, int32_t to, int32_t len)
{
    int32_t m = from + len / QUICK_SORT_MIDDLE;
    if (len > QUICK_SORT_SMALL_LEN) {
        int32_t l = from;
        int32_t n = to - 1;
        if (len > QUICK_SORT_BIG_LEN) {
            int32_t s = len / QUICK_SORT_STEP_SIZE;
            l = Median3Double(sortAscendings, sortNullFirsts, valueAddresses, columns, l, l + s,
                l + QUICK_SORT_MIDDLE * s);
            m = Median3Double(sortAscendings, sortNullFirsts, valueAddresses, columns, m - s, m, m + s);
            n = Median3Double(sortAscendings, sortNullFirsts, valueAddresses, columns, n - QUICK_SORT_MIDDLE * s, n - s,
                n);
        }
        m = Median3Double(sortAscendings, sortNullFirsts, valueAddresses, columns, l, m, n);
    }
    return m;
}

void QuickSortColumnDouble(const int32_t sortAscendings, const int32_t sortNullFirsts, uint64_t *valueAddresses,
    Vector **columns, int32_t from, int32_t to)
{
    int32_t len = to - from;
    if (len < QUICK_SORT_SMALL_LEN) {
        QuickSortColumnDoubleSmall(sortAscendings, sortNullFirsts, valueAddresses, columns, from, to);
        return;
    }

    int32_t m = GetMedianPositionDouble(sortAscendings, sortNullFirsts, valueAddresses, columns, from, to, len);

    int32_t a = from;
    int32_t b = a;
    int32_t c = to - 1;
    int32_t d = c;
    while (true) {
        LeftAdvanceDouble(sortAscendings, sortNullFirsts, valueAddresses, columns, c, m, a, b);
        RightAdvanceDouble(sortAscendings, sortNullFirsts, valueAddresses, columns, b, m, c, d);
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
        QuickSortColumnDouble(sortAscendings, sortNullFirsts, valueAddresses, columns, from, from + s);
    }
    if ((s = d - c) > 1) {
        QuickSortColumnDouble(sortAscendings, sortNullFirsts, valueAddresses, columns, n - s, n);
    }
}

void LeftAdvanceVarChar(const int32_t sortAscendings, const int32_t sortNullFirsts, uint64_t *valueAddresses,
    Vector **columns, int32_t c, int32_t &m, int32_t &a, int32_t &b)
{
    int32_t comparison = 0;
    while (b <= c &&
        ((comparison = CompareToVarChar(sortAscendings, sortNullFirsts, valueAddresses, columns, b, m)) <= 0)) {
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

void RightAdvanceVarChar(const int32_t sortAscendings, const int32_t sortNullFirsts, uint64_t *valueAddresses,
    Vector **columns, int32_t b, int32_t &m, int32_t &c, int32_t &d)
{
    int32_t comparison = 0;
    while (c >= b &&
        ((comparison = CompareToVarChar(sortAscendings, sortNullFirsts, valueAddresses, columns, c, m)) >= 0)) {
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

int32_t GetMedianPositionVarChar(const int32_t sortAscendings, const int32_t sortNullFirsts, uint64_t *valueAddresses,
    Vector **columns, int32_t from, int32_t to, int32_t len)
{
    int32_t m = from + len / QUICK_SORT_MIDDLE;
    if (len > QUICK_SORT_SMALL_LEN) {
        int32_t l = from;
        int32_t n = to - 1;
        if (len > QUICK_SORT_BIG_LEN) {
            int32_t s = len / QUICK_SORT_STEP_SIZE;
            l = Median3VarChar(sortAscendings, sortNullFirsts, valueAddresses, columns, l, l + s,
                l + QUICK_SORT_MIDDLE * s);
            m = Median3VarChar(sortAscendings, sortNullFirsts, valueAddresses, columns, m - s, m, m + s);
            n = Median3VarChar(sortAscendings, sortNullFirsts, valueAddresses, columns, n - QUICK_SORT_MIDDLE * s,
                n - s, n);
        }
        m = Median3VarChar(sortAscendings, sortNullFirsts, valueAddresses, columns, l, m, n);
    }
    return m;
}

void QuickSortColumnVarChar(const int32_t sortAscendings, const int32_t sortNullFirsts, uint64_t *valueAddresses,
    Vector **columns, int32_t from, int32_t to)
{
    int32_t len = to - from;
    if (len < QUICK_SORT_SMALL_LEN) {
        QuickSortColumnVarCharSmall(sortAscendings, sortNullFirsts, valueAddresses, columns, from, to);
        return;
    }

    int32_t m = GetMedianPositionVarChar(sortAscendings, sortNullFirsts, valueAddresses, columns, from, to, len);

    int32_t a = from;
    int32_t b = a;
    int32_t c = to - 1;
    int32_t d = c;
    while (true) {
        LeftAdvanceVarChar(sortAscendings, sortNullFirsts, valueAddresses, columns, c, m, a, b);
        RightAdvanceVarChar(sortAscendings, sortNullFirsts, valueAddresses, columns, b, m, c, d);
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
        QuickSortColumnVarChar(sortAscendings, sortNullFirsts, valueAddresses, columns, from, from + s);
    }
    if ((s = d - c) > 1) {
        QuickSortColumnVarChar(sortAscendings, sortNullFirsts, valueAddresses, columns, n - s, n);
    }
}

void LeftAdvanceDec128(const int32_t sortAscendings, const int32_t sortNullFirsts, uint64_t *valueAddresses,
    Vector **columns, int32_t c, int32_t &m, int32_t &a, int32_t &b)
{
    int32_t comparison = 0;
    while (b <= c &&
        ((comparison = CompareToDec128(sortAscendings, sortNullFirsts, valueAddresses, columns, b, m)) <= 0)) {
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

void RightAdvanceDec128(const int32_t sortAscendings, const int32_t sortNullFirsts, uint64_t *valueAddresses,
    Vector **columns, int32_t b, int32_t &m, int32_t &c, int32_t &d)
{
    int32_t comparison = 0;
    while (c >= b &&
        ((comparison = CompareToDec128(sortAscendings, sortNullFirsts, valueAddresses, columns, c, m)) >= 0)) {
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

int32_t GetMedianPositionDec128(const int32_t sortAscendings, const int32_t sortNullFirsts, uint64_t *valueAddresses,
    Vector **columns, int32_t from, int32_t to, int32_t len)
{
    int32_t m = from + len / QUICK_SORT_MIDDLE;
    if (len > QUICK_SORT_SMALL_LEN) {
        int32_t l = from;
        int32_t n = to - 1;
        if (len > QUICK_SORT_BIG_LEN) {
            int32_t s = len / QUICK_SORT_STEP_SIZE;
            l = Median3Dec128(sortAscendings, sortNullFirsts, valueAddresses, columns, l, l + s,
                l + QUICK_SORT_MIDDLE * s);
            m = Median3Dec128(sortAscendings, sortNullFirsts, valueAddresses, columns, m - s, m, m + s);
            n = Median3Dec128(sortAscendings, sortNullFirsts, valueAddresses, columns, n - QUICK_SORT_MIDDLE * s, n - s,
                n);
        }
        m = Median3Dec128(sortAscendings, sortNullFirsts, valueAddresses, columns, l, m, n);
    }
    return m;
}

void QuickSortColumnDec128(const int32_t sortAscendings, const int32_t sortNullFirsts, uint64_t *valueAddresses,
    Vector **columns, int32_t from, int32_t to)
{
    int32_t len = to - from;
    if (len < QUICK_SORT_SMALL_LEN) {
        QuickSortColumnDec128Small(sortAscendings, sortNullFirsts, valueAddresses, columns, from, to);
        return;
    }
    int32_t m = GetMedianPositionDec128(sortAscendings, sortNullFirsts, valueAddresses, columns, from, to, len);
    int32_t a = from;
    int32_t b = a;
    int32_t c = to - 1;
    int32_t d = c;
    while (true) {
        LeftAdvanceDec128(sortAscendings, sortNullFirsts, valueAddresses, columns, c, m, a, b);
        RightAdvanceDec128(sortAscendings, sortNullFirsts, valueAddresses, columns, b, m, c, d);
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
        QuickSortColumnDec128(sortAscendings, sortNullFirsts, valueAddresses, columns, from, from + s);
    }
    if ((s = d - c) > 1) {
        QuickSortColumnDec128(sortAscendings, sortNullFirsts, valueAddresses, columns, n - s, n);
    }
}

std::vector<std::tuple<int32_t, int32_t>> GetRanges(uint64_t *valueAddresses, Vector **columns, int32_t from,
    int32_t to, CompareFunc compareFunc)
{
    std::vector<std::tuple<int32_t, int32_t>> ranges;
    int64_t valueAddress = valueAddresses[from];
    int32_t columnIndex = DecodeSliceIndex(valueAddress);
    int32_t columnPosition = DecodePosition(valueAddress);
    Vector *column = columns[columnIndex];
    int32_t originalColumnPosition;
    column = VectorHelper::ExpandVectorAndIndex(column, columnPosition, originalColumnPosition);
    bool currentIsNull = column->IsValueNull(originalColumnPosition);
    bool valueIsNull = false;
    Vector *currentColumn = nullptr;
    int32_t currentColumnPosition = -1;
    if (!currentIsNull) {
        currentColumn = column;
        currentColumnPosition = originalColumnPosition;
    }
    int32_t start = from;
    for (int32_t i = from + 1; i < to; ++i) {
        valueAddress = valueAddresses[i];
        columnIndex = DecodeSliceIndex(valueAddress);
        columnPosition = DecodePosition(valueAddress);
        column = columns[columnIndex];
        column = VectorHelper::ExpandVectorAndIndex(column, columnPosition, originalColumnPosition);
        // we are still null
        valueIsNull = column->IsValueNull(originalColumnPosition);
        if (currentIsNull && valueIsNull) {
            continue;
        } else if (currentIsNull == valueIsNull) {
            // we still have the same value
            if (compareFunc(currentColumn, currentColumnPosition, column, originalColumnPosition) == 0) {
                continue;
            }
        }
        if (valueIsNull) {
            currentIsNull = true;
        } else {
            currentIsNull = false;
            currentColumn = column;
            currentColumnPosition = originalColumnPosition;
        }
        // single value range ?
        if (start + 1 != i) {
            ranges.emplace_back(start, i);
        }
        start = i;
    }
    if (start + 1 != to) {
        ranges.emplace_back(start, to);
    }
    return ranges;
}

template <typename V, typename T>
std::vector<std::tuple<int32_t, int32_t>> GetRanges(uint64_t *valueAddresses, Vector **columns, int32_t from,
    int32_t to)
{
    return GetRanges(valueAddresses, columns, from, to, OperatorUtil::CompareTemplate<V>);
}

std::vector<std::tuple<int32_t, int32_t>> GetRangesDouble(uint64_t *valueAddresses, Vector **columns, int32_t from,
    int32_t to)
{
    return GetRanges(valueAddresses, columns, from, to, OperatorUtil::CompareDouble);
}

std::vector<std::tuple<int32_t, int32_t>> GetRangesVarChar(uint64_t *valueAddresses, Vector **columns, int32_t from,
    int32_t to)
{
    return GetRanges(valueAddresses, columns, from, to, OperatorUtil::CompareVarchar);
}

std::vector<std::tuple<int32_t, int32_t>> GetRangesDec128(uint64_t *valueAddresses, Vector **columns, int32_t from,
    int32_t to)
{
    return GetRanges(valueAddresses, columns, from, to, OperatorUtil::CompareDecimal128);
}

template <typename V, typename T>
void ColumnarSort(const int32_t *sortCols, const int32_t *sortColTypes, const int32_t *sortAscendings,
    const int32_t *sortNullFirsts, int32_t sortColCount, uint64_t *valueAddresses, Vector ***columns, int32_t from,
    int32_t to, int32_t currentCol)
{
    // sort on specific range on specific column
    QuickSortColumn<V>(sortAscendings[currentCol], sortNullFirsts[currentCol], valueAddresses,
        columns[sortCols[currentCol]], from, to);
    // are we the last column ?
    if (currentCol == sortColCount - 1) {
        return;
    }
    // get the duplicate range for sub sorting
    std::vector<std::tuple<int32_t, int32_t>> ranges =
        GetRanges<V, T>(valueAddresses, columns[sortCols[currentCol]], from, to);
    // iterate over each range
    for (std::tuple<int32_t, int32_t> t : ranges) {
        ColumnarSort(sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, valueAddresses, columns,
            std::get<0>(t), std::get<1>(t), currentCol + 1);
    }
}


void ColumnarSortDouble(const int32_t *sortCols, const int32_t *sortColTypes, const int32_t *sortAscendings,
    const int32_t *sortNullFirsts, int32_t sortColCount, uint64_t *valueAddresses, Vector ***columns, int32_t from,
    int32_t to, int32_t currentCol)
{
    // sort on specific range on specific column
    QuickSortColumnDouble(sortAscendings[currentCol], sortNullFirsts[currentCol], valueAddresses,
        columns[sortCols[currentCol]], from, to);
    // are we the last column ?
    if (currentCol == sortColCount - 1) {
        return;
    }
    // get the duplicate range for sub sorting
    std::vector<std::tuple<int32_t, int32_t>> ranges =
        GetRangesDouble(valueAddresses, columns[sortCols[currentCol]], from, to);
    // iterate over each range
    for (std::tuple<int32_t, int32_t> t : ranges) {
        ColumnarSort(sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, valueAddresses, columns,
            std::get<0>(t), std::get<1>(t), currentCol + 1);
    }
}

void ColumnarSortVarChar(const int32_t *sortCols, const int32_t *sortColTypes, const int32_t *sortAscendings,
    const int32_t *sortNullFirsts, int32_t sortColCount, uint64_t *valueAddresses, Vector ***columns, int32_t from,
    int32_t to, int32_t currentCol)
{
    // sort on specific range on specific column
    QuickSortColumnVarChar(sortAscendings[currentCol], sortNullFirsts[currentCol], valueAddresses,
        columns[sortCols[currentCol]], from, to);
    // are we the last column ?
    if (currentCol == sortColCount - 1) {
        return;
    }
    // get the duplicate range for sub sorting
    std::vector<std::tuple<int32_t, int32_t>> ranges =
        GetRangesVarChar(valueAddresses, columns[sortCols[currentCol]], from, to);
    // iterate over each range
    for (std::tuple<int32_t, int32_t> t : ranges) {
        ColumnarSort(sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, valueAddresses, columns,
            std::get<0>(t), std::get<1>(t), currentCol + 1);
    }
}

void ColumnarSortDec128(const int32_t *sortCols, const int32_t *sortColTypes, const int32_t *sortAscendings,
    const int32_t *sortNullFirsts, int32_t sortColCount, uint64_t *valueAddresses, Vector ***columns, int32_t from,
    int32_t to, int32_t currentCol)
{
    // sort on specific range on specific column
    QuickSortColumnDec128(sortAscendings[currentCol], sortNullFirsts[currentCol], valueAddresses,
        columns[sortCols[currentCol]], from, to);
    // are we the last column ?
    if (currentCol == sortColCount - 1) {
        return;
    }
    // get the duplicate range for sub sorting
    std::vector<std::tuple<int32_t, int32_t>> ranges =
        GetRangesDec128(valueAddresses, columns[sortCols[currentCol]], from, to);
    // iterate over each range
    for (std::tuple<int32_t, int32_t> t : ranges) {
        ColumnarSort(sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, valueAddresses, columns,
            std::get<0>(t), std::get<1>(t), currentCol + 1);
    }
}

void ColumnarSort(const int32_t *sortCols, const int32_t *sortColTypes, const int32_t *sortAscendings,
    const int32_t *sortNullFirsts, int32_t sortColCount, uint64_t *valueAddresses, Vector ***columns, int32_t from,
    int32_t to, int32_t currentCol)
{
    switch (sortColTypes[currentCol]) {
        case OMNI_INT:
        case OMNI_DATE32:
            ColumnarSort<omniruntime::vec::IntVector, int32_t>(sortCols, sortColTypes, sortAscendings, sortNullFirsts,
                sortColCount, valueAddresses, columns, from, to, currentCol);
            break;
        case OMNI_LONG:
        case OMNI_DECIMAL64:
            ColumnarSort<omniruntime::vec::LongVector, int64_t>(sortCols, sortColTypes, sortAscendings, sortNullFirsts,
                sortColCount, valueAddresses, columns, from, to, currentCol);
            break;
        case OMNI_DOUBLE:
            ColumnarSortDouble(sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, valueAddresses,
                columns, from, to, currentCol);
            break;
        case OMNI_BOOLEAN:
            ColumnarSort<omniruntime::vec::BooleanVector, bool>(sortCols, sortColTypes, sortAscendings, sortNullFirsts,
                sortColCount, valueAddresses, columns, from, to, currentCol);
            break;
        case OMNI_VARCHAR:
        case OMNI_CHAR:
            ColumnarSortVarChar(sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, valueAddresses,
                columns, from, to, currentCol);
            break;
        case OMNI_DECIMAL128:
            ColumnarSortDec128(sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, valueAddresses,
                columns, from, to, currentCol);
            break;
        default:
            break;
    }
}

void PagesIndex::Sort(const int32_t *sortCols, const int32_t *sortColTypes, const int32_t *sortAscendings,
    const int32_t *sortNullFirsts, int32_t sortColCount, int32_t from, int32_t to) const
{
    ColumnarSort(sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, valueAddresses, columns, from,
        to, 0);
}

SPECIALIZE(OMNIJIT_PAGE_INDEX_GET_OUTPUT)
void PagesIndex::GetOutput(int32_t *outputCols, int32_t outputColsCount, VectorBatch *outputVecBatch,
    const int32_t *sourceTypes, int32_t offset, int32_t length, VectorAllocator *vecAllocator) const
{
    Vector ***inputVecBatches = this->columns;

    int32_t outputCol = 0;
    int colTypeId = 0;
    for (int32_t j = 0; j < outputColsCount; j++) {
        outputCol = outputCols[j];
        colTypeId = sourceTypes[outputCol];
        Vector **inputVecBatch = inputVecBatches[outputCol];

        switch (colTypeId) {
            case OMNI_INT:
            case OMNI_DATE32:
                outputVecBatch->SetVector(j,
                    ConstructVector<IntVector>(valueAddresses, offset, length, inputVecBatch, vecAllocator));
                break;
            case OMNI_LONG:
            case OMNI_DECIMAL64:
                outputVecBatch->SetVector(j,
                    ConstructVector<LongVector>(valueAddresses, offset, length, inputVecBatch, vecAllocator));
                break;
            case OMNI_DOUBLE:
                outputVecBatch->SetVector(j,
                    ConstructVector<DoubleVector>(valueAddresses, offset, length, inputVecBatch, vecAllocator));
                break;
            case OMNI_BOOLEAN:
                outputVecBatch->SetVector(j,
                    ConstructVector<BooleanVector>(valueAddresses, offset, length, inputVecBatch, vecAllocator));
                break;
            case OMNI_VARCHAR:
            case OMNI_CHAR: {
                VarcharVector *varcharVector = ConstructVarcharVector(valueAddresses, offset, length, inputVecBatch,
                    (dataTypes[outputCol]).GetWidth(), vecAllocator);
                outputVecBatch->SetVector(j, varcharVector);
                break;
            }
            case OMNI_DECIMAL128:
                outputVecBatch->SetVector(j,
                    ConstructVector<Decimal128Vector>(valueAddresses, offset, length, inputVecBatch, vecAllocator));
                break;
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

template <typename T> void SetValue(Vector *inputVector, int32_t inputIndex, T *outputVector, int32_t outputIndex)
{
    if (inputVector->GetEncoding() == OMNI_VEC_ENCODING_DICTIONARY) {
        auto dictionaryVector = static_cast<DictionaryVector *>(inputVector);
        SetValue(dictionaryVector->GetDictionary(), dictionaryVector->GetId(inputIndex), outputVector, outputIndex);
    } else {
        outputVector->SetValueNull(outputIndex, inputVector->IsValueNull(inputIndex));
        outputVector->SetValue(outputIndex, static_cast<T *>(inputVector)->GetValue(inputIndex));
    }
}

template <typename T>
T *ConstructVector(uint64_t *valueAddresses, int32_t offset, int32_t length, Vector **inputVecBatch,
    VectorAllocator *vecAllocator)
{
    uint64_t valueAddress = 0;
    Vector *inputVector = nullptr;
    int32_t pageIndex = 0;
    int32_t position = 0;
    auto outputVector = new T(vecAllocator, length);
    int32_t start = offset;
    int32_t end = offset + length;
    int32_t outputIndex = 0;
    for (int32_t i = start; i < end; i++) {
        valueAddress = valueAddresses[i];
        pageIndex = DecodeSliceIndex(valueAddress);
        position = DecodePosition(valueAddress);
        inputVector = inputVecBatch[pageIndex];
        SetValue(inputVector, position, outputVector, outputIndex++);
    }
    return outputVector;
}

void SetVarcharValue(Vector *inputVector, int32_t inputIndex, VarcharVector *outputVector, int32_t outputIndex)
{
    if (inputVector->GetEncoding() == OMNI_VEC_ENCODING_DICTIONARY) {
        auto dictionaryVector = static_cast<DictionaryVector *>(inputVector);
        SetVarcharValue(dictionaryVector->GetDictionary(), dictionaryVector->GetId(inputIndex), outputVector,
            outputIndex);
    } else {
        if (inputVector->IsValueNull(inputIndex)) {
            static_cast<VarcharVector *>(outputVector)->SetValueNull(outputIndex);
        } else {
            uint8_t *value = nullptr;
            int32_t valueLength = static_cast<VarcharVector *>(inputVector)->GetValue(inputIndex, &value);
            outputVector->SetValue(outputIndex, value, valueLength);
        }
    }
}

VarcharVector *ConstructVarcharVector(uint64_t *valueAddresses, int32_t offset, int32_t length, Vector **inputVecBatch,
    uint32_t width, VectorAllocator *vecAllocator)
{
    int64_t valueAddress = 0;
    Vector *inputVector = nullptr;
    int32_t pageIndex = 0;
    int32_t position = 0;

    VarcharVector *outputVector = new VarcharVector(vecAllocator, length * width, length);
    int32_t start = offset;
    int32_t end = offset + length;
    int32_t outputIndex = 0;
    for (int32_t i = start; i < end; i++) {
        valueAddress = valueAddresses[i];
        pageIndex = DecodeSliceIndex(valueAddress);
        position = DecodePosition(valueAddress);
        inputVector = inputVecBatch[pageIndex];
        SetVarcharValue(inputVector, position, outputVector, outputIndex++);
    }
    return outputVector;
}
}
}