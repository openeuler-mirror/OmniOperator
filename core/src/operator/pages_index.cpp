/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: pages index implementations
 */
#include "pages_index.h"
#include <algorithm>
#include "vector/vector.h"
#include "type/data_type.h"

using namespace omniruntime::vec;
using namespace omniruntime::type;

namespace omniruntime {
namespace op {
const int32_t QUICK_SORT_SMALL_LEN = 7;
const int32_t QUICK_SORT_BIG_LEN = 40;
const int32_t QUICK_SORT_STEP_SIZE = 8;
const int32_t QUICK_SORT_MIDDLE = 2;

void ColumnarSort(const int32_t *sortCols, const int32_t *sortColTypes, const int32_t *sortAscendings,
    const int32_t *sortNullFirsts, int32_t sortColCount, uint64_t *valueAddresses, BaseVector ***columns,
    const std::vector<bool> &mayHaveNulls, int32_t from, int32_t to, int32_t currentCol);

template <typename T>
std::unique_ptr<BaseVector> ConstructVector(uint64_t *valueAddresses, int32_t offset, int32_t length,
    BaseVector **inputVecBatch);

std::unique_ptr<BaseVector> ConstructVarcharVector(uint64_t *valueAddresses, int32_t offset, int32_t length,
    BaseVector **inputVecBatch);

int32_t GetMedianPosition(const int32_t *sortCols, const int32_t *sortColTypes, const int32_t *sortAscendings,
    const int32_t *sortNullFirsts, int32_t sortColCount, uint64_t *valueAddresses, BaseVector ***columns, int32_t from,
    int32_t to, int32_t len);

// function implements for class PagesIndex
PagesIndex::PagesIndex(const DataTypes &types)
    : dataTypes(types),
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

void PagesIndex::AddVecBatch(omniruntime::vec::VectorBatch *vecBatch)
{
    inputVecBatches.push_back(vecBatch);
    positionCount += vecBatch->GetRowCount();
}

void PagesIndex::Prepare()
{
    int32_t vecBatchCount = inputVecBatches.size();
    uint32_t columnCount = this->typesCount;

    this->valueAddresses = new uint64_t[this->positionCount];
    this->columns = new BaseVector **[columnCount];
    this->mayHaveNulls.resize(columnCount, false);
    for (uint32_t colIdx = 0; colIdx < columnCount; ++colIdx) {
        this->columns[colIdx] = new BaseVector *[vecBatchCount];
    }

    int32_t valueIndex = 0;
    for (int32_t vecBatchIdx = 0; vecBatchIdx < vecBatchCount; ++vecBatchIdx) {
        VectorBatch *vecBatch = inputVecBatches[vecBatchIdx];
        auto rowCount = static_cast<uint32_t>(vecBatch->GetRowCount());
        // generate value address.
        for (uint32_t rowIdx = 0; rowIdx < rowCount; rowIdx++) {
            uint64_t valueAddress = EncodeSyntheticAddress(vecBatchIdx, rowIdx);
            this->valueAddresses[valueIndex++] = valueAddress;
        }

        // put vectors to a collector.
        for (uint32_t colIdx = 0; colIdx < columnCount; ++colIdx) {
            auto vector = vecBatch->Get(static_cast<int32_t>(colIdx));
            this->columns[colIdx][vecBatchIdx] = vector;
            if (!this->mayHaveNulls[colIdx] && vector->HasNull()) {
                this->mayHaveNulls[colIdx] = true;
            }
        }
    }
}

template <typename V, bool columnsNullFlag, int32_t sortAscendings>
int32_t CompareTo(const int32_t sortNullFirsts, const uint64_t *valueAddresses, BaseVector **columns,
    int32_t leftPosition, int32_t rightPosition)
{
    return Compare<columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns, leftPosition,
        rightPosition, OperatorUtil::CompareTemplate<V>);
}

template <bool columnsNullFlag, int32_t sortAscendings>
int32_t CompareToDouble(const int32_t sortNullFirsts, const uint64_t *valueAddresses, BaseVector **columns,
    int32_t leftPosition, int32_t rightPosition)
{
    return Compare<columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns, leftPosition,
        rightPosition, OperatorUtil::CompareDouble);
}

template <bool columnsNullFlag, int32_t sortAscendings>
int32_t CompareToVarChar(const int32_t sortNullFirsts, const uint64_t *valueAddresses, BaseVector **columns,
    int32_t leftPosition, int32_t rightPosition)
{
    return Compare<columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns, leftPosition,
        rightPosition, OperatorUtil::CompareVarchar);
}

template <bool columnsNullFlag, int32_t sortAscendings>
int32_t CompareToDec128(const int32_t sortNullFirsts, const uint64_t *valueAddresses, BaseVector **columns,
    int32_t leftPosition, int32_t rightPosition)
{
    return Compare<columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns, leftPosition,
        rightPosition, OperatorUtil::CompareDecimal128);
}

template <typename V, bool columnsNullFlag, int32_t sortAscendings>
void QuickSortColumnSmall(const int32_t sortNullFirsts, uint64_t *valueAddresses, BaseVector **columns, int32_t from,
    int32_t to)
{
    for (int32_t i = from; i < to; i++) {
        for (int32_t j = i; j > from &&
            (CompareTo<V, columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns, j - 1, j)) > 0;
            j--) {
            Swap(valueAddresses, j, j - 1);
        }
    }
}

template <bool columnsNullFlag, int32_t sortAscendings>
void QuickSortColumnDoubleSmall(const int32_t sortNullFirsts, uint64_t *valueAddresses, BaseVector **columns,
    int32_t from, int32_t to)
{
    for (int32_t i = from; i < to; i++) {
        for (int32_t j = i; j > from &&
            (CompareToDouble<columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns, j - 1, j)) > 0;
            j--) {
            Swap(valueAddresses, j, j - 1);
        }
    }
}

template <bool columnsNullFlag, int32_t sortAscendings>
void QuickSortColumnVarCharSmall(const int32_t sortNullFirsts, uint64_t *valueAddresses, BaseVector **columns,
    int32_t from, int32_t to)
{
    for (int32_t i = from; i < to; i++) {
        for (int32_t j = i; j > from &&
            (CompareToVarChar<columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns, j - 1, j)) > 0;
            j--) {
            Swap(valueAddresses, j, j - 1);
        }
    }
}

template <bool columnsNullFlag, int32_t sortAscendings>
void QuickSortColumnDec128Small(const int32_t sortNullFirsts, uint64_t *valueAddresses, BaseVector **columns,
    int32_t from, int32_t to)
{
    for (int32_t i = from; i < to; i++) {
        for (int32_t j = i; j > from &&
            (CompareToDec128<columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns, j - 1, j)) > 0;
            j--) {
            Swap(valueAddresses, j, j - 1);
        }
    }
}

template <typename V, bool columnsNullFlag, int32_t sortAscendings>
int32_t Median3(const int32_t sortNullFirsts, uint64_t *valueAddresses, BaseVector **columns, int32_t a, int32_t b,
    int32_t c)
{
    int32_t ab = CompareTo<V, columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns, a, b);
    int32_t ac = CompareTo<V, columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns, a, c);
    int32_t bc = CompareTo<V, columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns, b, c);
    return ((ab < 0) ? (bc < 0 ? b : ac < 0 ? c : a) : (bc > 0 ? b : ac > 0 ? c : a));
}

template <bool columnsNullFlag, int32_t sortAscendings>
int32_t Median3Double(const int32_t sortNullFirsts, uint64_t *valueAddresses, BaseVector **columns, int32_t a,
    int32_t b, int32_t c)
{
    int32_t ab = CompareToDouble<columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns, a, b);
    int32_t ac = CompareToDouble<columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns, a, c);
    int32_t bc = CompareToDouble<columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns, b, c);
    return ((ab < 0) ? (bc < 0 ? b : ac < 0 ? c : a) : (bc > 0 ? b : ac > 0 ? c : a));
}

template <bool columnsNullFlag, int32_t sortAscendings>
int32_t Median3VarChar(const int32_t sortNullFirsts, uint64_t *valueAddresses, BaseVector **columns, int32_t a,
    int32_t b, int32_t c)
{
    int32_t ab = CompareToVarChar<columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns, a, b);
    int32_t ac = CompareToVarChar<columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns, a, c);
    int32_t bc = CompareToVarChar<columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns, b, c);
    return (ab < 0 ? (bc < 0 ? b : ac < 0 ? c : a) : (bc > 0 ? b : ac > 0 ? c : a));
}

template <bool columnsNullFlag, int32_t sortAscendings>
int32_t Median3Dec128(const int32_t sortNullFirsts, uint64_t *valueAddresses, BaseVector **columns, int32_t a,
    int32_t b, int32_t c)
{
    int32_t ab = CompareToDec128<columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns, a, b);
    int32_t ac = CompareToDec128<columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns, a, c);
    int32_t bc = CompareToDec128<columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns, b, c);
    return ((ab < 0) ? (bc < 0 ? b : ac < 0 ? c : a) : (bc > 0 ? b : ac > 0 ? c : a));
}

template <typename V, bool columnsNullFlag, int32_t sortAscendings>
void LeftAdvance(const int32_t sortNullFirsts, uint64_t *valueAddresses, BaseVector **columns, int32_t c, int32_t &m,
    int32_t &a, int32_t &b)
{
    int32_t comparison = 0;
    while (b <= c && ((comparison = CompareTo<V, columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses,
        columns, b, m)) <= 0)) {
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

template <typename V, bool columnsNullFlag, int32_t sortAscendings>
void RightAdvance(const int32_t sortNullFirsts, uint64_t *valueAddresses, BaseVector **columns, int32_t b, int32_t &m,
    int32_t &c, int32_t &d)
{
    int32_t comparison = 0;
    while (c >= b && ((comparison = CompareTo<V, columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses,
        columns, c, m)) >= 0)) {
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

template <typename V, bool columnsNullFlag, int32_t sortAscendings>
int32_t GetMedianPosition(const int32_t sortNullFirsts, uint64_t *valueAddresses, BaseVector **columns, int32_t from,
    int32_t to, int32_t len)
{
    int32_t m = from + len / QUICK_SORT_MIDDLE;
    if (len > QUICK_SORT_SMALL_LEN) {
        int32_t l = from;
        int32_t n = to - 1;
        if (len > QUICK_SORT_BIG_LEN) {
            int32_t s = len / QUICK_SORT_STEP_SIZE;
            l = Median3<V, columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns, l, l + s,
                l + QUICK_SORT_MIDDLE * s);
            m = Median3<V, columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns, m - s, m, m + s);
            n = Median3<V, columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns,
                n - QUICK_SORT_MIDDLE * s, n - s, n);
        }
        m = Median3<V, columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns, l, m, n);
    }
    return m;
}

template <typename V, bool columnsNullFlag, int32_t sortAscendings>
void QuickSortColumn(const int32_t sortNullFirsts, uint64_t *valueAddresses, BaseVector **columns, int32_t from,
    int32_t to)
{
    int32_t len = to - from;
    if (len < QUICK_SORT_SMALL_LEN) {
        QuickSortColumnSmall<V, columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns, from, to);
        return;
    }

    int32_t m =
        GetMedianPosition<V, columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns, from, to, len);

    int32_t a = from;
    int32_t b = a;
    int32_t c = to - 1;
    int32_t d = c;
    while (true) {
        LeftAdvance<V, columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns, c, m, a, b);
        RightAdvance<V, columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns, b, m, c, d);
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
        QuickSortColumn<V, columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns, from, from + s);
    }
    if ((s = d - c) > 1) {
        QuickSortColumn<V, columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns, n - s, n);
    }
}

template <bool columnsNullFlag, int32_t sortAscendings>
void LeftAdvanceDouble(const int32_t sortNullFirsts, uint64_t *valueAddresses, BaseVector **columns, int32_t c,
    int32_t &m, int32_t &a, int32_t &b)
{
    int32_t comparison = 0;
    while (b <= c && ((comparison = CompareToDouble<columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses,
        columns, b, m)) <= 0)) {
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

template <bool columnsNullFlag, int32_t sortAscendings>
void RightAdvanceDouble(const int32_t sortNullFirsts, uint64_t *valueAddresses, BaseVector **columns, int32_t b,
    int32_t &m, int32_t &c, int32_t &d)
{
    int32_t comparison = 0;
    while (c >= b && ((comparison = CompareToDouble<columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses,
        columns, c, m)) >= 0)) {
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

template <bool columnsNullFlag, int32_t sortAscendings>
int32_t GetMedianPositionDouble(const int32_t sortNullFirsts, uint64_t *valueAddresses, BaseVector **columns,
    int32_t from, int32_t to, int32_t len)
{
    int32_t m = from + len / QUICK_SORT_MIDDLE;
    if (len > QUICK_SORT_SMALL_LEN) {
        int32_t l = from;
        int32_t n = to - 1;
        if (len > QUICK_SORT_BIG_LEN) {
            int32_t s = len / QUICK_SORT_STEP_SIZE;
            l = Median3Double<columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns, l, l + s,
                l + QUICK_SORT_MIDDLE * s);
            m = Median3Double<columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns, m - s, m,
                m + s);
            n = Median3Double<columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns,
                n - QUICK_SORT_MIDDLE * s, n - s, n);
        }
        m = Median3Double<columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns, l, m, n);
    }
    return m;
}

template <bool columnsNullFlag, int32_t sortAscendings>
void QuickSortColumnDouble(const int32_t sortNullFirsts, uint64_t *valueAddresses, BaseVector **columns, int32_t from,
    int32_t to)
{
    int32_t len = to - from;
    if (len < QUICK_SORT_SMALL_LEN) {
        QuickSortColumnDoubleSmall<columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns, from, to);
        return;
    }

    int32_t m = GetMedianPositionDouble<columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns, from,
        to, len);

    int32_t a = from;
    int32_t b = a;
    int32_t c = to - 1;
    int32_t d = c;
    while (true) {
        LeftAdvanceDouble<columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns, c, m, a, b);
        RightAdvanceDouble<columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns, b, m, c, d);
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
        QuickSortColumnDouble<columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns, from, from + s);
    }
    if ((s = d - c) > 1) {
        QuickSortColumnDouble<columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns, n - s, n);
    }
}

template <bool columnsNullFlag, int32_t sortAscendings>
void LeftAdvanceVarChar(const int32_t sortNullFirsts, uint64_t *valueAddresses, BaseVector **columns, int32_t c,
    int32_t &m, int32_t &a, int32_t &b)
{
    int32_t comparison = 0;
    while (b <= c && ((comparison = CompareToVarChar<columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses,
        columns, b, m)) <= 0)) {
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

template <bool columnsNullFlag, int32_t sortAscendings>
void RightAdvanceVarChar(const int32_t sortNullFirsts, uint64_t *valueAddresses, BaseVector **columns, int32_t b,
    int32_t &m, int32_t &c, int32_t &d)
{
    int32_t comparison = 0;
    while (c >= b && ((comparison = CompareToVarChar<columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses,
        columns, c, m)) >= 0)) {
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

template <bool columnsNullFlag, int32_t sortAscendings>
int32_t GetMedianPositionVarChar(const int32_t sortNullFirsts, uint64_t *valueAddresses, BaseVector **columns,
    int32_t from, int32_t to, int32_t len)
{
    int32_t m = from + len / QUICK_SORT_MIDDLE;
    if (len > QUICK_SORT_SMALL_LEN) {
        int32_t l = from;
        int32_t n = to - 1;
        if (len > QUICK_SORT_BIG_LEN) {
            int32_t s = len / QUICK_SORT_STEP_SIZE;
            l = Median3VarChar<columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns, l, l + s,
                l + QUICK_SORT_MIDDLE * s);
            m = Median3VarChar<columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns, m - s, m,
                m + s);
            n = Median3VarChar<columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns,
                n - QUICK_SORT_MIDDLE * s, n - s, n);
        }
        m = Median3VarChar<columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns, l, m, n);
    }
    return m;
}

template <bool columnsNullFlag, int32_t sortAscendings>
void QuickSortColumnVarChar(const int32_t sortNullFirsts, uint64_t *valueAddresses, BaseVector **columns, int32_t from,
    int32_t to)
{
    int32_t len = to - from;
    if (len < QUICK_SORT_SMALL_LEN) {
        QuickSortColumnVarCharSmall<columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns, from, to);
        return;
    }

    int32_t m = GetMedianPositionVarChar<columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns, from,
        to, len);

    int32_t a = from;
    int32_t b = a;
    int32_t c = to - 1;
    int32_t d = c;
    while (true) {
        LeftAdvanceVarChar<columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns, c, m, a, b);
        RightAdvanceVarChar<columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns, b, m, c, d);
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
        QuickSortColumnVarChar<columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns, from,
            from + s);
    }
    if ((s = d - c) > 1) {
        QuickSortColumnVarChar<columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns, n - s, n);
    }
}

template <bool columnsNullFlag, int32_t sortAscendings>
void LeftAdvanceDec128(const int32_t sortNullFirsts, uint64_t *valueAddresses, BaseVector **columns, int32_t c,
    int32_t &m, int32_t &a, int32_t &b)
{
    int32_t comparison = 0;
    while (b <= c && ((comparison = CompareToDec128<columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses,
        columns, b, m)) <= 0)) {
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

template <bool columnsNullFlag, int32_t sortAscendings>
void RightAdvanceDec128(const int32_t sortNullFirsts, uint64_t *valueAddresses, BaseVector **columns, int32_t b,
    int32_t &m, int32_t &c, int32_t &d)
{
    int32_t comparison = 0;
    while (c >= b && ((comparison = CompareToDec128<columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses,
        columns, c, m)) >= 0)) {
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

template <bool columnsNullFlag, int32_t sortAscendings>
int32_t GetMedianPositionDec128(const int32_t sortNullFirsts, uint64_t *valueAddresses, BaseVector **columns,
    int32_t from, int32_t to, int32_t len)
{
    int32_t m = from + len / QUICK_SORT_MIDDLE;
    if (len > QUICK_SORT_SMALL_LEN) {
        int32_t l = from;
        int32_t n = to - 1;
        if (len > QUICK_SORT_BIG_LEN) {
            int32_t s = len / QUICK_SORT_STEP_SIZE;
            l = Median3Dec128<columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns, l, l + s,
                l + QUICK_SORT_MIDDLE * s);
            m = Median3Dec128<columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns, m - s, m,
                m + s);
            n = Median3Dec128<columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns,
                n - QUICK_SORT_MIDDLE * s, n - s, n);
        }
        m = Median3Dec128<columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns, l, m, n);
    }
    return m;
}

template <bool columnsNullFlag, int32_t sortAscendings>
void QuickSortColumnDec128(const int32_t sortNullFirsts, uint64_t *valueAddresses, BaseVector **columns, int32_t from,
    int32_t to)
{
    int32_t len = to - from;
    if (len < QUICK_SORT_SMALL_LEN) {
        QuickSortColumnDec128Small<columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns, from, to);
        return;
    }
    int32_t m = GetMedianPositionDec128<columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns, from,
        to, len);
    int32_t a = from;
    int32_t b = a;
    int32_t c = to - 1;
    int32_t d = c;
    while (true) {
        LeftAdvanceDec128<columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns, c, m, a, b);
        RightAdvanceDec128<columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns, b, m, c, d);
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
        QuickSortColumnDec128<columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns, from, from + s);
    }
    if ((s = d - c) > 1) {
        QuickSortColumnDec128<columnsNullFlag, sortAscendings>(sortNullFirsts, valueAddresses, columns, n - s, n);
    }
}

std::vector<std::tuple<int32_t, int32_t>> GetRanges(uint64_t *valueAddresses, BaseVector **columns, int32_t from,
    int32_t to, OperatorUtil::CompareFunc compareFunc)
{
    std::vector<std::tuple<int32_t, int32_t>> ranges;
    uint64_t valueAddress = valueAddresses[from];
    uint32_t columnIndex = DecodeSliceIndex(valueAddress);
    uint32_t columnPosition = DecodePosition(valueAddress);
    BaseVector *column = columns[columnIndex];

    bool currentIsNull = column->IsNull(columnPosition);
    bool valueIsNull = false;
    BaseVector *currentColumn = nullptr;
    int32_t currentColumnPosition = -1;
    if (!currentIsNull) {
        currentColumn = column;
        currentColumnPosition = columnPosition;
    }
    int32_t start = from;
    for (int32_t i = from + 1; i < to; ++i) {
        valueAddress = valueAddresses[i];
        columnIndex = DecodeSliceIndex(valueAddress);
        columnPosition = DecodePosition(valueAddress);
        column = columns[columnIndex];

        valueIsNull = column->IsNull(columnPosition);
        if (currentIsNull && valueIsNull) {
            continue;
        } else if (currentIsNull == valueIsNull) {
            // we still have the same value
            if (compareFunc(currentColumn, currentColumnPosition, column, columnPosition) == 0) {
                continue;
            }
        }
        if (valueIsNull) {
            currentIsNull = true;
        } else {
            currentIsNull = false;
            currentColumn = column;
            currentColumnPosition = columnPosition;
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

template <typename T>
std::vector<std::tuple<int32_t, int32_t>> GetRanges(uint64_t *valueAddresses, BaseVector **columns, int32_t from,
    int32_t to)
{
    return GetRanges(valueAddresses, columns, from, to, OperatorUtil::CompareTemplate<T>);
}

std::vector<std::tuple<int32_t, int32_t>> GetRangesDouble(uint64_t *valueAddresses, BaseVector **columns, int32_t from,
    int32_t to)
{
    return GetRanges(valueAddresses, columns, from, to, OperatorUtil::CompareDouble);
}

std::vector<std::tuple<int32_t, int32_t>> GetRangesVarChar(uint64_t *valueAddresses, BaseVector **columns, int32_t from,
    int32_t to)
{
    return GetRanges(valueAddresses, columns, from, to, OperatorUtil::CompareVarchar);
}

std::vector<std::tuple<int32_t, int32_t>> GetRangesDec128(uint64_t *valueAddresses, BaseVector **columns, int32_t from,
    int32_t to)
{
    return GetRanges(valueAddresses, columns, from, to, OperatorUtil::CompareDecimal128);
}

template <typename T>
void ColumnarSort(const int32_t *sortCols, const int32_t *sortColTypes, const int32_t *sortAscendings,
    const int32_t *sortNullFirsts, int32_t sortColCount, uint64_t *valueAddresses, BaseVector ***columns,
    const std::vector<bool> &mayHaveNulls, int32_t from, int32_t to, int32_t currentCol)
{
    // sort on specific range on specific column
    if (mayHaveNulls[currentCol]) {
        if (sortAscendings[currentCol] == 0) {
            QuickSortColumn<T, true, 0>(sortNullFirsts[currentCol], valueAddresses, columns[sortCols[currentCol]], from,
                to);
        } else {
            QuickSortColumn<T, true, 1>(sortNullFirsts[currentCol], valueAddresses, columns[sortCols[currentCol]], from,
                to);
        }
    } else {
        if (sortAscendings[currentCol] == 0) {
            QuickSortColumn<T, false, 0>(sortNullFirsts[currentCol], valueAddresses, columns[sortCols[currentCol]],
                from, to);
        } else {
            QuickSortColumn<T, false, 1>(sortNullFirsts[currentCol], valueAddresses, columns[sortCols[currentCol]],
                from, to);
        }
    }
    // are we the last column ?
    if (currentCol == sortColCount - 1) {
        return;
    }
    // get the duplicate range for sub sorting
    std::vector<std::tuple<int32_t, int32_t>> ranges =
        GetRanges<T>(valueAddresses, columns[sortCols[currentCol]], from, to);
    // iterate over each range
    for (std::tuple<int32_t, int32_t> t : ranges) {
        ColumnarSort(sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, valueAddresses, columns,
            mayHaveNulls, std::get<0>(t), std::get<1>(t), currentCol + 1);
    }
}


void ColumnarSortDouble(const int32_t *sortCols, const int32_t *sortColTypes, const int32_t *sortAscendings,
    const int32_t *sortNullFirsts, int32_t sortColCount, uint64_t *valueAddresses, BaseVector ***columns,
    const std::vector<bool> &mayHaveNulls, int32_t from, int32_t to, int32_t currentCol)
{
    // sort on specific range on specific column
    if (mayHaveNulls[currentCol]) {
        if (sortAscendings[currentCol] == 0) {
            QuickSortColumnDouble<true, 0>(sortNullFirsts[currentCol], valueAddresses, columns[sortCols[currentCol]],
                from, to);
        } else {
            QuickSortColumnDouble<true, 1>(sortNullFirsts[currentCol], valueAddresses, columns[sortCols[currentCol]],
                from, to);
        }
    } else {
        if (sortAscendings[currentCol] == 0) {
            QuickSortColumnDouble<false, 0>(sortNullFirsts[currentCol], valueAddresses, columns[sortCols[currentCol]],
                from, to);
        } else {
            QuickSortColumnDouble<false, 1>(sortNullFirsts[currentCol], valueAddresses, columns[sortCols[currentCol]],
                from, to);
        }
    }
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
            mayHaveNulls, std::get<0>(t), std::get<1>(t), currentCol + 1);
    }
}

void ColumnarSortVarChar(const int32_t *sortCols, const int32_t *sortColTypes, const int32_t *sortAscendings,
    const int32_t *sortNullFirsts, int32_t sortColCount, uint64_t *valueAddresses, BaseVector ***columns,
    const std::vector<bool> &mayHaveNulls, int32_t from, int32_t to, int32_t currentCol)
{
    // sort on specific range on specific column
    if (mayHaveNulls[currentCol]) {
        if (sortAscendings[currentCol] == 0) {
            QuickSortColumnVarChar<true, 0>(sortNullFirsts[currentCol], valueAddresses, columns[sortCols[currentCol]],
                from, to);
        } else {
            QuickSortColumnVarChar<true, 1>(sortNullFirsts[currentCol], valueAddresses, columns[sortCols[currentCol]],
                from, to);
        }
    } else {
        if (sortAscendings[currentCol] == 0) {
            QuickSortColumnVarChar<false, 0>(sortNullFirsts[currentCol], valueAddresses, columns[sortCols[currentCol]],
                from, to);
        } else {
            QuickSortColumnVarChar<false, 1>(sortNullFirsts[currentCol], valueAddresses, columns[sortCols[currentCol]],
                from, to);
        }
    }
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
            mayHaveNulls, std::get<0>(t), std::get<1>(t), currentCol + 1);
    }
}

void ColumnarSortDec128(const int32_t *sortCols, const int32_t *sortColTypes, const int32_t *sortAscendings,
    const int32_t *sortNullFirsts, int32_t sortColCount, uint64_t *valueAddresses, BaseVector ***columns,
    const std::vector<bool> &mayHaveNulls, int32_t from, int32_t to, int32_t currentCol)
{
    // sort on specific range on specific column
    if (mayHaveNulls[currentCol]) {
        if (sortAscendings[currentCol] == 0) {
            QuickSortColumnDec128<true, 0>(sortNullFirsts[currentCol], valueAddresses, columns[sortCols[currentCol]],
                from, to);
        } else {
            QuickSortColumnDec128<true, 1>(sortNullFirsts[currentCol], valueAddresses, columns[sortCols[currentCol]],
                from, to);
        }
    } else {
        if (sortAscendings[currentCol] == 0) {
            QuickSortColumnDec128<false, 0>(sortNullFirsts[currentCol], valueAddresses, columns[sortCols[currentCol]],
                from, to);
        } else {
            QuickSortColumnDec128<false, 1>(sortNullFirsts[currentCol], valueAddresses, columns[sortCols[currentCol]],
                from, to);
        }
    }
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
            mayHaveNulls, std::get<0>(t), std::get<1>(t), currentCol + 1);
    }
}

void ColumnarSort(const int32_t *sortCols, const int32_t *sortColTypes, const int32_t *sortAscendings,
    const int32_t *sortNullFirsts, int32_t sortColCount, uint64_t *valueAddresses, BaseVector ***columns,
    const std::vector<bool> &mayHaveNulls, int32_t from, int32_t to, int32_t currentCol)
{
    switch (sortColTypes[currentCol]) {
        case OMNI_INT:
        case OMNI_DATE32:
            ColumnarSort<int32_t>(sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, valueAddresses,
                columns, mayHaveNulls, from, to, currentCol);
            break;
        case OMNI_SHORT:
            ColumnarSort<int16_t>(sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, valueAddresses,
                columns, mayHaveNulls, from, to, currentCol);
            break;
        case OMNI_LONG:
        case OMNI_DECIMAL64:
            ColumnarSort<int64_t>(sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, valueAddresses,
                columns, mayHaveNulls, from, to, currentCol);
            break;
        case OMNI_DOUBLE:
            ColumnarSortDouble(sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, valueAddresses,
                columns, mayHaveNulls, from, to, currentCol);
            break;
        case OMNI_BOOLEAN:
            ColumnarSort<bool>(sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, valueAddresses,
                columns, mayHaveNulls, from, to, currentCol);
            break;
        case OMNI_VARCHAR:
        case OMNI_CHAR:
            ColumnarSortVarChar(sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, valueAddresses,
                columns, mayHaveNulls, from, to, currentCol);
            break;
        case OMNI_DECIMAL128:
            ColumnarSortDec128(sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, valueAddresses,
                columns, mayHaveNulls, from, to, currentCol);
            break;
        default:
            break;
    }
}

void PagesIndex::Sort(const int32_t *sortCols, const int32_t *sortColTypes, const int32_t *sortAscendings,
    const int32_t *sortNullFirsts, int32_t sortColCount, int32_t from, int32_t to) const
{
    ColumnarSort(sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, valueAddresses, columns,
        mayHaveNulls, from, to, 0);
}

void PagesIndex::GetOutput(int32_t *outputCols, int32_t outputColsCount, VectorBatch *outputVecBatch,
    const int32_t *sourceTypes, int32_t offset, int32_t length) const
{
    BaseVector ***inputVecBatches = this->columns;

    int32_t outputCol = 0;
    int colTypeId = 0;
    for (int32_t j = 0; j < outputColsCount; j++) {
        outputCol = outputCols[j];
        colTypeId = sourceTypes[outputCol];
        BaseVector **inputVecBatch = inputVecBatches[outputCol];

        switch (colTypeId) {
            case OMNI_INT:
            case OMNI_DATE32:
                outputVecBatch->Append(
                    ConstructVector<int32_t>(valueAddresses, offset, length, inputVecBatch).release());
                break;
            case OMNI_SHORT:
                outputVecBatch->Append(
                    ConstructVector<int16_t>(valueAddresses, offset, length, inputVecBatch).release());
                break;
            case OMNI_LONG:
            case OMNI_DECIMAL64:
                outputVecBatch->Append(
                    ConstructVector<int64_t>(valueAddresses, offset, length, inputVecBatch).release());
                break;
            case OMNI_DOUBLE:
                outputVecBatch->Append(
                    ConstructVector<double>(valueAddresses, offset, length, inputVecBatch).release());
                break;
            case OMNI_BOOLEAN:
                outputVecBatch->Append(ConstructVector<bool>(valueAddresses, offset, length, inputVecBatch).release());
                break;
            case OMNI_VARCHAR:
            case OMNI_CHAR: {
                outputVecBatch->Append(
                    ConstructVarcharVector(valueAddresses, offset, length, inputVecBatch).release());
                break;
            }
            case OMNI_DECIMAL128:
                outputVecBatch->Append(
                    ConstructVector<Decimal128>(valueAddresses, offset, length, inputVecBatch).release());
                break;
            default:
                break;
        }
    }
}

PagesIndex::~PagesIndex()
{
    Clear();
}

template <typename T>
void SetValue(BaseVector *inputVector, int32_t inputIndex, Vector<T> *outputVector, int32_t outputIndex)
{
    if (inputVector->IsNull(inputIndex)) {
        outputVector->SetNull(outputIndex);
        return;
    }

    if (inputVector->GetEncoding() == OMNI_DICTIONARY) {
        outputVector->SetValue(outputIndex,
            static_cast<Vector<DictionaryContainer<T>> *>(inputVector)->GetValue(inputIndex));
    } else {
        outputVector->SetValue(outputIndex, static_cast<Vector<T> *>(inputVector)->GetValue(inputIndex));
    }
}

template <typename T>
std::unique_ptr<BaseVector> ConstructVector(uint64_t *valueAddresses, int32_t offset, int32_t length,
    BaseVector **inputVecBatch)
{
    uint64_t valueAddress = 0;
    BaseVector *inputVector = nullptr;
    uint32_t pageIndex = 0;
    uint32_t position = 0;
    auto outputVector = std::make_unique<Vector<T>>(length);
    int32_t start = offset;
    int32_t end = offset + length;
    int32_t outputIndex = 0;
    for (int32_t i = start; i < end; i++) {
        valueAddress = valueAddresses[i];
        pageIndex = DecodeSliceIndex(valueAddress);
        position = DecodePosition(valueAddress);
        inputVector = inputVecBatch[pageIndex];
        SetValue(inputVector, static_cast<int32_t>(position), outputVector.get(), outputIndex++);
    }
    return std::move(outputVector);
}

void SetVarcharValue(BaseVector *inputVector, int32_t inputIndex,
    Vector<LargeStringContainer<std::string_view>> *outputVector, int32_t outputIndex)
{
    using VarcharVector = Vector<LargeStringContainer<std::string_view>>;
    using DictionaryVector = Vector<DictionaryContainer<std::string_view>>;
    if (inputVector->IsNull(inputIndex)) {
        outputVector->SetNull(outputIndex);
        return;
    }

    if (inputVector->GetEncoding() == OMNI_DICTIONARY) {
        auto value = reinterpret_cast<DictionaryVector *>(inputVector)->GetValue(inputIndex);
        outputVector->SetValue(outputIndex, value);
    } else {
        auto value = reinterpret_cast<VarcharVector *>(inputVector)->GetValue(inputIndex);
        outputVector->SetValue(outputIndex, value);
    }
}

std::unique_ptr<BaseVector> ConstructVarcharVector(uint64_t *valueAddresses, int32_t offset, int32_t length,
    BaseVector **inputVecBatch)
{
    uint64_t valueAddress = 0;
    BaseVector *inputVector = nullptr;
    uint32_t pageIndex = 0;
    uint32_t position = 0;

    auto outputVector = VectorHelper::CreateStringVector(length);
    int32_t start = offset;
    int32_t end = offset + length;
    int32_t outputIndex = 0;
    for (int32_t i = start; i < end; i++) {
        valueAddress = valueAddresses[i];
        pageIndex = DecodeSliceIndex(valueAddress);
        position = DecodePosition(valueAddress);
        inputVector = inputVecBatch[pageIndex];
        SetVarcharValue(inputVector, static_cast<int32_t>(position),
            reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(outputVector.get()), outputIndex++);
    }
    return std::move(outputVector);
}

void PagesIndex::GetSortedVecBatches(std::vector<int32_t> &outputCols, std::vector<VectorBatch *> &sortedVecBatches)
{
    int32_t outputColsCount = outputCols.size();
    int32_t maxRowCount = OperatorUtil::GetMaxRowCount(dataTypes.Get(), outputCols.data(), outputColsCount);
    int32_t vecBatchCount = OperatorUtil::GetVecBatchCount(positionCount, maxRowCount);
    sortedVecBatches.reserve(vecBatchCount);

    VectorBatch *result = nullptr;
    int32_t offset = 0;
    int32_t rowCount = 0;
    for (int32_t i = 0; i < vecBatchCount; i++) {
        rowCount = std::min(maxRowCount, static_cast<int32_t>(positionCount) - offset);
        result = new VectorBatch(rowCount);
        GetOutput(outputCols.data(), outputColsCount, result, dataTypes.GetIds(), offset, rowCount);
        offset += rowCount;
        sortedVecBatches.push_back(result);
    }
}

void PagesIndex::Clear()
{
    if (columns != nullptr) {
        for (uint32_t colIdx = 0; colIdx < typesCount; ++colIdx) {
            delete[] columns[colIdx];
        }
        delete[] columns;
        columns = nullptr;
    }

    if (valueAddresses != nullptr) {
        delete[] valueAddresses;
        valueAddresses = nullptr;
    }
    positionCount = 0;
    VectorHelper::FreeVecBatches(inputVecBatches);
    inputVecBatches.clear();
}
}
}
