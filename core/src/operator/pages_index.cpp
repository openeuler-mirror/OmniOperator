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
using VarcharVector = Vector<LargeStringContainer<std::string_view>>;
using DictionaryVarcharVector = Vector<DictionaryContainer<std::string_view>>;

const int32_t QUICK_SORT_SMALL_LEN = 16;
const int32_t QUICK_SORT_BIG_LEN = 64;
const int32_t QUICK_SORT_STEP_SIZE = 8;
const int32_t QUICK_SORT_MIDDLE = 2;

void ColumnarSort(const int32_t *sortCols, const int32_t *sortColTypes, const int32_t *sortAscendings,
    const int32_t *sortNullFirsts, int32_t sortColCount, BaseVector ***columns, std::vector<int64_t> &values,
    std::vector<uint32_t> &varcharLength, uint64_t *addresses, int32_t from, int32_t to, int32_t currentCol);

template <typename T> BaseVector *ConstructVector(uint64_t *vaStart, int32_t length, BaseVector **inputVecBatch);

BaseVector *ConstructVarcharVector(uint64_t *vaStart, int32_t length, BaseVector **inputVecBatch);

// function implements for class PagesIndex
PagesIndex::PagesIndex(const DataTypes &types)
    : dataTypes(types), typesCount(types.GetSize()), columns(nullptr), valueAddresses(nullptr), positionCount(0)
{}

void ALWAYS_INLINE Swap(std::vector<int64_t> &values, uint64_t *addresses, int32_t a, int32_t b)
{
    auto tmpValue = values[a];
    auto tmpAddr = addresses[a];
    values[a] = values[b];
    addresses[a] = addresses[b];
    values[b] = tmpValue;
    addresses[b] = tmpAddr;
}

__attribute__((noinline)) void VectorSwap(std::vector<int64_t> &values, uint64_t *addresses, int32_t from, int32_t l,
    int32_t s)
{
    int32_t i = 0;
    for (; i < s - 8; i += 8, from += 8, l += 8) {
        Swap(values, addresses, from, l);
        Swap(values, addresses, from + 1, l + 1);
        Swap(values, addresses, from + 2, l + 2);
        Swap(values, addresses, from + 3, l + 3);
        Swap(values, addresses, from + 4, l + 4);
        Swap(values, addresses, from + 5, l + 5);
        Swap(values, addresses, from + 6, l + 6);
        Swap(values, addresses, from + 7, l + 7);
    }
    for (; i < s; i++, from++, l++) {
        Swap(values, addresses, from, l);
    }
}

void PagesIndex::AddVecBatch(omniruntime::vec::VectorBatch *vecBatch)
{
    inputVecBatches.emplace_back(vecBatch);
    positionCount += vecBatch->GetRowCount();
}

void PagesIndex::Prepare()
{
    int32_t vecBatchCount = inputVecBatches.size();
    uint32_t columnCount = this->typesCount;

    this->valueAddresses = new uint64_t[this->positionCount];
    this->columns = new BaseVector **[columnCount];
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
        }
    }
}

template <int32_t sortAscendings>
static int32_t ALWAYS_INLINE NewOnlyCompareVarChar(int64_t leftValue, uint32_t leftLength, int64_t rightValue,
    uint32_t rightLength)
{
    if constexpr (sortAscendings == 1) {
        int32_t result = memcmp((void *)leftValue, (void *)rightValue, std::min(leftLength, rightLength));
        if (result != 0) {
            return result;
        }
        return leftLength - rightLength;
    } else {
        int32_t result = memcmp((void *)rightValue, (void *)leftValue, std::min(rightLength, leftLength));
        if (result != 0) {
            return result;
        }
        return rightLength - leftLength;
    }
}

template <int32_t sortAscendings>
void QuickSortColumnSmallVarChar(std::vector<int64_t> &values, std::vector<uint32_t> &varcharLength,
    uint64_t *addresses, int32_t from, int32_t to)
{
    for (int32_t i = from + 1; i < to; ++i) {
        int64_t iPtr = values[i];
        uint32_t iLength = varcharLength[i];
        uint64_t iAddr = addresses[i];
        int32_t j = i - 1;
        while (j >= from) {
            if (NewOnlyCompareVarChar<sortAscendings>(values[j], varcharLength[j], iPtr, iLength) <= 0) {
                break;
            }
            values[j + 1] = values[j];
            varcharLength[j + 1] = varcharLength[j];
            addresses[j + 1] = addresses[j];
            --j;
        }
        values[j + 1] = iPtr;
        varcharLength[j + 1] = iLength;
        addresses[j + 1] = iAddr;
    }
}

template <int32_t sortAscendings>
int32_t NewMedian3VarChar(std::vector<int64_t> &values, std::vector<uint32_t> &varcharLength, int32_t a, int32_t b,
    int32_t c)
{
    int32_t ab = NewOnlyCompareVarChar<sortAscendings>(values[a], varcharLength[a], values[b], varcharLength[b]);
    int32_t ac = NewOnlyCompareVarChar<sortAscendings>(values[a], varcharLength[a], values[c], varcharLength[c]);
    int32_t bc = NewOnlyCompareVarChar<sortAscendings>(values[b], varcharLength[b], values[c], varcharLength[c]);
    return ((ab < 0) ? (bc < 0 ? b : ac < 0 ? c : a) : (bc > 0 ? b : ac > 0 ? c : a));
}

template <int32_t sortAscendings>
int32_t NO_INLINE NewGetMedianPositionVarChar(std::vector<int64_t> &values, std::vector<uint32_t> &varcharLength,
    int32_t from, int32_t to, int32_t len)
{
    int32_t l = from;
    int32_t n = to - 1;
    int32_t m = from + len / QUICK_SORT_MIDDLE;
    if (len > QUICK_SORT_BIG_LEN) {
        int32_t s = len / QUICK_SORT_STEP_SIZE;
        l = NewMedian3VarChar<sortAscendings>(values, varcharLength, l, l + s, l + QUICK_SORT_MIDDLE * s);
        m = NewMedian3VarChar<sortAscendings>(values, varcharLength, m - s, m, m + s);
        n = NewMedian3VarChar<sortAscendings>(values, varcharLength, n - QUICK_SORT_MIDDLE * s, n - s, n);
    }
    return NewMedian3VarChar<sortAscendings>(values, varcharLength, l, m, n);
}

// bigger NCHUNK leads to ROB stall and wasted comparison due to b > c
static constexpr int32_t NCHUNK = 16;
// bigger NSTEP leads to reg spilling and hence increased inst count
static constexpr int32_t NSTEP = 12;
// Currently the comparison using subtraction includes 9 insts. ROB-size=96, 96/9=10
static constexpr int32_t NMAX_SIZE = NCHUNK * NSTEP;

template <int32_t sortAscendings>
int32_t ALWAYS_INLINE NewGetNextCompareLeftVarChar(int32_t *comparetmp, int32_t &k, int32_t &limit, int32_t b,
    int32_t c, std::vector<int64_t> &values, std::vector<uint32_t> &varcharLength, int64_t pivotValue,
    uint32_t pivotLength)
{
    if (LIKELY(k < limit)) {
        return comparetmp[k++];
    }
    k = 1;
    // use up
    limit = std::min(c - b + 1, NMAX_SIZE);

    int32_t i = b;
    int32_t j = 0;
    for (; j < limit - NSTEP; i += NSTEP, j += NSTEP) {
        comparetmp[j] = NewOnlyCompareVarChar<sortAscendings>(values[i], varcharLength[i], pivotValue, pivotLength);
        comparetmp[j + 1] =
            NewOnlyCompareVarChar<sortAscendings>(values[i + 1], varcharLength[i + 1], pivotValue, pivotLength);
        comparetmp[j + 2] =
            NewOnlyCompareVarChar<sortAscendings>(values[i + 2], varcharLength[i + 2], pivotValue, pivotLength);
        comparetmp[j + 3] =
            NewOnlyCompareVarChar<sortAscendings>(values[i + 3], varcharLength[i + 3], pivotValue, pivotLength);
        comparetmp[j + 4] =
            NewOnlyCompareVarChar<sortAscendings>(values[i + 4], varcharLength[i + 4], pivotValue, pivotLength);
        comparetmp[j + 5] =
            NewOnlyCompareVarChar<sortAscendings>(values[i + 5], varcharLength[i + 5], pivotValue, pivotLength);
        comparetmp[j + 6] =
            NewOnlyCompareVarChar<sortAscendings>(values[i + 6], varcharLength[i + 6], pivotValue, pivotLength);
        comparetmp[j + 7] =
            NewOnlyCompareVarChar<sortAscendings>(values[i + 7], varcharLength[i + 7], pivotValue, pivotLength);
        comparetmp[j + 8] =
            NewOnlyCompareVarChar<sortAscendings>(values[i + 8], varcharLength[i + 8], pivotValue, pivotLength);
        comparetmp[j + 9] =
            NewOnlyCompareVarChar<sortAscendings>(values[i + 9], varcharLength[i + 9], pivotValue, pivotLength);
        comparetmp[j + 10] =
            NewOnlyCompareVarChar<sortAscendings>(values[i + 10], varcharLength[i + 10], pivotValue, pivotLength);
        comparetmp[j + 11] =
            NewOnlyCompareVarChar<sortAscendings>(values[i + 11], varcharLength[i + 11], pivotValue, pivotLength);
    }
    for (; j < limit; ++i, ++j) {
        comparetmp[j] = NewOnlyCompareVarChar<sortAscendings>(values[i], varcharLength[i], pivotValue, pivotLength);
    }
    return comparetmp[0];
}

template <int32_t sortAscendings>
int32_t ALWAYS_INLINE NewGetNextCompareRightVarChar(int32_t *comparetmp, int32_t &k, int32_t &limit, int32_t b,
    int32_t c, std::vector<int64_t> &values, std::vector<uint32_t> &varcharLength, int64_t pivotValue,
    uint32_t pivotLength)
{
    if (LIKELY(k < limit)) {
        return comparetmp[k++];
    }
    k = 1;
    // use up
    limit = std::min(c - b + 1, NMAX_SIZE);

    int32_t i = c;
    int32_t j = 0;
    for (; j < limit - NSTEP; i -= NSTEP, j += NSTEP) {
        comparetmp[j] = NewOnlyCompareVarChar<sortAscendings>(values[i], varcharLength[i], pivotValue, pivotLength);
        comparetmp[j + 1] =
            NewOnlyCompareVarChar<sortAscendings>(values[i - 1], varcharLength[i - 1], pivotValue, pivotLength);
        comparetmp[j + 2] =
            NewOnlyCompareVarChar<sortAscendings>(values[i - 2], varcharLength[i - 2], pivotValue, pivotLength);
        comparetmp[j + 3] =
            NewOnlyCompareVarChar<sortAscendings>(values[i - 3], varcharLength[i - 3], pivotValue, pivotLength);
        comparetmp[j + 4] =
            NewOnlyCompareVarChar<sortAscendings>(values[i - 4], varcharLength[i - 4], pivotValue, pivotLength);
        comparetmp[j + 5] =
            NewOnlyCompareVarChar<sortAscendings>(values[i - 5], varcharLength[i - 5], pivotValue, pivotLength);
        comparetmp[j + 6] =
            NewOnlyCompareVarChar<sortAscendings>(values[i - 6], varcharLength[i - 6], pivotValue, pivotLength);
        comparetmp[j + 7] =
            NewOnlyCompareVarChar<sortAscendings>(values[i - 7], varcharLength[i - 7], pivotValue, pivotLength);
        comparetmp[j + 8] =
            NewOnlyCompareVarChar<sortAscendings>(values[i - 8], varcharLength[i - 8], pivotValue, pivotLength);
        comparetmp[j + 9] =
            NewOnlyCompareVarChar<sortAscendings>(values[i - 9], varcharLength[i - 9], pivotValue, pivotLength);
        comparetmp[j + 10] =
            NewOnlyCompareVarChar<sortAscendings>(values[i - 10], varcharLength[i - 10], pivotValue, pivotLength);
        comparetmp[j + 11] =
            NewOnlyCompareVarChar<sortAscendings>(values[i - 11], varcharLength[i - 11], pivotValue, pivotLength);
    }
    for (; j < limit; --i, ++j) {
        comparetmp[j] = NewOnlyCompareVarChar<sortAscendings>(values[i], varcharLength[i], pivotValue, pivotLength);
    }
    return comparetmp[0];
}

void ALWAYS_INLINE SwapVarchar(std::vector<int64_t> &values, std::vector<uint32_t> &varcharLength, uint64_t *addresses,
    int32_t a, int32_t b)
{
    auto tmpValuePtr = values[a];
    auto tmpLength = varcharLength[a];
    auto tmpAddress = addresses[a];
    values[a] = values[b];
    varcharLength[a] = varcharLength[b];
    addresses[a] = addresses[b];
    values[b] = tmpValuePtr;
    varcharLength[b] = tmpLength;
    addresses[b] = tmpAddress;
}

void NO_INLINE VectorSwapVarChar(std::vector<int64_t> &values, std::vector<uint32_t> &varcharLength,
    uint64_t *addresses, int32_t from, int32_t l, int32_t s)
{
    int32_t i = 0;
    for (; i < s - 8; i += 8, from += 8, l += 8) {
        SwapVarchar(values, varcharLength, addresses, from, l);
        SwapVarchar(values, varcharLength, addresses, from + 1, l + 1);
        SwapVarchar(values, varcharLength, addresses, from + 2, l + 2);
        SwapVarchar(values, varcharLength, addresses, from + 3, l + 3);
        SwapVarchar(values, varcharLength, addresses, from + 4, l + 4);
        SwapVarchar(values, varcharLength, addresses, from + 5, l + 5);
        SwapVarchar(values, varcharLength, addresses, from + 6, l + 6);
        SwapVarchar(values, varcharLength, addresses, from + 7, l + 7);
    }
    for (; i < s; i++, from++, l++) {
        SwapVarchar(values, varcharLength, addresses, from, l);
    }
}

template <int32_t sortAscendings>
void QuickSortColumnInternalVarchar(std::vector<int64_t> &values, std::vector<uint32_t> &varcharLength,
    uint64_t *addresses, int32_t from, int32_t to, int32_t *comparetmp)
{
    int32_t len = to - from;
    if (len <= QUICK_SORT_SMALL_LEN) {
        QuickSortColumnSmallVarChar<sortAscendings>(values, varcharLength, addresses, from, to);
        return;
    }

    int32_t m = NewGetMedianPositionVarChar<sortAscendings>(values, varcharLength, from, to, len);

    int64_t pivotValue = values[m];
    uint32_t pivotLength = varcharLength[m];

    int32_t a = from;
    int32_t b = a;
    int32_t c = to - 1;
    int32_t d = c;

    int32_t bk = 0;
    int32_t blim = 0;
    int32_t ck = 0;
    int32_t clim = 0;
    int32_t *leftComparetmp = comparetmp;
    int32_t *rightComparetmp = comparetmp + NMAX_SIZE;
    while (true) {
        int32_t comparison;
        while (b <= c && (comparison = NewGetNextCompareLeftVarChar<sortAscendings>(leftComparetmp, bk, blim, b, c,
            values, varcharLength, pivotValue, pivotLength)) <= 0) {
            if (UNLIKELY(comparison == 0)) {
                SwapVarchar(values, varcharLength, addresses, a++, b);
            }
            b++;
        }
        while (c >= b && (comparison = NewGetNextCompareRightVarChar<sortAscendings>(rightComparetmp, ck, clim, b, c,
            values, varcharLength, pivotValue, pivotLength)) >= 0) {
            if (UNLIKELY(comparison == 0)) {
                SwapVarchar(values, varcharLength, addresses, c, d--);
            }
            c--;
        }
        if (b > c) {
            break;
        }
        SwapVarchar(values, varcharLength, addresses, b++, c--);
    }

    // Swap partition elements back to middle
    int32_t s;
    int32_t n = to;
    s = std::min(a - from, b - a);
    VectorSwapVarChar(values, varcharLength, addresses, from, b - s, s);
    s = std::min(d - c, n - d - 1);
    VectorSwapVarChar(values, varcharLength, addresses, b, n - s, s);

    // Recursively sort non-partition-elements
    if ((s = b - a) > 1) {
        QuickSortColumnInternalVarchar<sortAscendings>(values, varcharLength, addresses, from, from + s, comparetmp);
    }
    if ((s = d - c) > 1) {
        QuickSortColumnInternalVarchar<sortAscendings>(values, varcharLength, addresses, n - s, n, comparetmp);
    }
}

template <int32_t sortAscendings>
void QuickSortColumnVarChar(std::vector<int64_t> &values, std::vector<uint32_t> &varcharLength, uint64_t *addresses,
    int32_t from, int32_t to)
{
    int32_t compareResult[NMAX_SIZE + NMAX_SIZE];
    QuickSortColumnInternalVarchar<sortAscendings>(values, varcharLength, addresses, from, to, compareResult);
}

template <DataTypeId D> static ALWAYS_INLINE typename NativeType<D>::type NewGetValue(int64_t valuePtr)
{
    return *reinterpret_cast<typename NativeType<D>::type *>(valuePtr);
}

template <DataTypeId D>
static int8_t ALWAYS_INLINE NewOnlyCompareAscending(typename NativeType<D>::type &left,
    typename NativeType<D>::type &right)
{
    if constexpr (D == OMNI_INT || D == OMNI_DATE32 || D == OMNI_SHORT || D == OMNI_LONG || D == OMNI_DECIMAL64 ||
        D == OMNI_BOOLEAN) {
        return left > right ?
            OperatorUtil::COMPARE_STATUS_GREATER_THAN :
            left < right ? OperatorUtil::COMPARE_STATUS_LESS_THAN : OperatorUtil::COMPARE_STATUS_EQUAL;
    } else if constexpr (D == OMNI_DECIMAL128) {
        return (left == right) ? omniruntime::op::OperatorUtil::COMPARE_STATUS_EQUAL : left.Compare(right);
    } else if constexpr (D == OMNI_DOUBLE) {
        double diff = left - right;
        if (std::abs(diff) < __DBL_EPSILON__) {
            return omniruntime::op::OperatorUtil::COMPARE_STATUS_EQUAL;
        }
        return left < right ? omniruntime::op::OperatorUtil::COMPARE_STATUS_LESS_THAN :
                              omniruntime::op::OperatorUtil::COMPARE_STATUS_GREATER_THAN;
    } else {
        // return COMPARE_STATUS_EQUAL for data types that are not supported currently
        return OperatorUtil::COMPARE_STATUS_EQUAL;
    }
}

template <DataTypeId D, int32_t sortAscendings>
static int8_t ALWAYS_INLINE NewOnlyCompare(typename NativeType<D>::type &left, typename NativeType<D>::type &right)
{
    if constexpr (sortAscendings == 1) {
        return NewOnlyCompareAscending<D>(left, right);
    } else {
        return NewOnlyCompareAscending<D>(right, left);
    }
}

template <DataTypeId D, int32_t sortAscendings>
void QuickSortColumnSmall(std::vector<int64_t> &values, uint64_t *addresses, int32_t from, int32_t to)
{
    for (int32_t i = from + 1; i < to; ++i) {
        int64_t iPtr = values[i];
        auto iValue = NewGetValue<D>(iPtr);
        auto iAddr = addresses[i];
        int32_t j = i - 1;
        while (j >= from) {
            auto jValue = NewGetValue<D>(values[j]);
            if (NewOnlyCompare<D, sortAscendings>(jValue, iValue) <= 0) {
                break;
            }
            values[j + 1] = values[j];
            addresses[j + 1] = addresses[j];
            --j;
        }
        values[j + 1] = iPtr;
        addresses[j + 1] = iAddr;
    }
}

template <DataTypeId D, int32_t sortAscendings>
int32_t NewMedian3(std::vector<int64_t> &values, int32_t a, int32_t b, int32_t c)
{
    auto va = NewGetValue<D>(values[a]);
    auto vb = NewGetValue<D>(values[b]);
    auto vc = NewGetValue<D>(values[c]);
    int8_t ab = NewOnlyCompare<D, sortAscendings>(va, vb);
    int8_t ac = NewOnlyCompare<D, sortAscendings>(va, vc);
    int8_t bc = NewOnlyCompare<D, sortAscendings>(vb, vc);
    return ((ab < 0) ? (bc < 0 ? b : ac < 0 ? c : a) : (bc > 0 ? b : ac > 0 ? c : a));
}

template <DataTypeId D, int32_t sortAscendings>
int32_t NO_INLINE NewGetMedianPosition(std::vector<int64_t> &values, int32_t from, int32_t to, int32_t len)
{
    int32_t l = from;
    int32_t n = to - 1;
    int32_t m = from + len / QUICK_SORT_MIDDLE;
    if (len > QUICK_SORT_BIG_LEN) {
        int32_t s = len / QUICK_SORT_STEP_SIZE;
        l = NewMedian3<D, sortAscendings>(values, l, l + s, l + QUICK_SORT_MIDDLE * s);
        m = NewMedian3<D, sortAscendings>(values, m - s, m, m + s);
        n = NewMedian3<D, sortAscendings>(values, n - QUICK_SORT_MIDDLE * s, n - s, n);
    }
    return NewMedian3<D, sortAscendings>(values, l, m, n);
}

template <DataTypeId D, int32_t sortAscendings>
int8_t ALWAYS_INLINE NewGetNextCompareLeft(int8_t *comparetmp, int32_t &k, int32_t &limit, int32_t b, int32_t c,
    std::vector<int64_t> &values, typename NativeType<D>::type &pivotValue)
{
    if (k < limit) {
        return comparetmp[k++];
    }
    k = 1;
    // use up
    limit = std::min(c - b + 1, NMAX_SIZE);

    int32_t i = b;
    int32_t j = 0;
    for (; j < limit - NSTEP; i += NSTEP, j += NSTEP) {
        auto v0 = NewGetValue<D>(values[i]);
        auto v1 = NewGetValue<D>(values[i + 1]);
        auto v2 = NewGetValue<D>(values[i + 2]);
        auto v3 = NewGetValue<D>(values[i + 3]);
        comparetmp[j] = NewOnlyCompare<D, sortAscendings>(v0, pivotValue);
        comparetmp[j + 1] = NewOnlyCompare<D, sortAscendings>(v1, pivotValue);
        comparetmp[j + 2] = NewOnlyCompare<D, sortAscendings>(v2, pivotValue);
        comparetmp[j + 3] = NewOnlyCompare<D, sortAscendings>(v3, pivotValue);
        auto v4 = NewGetValue<D>(values[i + 4]);
        auto v5 = NewGetValue<D>(values[i + 5]);
        auto v6 = NewGetValue<D>(values[i + 6]);
        auto v7 = NewGetValue<D>(values[i + 7]);
        comparetmp[j + 4] = NewOnlyCompare<D, sortAscendings>(v4, pivotValue);
        comparetmp[j + 5] = NewOnlyCompare<D, sortAscendings>(v5, pivotValue);
        comparetmp[j + 6] = NewOnlyCompare<D, sortAscendings>(v6, pivotValue);
        comparetmp[j + 7] = NewOnlyCompare<D, sortAscendings>(v7, pivotValue);
        auto v8 = NewGetValue<D>(values[i + 8]);
        auto v9 = NewGetValue<D>(values[i + 9]);
        auto v10 = NewGetValue<D>(values[i + 10]);
        auto v11 = NewGetValue<D>(values[i + 11]);
        comparetmp[j + 8] = NewOnlyCompare<D, sortAscendings>(v8, pivotValue);
        comparetmp[j + 9] = NewOnlyCompare<D, sortAscendings>(v9, pivotValue);
        comparetmp[j + 10] = NewOnlyCompare<D, sortAscendings>(v10, pivotValue);
        comparetmp[j + 11] = NewOnlyCompare<D, sortAscendings>(v11, pivotValue);
    }
    for (; j < limit; ++i, ++j) {
        auto v = NewGetValue<D>(values[i]);
        comparetmp[j] = NewOnlyCompare<D, sortAscendings>(v, pivotValue);
    }
    return comparetmp[0];
}

template <DataTypeId D, int32_t sortAscendings>
inline int8_t NewGetNextCompareRight(int8_t *comparetmp, int32_t &k, int32_t &limit, int32_t b, int32_t c,
    std::vector<int64_t> &values, typename NativeType<D>::type &pivotValue)
{
    if (k < limit) {
        return comparetmp[k++];
    }
    k = 1;
    // use up
    limit = std::min(c - b + 1, NMAX_SIZE);

    int32_t i = c;
    int32_t j = 0;
    for (; j < limit - NSTEP; i -= NSTEP, j += NSTEP) {
        auto v0 = NewGetValue<D>(values[i]);
        auto v1 = NewGetValue<D>(values[i - 1]);
        auto v2 = NewGetValue<D>(values[i - 2]);
        auto v3 = NewGetValue<D>(values[i - 3]);
        comparetmp[j] = NewOnlyCompare<D, sortAscendings>(v0, pivotValue);
        comparetmp[j + 1] = NewOnlyCompare<D, sortAscendings>(v1, pivotValue);
        comparetmp[j + 2] = NewOnlyCompare<D, sortAscendings>(v2, pivotValue);
        comparetmp[j + 3] = NewOnlyCompare<D, sortAscendings>(v3, pivotValue);
        auto v4 = NewGetValue<D>(values[i - 4]);
        auto v5 = NewGetValue<D>(values[i - 5]);
        auto v6 = NewGetValue<D>(values[i - 6]);
        auto v7 = NewGetValue<D>(values[i - 7]);
        comparetmp[j + 4] = NewOnlyCompare<D, sortAscendings>(v4, pivotValue);
        comparetmp[j + 5] = NewOnlyCompare<D, sortAscendings>(v5, pivotValue);
        comparetmp[j + 6] = NewOnlyCompare<D, sortAscendings>(v6, pivotValue);
        comparetmp[j + 7] = NewOnlyCompare<D, sortAscendings>(v7, pivotValue);
        auto v8 = NewGetValue<D>(values[i - 8]);
        auto v9 = NewGetValue<D>(values[i - 9]);
        auto v10 = NewGetValue<D>(values[i - 10]);
        auto v11 = NewGetValue<D>(values[i - 11]);
        comparetmp[j + 8] = NewOnlyCompare<D, sortAscendings>(v8, pivotValue);
        comparetmp[j + 9] = NewOnlyCompare<D, sortAscendings>(v9, pivotValue);
        comparetmp[j + 10] = NewOnlyCompare<D, sortAscendings>(v10, pivotValue);
        comparetmp[j + 11] = NewOnlyCompare<D, sortAscendings>(v11, pivotValue);
    }
    for (; j < limit; --i, ++j) {
        auto v = NewGetValue<D>(values[i]);
        comparetmp[j] = NewOnlyCompare<D, sortAscendings>(v, pivotValue);
    }
    return comparetmp[0];
}

template <DataTypeId D, int32_t sortAscendings>
void QuickSortColumnInternal(std::vector<int64_t> &values, uint64_t *addresses, int32_t from, int32_t to,
    int8_t *comparetmp)
{
    int32_t len = to - from;
    if (len <= QUICK_SORT_SMALL_LEN) {
        QuickSortColumnSmall<D, sortAscendings>(values, addresses, from, to);
        return;
    }

    int32_t m = NewGetMedianPosition<D, sortAscendings>(values, from, to, len);

    auto pivotValue = NewGetValue<D>(values[m]);

    int32_t a = from;
    int32_t b = a;
    int32_t c = to - 1;
    int32_t d = c;

    int32_t bk = 0;
    int32_t blim = 0;
    int32_t ck = 0;
    int32_t clim = 0;
    int8_t *leftComparetmp = comparetmp;
    int8_t *rightComparetmp = comparetmp + NMAX_SIZE;
    while (true) {
        int8_t comparison;
        while (b <= c && (comparison = NewGetNextCompareLeft<D, sortAscendings>(leftComparetmp, bk, blim, b, c, values,
            pivotValue)) <= 0) {
            if (UNLIKELY(comparison == 0)) {
                Swap(values, addresses, a++, b);
            }
            b++;
        }
        while (c >= b && (comparison = NewGetNextCompareRight<D, sortAscendings>(rightComparetmp, ck, clim, b, c,
            values, pivotValue)) >= 0) {
            if (UNLIKELY(comparison == 0)) {
                Swap(values, addresses, c, d--);
            }
            c--;
        }
        if (b > c) {
            break;
        }
        Swap(values, addresses, b++, c--);
    }

    // Swap partition elements back to middle
    int32_t s;
    int32_t n = to;
    s = std::min(a - from, b - a);
    VectorSwap(values, addresses, from, b - s, s);
    s = std::min(d - c, n - d - 1);
    VectorSwap(values, addresses, b, n - s, s);

    // Recursively sort non-partition-elements
    if ((s = b - a) > 1) {
        QuickSortColumnInternal<D, sortAscendings>(values, addresses, from, from + s, comparetmp);
    }
    if ((s = d - c) > 1) {
        QuickSortColumnInternal<D, sortAscendings>(values, addresses, n - s, n, comparetmp);
    }
}

template <DataTypeId D, int32_t sortAscendings>
void QuickSortColumn(std::vector<int64_t> &values, uint64_t *addresses, int32_t from, int32_t to)
{
    int8_t comparetmp[NMAX_SIZE + NMAX_SIZE];
    QuickSortColumnInternal<D, sortAscendings>(values, addresses, from, to, comparetmp);
}

template <DataTypeId D>
void ColumnarSort(const int32_t *sortCols, const int32_t *sortColTypes, const int32_t *sortAscendings,
    const int32_t *sortNullFirsts, int32_t sortColCount, BaseVector ***columns, std::vector<int64_t> &values,
    std::vector<uint32_t> &varcharLength, uint64_t *addresses, int32_t from, int32_t to, int32_t currentCol)
{
    auto sortNullFirst = sortNullFirsts[currentCol];
    auto sortAscending = sortAscendings[currentCol];
    auto batchesOfCurrentColumn = columns[sortCols[currentCol]];
    int32_t nonNullFrom = from;
    int32_t nonNullTo = to; // [nonNullFrom, nonNullTo) mark the range of non-null values
    // we are going to sort one column, so we extract all the rows of the column between from and to
    int32_t i = from;
    while (i < nonNullTo) {
        uint64_t encodedIndex = addresses[i];
        uint32_t vecBatchIdx = DecodeSliceIndex(encodedIndex);
        uint32_t rowIdx = DecodePosition(encodedIndex);
        auto column = batchesOfCurrentColumn[vecBatchIdx];
        if constexpr (D == OMNI_VARCHAR || D == OMNI_CHAR) {
            if (UNLIKELY(column->IsNull(rowIdx))) {
                // first, put all nulls at the first or at the last according sortNullFirst
                if (sortNullFirst) {
                    SwapVarchar(values, varcharLength, addresses, i++, nonNullFrom++); // [0, nonNullFrom)
                } else { // we swap the last nonNull element back to i -- we need to CHECK the new i-th element again!
                    SwapVarchar(values, varcharLength, addresses, i, --nonNullTo); // [i, nonNullTo)
                }
            } else {
                std::string_view value;
                if (column->GetEncoding() == OMNI_DICTIONARY) {
                    value = static_cast<DictionaryVarcharVector *>(column)->GetValue(rowIdx);
                } else {
                    value = static_cast<VarcharVector *>(column)->GetValue(rowIdx);
                }
                values[i] = reinterpret_cast<long>(const_cast<char *>(value.data()));
                varcharLength[i] = value.length();
                ++i;
            }
        } else {
            if (UNLIKELY(column->IsNull(rowIdx))) {
                // first, put all nulls at the first or at the last according sortNullFirst
                if (sortNullFirst) {
                    Swap(values, addresses, i++, nonNullFrom++); // [0, nonNullFrom)
                } else { // we swap the last nonNull element back to i -- we need to CHECK the new i-th element again!
                    Swap(values, addresses, i, --nonNullTo); // [i, nonNullTo)
                }
            } else {
                // get the value raw addr
                using T = typename NativeType<D>::type;
                if (column->GetEncoding() == OMNI_DICTIONARY) {
                    using DictionaryFlatVector = vec::Vector<DictionaryContainer<T>>;
                    auto dictionaryVector = reinterpret_cast<DictionaryFlatVector *>(column);
                    T *valuePtr = unsafe::UnsafeDictionaryVector::GetDictionary(dictionaryVector);
                    int32_t originalRowIndex = unsafe::UnsafeDictionaryVector::GetIds(dictionaryVector)[rowIdx];
                    values[i] = reinterpret_cast<long>(valuePtr + originalRowIndex);
                } else {
                    using FlatVector = Vector<T>;
                    values[i] = reinterpret_cast<long>(
                        unsafe::UnsafeVector::GetRawValues(reinterpret_cast<FlatVector *>(column)) + rowIdx);
                }
                ++i;
            }
        }
    }

    if (nonNullFrom + 1 < nonNullTo) {
        // second, sort all non-null values
        if constexpr (D == OMNI_VARCHAR || D == OMNI_CHAR) {
            if (sortAscending == 0) {
                QuickSortColumnVarChar<0>(values, varcharLength, addresses, nonNullFrom, nonNullTo);
            } else {
                QuickSortColumnVarChar<1>(values, varcharLength, addresses, nonNullFrom, nonNullTo);
            }
        } else {
            if (sortAscending == 0) {
                QuickSortColumn<D, 0>(values, addresses, nonNullFrom, nonNullTo);
            } else {
                QuickSortColumn<D, 1>(values, addresses, nonNullFrom, nonNullTo);
            }
        }
    }

    if (currentCol == sortColCount - 1) {
        // currently the last column has been sorted.
        return;
    }

    // third, sort next column for the null range
    auto nextCol = currentCol + 1;
    if (nonNullFrom != from && from + 1 < nonNullFrom) {
        ColumnarSort(sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, columns, values,
            varcharLength, addresses, from, nonNullFrom, nextCol);
    } else if (nonNullTo != to && nonNullTo + 1 < to) {
        ColumnarSort(sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, columns, values,
            varcharLength, addresses, nonNullTo, to, nextCol);
    }

    if (nonNullFrom + 1 >= nonNullTo) {
        // if the non-null range has only one or zero element, the sorting is not required.
        return;
    }

    // fourth, divide ranges for non-null values first, and continue to sort the next column for each range
    if constexpr (D == OMNI_VARCHAR || D == OMNI_CHAR) {
        int64_t currentValue = values[nonNullFrom];
        uint32_t currentLength = varcharLength[nonNullFrom];
        int32_t start = nonNullFrom;
        for (int32_t i = nonNullFrom + 1; i < nonNullTo; ++i) {
            int64_t value = values[i];
            uint32_t length = varcharLength[i];
            if (NewOnlyCompareVarChar<0>(value, length, currentValue, currentLength) != 0) {
                currentValue = value;
                currentLength = length;
                if (start + 1 != i) { // sort next column when |equivalent class| > 1
                    ColumnarSort(sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, columns, values,
                        varcharLength, addresses, start, i, nextCol);
                }
                start = i;
            }
        }
        if (start + 1 != nonNullTo) {
            ColumnarSort(sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, columns, values,
                varcharLength, addresses, start, nonNullTo, nextCol);
        }
    } else {
        typename NativeType<D>::type currentValue = NewGetValue<D>(values[nonNullFrom]);
        int32_t start = nonNullFrom;
        for (int32_t i = nonNullFrom + 1; i < nonNullTo; ++i) {
            typename NativeType<D>::type value = NewGetValue<D>(values[i]);
            if (NewOnlyCompare<D, 0>(value, currentValue) != 0) {
                currentValue = value;
                if (start + 1 != i) { // sort next column when |equivalent class| > 1
                    ColumnarSort(sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, columns, values,
                        varcharLength, addresses, start, i, nextCol);
                }
                start = i;
            }
        }
        if (start + 1 != nonNullTo) {
            ColumnarSort(sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, columns, values,
                varcharLength, addresses, start, nonNullTo, nextCol);
        }
    }
}

void ColumnarSort(const int32_t *sortCols, const int32_t *sortColTypes, const int32_t *sortAscendings,
    const int32_t *sortNullFirsts, int32_t sortColCount, BaseVector ***columns, std::vector<int64_t> &values,
    std::vector<uint32_t> &varcharLength, uint64_t *addresses, int32_t from, int32_t to, int32_t currentCol)
{
    switch (sortColTypes[currentCol]) {
        case OMNI_INT:
        case OMNI_DATE32:
            ColumnarSort<OMNI_INT>(sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, columns,
                values, varcharLength, addresses, from, to, currentCol);
            break;
        case OMNI_SHORT:
            ColumnarSort<OMNI_SHORT>(sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, columns,
                values, varcharLength, addresses, from, to, currentCol);
            break;
        case OMNI_LONG:
        case OMNI_DECIMAL64:
            ColumnarSort<OMNI_LONG>(sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, columns,
                values, varcharLength, addresses, from, to, currentCol);
            break;
        case OMNI_DOUBLE:
            ColumnarSort<OMNI_DOUBLE>(sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, columns,
                values, varcharLength, addresses, from, to, currentCol);
            break;
        case OMNI_BOOLEAN:
            ColumnarSort<OMNI_BOOLEAN>(sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, columns,
                values, varcharLength, addresses, from, to, currentCol);
            break;
        case OMNI_VARCHAR:
        case OMNI_CHAR:
            ColumnarSort<OMNI_VARCHAR>(sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, columns,
                values, varcharLength, addresses, from, to, currentCol);
            break;
        case OMNI_DECIMAL128:
            ColumnarSort<OMNI_DECIMAL128>(sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, columns,
                values, varcharLength, addresses, from, to, currentCol);
            break;
        default:
            break;
    }
}

void PagesIndex::Sort(const int32_t *sortCols, const int32_t *sortColTypes, const int32_t *sortAscendings,
    const int32_t *sortNullFirsts, int32_t sortColCount, int32_t from, int32_t to)
{
    std::vector<int64_t> values(this->positionCount);
    std::vector<uint32_t> varcharLength;
    bool hasVarCharCol = false;
    for (int32_t i = 0; i < sortColCount; ++i) {
        if (sortColTypes[i] == OMNI_CHAR || sortColTypes[i] == OMNI_VARCHAR) {
            hasVarCharCol = true;
            break;
        }
    }
    if (hasVarCharCol) {
        varcharLength.resize(this->positionCount);
    }
    ColumnarSort(sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, columns, values, varcharLength,
        valueAddresses, from, to, 0);
}

void PagesIndex::GetOutput(int32_t *outputCols, int32_t outputColsCount, VectorBatch *outputVecBatch,
    const int32_t *sourceTypes, int32_t offset, int32_t length) const
{
    BaseVector ***inputVecBatches = this->columns;
    uint64_t *vaStart = valueAddresses + offset;

    for (int32_t j = 0; j < outputColsCount; j++) {
        int32_t outputCol = outputCols[j];
        int32_t colTypeId = sourceTypes[outputCol];
        BaseVector **inputVecBatch = inputVecBatches[outputCol];
        switch (colTypeId) {
            case OMNI_INT:
            case OMNI_DATE32:
                outputVecBatch->Append(ConstructVector<int32_t>(vaStart, length, inputVecBatch));
                break;
            case OMNI_SHORT:
                outputVecBatch->Append(ConstructVector<int16_t>(vaStart, length, inputVecBatch));
                break;
            case OMNI_LONG:
            case OMNI_DECIMAL64:
                outputVecBatch->Append(ConstructVector<int64_t>(vaStart, length, inputVecBatch));
                break;
            case OMNI_DOUBLE:
                outputVecBatch->Append(ConstructVector<double>(vaStart, length, inputVecBatch));
                break;
            case OMNI_BOOLEAN:
                outputVecBatch->Append(ConstructVector<bool>(vaStart, length, inputVecBatch));
                break;
            case OMNI_VARCHAR:
            case OMNI_CHAR: {
                outputVecBatch->Append(ConstructVarcharVector(vaStart, length, inputVecBatch));
                break;
            }
            case OMNI_DECIMAL128:
                outputVecBatch->Append(ConstructVector<Decimal128>(vaStart, length, inputVecBatch));
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
static ALWAYS_INLINE void SetValue(BaseVector *inputVector, int32_t inputIndex, BaseVector *outputVector,
    int32_t outputIndex)
{
    if (UNLIKELY(inputVector->IsNull(inputIndex))) {
        outputVector->SetNull(outputIndex);
        return;
    }
    T value;
    if (UNLIKELY(inputVector->GetEncoding() == OMNI_DICTIONARY)) {
        value = static_cast<Vector<DictionaryContainer<T>> *>(inputVector)->GetValue(inputIndex);
    } else {
        value = static_cast<Vector<T> *>(inputVector)->GetValue(inputIndex);
    }
    static_cast<Vector<T> *>(outputVector)->SetValue(outputIndex, value);
}

template <typename T>
NO_INLINE BaseVector *ConstructVector(uint64_t *vaStart, int32_t length, BaseVector **inputVecBatch)
{
    auto outputVector = new Vector<T>(length);
    int32_t outputIndex = 0;
    uint64_t *vaEnd = vaStart + length;
    while (vaStart < vaEnd) { // here unroll is almost useless due to the excessive checks in SetValue
        uint64_t valueAddress = *(vaStart++);
        uint32_t pageIndex = DecodeSliceIndex(valueAddress);
        auto inputVector = inputVecBatch[pageIndex];
        uint32_t position = DecodePosition(valueAddress);
        SetValue<T>(inputVector, static_cast<int32_t>(position), outputVector, outputIndex++);
    }
    return outputVector;
}

static ALWAYS_INLINE void SetVarcharValue(BaseVector *inputVector, int32_t inputIndex, BaseVector *outputVector,
    int32_t outputIndex)
{
    if (UNLIKELY(inputVector->IsNull(inputIndex))) {
        reinterpret_cast<VarcharVector *>(outputVector)->SetNull(outputIndex);
        return;
    }

    std::string_view value;
    if (UNLIKELY(inputVector->GetEncoding() == OMNI_DICTIONARY)) {
        value = static_cast<DictionaryVarcharVector *>(inputVector)->GetValue(inputIndex);
    } else {
        value = static_cast<VarcharVector *>(inputVector)->GetValue(inputIndex);
    }
    static_cast<VarcharVector *>(outputVector)->SetValue(outputIndex, value);
}

NO_INLINE BaseVector *ConstructVarcharVector(uint64_t *vaStart, int32_t length, BaseVector **inputVecBatch)
{
    auto *outputVector = new VarcharVector(length);
    int32_t outputIndex = 0;
    uint64_t *vaEnd = vaStart + length;
    while (vaStart < vaEnd) {
        uint64_t valueAddress = *(vaStart++);
        uint32_t pageIndex = DecodeSliceIndex(valueAddress);
        auto inputVector = inputVecBatch[pageIndex];
        uint32_t position = DecodePosition(valueAddress);
        SetVarcharValue(inputVector, static_cast<int32_t>(position), outputVector, outputIndex++);
    }
    return outputVector;
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
