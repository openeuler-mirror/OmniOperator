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
const int32_t QUICK_SORT_SMALL_LEN = 16;
const int32_t QUICK_SORT_BIG_LEN = 64;
const int32_t QUICK_SORT_STEP_SIZE = 8;
const int32_t QUICK_SORT_MIDDLE = 2;

void ColumnarSort(const int32_t *sortCols, const int32_t *sortColTypes, const int32_t *sortAscendings,
    const int32_t *sortNullFirsts, int32_t sortColCount, BaseVector ***columns,
    std::vector<std::pair<void*, uint64_t>> &valuePtrs, std::vector<uint32_t> &varcharLength, int32_t from, int32_t to, int32_t currentCol);

template <typename T>
std::unique_ptr<BaseVector> ConstructVector(uint64_t *vaStart, int32_t length, BaseVector **inputVecBatch);

std::unique_ptr<BaseVector> ConstructVarcharVector(uint64_t *vaStart, int32_t length, BaseVector **inputVecBatch);

// function implements for class PagesIndex
PagesIndex::PagesIndex(const DataTypes &types)
    : dataTypes(types),
      dataTypeIds(types.GetIds()),
      typesCount(types.GetSize()),
      columns(nullptr),
      valueAddresses(nullptr),
      positionCount(0)
{}

void ALWAYS_INLINE Swap(std::vector<std::pair<void*, uint64_t>> &valuePtrs, int32_t a, int32_t b)
{
    auto temp = valuePtrs[a];
    valuePtrs[a] = valuePtrs[b];
    valuePtrs[b] = temp;
}

__attribute__((noinline))
void VectorSwap(std::vector<std::pair<void*, uint64_t>> &valuePtrs, int32_t from, int32_t l, int32_t s)
{
    int i = 0;
    // for (; i < s-4; i+=4, from+=4, l+=4) {
    for (; i < s-8; i+=8, from+=8, l+=8) {
        Swap(valuePtrs, from, l);
        Swap(valuePtrs, from+1, l+1);
        Swap(valuePtrs, from+2, l+2);
        Swap(valuePtrs, from+3, l+3);
        Swap(valuePtrs, from+4, l+4);
        Swap(valuePtrs, from+5, l+5);
        Swap(valuePtrs, from+6, l+6);
        Swap(valuePtrs, from+7, l+7);
    }
    for (; i < s; i++, from++, l++) {
        Swap(valuePtrs, from, l);
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

//-----------------------------------------------------------------------------------------------------
// freaking varchar ...
//-----------------------------------------------------------------------------------------------------
template <int32_t sortAscendings>
static int32_t ALWAYS_INLINE NewOnlyCompareVarChar(void *leftValue, uint32_t leftLength, void *rightValue, uint32_t rightLength)
{
    if constexpr (sortAscendings == 1) {
        int32_t result = memcmp(leftValue, rightValue, std::min(leftLength, rightLength));
        if (result != 0) {
            return result;
        }
        return leftLength - rightLength;
    } else {
        int32_t result = memcmp(rightValue, leftValue, std::min(rightLength, leftLength));
        if (result != 0) {
            return result;
        }
        return rightLength - leftLength;
    }
}

template <int32_t sortAscendings>
void QuickSortColumnSmallVarChar(std::vector<std::pair<void*, uint64_t>> &valuePtrs, std::vector<uint32_t> &varcharLength, int32_t from, int32_t to)
{
    for (int i = from + 1; i < to; ++i) {
        auto iPair = valuePtrs[i];
        void* iPtr = iPair.first;
        uint32_t iLength = varcharLength[i];
        int j = i - 1;
        while (j >= from) {
            if (NewOnlyCompareVarChar<sortAscendings>(valuePtrs[j].first, varcharLength[j], iPtr, iLength) <= 0) {
                break;
            }
            valuePtrs[j + 1] = valuePtrs[j];
            varcharLength[j + 1] = varcharLength[j];
            --j;
        }
        valuePtrs[j + 1] = iPair;
        varcharLength[j + 1] = iLength;
    }
}

template <int32_t sortAscendings>
int32_t NewMedian3VarChar(std::vector<std::pair<void*, uint64_t>> &valuePtrs, std::vector<uint32_t> &varcharLength, int32_t a, int32_t b, int32_t c)
{
    int32_t ab = NewOnlyCompareVarChar<sortAscendings>(valuePtrs[a].first, varcharLength[a], valuePtrs[b].first, varcharLength[b]);
    int32_t ac = NewOnlyCompareVarChar<sortAscendings>(valuePtrs[a].first, varcharLength[a], valuePtrs[c].first, varcharLength[c]);
    int32_t bc = NewOnlyCompareVarChar<sortAscendings>(valuePtrs[b].first, varcharLength[b], valuePtrs[c].first, varcharLength[c]);
    return ((ab < 0) ? (bc < 0 ? b : ac < 0 ? c : a) : (bc > 0 ? b : ac > 0 ? c : a));
}

template <int32_t sortAscendings>
int32_t NO_INLINE NewGetMedianPositionVarChar(std::vector<std::pair<void*, uint64_t>> &valuePtrs, std::vector<uint32_t> &varcharLength,
    int32_t from, int32_t to, int32_t len)
{
    int32_t l = from;
    int32_t n = to - 1;
    int32_t m = from + len / QUICK_SORT_MIDDLE;
    if (len > QUICK_SORT_BIG_LEN) {
        int32_t s = len / QUICK_SORT_STEP_SIZE;
        l = NewMedian3VarChar<sortAscendings>(valuePtrs, varcharLength, l, l + s, l + QUICK_SORT_MIDDLE * s);
        m = NewMedian3VarChar<sortAscendings>(valuePtrs, varcharLength, m - s, m, m + s);
        n = NewMedian3VarChar<sortAscendings>(valuePtrs, varcharLength, n - QUICK_SORT_MIDDLE * s, n - s, n);
    }
    return NewMedian3VarChar<sortAscendings>(valuePtrs, varcharLength, l, m, n);
}

#define NCHUNK 16 // bigger NCHUNK leads to ROB stall and wasted comparison due to b > c
#define NSTEP 12 // bigger NSTEP leads to reg spilling and hence increased inst count
// Currently the comparison using subtraction includes 9 insts. ROB-size=96, 96/9=10
#define NMAX_SIZE (NCHUNK*NSTEP)

template <int32_t sortAscendings>
int32_t ALWAYS_INLINE NewGetNextCompareLeftVarChar(int32_t *comparetmp, int &k, int &limit, int32_t b, int32_t c,
    std::vector<std::pair<void*, uint64_t>> &valuePtrs, std::vector<uint32_t> &varcharLength, void *pivotValue, uint32_t pivotLength)
{
    if (LIKELY(k < limit)) {
        return comparetmp[k++];
    }
    k = 1;
    // use up
    limit = std::min(c - b + 1, NMAX_SIZE);

    int i, j;
    for (i = b, j = 0; j < limit - NSTEP; i += NSTEP, j += NSTEP) {
        comparetmp[j]    = NewOnlyCompareVarChar<sortAscendings>(valuePtrs[i].first, varcharLength[i], pivotValue, pivotLength);
        comparetmp[j+1]  = NewOnlyCompareVarChar<sortAscendings>(valuePtrs[i+1].first, varcharLength[i+1], pivotValue, pivotLength);
        comparetmp[j+2]  = NewOnlyCompareVarChar<sortAscendings>(valuePtrs[i+2].first, varcharLength[i+2], pivotValue, pivotLength);
        comparetmp[j+3]  = NewOnlyCompareVarChar<sortAscendings>(valuePtrs[i+3].first, varcharLength[i+3], pivotValue, pivotLength);
        comparetmp[j+4]  = NewOnlyCompareVarChar<sortAscendings>(valuePtrs[i+4].first, varcharLength[i+4], pivotValue, pivotLength);
        comparetmp[j+5]  = NewOnlyCompareVarChar<sortAscendings>(valuePtrs[i+5].first, varcharLength[i+5], pivotValue, pivotLength);
        comparetmp[j+6]  = NewOnlyCompareVarChar<sortAscendings>(valuePtrs[i+6].first, varcharLength[i+6], pivotValue, pivotLength);
        comparetmp[j+7]  = NewOnlyCompareVarChar<sortAscendings>(valuePtrs[i+7].first, varcharLength[i+7], pivotValue, pivotLength);
        comparetmp[j+8]  = NewOnlyCompareVarChar<sortAscendings>(valuePtrs[i+8].first, varcharLength[i+8], pivotValue, pivotLength);
        comparetmp[j+9]  = NewOnlyCompareVarChar<sortAscendings>(valuePtrs[i+9].first, varcharLength[i+9], pivotValue, pivotLength);
        comparetmp[j+10] = NewOnlyCompareVarChar<sortAscendings>(valuePtrs[i+10].first, varcharLength[i+10], pivotValue, pivotLength);
        comparetmp[j+11] = NewOnlyCompareVarChar<sortAscendings>(valuePtrs[i+11].first, varcharLength[i+11], pivotValue, pivotLength);
    }
    for (; j < limit; ++i, ++j) {
        comparetmp[j] = NewOnlyCompareVarChar<sortAscendings>(valuePtrs[i].first, varcharLength[i], pivotValue, pivotLength);
    }
    return comparetmp[0];
}

template <int32_t sortAscendings>
int32_t ALWAYS_INLINE NewGetNextCompareRightVarChar(int32_t *comparetmp, int &k, int &limit, int32_t b, int32_t c,
    std::vector<std::pair<void*, uint64_t>> &valuePtrs, std::vector<uint32_t> &varcharLength, void *pivotValue, uint32_t pivotLength)
{
    if (LIKELY(k < limit)) {
        return comparetmp[k++];
    }
    k = 1;
    // use up
    limit = std::min(c - b + 1, NMAX_SIZE);

    int i, j;
    for (i = c, j = 0; j < limit - NSTEP; i -= NSTEP, j += NSTEP) {
        comparetmp[j]    = NewOnlyCompareVarChar<sortAscendings>(valuePtrs[i].first, varcharLength[i], pivotValue, pivotLength);
        comparetmp[j+1]  = NewOnlyCompareVarChar<sortAscendings>(valuePtrs[i-1].first, varcharLength[i-1], pivotValue, pivotLength);
        comparetmp[j+2]  = NewOnlyCompareVarChar<sortAscendings>(valuePtrs[i-2].first, varcharLength[i-2], pivotValue, pivotLength);
        comparetmp[j+3]  = NewOnlyCompareVarChar<sortAscendings>(valuePtrs[i-3].first, varcharLength[i-3], pivotValue, pivotLength);
        comparetmp[j+4]  = NewOnlyCompareVarChar<sortAscendings>(valuePtrs[i-4].first, varcharLength[i-4], pivotValue, pivotLength);
        comparetmp[j+5]  = NewOnlyCompareVarChar<sortAscendings>(valuePtrs[i-5].first, varcharLength[i-5], pivotValue, pivotLength);
        comparetmp[j+6]  = NewOnlyCompareVarChar<sortAscendings>(valuePtrs[i-6].first, varcharLength[i-6], pivotValue, pivotLength);
        comparetmp[j+7]  = NewOnlyCompareVarChar<sortAscendings>(valuePtrs[i-7].first, varcharLength[i-7], pivotValue, pivotLength);
        comparetmp[j+8]  = NewOnlyCompareVarChar<sortAscendings>(valuePtrs[i-8].first, varcharLength[i-8], pivotValue, pivotLength);
        comparetmp[j+9]  = NewOnlyCompareVarChar<sortAscendings>(valuePtrs[i-9].first, varcharLength[i-9], pivotValue, pivotLength);
        comparetmp[j+10] = NewOnlyCompareVarChar<sortAscendings>(valuePtrs[i-10].first, varcharLength[i-10], pivotValue, pivotLength);
        comparetmp[j+11] = NewOnlyCompareVarChar<sortAscendings>(valuePtrs[i-11].first, varcharLength[i-11], pivotValue, pivotLength);
    }
    for (; j < limit; --i, ++j) {
        comparetmp[j] = NewOnlyCompareVarChar<sortAscendings>(valuePtrs[i].first, varcharLength[i], pivotValue, pivotLength);
    }
    return comparetmp[0];
}

void ALWAYS_INLINE SwapVarChar(std::vector<std::pair<void*, uint64_t>> &valuePtrs, std::vector<uint32_t> &varcharLength, int32_t a, int32_t b)
{
    auto temp = valuePtrs[a];
    auto temp2 = varcharLength[a];
    valuePtrs[a] = valuePtrs[b];
    varcharLength[a] = varcharLength[b];
    valuePtrs[b] = temp;
    varcharLength[b] = temp2;
}

void NO_INLINE VectorSwapVarChar(std::vector<std::pair<void*, uint64_t>> &valuePtrs, std::vector<uint32_t> &varcharLength, int32_t from, int32_t l, int32_t s)
{
    int i = 0;
    // for (; i < s-4; i+=4, from+=4, l+=4) {
    for (; i < s-8; i+=8, from+=8, l+=8) {
        SwapVarChar(valuePtrs, varcharLength, from, l);
        SwapVarChar(valuePtrs, varcharLength, from+1, l+1);
        SwapVarChar(valuePtrs, varcharLength, from+2, l+2);
        SwapVarChar(valuePtrs, varcharLength, from+3, l+3);
        SwapVarChar(valuePtrs, varcharLength, from+4, l+4);
        SwapVarChar(valuePtrs, varcharLength, from+5, l+5);
        SwapVarChar(valuePtrs, varcharLength, from+6, l+6);
        SwapVarChar(valuePtrs, varcharLength, from+7, l+7);
    }
    for (; i < s; i++, from++, l++) {
        SwapVarChar(valuePtrs, varcharLength, from, l);
    }
}

template <int32_t sortAscendings>
void QuickSortColumnInternalVarChar(std::vector<std::pair<void*, uint64_t>> &valuePtrs, std::vector<uint32_t> &varcharLength, int32_t from, int32_t to, int32_t *comparetmp)
{
    int32_t len = to - from;
    if (len <= QUICK_SORT_SMALL_LEN) {
        QuickSortColumnSmallVarChar<sortAscendings>(valuePtrs, varcharLength, from, to);
        return;
    }

    int32_t m = NewGetMedianPositionVarChar<sortAscendings>(valuePtrs, varcharLength, from, to, len);

    void* pivotValue = valuePtrs[m].first;
    uint32_t pivotLength = varcharLength[m];

    int32_t a = from;
    int32_t b = a;
    int32_t c = to - 1;
    int32_t d = c;

    int bk = 0, blim = 0;
    int ck = 0, clim = 0;

    int32_t *leftComparetmp = comparetmp, *rightComparetmp = comparetmp + NMAX_SIZE;
    while (true) {
        int32_t comparison;
        while (b <= c && (comparison = NewGetNextCompareLeftVarChar<sortAscendings>(leftComparetmp, bk, blim, b, c, valuePtrs, varcharLength, pivotValue, pivotLength)) <= 0) {
            if (UNLIKELY(comparison == 0)) {
                SwapVarChar(valuePtrs, varcharLength, a++, b);
            }
            b++;
        }
        while (c >= b && (comparison = NewGetNextCompareRightVarChar<sortAscendings>(rightComparetmp, ck, clim, b, c, valuePtrs, varcharLength, pivotValue, pivotLength)) >= 0) {
            if (UNLIKELY(comparison == 0)) {
                SwapVarChar(valuePtrs, varcharLength, c, d--);
            }
            c--;
        }
        if (b > c) {
            break;
        }
        SwapVarChar(valuePtrs, varcharLength, b++, c--);
    }

    // Swap partition elements back to middle
    int32_t s;
    int32_t n = to;
    s = std::min(a - from, b - a);
    VectorSwapVarChar(valuePtrs, varcharLength, from, b - s, s);
    s = std::min(d - c, n - d - 1);
    VectorSwapVarChar(valuePtrs, varcharLength, b, n - s, s);

    // Recursively sort non-partition-elements
    if ((s = b - a) > 1) {
        QuickSortColumnInternalVarChar<sortAscendings>(valuePtrs, varcharLength, from, from + s, comparetmp);
    }
    if ((s = d - c) > 1) {
        QuickSortColumnInternalVarChar<sortAscendings>(valuePtrs, varcharLength, n - s, n, comparetmp);
    }
}

template <int32_t sortAscendings>
void QuickSortColumnVarChar(std::vector<std::pair<void*, uint64_t>> &valuePtrs, std::vector<uint32_t> &varcharLength,
    int32_t from, int32_t to)
{
    int32_t comparetmp[NMAX_SIZE+NMAX_SIZE];
    QuickSortColumnInternalVarChar<sortAscendings>(valuePtrs, varcharLength, from, to, comparetmp);
}


// freaking varchar ...
//-----------------------------------------------------------------------------------------------------

template <DataTypeId D>
static ALWAYS_INLINE typename NativeType<D>::type NewGetValue(void* valuePtr)
{
    return *reinterpret_cast<typename NativeType<D>::type*>(valuePtr);
}

template <DataTypeId D>
static int32_t ALWAYS_INLINE NewOnlyCompareAscending(typename NativeType<D>::type &left, typename NativeType<D>::type &right)
{
    if constexpr (D == OMNI_INT || D == OMNI_DATE32 || D == OMNI_SHORT ||
                  D == OMNI_LONG || D == OMNI_DECIMAL64 || D == OMNI_BOOLEAN) {
        return (left - right);
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
        // TODO implement the comparison semantics of the following types
        // OMNI_TIME32 = 10,
        // OMNI_TIME64 = 11,
        // OMNI_TIMESTAMP = 12,
        // OMNI_INTERVAL_MONTHS = 13,
        // OMNI_INTERVAL_DAY_TIME = 14,
    }
}

template <DataTypeId D, int32_t sortAscendings>
static int32_t ALWAYS_INLINE NewOnlyCompare(typename NativeType<D>::type &left, typename NativeType<D>::type &right)
{
    if constexpr (sortAscendings == 1) {
        return NewOnlyCompareAscending<D>(left, right);
    } else {
        return NewOnlyCompareAscending<D>(right, left);
    }
}

template <DataTypeId D, int32_t sortAscendings>
void QuickSortColumnSmall(std::vector<std::pair<void*, uint64_t>> &valuePtrs, int32_t from, int32_t to)
{
    for (int i = from + 1; i < to; ++i) {
        auto iPair = valuePtrs[i];
        void* iPtr = iPair.first;
        auto iValue = NewGetValue<D>(iPtr);
        int j = i - 1;
        while (j >= from) {
            auto jValue = NewGetValue<D>(valuePtrs[j].first);
            if (NewOnlyCompare<D, sortAscendings>(jValue, iValue) <= 0) {
                break;
            }
            valuePtrs[j + 1] = valuePtrs[j];
            --j;
        }
        valuePtrs[j + 1] = iPair;
    }
}

template <DataTypeId D, int32_t sortAscendings>
int32_t NewMedian3(std::vector<std::pair<void*, uint64_t>> &valuePtrs, int32_t a, int32_t b, int32_t c)
{
    auto va = NewGetValue<D>(valuePtrs[a].first);
    auto vb = NewGetValue<D>(valuePtrs[b].first);
    auto vc = NewGetValue<D>(valuePtrs[c].first);
    int32_t ab = NewOnlyCompare<D, sortAscendings>(va, vb);
    int32_t ac = NewOnlyCompare<D, sortAscendings>(va, vc);
    int32_t bc = NewOnlyCompare<D, sortAscendings>(vb, vc);
    return ((ab < 0) ? (bc < 0 ? b : ac < 0 ? c : a) : (bc > 0 ? b : ac > 0 ? c : a));
}

template <DataTypeId D, int32_t sortAscendings>
int32_t NO_INLINE NewGetMedianPosition(std::vector<std::pair<void*, uint64_t>> &valuePtrs,
    int32_t from, int32_t to, int32_t len)
{
    int32_t l = from;
    int32_t n = to - 1;
    int32_t m = from + len / QUICK_SORT_MIDDLE;
    if (len > QUICK_SORT_BIG_LEN) {
        int32_t s = len / QUICK_SORT_STEP_SIZE;
        l = NewMedian3<D, sortAscendings>(valuePtrs, l, l + s, l + QUICK_SORT_MIDDLE * s);
        m = NewMedian3<D, sortAscendings>(valuePtrs, m - s, m, m + s);
        n = NewMedian3<D, sortAscendings>(valuePtrs, n - QUICK_SORT_MIDDLE * s, n - s, n);
    }
    return NewMedian3<D, sortAscendings>(valuePtrs, l, m, n);
}

template <DataTypeId D, int32_t sortAscendings>
int32_t ALWAYS_INLINE NewGetNextCompareLeft(int32_t *comparetmp, int &k, int &limit, int32_t b, int32_t c,
    std::vector<std::pair<void*, uint64_t>> &valuePtrs, typename NativeType<D>::type &pivotValue)
{
    if (k < limit) {
        return comparetmp[k++];
    }
    k = 1;
    // use up
    limit = std::min(c - b + 1, NMAX_SIZE);

    int i, j;
    for (i = b, j = 0; j < limit - NSTEP; i += NSTEP, j += NSTEP) {
        auto v0 = NewGetValue<D>(valuePtrs[i].first);
        auto v1 = NewGetValue<D>(valuePtrs[i+1].first);
        auto v2 = NewGetValue<D>(valuePtrs[i+2].first);
        auto v3 = NewGetValue<D>(valuePtrs[i+3].first);
        comparetmp[j]   = NewOnlyCompare<D, sortAscendings>(v0, pivotValue);
        comparetmp[j+1] = NewOnlyCompare<D, sortAscendings>(v1, pivotValue);
        comparetmp[j+2] = NewOnlyCompare<D, sortAscendings>(v2, pivotValue);
        comparetmp[j+3] = NewOnlyCompare<D, sortAscendings>(v3, pivotValue);
        auto v4 = NewGetValue<D>(valuePtrs[i+4].first);
        auto v5 = NewGetValue<D>(valuePtrs[i+5].first);
        auto v6 = NewGetValue<D>(valuePtrs[i+6].first);
        auto v7 = NewGetValue<D>(valuePtrs[i+7].first);
        comparetmp[j+4] = NewOnlyCompare<D, sortAscendings>(v4, pivotValue);
        comparetmp[j+5] = NewOnlyCompare<D, sortAscendings>(v5, pivotValue);
        comparetmp[j+6] = NewOnlyCompare<D, sortAscendings>(v6, pivotValue);
        comparetmp[j+7] = NewOnlyCompare<D, sortAscendings>(v7, pivotValue);
        auto v8 = NewGetValue<D>(valuePtrs[i+8].first);
        auto v9 = NewGetValue<D>(valuePtrs[i+9].first);
        auto v10 = NewGetValue<D>(valuePtrs[i+10].first);
        auto v11 = NewGetValue<D>(valuePtrs[i+11].first);
        comparetmp[j+8] = NewOnlyCompare<D, sortAscendings> (v8, pivotValue);
        comparetmp[j+9] = NewOnlyCompare<D, sortAscendings> (v9, pivotValue);
        comparetmp[j+10] = NewOnlyCompare<D, sortAscendings>(v10, pivotValue);
        comparetmp[j+11] = NewOnlyCompare<D, sortAscendings>(v11, pivotValue);
    }
    for (; j < limit; ++i, ++j) {
        auto v = NewGetValue<D>(valuePtrs[i].first);
        comparetmp[j] = NewOnlyCompare<D, sortAscendings>(v, pivotValue);
    }
    return comparetmp[0];
}

template <DataTypeId D, int32_t sortAscendings>
inline int32_t NewGetNextCompareRight(int32_t *comparetmp, int &k, int &limit, int32_t b, int32_t c,
    std::vector<std::pair<void*, uint64_t>> &valuePtrs, typename NativeType<D>::type &pivotValue)
{
    if (k < limit) {
        return comparetmp[k++];
    }
    k = 1;
    // use up
    limit = std::min(c - b + 1, NMAX_SIZE);

    int i, j;
    for (i = c, j = 0; j < limit - NSTEP; i -= NSTEP, j += NSTEP) {
        auto v0 = NewGetValue<D>(valuePtrs[i].first);
        auto v1 = NewGetValue<D>(valuePtrs[i-1].first);
        auto v2 = NewGetValue<D>(valuePtrs[i-2].first);
        auto v3 = NewGetValue<D>(valuePtrs[i-3].first);
        comparetmp[j]   = NewOnlyCompare<D, sortAscendings>(v0, pivotValue);
        comparetmp[j+1] = NewOnlyCompare<D, sortAscendings>(v1, pivotValue);
        comparetmp[j+2] = NewOnlyCompare<D, sortAscendings>(v2, pivotValue);
        comparetmp[j+3] = NewOnlyCompare<D, sortAscendings>(v3, pivotValue);
        auto v4 = NewGetValue<D>(valuePtrs[i-4].first);
        auto v5 = NewGetValue<D>(valuePtrs[i-5].first);
        auto v6 = NewGetValue<D>(valuePtrs[i-6].first);
        auto v7 = NewGetValue<D>(valuePtrs[i-7].first);
        comparetmp[j+4] = NewOnlyCompare<D, sortAscendings>(v4, pivotValue);
        comparetmp[j+5] = NewOnlyCompare<D, sortAscendings>(v5, pivotValue);
        comparetmp[j+6] = NewOnlyCompare<D, sortAscendings>(v6, pivotValue);
        comparetmp[j+7] = NewOnlyCompare<D, sortAscendings>(v7, pivotValue);
        auto v8 = NewGetValue<D>(valuePtrs[i-8].first);
        auto v9 = NewGetValue<D>(valuePtrs[i-9].first);
        auto v10 = NewGetValue<D>(valuePtrs[i-10].first);
        auto v11 = NewGetValue<D>(valuePtrs[i-11].first);
        comparetmp[j+8] = NewOnlyCompare<D, sortAscendings>(v8, pivotValue);
        comparetmp[j+9] = NewOnlyCompare<D, sortAscendings>(v9, pivotValue);
        comparetmp[j+10] = NewOnlyCompare<D, sortAscendings>(v10, pivotValue);
        comparetmp[j+11] = NewOnlyCompare<D, sortAscendings>(v11, pivotValue);
    }
    for (; j < limit; --i, ++j) {
        auto v = NewGetValue<D>(valuePtrs[i].first);
        comparetmp[j] = NewOnlyCompare<D, sortAscendings>(v, pivotValue);
    }
    return comparetmp[0];
}

template <DataTypeId D, int32_t sortAscendings>
void QuickSortColumnInternal(std::vector<std::pair<void*, uint64_t>> &valuePtrs, int32_t from, int32_t to, int32_t *comparetmp)
{
    int32_t len = to - from;
    if (len <= QUICK_SORT_SMALL_LEN) {
        QuickSortColumnSmall<D, sortAscendings>(valuePtrs, from, to);
        return;
    }

    int32_t m = NewGetMedianPosition<D, sortAscendings>(valuePtrs, from, to, len);

    auto pivotValue = NewGetValue<D>(valuePtrs[m].first);

    int32_t a = from;
    int32_t b = a;
    int32_t c = to - 1;
    int32_t d = c;

    int bk = 0, blim = 0;
    int ck = 0, clim = 0;

    int32_t *leftComparetmp = comparetmp, *rightComparetmp = comparetmp + NMAX_SIZE;
    while (true) {
        int32_t comparison;
        while (b <= c && (comparison = NewGetNextCompareLeft<D, sortAscendings>(leftComparetmp, bk, blim, b, c, valuePtrs, pivotValue)) <= 0) {
            if (UNLIKELY(comparison == 0)) {
                Swap(valuePtrs, a++, b);
            }
            b++;
        }
        while (c >= b && (comparison = NewGetNextCompareRight<D, sortAscendings>(rightComparetmp, ck, clim, b, c, valuePtrs, pivotValue)) >= 0) {
            if (UNLIKELY(comparison == 0)) {
                Swap(valuePtrs, c, d--);
            }
            c--;
        }
        if (b > c) {
            break;
        }
        Swap(valuePtrs, b++, c--);
    }

    // Swap partition elements back to middle
    int32_t s;
    int32_t n = to;
    s = std::min(a - from, b - a);
    VectorSwap(valuePtrs, from, b - s, s);
    s = std::min(d - c, n - d - 1);
    VectorSwap(valuePtrs, b, n - s, s);

    // Recursively sort non-partition-elements
    if ((s = b - a) > 1) {
        QuickSortColumnInternal<D, sortAscendings>(valuePtrs, from, from + s, comparetmp);
    }
    if ((s = d - c) > 1) {
        QuickSortColumnInternal<D, sortAscendings>(valuePtrs, n - s, n, comparetmp);
    }
}

template <DataTypeId D, int32_t sortAscendings>
void QuickSortColumn(std::vector<std::pair<void*, uint64_t>> &valuePtrs,
    int32_t from, int32_t to)
{
    int32_t comparetmp[NMAX_SIZE+NMAX_SIZE];
    QuickSortColumnInternal<D, sortAscendings>(valuePtrs, from, to, comparetmp);
}

template <DataTypeId D>
void ColumnarSort(const int32_t *sortCols, const int32_t *sortColTypes, const int32_t *sortAscendings,
    const int32_t *sortNullFirsts, int32_t sortColCount, Vector ***columns,
    std::vector<std::pair<void*, uint64_t>> &valuePtrs, std::vector<uint32_t> &varcharLength, int32_t from, int32_t to, int32_t currentCol)
{
    bool sortNullFirst = sortNullFirsts[currentCol];
    Vector **batchesOfCurrentColumn = columns[sortCols[currentCol]];
    int32_t nonNullFrom = from, nonNullTo = to; // [nonNullFrom, nonNullTo) mark the range of non-null values
    // we are going to sort one column, so we extract all the rows of the column between from and to
    for (int i = from; i < nonNullTo; ) {
        uint64_t encodedIndex = valuePtrs[i].second;
        uint32_t vecBatchIdx = DecodeSliceIndex(encodedIndex);
        uint32_t rowIdx = DecodePosition(encodedIndex);
        auto block = batchesOfCurrentColumn[vecBatchIdx];
        int32_t originalRowIdx;
        block = VectorHelper::ExpandVectorAndIndex(block, rowIdx, originalRowIdx);

        if (UNLIKELY(block->IsValueNull(originalRowIdx))) {
            if (sortNullFirst) {
                Swap(valuePtrs, i++, nonNullFrom++);
            } else { // we swap the last nonNull element back to i -- we need to CHECK the new i-th element again!
                Swap(valuePtrs, i, --nonNullTo);
            }
        } else {
            if constexpr (D == OMNI_VARCHAR) {
                uint8_t *tmp;
                varcharLength[i] = static_cast<VarcharVector *>(block)->GetValue(originalRowIdx, &tmp);
                valuePtrs[i].first = tmp;
            } else {
                valuePtrs[i].first = reinterpret_cast<typename NativeType<D>::type*>(block->GetValues()) + block->GetPositionOffset() + originalRowIdx;
            }
            ++i;
        }
    }
    if constexpr (D == OMNI_VARCHAR) {
        if (sortAscendings[currentCol] == 0) {
            QuickSortColumnVarChar<0>(valuePtrs, varcharLength, nonNullFrom, nonNullTo);
        } else {
            QuickSortColumnVarChar<1>(valuePtrs, varcharLength, nonNullFrom, nonNullTo);
        }
    } else {
        if (sortAscendings[currentCol] == 0) {
            QuickSortColumn<D, 0>(valuePtrs, nonNullFrom, nonNullTo);
        } else {
            QuickSortColumn<D, 1>(valuePtrs, nonNullFrom, nonNullTo);
        }
    }

    if (currentCol == sortColCount - 1) { // are we the last column ?
        return;
    }

    // sort the NULL range. we can only sort null first or last, not both
    if (nonNullFrom != from && from + 1 < nonNullFrom) {
        ColumnarSort(sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, columns, valuePtrs, varcharLength, from, nonNullFrom, currentCol + 1);
    } else if (nonNullTo != to && nonNullTo + 1 < to) {
        ColumnarSort(sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, columns, valuePtrs, varcharLength, nonNullTo, to, currentCol + 1);
    }

    // now we call "get ranges"
    if (D == OMNI_VARCHAR) {
        void *currentValue = valuePtrs[nonNullFrom].first;
        uint32_t currentLength = varcharLength[nonNullFrom];
        int32_t start = nonNullFrom;
        for (int32_t i = nonNullFrom + 1; i < nonNullTo; ++i) {
            void *value = valuePtrs[i].first;
            uint32_t length = varcharLength[i];
            if (NewOnlyCompareVarChar<0>(value, length, currentValue, currentLength) != 0) {
                currentValue = value;
                currentLength = length;
                if (start + 1 != i) { // sort next column when |equivalent class| > 1
                    ColumnarSort(sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, columns, valuePtrs, varcharLength, start, i, currentCol + 1);
                }
                start = i;
            }
        }
        if (start + 1 != nonNullTo) {
            ColumnarSort(sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, columns, valuePtrs, varcharLength, start, nonNullTo, currentCol + 1);
        }
    } else {
        typename NativeType<D>::type currentValue = NewGetValue<D>(valuePtrs[nonNullFrom].first);
        int32_t start = nonNullFrom;
        for (int32_t i = nonNullFrom + 1; i < nonNullTo; ++i) {
            typename NativeType<D>::type value = NewGetValue<D>(valuePtrs[i].first);
            if (NewOnlyCompare<D, 0>(value, currentValue) != 0) {
                currentValue = value;
                if (start + 1 != i) { // sort next column when |equivalent class| > 1
                    ColumnarSort(sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, columns, valuePtrs, varcharLength, start, i, currentCol + 1);
                }
                start = i;
            }
        }
        if (start + 1 != nonNullTo) {
            ColumnarSort(sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, columns, valuePtrs, varcharLength, start, nonNullTo, currentCol + 1);
        }
    }
}

void ColumnarSort(const int32_t *sortCols, const int32_t *sortColTypes, const int32_t *sortAscendings,
    const int32_t *sortNullFirsts, int32_t sortColCount, Vector ***columns,
    std::vector<std::pair<void*, uint64_t>> &valuePtrs, std::vector<uint32_t> &varcharLength,
    int32_t from, int32_t to, int32_t currentCol)
{
    switch (sortColTypes[currentCol]) {
        case OMNI_INT:
        case OMNI_DATE32:
            ColumnarSort<omniruntime::vec::OMNI_INT>(sortCols, sortColTypes, sortAscendings, sortNullFirsts,
                sortColCount, columns, valuePtrs, varcharLength, from, to, currentCol);
            break;
        case OMNI_SHORT:
            ColumnarSort<omniruntime::vec::OMNI_SHORT>(sortCols, sortColTypes, sortAscendings, sortNullFirsts,
                sortColCount, columns, valuePtrs, varcharLength, from, to, currentCol);
            break;
        case OMNI_LONG:
        case OMNI_DECIMAL64:
            ColumnarSort<omniruntime::vec::OMNI_LONG>(sortCols, sortColTypes, sortAscendings, sortNullFirsts,
                sortColCount, columns, valuePtrs, varcharLength, from, to, currentCol);
            break;
        case OMNI_DOUBLE:
            ColumnarSort<omniruntime::vec::OMNI_DOUBLE>(sortCols, sortColTypes, sortAscendings, sortNullFirsts,
                sortColCount, columns, valuePtrs, varcharLength, from, to, currentCol);
            break;
        case OMNI_BOOLEAN:
            ColumnarSort<omniruntime::vec::OMNI_BOOLEAN>(sortCols, sortColTypes, sortAscendings, sortNullFirsts,
                sortColCount, columns, valuePtrs, varcharLength, from, to, currentCol);
            break;
        case OMNI_VARCHAR:
        case OMNI_CHAR:
            ColumnarSort<omniruntime::vec::OMNI_VARCHAR>(sortCols, sortColTypes, sortAscendings, sortNullFirsts,
                sortColCount, columns, valuePtrs, varcharLength, from, to, currentCol);
            break;
        case OMNI_DECIMAL128:
            ColumnarSort<omniruntime::vec::OMNI_DECIMAL128>(sortCols, sortColTypes, sortAscendings, sortNullFirsts,
                sortColCount, columns, valuePtrs, varcharLength, from, to, currentCol);
            break;
        default:
            break;
    }
}

void PagesIndex::Sort(const int32_t *sortCols, const int32_t *sortColTypes, const int32_t *sortAscendings,
    const int32_t *sortNullFirsts, int32_t sortColCount, int32_t from, int32_t to)
{
    std::vector<std::pair<void*, uint64_t>> valuePtrs(this->positionCount);
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
    for (int32_t i = from; i < to; ++i) {
        valuePtrs[i].second = this->valueAddresses[i];
    }
    ColumnarSort(sortCols, sortColTypes, sortAscendings, sortNullFirsts, sortColCount, columns, valuePtrs, varcharLength, from, to, 0);
    for (int32_t i = from; i < to; ++i) {
        this->valueAddresses[i] = valuePtrs[i].second;
    }
}

void PagesIndex::GetOutput(int32_t *outputCols, int32_t outputColsCount, VectorBatch *outputVecBatch,
    const int32_t *sourceTypes, int32_t offset, int32_t length, VectorAllocator *vecAllocator) const
{
    Vector ***inputVecBatches = this->columns;
    uint64_t *vaStart = valueAddresses + offset;

    for (int32_t j = 0; j < outputColsCount; j++) {
        int32_t outputCol = outputCols[j];
        int colTypeId = sourceTypes[outputCol];
        Vector **inputVecBatch = inputVecBatches[outputCol];
        Vector *v = nullptr;
        switch (colTypeId) {
            case OMNI_INT:
            case OMNI_DATE32:
                v = ConstructVector<IntVector>(vaStart, length, inputVecBatch, vecAllocator);
                break;
            case OMNI_SHORT:
                v = ConstructVector<ShortVector>(vaStart, length, inputVecBatch, vecAllocator);
                break;
            case OMNI_LONG:
            case OMNI_DECIMAL64:
                v = ConstructVector<LongVector>(vaStart, length, inputVecBatch, vecAllocator);
                break;
            case OMNI_DOUBLE:
                v = ConstructVector<DoubleVector>(vaStart, length, inputVecBatch, vecAllocator);
                break;
            case OMNI_BOOLEAN:
                v = ConstructVector<BooleanVector>(vaStart, length, inputVecBatch, vecAllocator);
                break;
            case OMNI_VARCHAR:
            case OMNI_CHAR: {
                v = ConstructVarcharVector(vaStart, length, inputVecBatch, vecAllocator);
                break;
            }
            case OMNI_DECIMAL128:
                v = ConstructVector<Decimal128Vector>(vaStart, length, inputVecBatch, vecAllocator);
                break;
            default:
                break;
        }
        outputVecBatch->SetVector(j, v);
    }
}

PagesIndex::~PagesIndex()
{
    Clear();
}

template <typename T>
static ALWAYS_INLINE void SetValue(Vector *inputVector, int32_t inputIndex, T *outputVector, int32_t outputIndex)
{
    if (UNLIKELY(inputVector->GetEncoding() == OMNI_VEC_ENCODING_DICTIONARY)) {
        auto dictionaryVector = static_cast<DictionaryVector *>(inputVector);
        inputVector = dictionaryVector->GetDictionary();
        inputIndex = dictionaryVector->GetId(inputIndex);
    }
    if (UNLIKELY(inputVector->IsValueNull(inputIndex))) {
        outputVector->SetValueNull(outputIndex);
    } else {
        outputVector->SetValue(outputIndex, static_cast<T *>(inputVector)->GetValue(inputIndex));
    }
}

template <typename T>
NO_INLINE T *ConstructVector(uint64_t *vaStart, int32_t length, Vector **inputVecBatch,
    VectorAllocator *vecAllocator)
{
    auto outputVector = new T(vecAllocator, length);
    int32_t outputIndex = 0;
    uint64_t *vaEnd = vaStart + length;
    while (vaStart < vaEnd) { // here unroll is almost useless due to the excessive checks in SetValue
        uint64_t valueAddress = *(vaStart++);
        uint32_t pageIndex = DecodeSliceIndex(valueAddress);
        Vector *inputVector = inputVecBatch[pageIndex];
        uint32_t position = DecodePosition(valueAddress);
        SetValue(inputVector, static_cast<int32_t>(position), outputVector, outputIndex++);
    }
    return outputVector;
}

static ALWAYS_INLINE void SetVarcharValue(Vector *inputVector, int32_t inputIndex, VarcharVector *outputVector, int32_t outputIndex)
{
    if (UNLIKELY(inputVector->GetEncoding() == OMNI_VEC_ENCODING_DICTIONARY)) {
        auto dictionaryVector = static_cast<DictionaryVector *>(inputVector);
        inputVector = dictionaryVector->GetDictionary();
        inputIndex = dictionaryVector->GetId(inputIndex);
    }
    if (UNLIKELY(inputVector->IsValueNull(inputIndex))) {
        static_cast<VarcharVector *>(outputVector)->SetValueNull(outputIndex);
    } else {
        uint8_t *value = nullptr;
        int32_t valueLength = static_cast<VarcharVector *>(inputVector)->GetValue(inputIndex, &value);
        outputVector->SetValue(outputIndex, value, valueLength);
    }
}

NO_INLINE VarcharVector *ConstructVarcharVector(uint64_t *vaStart, int32_t length, Vector **inputVecBatch,
    VectorAllocator *vecAllocator)
{
    auto *outputVector = new VarcharVector(vecAllocator, length);
    int32_t outputIndex = 0;
    uint64_t *vaEnd = vaStart + length;
    while (vaStart < vaEnd) {
        uint64_t valueAddress = *(vaStart++);
        uint32_t pageIndex = DecodeSliceIndex(valueAddress);
        Vector *inputVector = inputVecBatch[pageIndex];
        uint32_t position = DecodePosition(valueAddress);
        SetVarcharValue(inputVector, static_cast<int32_t>(position), outputVector, outputIndex++);
    }
    return outputVector;
}

void PagesIndex::GetSortedVecBatches(VectorAllocator *vectorAllocator, std::vector<int32_t> &outputCols,
    std::vector<VectorBatch *> &sortedVecBatches)
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
        result = new VectorBatch(outputColsCount, rowCount);
        GetOutput(outputCols.data(), outputColsCount, result, dataTypes.GetIds(), offset, rowCount, vectorAllocator);
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
