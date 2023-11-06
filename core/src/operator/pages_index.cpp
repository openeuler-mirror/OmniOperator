/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: pages index implementations
 */
#include "pages_index.h"
#include <algorithm>
#include "vector/vector.h"
#include "type/data_type.h"
#include "quick_sort_simd.h"

using namespace omniruntime::vec;
using namespace omniruntime::type;

namespace omniruntime::op {
using VarcharVector = Vector<LargeStringContainer<std::string_view>>;
using DictionaryVarcharVector = Vector<DictionaryContainer<std::string_view>>;

const int32_t QUICK_SORT_SMALL_LEN = 16;
const int32_t QUICK_SORT_BIG_LEN = 64;
const int32_t QUICK_SORT_STEP_SIZE = 8;
const int32_t QUICK_SORT_MIDDLE = 2;

template <type::DataTypeId dataTypeId>
BaseVector *ConstructVector(uint64_t *vaStart, int32_t length, BaseVector **inputVecBatch, bool hasNull,
    bool hasDictionary);

// function implements for class PagesIndex
PagesIndex::PagesIndex(const DataTypes &types)
    : dataTypes(types), typesCount(types.GetSize()), columns(nullptr), valueAddresses(nullptr), positionCount(0)
{
    hasDictionaries.resize(typesCount);
    hasNulls.resize(typesCount);
    std::fill(hasDictionaries.begin(), hasDictionaries.end(), false);
    std::fill(hasNulls.begin(), hasNulls.end(), false);
}

void ALWAYS_INLINE Swap(int64_t *values, uint64_t *addresses, int32_t a, int32_t b)
{
    auto tmpValue = values[a];
    auto tmpAddr = addresses[a];
    values[a] = values[b];
    addresses[a] = addresses[b];
    values[b] = tmpValue;
    addresses[b] = tmpAddr;
}

__attribute__((noinline)) void VectorSwap(int64_t *values, uint64_t *addresses, int32_t from, int32_t l, int32_t s)
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
            if (vector->HasNull()) {
                hasNulls[colIdx] = true;
            }
            if (vector->GetEncoding() == OMNI_DICTIONARY) {
                hasDictionaries[colIdx] = true;
            }
            this->columns[colIdx][vecBatchIdx] = vector;
        }
    }
}

static bool ALWAYS_INLINE NewOnlyEqualVarChar(int64_t leftValue, uint32_t leftLength, int64_t rightValue,
    uint32_t rightLength)
{
    if (leftLength != rightLength) {
        return false;
    } else {
        return memcmp((void *)rightValue, (void *)leftValue, leftLength) == 0;
    }
}

template <int32_t sortAscending>
static int32_t ALWAYS_INLINE NewOnlyCompareVarChar(int64_t leftValue, uint32_t leftLength, int64_t rightValue,
    uint32_t rightLength)
{
    if constexpr (sortAscending == 1) {
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

template <int32_t sortAscending>
void QuickSortVarCharSmall(int64_t *values, std::vector<uint32_t> &varcharLength, uint64_t *addresses,
    int32_t from, int32_t to)
{
    for (int32_t i = from + 1; i < to; ++i) {
        int64_t iPtr = values[i];
        uint32_t iLength = varcharLength[i];
        uint64_t iAddr = addresses[i];
        int32_t j = i - 1;
        while (j >= from) {
            if (NewOnlyCompareVarChar<sortAscending>(values[j], varcharLength[j], iPtr, iLength) <= 0) {
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

template <int32_t sortAscending>
int32_t NewMedian3VarChar(int64_t *values, std::vector<uint32_t> &varcharLength, int32_t a, int32_t b, int32_t c)
{
    int32_t ab = NewOnlyCompareVarChar<sortAscending>(values[a], varcharLength[a], values[b], varcharLength[b]);
    int32_t ac = NewOnlyCompareVarChar<sortAscending>(values[a], varcharLength[a], values[c], varcharLength[c]);
    int32_t bc = NewOnlyCompareVarChar<sortAscending>(values[b], varcharLength[b], values[c], varcharLength[c]);
    return ((ab < 0) ? (bc < 0 ? b : ac < 0 ? c : a) : (bc > 0 ? b : ac > 0 ? c : a));
}

template <int32_t sortAscending>
int32_t NO_INLINE NewGetMedianPositionVarChar(int64_t *values, std::vector<uint32_t> &varcharLength, int32_t from,
    int32_t to, int32_t len)
{
    int32_t l = from;
    int32_t n = to - 1;
    int32_t m = from + len / QUICK_SORT_MIDDLE;
    if (len > QUICK_SORT_BIG_LEN) {
        int32_t s = len / QUICK_SORT_STEP_SIZE;
        l = NewMedian3VarChar<sortAscending>(values, varcharLength, l, l + s, l + QUICK_SORT_MIDDLE * s);
        m = NewMedian3VarChar<sortAscending>(values, varcharLength, m - s, m, m + s);
        n = NewMedian3VarChar<sortAscending>(values, varcharLength, n - QUICK_SORT_MIDDLE * s, n - s, n);
    }
    return NewMedian3VarChar<sortAscending>(values, varcharLength, l, m, n);
}

// bigger NCHUNK leads to ROB stall and wasted comparison due to b > c
static constexpr int32_t NCHUNK = 16;
// bigger NSTEP leads to reg spilling and hence increased inst count
static constexpr int32_t NSTEP = 12;
// Currently the comparison using subtraction includes 9 insts. ROB-size=96, 96/9=10
static constexpr int32_t NMAX_SIZE = NCHUNK * NSTEP;

template <int32_t sortAscending>
int32_t ALWAYS_INLINE NewGetNextCompareLeftVarChar(int32_t *comparetmp, int32_t &k, int32_t &limit, int32_t b,
    int32_t c, int64_t *values, std::vector<uint32_t> &varcharLength, int64_t pivotValue, uint32_t pivotLength)
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
        comparetmp[j] = NewOnlyCompareVarChar<sortAscending>(values[i], varcharLength[i], pivotValue, pivotLength);
        comparetmp[j + 1] =
            NewOnlyCompareVarChar<sortAscending>(values[i + 1], varcharLength[i + 1], pivotValue, pivotLength);
        comparetmp[j + 2] =
            NewOnlyCompareVarChar<sortAscending>(values[i + 2], varcharLength[i + 2], pivotValue, pivotLength);
        comparetmp[j + 3] =
            NewOnlyCompareVarChar<sortAscending>(values[i + 3], varcharLength[i + 3], pivotValue, pivotLength);
        comparetmp[j + 4] =
            NewOnlyCompareVarChar<sortAscending>(values[i + 4], varcharLength[i + 4], pivotValue, pivotLength);
        comparetmp[j + 5] =
            NewOnlyCompareVarChar<sortAscending>(values[i + 5], varcharLength[i + 5], pivotValue, pivotLength);
        comparetmp[j + 6] =
            NewOnlyCompareVarChar<sortAscending>(values[i + 6], varcharLength[i + 6], pivotValue, pivotLength);
        comparetmp[j + 7] =
            NewOnlyCompareVarChar<sortAscending>(values[i + 7], varcharLength[i + 7], pivotValue, pivotLength);
        comparetmp[j + 8] =
            NewOnlyCompareVarChar<sortAscending>(values[i + 8], varcharLength[i + 8], pivotValue, pivotLength);
        comparetmp[j + 9] =
            NewOnlyCompareVarChar<sortAscending>(values[i + 9], varcharLength[i + 9], pivotValue, pivotLength);
        comparetmp[j + 10] =
            NewOnlyCompareVarChar<sortAscending>(values[i + 10], varcharLength[i + 10], pivotValue, pivotLength);
        comparetmp[j + 11] =
            NewOnlyCompareVarChar<sortAscending>(values[i + 11], varcharLength[i + 11], pivotValue, pivotLength);
    }
    for (; j < limit; ++i, ++j) {
        comparetmp[j] = NewOnlyCompareVarChar<sortAscending>(values[i], varcharLength[i], pivotValue, pivotLength);
    }
    return comparetmp[0];
}

template <int32_t sortAscending>
int32_t ALWAYS_INLINE NewGetNextCompareRightVarChar(int32_t *comparetmp, int32_t &k, int32_t &limit, int32_t b,
    int32_t c, int64_t *values, std::vector<uint32_t> &varcharLength, int64_t pivotValue, uint32_t pivotLength)
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
        comparetmp[j] = NewOnlyCompareVarChar<sortAscending>(values[i], varcharLength[i], pivotValue, pivotLength);
        comparetmp[j + 1] =
            NewOnlyCompareVarChar<sortAscending>(values[i - 1], varcharLength[i - 1], pivotValue, pivotLength);
        comparetmp[j + 2] =
            NewOnlyCompareVarChar<sortAscending>(values[i - 2], varcharLength[i - 2], pivotValue, pivotLength);
        comparetmp[j + 3] =
            NewOnlyCompareVarChar<sortAscending>(values[i - 3], varcharLength[i - 3], pivotValue, pivotLength);
        comparetmp[j + 4] =
            NewOnlyCompareVarChar<sortAscending>(values[i - 4], varcharLength[i - 4], pivotValue, pivotLength);
        comparetmp[j + 5] =
            NewOnlyCompareVarChar<sortAscending>(values[i - 5], varcharLength[i - 5], pivotValue, pivotLength);
        comparetmp[j + 6] =
            NewOnlyCompareVarChar<sortAscending>(values[i - 6], varcharLength[i - 6], pivotValue, pivotLength);
        comparetmp[j + 7] =
            NewOnlyCompareVarChar<sortAscending>(values[i - 7], varcharLength[i - 7], pivotValue, pivotLength);
        comparetmp[j + 8] =
            NewOnlyCompareVarChar<sortAscending>(values[i - 8], varcharLength[i - 8], pivotValue, pivotLength);
        comparetmp[j + 9] =
            NewOnlyCompareVarChar<sortAscending>(values[i - 9], varcharLength[i - 9], pivotValue, pivotLength);
        comparetmp[j + 10] =
            NewOnlyCompareVarChar<sortAscending>(values[i - 10], varcharLength[i - 10], pivotValue, pivotLength);
        comparetmp[j + 11] =
            NewOnlyCompareVarChar<sortAscending>(values[i - 11], varcharLength[i - 11], pivotValue, pivotLength);
    }
    for (; j < limit; --i, ++j) {
        comparetmp[j] = NewOnlyCompareVarChar<sortAscending>(values[i], varcharLength[i], pivotValue, pivotLength);
    }
    return comparetmp[0];
}

void ALWAYS_INLINE SwapVarchar(int64_t *values, std::vector<uint32_t> &varcharLength, uint64_t *addresses, int32_t a,
    int32_t b)
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

void NO_INLINE VectorSwapVarChar(int64_t *values, std::vector<uint32_t> &varcharLength, uint64_t *addresses,
    int32_t from, int32_t l, int32_t s)
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

template <int32_t sortAscending>
void QuickSortVarcharInternal(int64_t *values, std::vector<uint32_t> &varcharLength, uint64_t *addresses,
    int32_t from, int32_t to, int32_t *comparetmp)
{
    int32_t len = to - from;
    if (len <= QUICK_SORT_SMALL_LEN) {
        QuickSortVarCharSmall<sortAscending>(values, varcharLength, addresses, from, to);
        return;
    }

    int32_t m = NewGetMedianPositionVarChar<sortAscending>(values, varcharLength, from, to, len);

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
        while (b <= c && (comparison = NewGetNextCompareLeftVarChar<sortAscending>(leftComparetmp, bk, blim, b, c,
            values, varcharLength, pivotValue, pivotLength)) <= 0) {
            if (UNLIKELY(comparison == 0)) {
                SwapVarchar(values, varcharLength, addresses, a++, b);
            }
            b++;
        }
        while (c >= b && (comparison = NewGetNextCompareRightVarChar<sortAscending>(rightComparetmp, ck, clim, b, c,
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
        QuickSortVarcharInternal<sortAscending>(values, varcharLength, addresses, from, from + s, comparetmp);
    }
    if ((s = d - c) > 1) {
        QuickSortVarcharInternal<sortAscending>(values, varcharLength, addresses, n - s, n, comparetmp);
    }
}

template <int32_t sortAscending>
void QuickSortVarChar(int64_t *values, std::vector<uint32_t> &varcharLength, uint64_t *addresses, int32_t from,
    int32_t to)
{
    int32_t compareResult[NMAX_SIZE + NMAX_SIZE];
    QuickSortVarcharInternal<sortAscending>(values, varcharLength, addresses, from, to, compareResult);
}

template <typename RawType> static bool ALWAYS_INLINE NewOnlyEqual(RawType &left, RawType &right)
{
    if constexpr (std::is_same_v<RawType, double>) {
        double diff = left - right;
        if (std::abs(diff) < __DBL_EPSILON__) {
            return true;
        } else {
            return false;
        }
    } else {
        return left == right;
    }
}

template <int32_t sortAscending> static int8_t ALWAYS_INLINE FixedLengthCompare(int64_t left, int64_t right)
{
    if constexpr (sortAscending == 1) {
        return left > right ? OperatorUtil::COMPARE_STATUS_GREATER_THAN : left < right ? OperatorUtil::COMPARE_STATUS_LESS_THAN : OperatorUtil::COMPARE_STATUS_EQUAL;
    } else {
        return right > left ? OperatorUtil::COMPARE_STATUS_GREATER_THAN : right < left ? OperatorUtil::COMPARE_STATUS_LESS_THAN : OperatorUtil::COMPARE_STATUS_EQUAL;
    }
}

template <int32_t sortAscending> static int8_t ALWAYS_INLINE Decimal128Compare(Decimal128 &left, Decimal128 &right)
{
    if constexpr (sortAscending == 1) {
        return left > right ? OperatorUtil::COMPARE_STATUS_GREATER_THAN : left < right ? OperatorUtil::COMPARE_STATUS_LESS_THAN : OperatorUtil::COMPARE_STATUS_EQUAL;
    } else {
        return right > left ? OperatorUtil::COMPARE_STATUS_GREATER_THAN : right < left ? OperatorUtil::COMPARE_STATUS_LESS_THAN : OperatorUtil::COMPARE_STATUS_EQUAL;
    }
}

template <int32_t sortAscending>
void QuickSortDecimal128Small(int64_t *values, uint64_t *addresses, int32_t from, int32_t to)
{
    for (int32_t i = from + 1; i < to; ++i) {
        auto iValuePtr = values[i];
        auto iValue = *((Decimal128 *)iValuePtr);
        auto iAddr = addresses[i];
        int32_t j = i - 1;
        while (j >= from) {
            auto jValuePtr = values[j];
            auto jValue = *((Decimal128 *)jValuePtr);
            if constexpr (sortAscending == 0) {
                if (jValue >= iValue) {
                    break;
                }
            } else {
                if (jValue <= iValue) {
                    break;
                }
            }
            values[j + 1] = values[j];
            addresses[j + 1] = addresses[j];
            --j;
        }
        values[j + 1] = iValuePtr;
        addresses[j + 1] = iAddr;
    }
}

int32_t GetMedianDecimal128(int64_t *values, int32_t a, int32_t b, int32_t c, Decimal128 &pivotValue)
{
    Decimal128 va = *((Decimal128 *)(values[a]));
    Decimal128 vb = *((Decimal128 *)(values[b]));
    Decimal128 vc = *((Decimal128 *)(values[c]));
    if (va <= vb) {
        if (vb <= vc) {
            pivotValue = vb;
            return b;
        }
        if (va <= vc) {
            pivotValue = vc;
            return c;
        } else {
            pivotValue = va;
            return a;
        }
    } else {
        if (vb > vc) {
            pivotValue = vb;
            return b;
        }
        if (va > vc) {
            pivotValue = vc;
            return c;
        } else {
            pivotValue = va;
            return a;
        }
    }
}

Decimal128 GetMedianDecimal128Value(int64_t *values, int32_t from, int32_t to, int32_t len)
{
    int32_t l = from;
    int32_t n = to - 1;
    int32_t m = from + len / QUICK_SORT_MIDDLE;
    if (len > QUICK_SORT_BIG_LEN) {
        int32_t s = len / QUICK_SORT_STEP_SIZE;
        Decimal128 vl;
        Decimal128 vm;
        Decimal128 vn;
        l = GetMedianDecimal128(values, l, l + s, l + QUICK_SORT_MIDDLE * s, vl);
        m = GetMedianDecimal128(values, m - s, m, m + s, vm);
        n = GetMedianDecimal128(values, n - QUICK_SORT_MIDDLE * s, n - s, n, vn);
        if (vl <= vm) {
            if (vm <= vn) {
                return vm;
            }
            if (vl <= vn) {
                return vn;
            } else {
                return vl;
            }
        } else {
            if (vm > vn) {
                return vm;
            }
            if (vl > vn) {
                return vn;
            } else {
                return vl;
            }
        }
    } else {
        Decimal128 median;
        GetMedianDecimal128(values, l, m, n, median);
        return median;
    }
}

template <int32_t sortAscending>
int8_t ALWAYS_INLINE GetNextCompareDecimal128Left(int8_t *comparetmp, int32_t &k, int32_t &limit, int32_t b, int32_t c,
    int64_t *values, Decimal128 &pivotValue)
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
        auto v0 = *((Decimal128 *)(values[i]));
        auto v1 = *((Decimal128 *)(values[i + 1]));
        auto v2 = *((Decimal128 *)(values[i + 2]));
        auto v3 = *((Decimal128 *)(values[i + 3]));
        comparetmp[j] = Decimal128Compare<sortAscending>(v0, pivotValue);
        comparetmp[j + 1] = Decimal128Compare<sortAscending>(v1, pivotValue);
        comparetmp[j + 2] = Decimal128Compare<sortAscending>(v2, pivotValue);
        comparetmp[j + 3] = Decimal128Compare<sortAscending>(v3, pivotValue);
        auto v4 = *((Decimal128 *)(values[i + 4]));
        auto v5 = *((Decimal128 *)(values[i + 5]));
        auto v6 = *((Decimal128 *)(values[i + 6]));
        auto v7 = *((Decimal128 *)(values[i + 7]));
        comparetmp[j + 4] = Decimal128Compare<sortAscending>(v4, pivotValue);
        comparetmp[j + 5] = Decimal128Compare<sortAscending>(v5, pivotValue);
        comparetmp[j + 6] = Decimal128Compare<sortAscending>(v6, pivotValue);
        comparetmp[j + 7] = Decimal128Compare<sortAscending>(v7, pivotValue);
        auto v8 = *((Decimal128 *)(values[i + 8]));
        auto v9 = *((Decimal128 *)(values[i + 9]));
        auto v10 = *((Decimal128 *)(values[i + 10]));
        auto v11 = *((Decimal128 *)(values[i + 11]));
        comparetmp[j + 8] = Decimal128Compare<sortAscending>(v8, pivotValue);
        comparetmp[j + 9] = Decimal128Compare<sortAscending>(v9, pivotValue);
        comparetmp[j + 10] = Decimal128Compare<sortAscending>(v10, pivotValue);
        comparetmp[j + 11] = Decimal128Compare<sortAscending>(v11, pivotValue);
    }
    for (; j < limit; ++i, ++j) {
        auto v = *((Decimal128 *)(values[i]));
        comparetmp[j] = Decimal128Compare<sortAscending>(v, pivotValue);
    }
    return comparetmp[0];
}

template <int32_t sortAscending>
inline int8_t GetNextCompareDecimal128Right(int8_t *comparetmp, int32_t &k, int32_t &limit, int32_t b, int32_t c, int64_t *values,
    Decimal128 &pivotValue)
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
        auto v0 = *((Decimal128 *)(values[i]));
        auto v1 = *((Decimal128 *)(values[i - 1]));
        auto v2 = *((Decimal128 *)(values[i - 2]));
        auto v3 = *((Decimal128 *)(values[i - 3]));
        comparetmp[j] = Decimal128Compare<sortAscending>(v0, pivotValue);
        comparetmp[j + 1] = Decimal128Compare<sortAscending>(v1, pivotValue);
        comparetmp[j + 2] = Decimal128Compare<sortAscending>(v2, pivotValue);
        comparetmp[j + 3] = Decimal128Compare<sortAscending>(v3, pivotValue);
        auto v4 = *((Decimal128 *)(values[i - 4]));
        auto v5 = *((Decimal128 *)(values[i - 5]));
        auto v6 = *((Decimal128 *)(values[i - 6]));
        auto v7 = *((Decimal128 *)(values[i - 7]));
        comparetmp[j + 4] = Decimal128Compare<sortAscending>(v4, pivotValue);
        comparetmp[j + 5] = Decimal128Compare<sortAscending>(v5, pivotValue);
        comparetmp[j + 6] = Decimal128Compare<sortAscending>(v6, pivotValue);
        comparetmp[j + 7] = Decimal128Compare<sortAscending>(v7, pivotValue);
        auto v8 = *((Decimal128 *)(values[i - 8]));
        auto v9 = *((Decimal128 *)(values[i - 9]));
        auto v10 = *((Decimal128 *)(values[i - 10]));
        auto v11 = *((Decimal128 *)(values[i - 11]));
        comparetmp[j + 8] = Decimal128Compare<sortAscending>(v8, pivotValue);
        comparetmp[j + 9] = Decimal128Compare<sortAscending>(v9, pivotValue);
        comparetmp[j + 10] = Decimal128Compare<sortAscending>(v10, pivotValue);
        comparetmp[j + 11] = Decimal128Compare<sortAscending>(v11, pivotValue);
    }
    for (; j < limit; --i, ++j) {
        auto v = *((Decimal128 *)(values[i]));
        comparetmp[j] = Decimal128Compare<sortAscending>(v, pivotValue);
    }
    return comparetmp[0];
}

template <int32_t sortAscending>
void QuickSortDecimal128Internal(int64_t *values, uint64_t *addresses, int32_t from, int32_t to, int8_t *comparetmp)
{
    int32_t len = to - from;
    if (len <= QUICK_SORT_SMALL_LEN) { // point 3
        QuickSortDecimal128Small<sortAscending>(values, addresses, from, to);
        return;
    }

    auto pivotValue = GetMedianDecimal128Value(values, from, to, len);

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
        while (b <= c && (comparison = GetNextCompareDecimal128Left<sortAscending>(leftComparetmp, bk, blim, b, c, values,
            pivotValue)) <= 0) {
            if (UNLIKELY(comparison == 0)) {
                Swap(values, addresses, a++, b);
            }
            b++;
        }
        while (c >= b && (comparison = GetNextCompareDecimal128Right<sortAscending>(rightComparetmp, ck, clim, b, c, values,
            pivotValue)) >= 0) {
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
        QuickSortDecimal128Internal<sortAscending>(values, addresses, from, from + s, comparetmp);
    }
    if ((s = d - c) > 1) {
        QuickSortDecimal128Internal<sortAscending>(values, addresses, n - s, n, comparetmp);
    }
}

template <int32_t sortAscending>
void QuickSortDecimal128(int64_t *values, uint64_t *addresses, int32_t from, int32_t to)
{
    int8_t comparetmp[NMAX_SIZE + NMAX_SIZE];
    QuickSortDecimal128Internal<sortAscending>(values, addresses, from, to, comparetmp);
}

// start for compare scalar and simd test
int32_t GetMedian(int64_t *values, int32_t a, int32_t b, int32_t c, int64_t &pivotValue)
{
    int64_t va = values[a];
    int64_t vb = values[b];
    int64_t vc = values[c];
    if (va <= vb) {
        if (vb <= vc) {
            pivotValue = vb;
            return b;
        }
        if (va <= vc) {
            pivotValue = vc;
            return c;
        } else {
            pivotValue = va;
            return a;
        }
    } else {
        if (vb > vc) {
            pivotValue = vb;
            return b;
        }
        if (va > vc) {
            pivotValue = vc;
            return c;
        } else {
            pivotValue = va;
            return a;
        }
    }
}

int64_t GetMedianFixedLengthValue(int64_t *values, int32_t from, int32_t to, int32_t len)
{
    int32_t l = from;
    int32_t n = to - 1;
    int32_t m = from + len / QUICK_SORT_MIDDLE;
    if (len > QUICK_SORT_BIG_LEN) {
        int32_t s = len / QUICK_SORT_STEP_SIZE;
        int64_t vl;
        int64_t vm;
        int64_t vn;
        l = GetMedian(values, l, l + s, l + QUICK_SORT_MIDDLE * s, vl);
        m = GetMedian(values, m - s, m, m + s, vm);
        n = GetMedian(values, n - QUICK_SORT_MIDDLE * s, n - s, n, vn);
        if (vl <= vm) {
            if (vm <= vn) {
                return vm;
            }
            if (vl <= vn) {
                return vn;
            } else {
                return vl;
            }
        } else {
            if (vm > vn) {
                return vm;
            }
            if (vl > vn) {
                return vn;
            } else {
                return vl;
            }
        }
    } else {
        int64_t median;
        GetMedian(values, l, m, n, median);
        return median;
    }
}

template <int32_t sortAscending>
void QuickSortFixedLengthSmall(int64_t *values, uint64_t *addresses, int32_t from, int32_t to)
{
    for (int32_t i = from + 1; i < to; ++i) {
        auto iValue = values[i];
        auto iAddr = addresses[i];
        int32_t j = i - 1;
        while (j >= from) {
            auto jValue = values[j];
            if constexpr (sortAscending == 0) {
                if (jValue >= iValue) {
                    break;
                }
            } else {
                if (jValue <= iValue) {
                    break;
                }
            }
            values[j + 1] = jValue;
            addresses[j + 1] = addresses[j];
            --j;
        }
        values[j + 1] = iValue;
        addresses[j + 1] = iAddr;
    }
}

template <int32_t sortAscending>
int8_t ALWAYS_INLINE GetNextCompareFixedLengthLeft(int8_t *comparetmp, int32_t &k, int32_t &limit, int32_t b, int32_t c,
    int64_t *values, int64_t &pivotValue)
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
        auto v0 = values[i];
        auto v1 = values[i + 1];
        auto v2 = values[i + 2];
        auto v3 = values[i + 3];
        comparetmp[j] = FixedLengthCompare<sortAscending>(v0, pivotValue);
        comparetmp[j + 1] = FixedLengthCompare<sortAscending>(v1, pivotValue);
        comparetmp[j + 2] = FixedLengthCompare<sortAscending>(v2, pivotValue);
        comparetmp[j + 3] = FixedLengthCompare<sortAscending>(v3, pivotValue);
        auto v4 = values[i + 4];
        auto v5 = values[i + 5];
        auto v6 = values[i + 6];
        auto v7 = values[i + 7];
        comparetmp[j + 4] = FixedLengthCompare<sortAscending>(v4, pivotValue);
        comparetmp[j + 5] = FixedLengthCompare<sortAscending>(v5, pivotValue);
        comparetmp[j + 6] = FixedLengthCompare<sortAscending>(v6, pivotValue);
        comparetmp[j + 7] = FixedLengthCompare<sortAscending>(v7, pivotValue);
        auto v8 = values[i + 8];
        auto v9 = values[i + 9];
        auto v10 = values[i + 10];
        auto v11 = values[i + 11];
        comparetmp[j + 8] = FixedLengthCompare<sortAscending>(v8, pivotValue);
        comparetmp[j + 9] = FixedLengthCompare<sortAscending>(v9, pivotValue);
        comparetmp[j + 10] = FixedLengthCompare<sortAscending>(v10, pivotValue);
        comparetmp[j + 11] = FixedLengthCompare<sortAscending>(v11, pivotValue);
    }
    for (; j < limit; ++i, ++j) {
        auto v = values[i];
        comparetmp[j] = FixedLengthCompare<sortAscending>(v, pivotValue);
    }
    return comparetmp[0];
}

template <int32_t sortAscending>
inline int8_t GetNextCompareFixedLengthRight(int8_t *comparetmp, int32_t &k, int32_t &limit, int32_t b, int32_t c, int64_t *values,
    int64_t &pivotValue)
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
        auto v0 = values[i];
        auto v1 = values[i - 1];
        auto v2 = values[i - 2];
        auto v3 = values[i - 3];
        comparetmp[j] = FixedLengthCompare<sortAscending>(v0, pivotValue);
        comparetmp[j + 1] = FixedLengthCompare<sortAscending>(v1, pivotValue);
        comparetmp[j + 2] = FixedLengthCompare<sortAscending>(v2, pivotValue);
        comparetmp[j + 3] = FixedLengthCompare<sortAscending>(v3, pivotValue);
        auto v4 = values[i - 4];
        auto v5 = values[i - 5];
        auto v6 = values[i - 6];
        auto v7 = values[i - 7];
        comparetmp[j + 4] = FixedLengthCompare<sortAscending>(v4, pivotValue);
        comparetmp[j + 5] = FixedLengthCompare<sortAscending>(v5, pivotValue);
        comparetmp[j + 6] = FixedLengthCompare<sortAscending>(v6, pivotValue);
        comparetmp[j + 7] = FixedLengthCompare<sortAscending>(v7, pivotValue);
        auto v8 = values[i - 8];
        auto v9 = values[i - 9];
        auto v10 = values[i - 10];
        auto v11 = values[i - 11];
        comparetmp[j + 8] = FixedLengthCompare<sortAscending>(v8, pivotValue);
        comparetmp[j + 9] = FixedLengthCompare<sortAscending>(v9, pivotValue);
        comparetmp[j + 10] = FixedLengthCompare<sortAscending>(v10, pivotValue);
        comparetmp[j + 11] = FixedLengthCompare<sortAscending>(v11, pivotValue);
    }
    for (; j < limit; --i, ++j) {
        auto v = values[i];
        comparetmp[j] = FixedLengthCompare<sortAscending>(v, pivotValue);
    }
    return comparetmp[0];
}

template <int32_t sortAscending>
void QuickSortFixedLengthInternal(int64_t *values, uint64_t *addresses, int32_t from, int32_t to, int8_t *comparetmp)
{
    int32_t len = to - from;
    if (len <= QUICK_SORT_SMALL_LEN) { // point 3
        QuickSortFixedLengthSmall<sortAscending>(values, addresses, from, to);
        return;
    }

    auto pivotValue = GetMedianFixedLengthValue(values, from, to, len);

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
        while (b <= c && (comparison = GetNextCompareFixedLengthLeft<sortAscending>(leftComparetmp, bk, blim, b, c, values,
                pivotValue)) <= 0) {
            if (UNLIKELY(comparison == 0)) {
                Swap(values, addresses, a++, b);
            }
            b++;
        }
        while (c >= b && (comparison = GetNextCompareFixedLengthRight<sortAscending>(rightComparetmp, ck, clim, b, c, values,
                pivotValue)) >= 0) {
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
        QuickSortFixedLengthInternal<sortAscending>(values, addresses, from, from + s, comparetmp);
    }
    if ((s = d - c) > 1) {
        QuickSortFixedLengthInternal<sortAscending>(values, addresses, n - s, n, comparetmp);
    }
}

void QuickSortFixedLengthAsc(int64_t *values, uint64_t *addresses, int32_t from, int32_t to)
{
    int8_t comparetmp[NMAX_SIZE + NMAX_SIZE];
    QuickSortFixedLengthInternal<1>(values, addresses, from, to, comparetmp);
}

void QuickSortFixedLengthDec(int64_t *values, uint64_t *addresses, int32_t from, int32_t to)
{
    int8_t comparetmp[NMAX_SIZE + NMAX_SIZE];
    QuickSortFixedLengthInternal<0>(values, addresses, from, to, comparetmp);
}
// end for compare scalar and simd test

template <typename RawType, bool hasNull, bool hasDictionary, bool sortNullFirst>
void SortNullAndGetValue(BaseVector **sortColumn, int64_t *values, std::vector<uint32_t> &varcharLength,
    uint64_t *addresses, int32_t &from, int32_t &to)
{
    int32_t nonNullFrom = from;
    int32_t nonNullTo = to;
    int32_t i = from;
    while (i < nonNullTo) {
        uint64_t encodedIndex = addresses[i];
        uint32_t vecBatchIdx = DecodeSliceIndex(encodedIndex);
        uint32_t rowIdx = DecodePosition(encodedIndex);
        auto column = sortColumn[vecBatchIdx];
        if constexpr (hasNull) {
            if (UNLIKELY(column->IsNull(rowIdx))) {
                // first, put all nulls at the first or at the last according sortNullFirst
                if constexpr (sortNullFirst) {
                    Swap(values, addresses, i++, nonNullFrom++); // [0, nonNullFrom)
                } else { // swap the last nonNull element back to i--,need to CHECK the new i-th element again!
                    Swap(values, addresses, i, --nonNullTo); // [i, nonNullTo)
                }
                continue;
            }
        }

        if constexpr (hasDictionary) {
            if (column->GetEncoding() == OMNI_DICTIONARY) {
                using DictionaryFlatVector = vec::Vector<DictionaryContainer<RawType>>;
                auto dictionaryVector = static_cast<DictionaryFlatVector *>(column);
                if constexpr (std::is_same_v<RawType, Decimal128>) {
                    RawType *valuePtr = unsafe::UnsafeDictionaryVector::GetDictionary(dictionaryVector);
                    int32_t originalRowIndex = unsafe::UnsafeDictionaryVector::GetIds(dictionaryVector)[rowIdx];
                    values[i] = reinterpret_cast<int64_t>(valuePtr + originalRowIndex);
                } else if constexpr (std::is_same_v<RawType, double>) {
                    double value = dictionaryVector->GetValue(rowIdx);
                    memcpy(values + i, &value, sizeof(double));
                } else {
                    values[i] = dictionaryVector->GetValue(rowIdx);
                }
            } else {
                using FlatVector = Vector<RawType>;
                if constexpr (std::is_same_v<RawType, Decimal128>) {
                    values[i] = reinterpret_cast<int64_t>(
                        unsafe::UnsafeVector::GetRawValues(static_cast<FlatVector *>(column)) + rowIdx);
                } else if constexpr (std::is_same_v<RawType, double>) {
                    double value = static_cast<FlatVector *>(column)->GetValue(rowIdx);
                    memcpy(values + i, &value, sizeof(double));
                } else {
                    values[i] = static_cast<FlatVector *>(column)->GetValue(rowIdx);
                }
            }
        } else {
            using FlatVector = Vector<RawType>;
            if constexpr (std::is_same_v<RawType, Decimal128>) {
                values[i] = reinterpret_cast<int64_t>(
                    unsafe::UnsafeVector::GetRawValues(static_cast<FlatVector *>(column)) + rowIdx);
            } else if constexpr (std::is_same_v<RawType, double>) {
                double value = static_cast<FlatVector *>(column)->GetValue(rowIdx);
                memcpy(values + i, &value, sizeof(double));
            } else {
                values[i] = static_cast<FlatVector *>(column)->GetValue(rowIdx);
            }
        }
        ++i;
    }
    from = nonNullFrom;
    to = nonNullTo;
}

template <bool hasNull, bool hasDictionary, bool sortNullFirst>
void SortNullAndGetVarcharValue(BaseVector **sortColumn, int64_t *values, std::vector<uint32_t> &varcharLength,
    uint64_t *addresses, int32_t &from, int32_t &to)
{
    int32_t nonNullFrom = from;
    int32_t nonNullTo = to;
    int32_t i = from;
    while (i < nonNullTo) {
        uint64_t encodedIndex = addresses[i];
        uint32_t vecBatchIdx = DecodeSliceIndex(encodedIndex);
        uint32_t rowIdx = DecodePosition(encodedIndex);
        auto column = sortColumn[vecBatchIdx];
        if constexpr (hasNull) {
            if (UNLIKELY(column->IsNull(rowIdx))) {
                // first, put all nulls at the first or at the last according sortNullFirst
                if constexpr (sortNullFirst) {
                    SwapVarchar(values, varcharLength, addresses, i++, nonNullFrom++); // [0, nonNullFrom)
                } else { // swap the last nonNull element back to i--,need to CHECK the new i-th element again!
                    SwapVarchar(values, varcharLength, addresses, i, --nonNullTo); // [i, nonNullTo)
                }
                continue;
            }
        }

        std::string_view value;
        if constexpr (hasDictionary) {
            if (column->GetEncoding() == OMNI_DICTIONARY) {
                value = static_cast<DictionaryVarcharVector *>(column)->GetValue(rowIdx);
            } else {
                value = static_cast<VarcharVector *>(column)->GetValue(rowIdx);
            }
        } else {
            value = static_cast<VarcharVector *>(column)->GetValue(rowIdx);
        }
        values[i] = reinterpret_cast<int64_t>(const_cast<char *>(value.data()));
        varcharLength[i] = value.length();
        ++i;
    }
    from = nonNullFrom;
    to = nonNullTo;
}

template <typename RawType>
void PagesIndex::ColumnarSort(const int32_t *sortCols, const int32_t *sortAscendings, const int32_t *sortNullFirsts,
    int32_t sortColCount, int64_t *values, std::vector<uint32_t> &varcharLength, int32_t from, int32_t to,
    int32_t currentCol)
{
    auto sortCol = sortCols[currentCol];
    auto sortAscending = sortAscendings[currentCol];
    auto sortColumn = columns[sortCol];
    auto hasNull = hasNulls[sortCol];
    auto hasDictionary = hasDictionaries[sortCol];
    int32_t nonNullFrom = from;
    int32_t nonNullTo = to; // [nonNullFrom, nonNullTo) mark the range of non-null values
    // we are going to sort one column, so we extract all the rows of the column between from and to
    if (sortNullFirsts[currentCol] == 0) {
        if (hasNull && hasDictionary) {
            SortNullAndGetValue<RawType, true, true, false>(sortColumn, values, varcharLength, valueAddresses,
                nonNullFrom, nonNullTo);
        } else if (hasNull) {
            SortNullAndGetValue<RawType, true, false, false>(sortColumn, values, varcharLength, valueAddresses,
                nonNullFrom, nonNullTo);
        } else if (hasDictionary) {
            SortNullAndGetValue<RawType, false, true, false>(sortColumn, values, varcharLength, valueAddresses,
                nonNullFrom, nonNullTo);
        } else {
            SortNullAndGetValue<RawType, false, false, false>(sortColumn, values, varcharLength, valueAddresses,
                nonNullFrom, nonNullTo);
        }
    } else {
        if (hasNull && hasDictionary) {
            SortNullAndGetValue<RawType, true, true, true>(sortColumn, values, varcharLength, valueAddresses,
                nonNullFrom, nonNullTo);
        } else if (hasNull) {
            SortNullAndGetValue<RawType, true, false, true>(sortColumn, values, varcharLength, valueAddresses,
                nonNullFrom, nonNullTo);
        } else if (hasDictionary) {
            SortNullAndGetValue<RawType, false, true, true>(sortColumn, values, varcharLength, valueAddresses,
                nonNullFrom, nonNullTo);
        } else {
            SortNullAndGetValue<RawType, false, false, true>(sortColumn, values, varcharLength, valueAddresses,
                nonNullFrom, nonNullTo);
        }
    }

    if (nonNullFrom + 1 < nonNullTo) {
        // second, sort all non-null values
        if constexpr (std::is_same_v<RawType, Decimal128>) {
            if (sortAscending == 0) {
                QuickSortDecimal128<0>(values, valueAddresses, nonNullFrom, nonNullTo);
            } else {
                QuickSortDecimal128<1>(values, valueAddresses, nonNullFrom, nonNullTo);
            }
        } else {
            if (sortAscending == 0) {
                QuickSortDescSIMD(values, valueAddresses, nonNullFrom, nonNullTo);
                // QuickSortFixedLength<0>(values, valueAddresses, nonNullFrom, nonNullTo);
            } else {
                QuickSortAscSIMD(values, valueAddresses, nonNullFrom, nonNullTo);
                // QuickSortFixedLength<1>(values, valueAddresses, nonNullFrom, nonNullTo);
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
        ColumnarSort(sortCols, sortAscendings, sortNullFirsts, sortColCount, values, varcharLength, from, nonNullFrom,
            nextCol);
    } else if (nonNullTo != to && nonNullTo + 1 < to) {
        ColumnarSort(sortCols, sortAscendings, sortNullFirsts, sortColCount, values, varcharLength, nonNullTo, to,
            nextCol);
    }

    if (nonNullFrom + 1 >= nonNullTo) {
        // if the non-null range has only one or zero element, the sorting is not required.
        return;
    }

    // fourth, divide ranges for non-null values first, and continue to sort the next column for each range
    int32_t start = nonNullFrom;
    if constexpr (std::is_same_v<RawType, Decimal128>) {
        auto currentValuePtr = values[nonNullFrom];
        auto currentValue = *((Decimal128 *)currentValuePtr);
        for (int32_t i = nonNullFrom + 1; i < nonNullTo; ++i) {
            auto valuePtr = values[i];
            auto value = *((Decimal128 *)valuePtr);
            if (value != currentValue) {
                currentValue = value;
                if (start + 1 != i) { // sort next column when |equivalent class| > 1
                    ColumnarSort(sortCols, sortAscendings, sortNullFirsts, sortColCount, values, varcharLength, start,
                        i, nextCol);
                }
                start = i;
            }
        }
    } else {
        RawType currentValue = values[nonNullFrom];
        for (int32_t i = nonNullFrom + 1; i < nonNullTo; ++i) {
            RawType value = values[i];
            if (!NewOnlyEqual<RawType>(value, currentValue) != 0) {
                currentValue = value;
                if (start + 1 != i) { // sort next column when |equivalent class| > 1
                    ColumnarSort(sortCols, sortAscendings, sortNullFirsts, sortColCount, values, varcharLength, start,
                        i, nextCol);
                }
                start = i;
            }
        }
    }

    if (start + 1 != nonNullTo) {
        ColumnarSort(sortCols, sortAscendings, sortNullFirsts, sortColCount, values, varcharLength, start, nonNullTo,
            nextCol);
    }
}

void PagesIndex::VarcharColumnarSort(const int32_t *sortCols, const int32_t *sortAscendings,
    const int32_t *sortNullFirsts, int32_t sortColCount, int64_t *values, std::vector<uint32_t> &varcharLength,
    int32_t from, int32_t to, int32_t currentCol)
{
    auto sortCol = sortCols[currentCol];
    auto sortAscending = sortAscendings[currentCol];
    auto sortColumn = columns[sortCol];
    auto hasNull = hasNulls[sortCol];
    auto hasDictionary = hasDictionaries[sortCol];
    int32_t nonNullFrom = from;
    int32_t nonNullTo = to; // [nonNullFrom, nonNullTo) mark the range of non-null values
    // we are going to sort one column, so we extract all the rows of the column between from and to
    if (sortNullFirsts[currentCol] == 0) {
        if (hasNull && hasDictionary) {
            SortNullAndGetVarcharValue<true, true, false>(sortColumn, values, varcharLength, valueAddresses,
                nonNullFrom, nonNullTo);
        } else if (hasNull) {
            SortNullAndGetVarcharValue<true, false, false>(sortColumn, values, varcharLength, valueAddresses,
                nonNullFrom, nonNullTo);
        } else if (hasDictionary) {
            SortNullAndGetVarcharValue<false, true, false>(sortColumn, values, varcharLength, valueAddresses,
                nonNullFrom, nonNullTo);
        } else {
            SortNullAndGetVarcharValue<false, false, false>(sortColumn, values, varcharLength, valueAddresses,
                nonNullFrom, nonNullTo);
        }
    } else {
        if (hasNull && hasDictionary) {
            SortNullAndGetVarcharValue<true, true, true>(sortColumn, values, varcharLength, valueAddresses, nonNullFrom,
                nonNullTo);
        } else if (hasNull) {
            SortNullAndGetVarcharValue<true, false, true>(sortColumn, values, varcharLength, valueAddresses,
                nonNullFrom, nonNullTo);
        } else if (hasDictionary) {
            SortNullAndGetVarcharValue<false, true, true>(sortColumn, values, varcharLength, valueAddresses,
                nonNullFrom, nonNullTo);
        } else {
            SortNullAndGetVarcharValue<false, false, true>(sortColumn, values, varcharLength, valueAddresses,
                nonNullFrom, nonNullTo);
        }
    }

    if (nonNullFrom + 1 < nonNullTo) {
        // second, sort all non-null values
        if (sortAscending == 0) {
            QuickSortVarChar<0>(values, varcharLength, valueAddresses, nonNullFrom, nonNullTo);
        } else {
            QuickSortVarChar<1>(values, varcharLength, valueAddresses, nonNullFrom, nonNullTo);
        }
    }

    if (currentCol == sortColCount - 1) {
        // currently the last column has been sorted.
        return;
    }

    // third, sort next column for the null range
    auto nextCol = currentCol + 1;
    if (nonNullFrom != from && from + 1 < nonNullFrom) {
        ColumnarSort(sortCols, sortAscendings, sortNullFirsts, sortColCount, values, varcharLength, from, nonNullFrom,
            nextCol);
    } else if (nonNullTo != to && nonNullTo + 1 < to) {
        ColumnarSort(sortCols, sortAscendings, sortNullFirsts, sortColCount, values, varcharLength, nonNullTo, to,
            nextCol);
    }

    if (nonNullFrom + 1 >= nonNullTo) {
        // if the non-null range has only one or zero element, the sorting is not required.
        return;
    }

    // fourth, divide ranges for non-null values first, and continue to sort the next column for each range
    int64_t currentValue = values[nonNullFrom];
    uint32_t currentLength = varcharLength[nonNullFrom];
    int32_t start = nonNullFrom;
    for (int32_t i = nonNullFrom + 1; i < nonNullTo; ++i) {
        int64_t value = values[i];
        uint32_t length = varcharLength[i];
        if (!NewOnlyEqualVarChar(value, length, currentValue, currentLength)) {
            currentValue = value;
            currentLength = length;
            if (start + 1 != i) { // sort next column when |equivalent class| > 1
                ColumnarSort(sortCols, sortAscendings, sortNullFirsts, sortColCount, values, varcharLength, start, i,
                    nextCol);
            }
            start = i;
        }
    }
    if (start + 1 != nonNullTo) {
        ColumnarSort(sortCols, sortAscendings, sortNullFirsts, sortColCount, values, varcharLength, start, nonNullTo,
            nextCol);
    }
}

void PagesIndex::ColumnarSort(const int32_t *sortCols, const int32_t *sortAscendings, const int32_t *sortNullFirsts,
    int32_t sortColCount, int64_t *values, std::vector<uint32_t> &varcharLength, int32_t from, int32_t to,
    int32_t currentCol)
{
    switch (dataTypes.GetType(sortCols[currentCol])->GetId()) {
        case OMNI_INT:
        case OMNI_DATE32: {
            ColumnarSort<int32_t>(sortCols, sortAscendings, sortNullFirsts, sortColCount, values, varcharLength, from,
                to, currentCol);
            break;
        }
        case OMNI_SHORT: {
            ColumnarSort<int16_t>(sortCols, sortAscendings, sortNullFirsts, sortColCount, values, varcharLength, from,
                to, currentCol);
            break;
        }
        case OMNI_LONG:
        case OMNI_DECIMAL64: {
            ColumnarSort<int64_t>(sortCols, sortAscendings, sortNullFirsts, sortColCount, values, varcharLength, from,
                to, currentCol);
            break;
        }
        case OMNI_DOUBLE: {
            ColumnarSort<double>(sortCols, sortAscendings, sortNullFirsts, sortColCount, values, varcharLength, from,
                to, currentCol);
            break;
        }
        case OMNI_BOOLEAN: {
            ColumnarSort<bool>(sortCols, sortAscendings, sortNullFirsts, sortColCount, values, varcharLength, from, to,
                currentCol);
            break;
        }
        case OMNI_VARCHAR:
        case OMNI_CHAR: {
            VarcharColumnarSort(sortCols, sortAscendings, sortNullFirsts, sortColCount, (int64_t *)values,
                varcharLength, from, to, currentCol);
            break;
        }
        case OMNI_DECIMAL128: {
            ColumnarSort<Decimal128>(sortCols, sortAscendings, sortNullFirsts, sortColCount, values, varcharLength,
                from, to, currentCol);
            break;
        }
        default:
            break;
    }
}

void PagesIndex::SortInplace(const int32_t *sortCols, const int32_t *sortAscendings, const int32_t *sortNullFirsts,
    int32_t sortColCount, int32_t from, int32_t to)
{
    DYNAMIC_TYPE_DISPATCH(SortInplace, dataTypes.GetIds()[sortCols[0]], sortAscendings[0], sortNullFirsts[0], from, to);
}

template <DataTypeId typeId>
void PagesIndex::SortInplace(int32_t sortAscending, int32_t sortNullFirst, int32_t from, int32_t to)
{
    using T = typename NativeType<typeId>::type;
    auto *values = reinterpret_cast<T *>(VectorHelper::UnsafeGetValues(inplaceSortColumn));
    if (sortAscending) {
        auto comp = [this](T &left, T &right) { return left < right; };
        if (sortNullFirst) {
            // null values have been preprocessed and can be skipped directly
            std::sort(values + totalNullCount + from, values + to, comp);
        } else {
            std::sort(values + from, values + to - totalNullCount, comp);
        }
    } else {
        auto comp = [this](T &left, T &right) { return left > right; };
        if (sortNullFirst) {
            // null values have been preprocessed and can be skipped directly
            std::sort(values + totalNullCount + from, values + to, comp);
        } else {
            std::sort(values + from, values + to - totalNullCount, comp);
        }
    }
}

void PagesIndex::Sort(const int32_t *sortCols, const int32_t *sortAscendings, const int32_t *sortNullFirsts,
    int32_t sortColCount, int32_t from, int32_t to)
{
    // for varchar/char or decimal128, the value is ptr set, but for other types, the value is raw value set
    std::vector<int64_t> values(this->positionCount);
    std::vector<uint32_t> varcharLength;
    bool hasVarCharCol = false;
    auto dataTypeIds = dataTypes.GetIds();
    for (int32_t i = 0; i < sortColCount; ++i) {
        auto sortColTypeId = dataTypeIds[sortCols[i]];
        if (sortColTypeId == OMNI_CHAR || sortColTypeId == OMNI_VARCHAR) {
            hasVarCharCol = true;
            break;
        }
    }
    if (hasVarCharCol) {
        varcharLength.resize(this->positionCount);
    }
    ColumnarSort(sortCols, sortAscendings, sortNullFirsts, sortColCount, values.data(), varcharLength, from, to, 0);
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
        bool hasDictionary = hasDictionaries[outputCol];
        bool hasNull = hasNulls[outputCol];
        switch (colTypeId) {
            case OMNI_INT:
            case OMNI_DATE32:
                outputVecBatch->Append(
                    ConstructVector<OMNI_INT>(vaStart, length, inputVecBatch, hasNull, hasDictionary));
                break;
            case OMNI_SHORT:
                outputVecBatch->Append(
                    ConstructVector<OMNI_SHORT>(vaStart, length, inputVecBatch, hasNull, hasDictionary));
                break;
            case OMNI_LONG:
            case OMNI_DECIMAL64:
                outputVecBatch->Append(
                    ConstructVector<OMNI_LONG>(vaStart, length, inputVecBatch, hasNull, hasDictionary));
                break;
            case OMNI_DOUBLE:
                outputVecBatch->Append(
                    ConstructVector<OMNI_DOUBLE>(vaStart, length, inputVecBatch, hasNull, hasDictionary));
                break;
            case OMNI_BOOLEAN:
                outputVecBatch->Append(
                    ConstructVector<OMNI_BOOLEAN>(vaStart, length, inputVecBatch, hasNull, hasDictionary));
                break;
            case OMNI_VARCHAR:
            case OMNI_CHAR: {
                outputVecBatch->Append(
                    ConstructVector<OMNI_VARCHAR>(vaStart, length, inputVecBatch, hasNull, hasDictionary));
                break;
            }
            case OMNI_DECIMAL128:
                outputVecBatch->Append(
                    ConstructVector<OMNI_DECIMAL128>(vaStart, length, inputVecBatch, hasNull, hasDictionary));
                break;
            default:
                break;
        }
    }
}

void PagesIndex::GetOutputInplaceSort(int32_t *outputCols, int32_t outputColsCount,
    omniruntime::vec::VectorBatch *outputVecBatch, const int32_t *sourceTypes, int32_t offset, int32_t length) const
{
    outputVecBatch->Append(VectorHelper::SliceVector(inplaceSortColumn, offset, length));
}

PagesIndex::~PagesIndex()
{
    Clear();
}

template <type::DataTypeId dataTypeId, bool hasNull, bool hasDictionary>
static ALWAYS_INLINE void SetValue(BaseVector *inputVector, int32_t inputIndex, BaseVector *outputVector,
    int32_t outputIndex)
{
    if constexpr (hasNull) {
        if (UNLIKELY(inputVector->IsNull(inputIndex))) {
            if constexpr (dataTypeId == OMNI_VARCHAR) {
                static_cast<VarcharVector *>(outputVector)->SetNull(outputIndex);
            } else {
                outputVector->SetNull(outputIndex);
            }
            return;
        }
    }

    using T = typename NativeType<dataTypeId>::type;
    T value;
    if constexpr (dataTypeId == OMNI_VARCHAR) {
        if constexpr (hasDictionary) {
            if (UNLIKELY(inputVector->GetEncoding() == OMNI_DICTIONARY)) {
                value = static_cast<DictionaryVarcharVector *>(inputVector)->GetValue(inputIndex);
            } else {
                value = static_cast<VarcharVector *>(inputVector)->GetValue(inputIndex);
            }
        } else {
            value = static_cast<VarcharVector *>(inputVector)->GetValue(inputIndex);
        }
        static_cast<VarcharVector *>(outputVector)->SetValue(outputIndex, value);
    } else {
        if constexpr (hasDictionary) {
            if (UNLIKELY(inputVector->GetEncoding() == OMNI_DICTIONARY)) {
                value = static_cast<Vector<DictionaryContainer<T>> *>(inputVector)->GetValue(inputIndex);
            } else {
                value = static_cast<Vector<T> *>(inputVector)->GetValue(inputIndex);
            }
        } else {
            value = static_cast<Vector<T> *>(inputVector)->GetValue(inputIndex);
        }
        static_cast<Vector<T> *>(outputVector)->SetValue(outputIndex, value);
    }
}

template <type::DataTypeId dataTypeId>
NO_INLINE BaseVector *ConstructVector(uint64_t *vaStart, int32_t length, BaseVector **inputVecBatch, bool hasNull,
    bool hasDictionary)
{
    BaseVector *outputVector = nullptr;
    if constexpr (dataTypeId == OMNI_VARCHAR) {
        outputVector = new VarcharVector(length);
    } else {
        using T = typename NativeType<dataTypeId>::type;
        outputVector = new Vector<T>(length);
    }

    int32_t outputIndex = 0;
    uint64_t *vaEnd = vaStart + length;
    if (hasNull && hasDictionary) {
        while (vaStart < vaEnd) { // here unroll is almost useless due to the excessive checks in SetValue
            uint64_t valueAddress = *(vaStart++);
            uint32_t pageIndex = DecodeSliceIndex(valueAddress);
            auto inputVector = inputVecBatch[pageIndex];
            uint32_t position = DecodePosition(valueAddress);
            SetValue<dataTypeId, true, true>(inputVector, static_cast<int32_t>(position), outputVector, outputIndex++);
        }
    } else if (hasNull) {
        while (vaStart < vaEnd) { // here unroll is almost useless due to the excessive checks in SetValue
            uint64_t valueAddress = *(vaStart++);
            uint32_t pageIndex = DecodeSliceIndex(valueAddress);
            auto inputVector = inputVecBatch[pageIndex];
            uint32_t position = DecodePosition(valueAddress);
            SetValue<dataTypeId, true, false>(inputVector, static_cast<int32_t>(position), outputVector, outputIndex++);
        }
    } else if (hasDictionary) {
        while (vaStart < vaEnd) { // here unroll is almost useless due to the excessive checks in SetValue
            uint64_t valueAddress = *(vaStart++);
            uint32_t pageIndex = DecodeSliceIndex(valueAddress);
            auto inputVector = inputVecBatch[pageIndex];
            uint32_t position = DecodePosition(valueAddress);
            SetValue<dataTypeId, false, true>(inputVector, static_cast<int32_t>(position), outputVector, outputIndex++);
        }
    } else {
        while (vaStart < vaEnd) { // here unroll is almost useless due to the excessive checks in SetValue
            uint64_t valueAddress = *(vaStart++);
            uint32_t pageIndex = DecodeSliceIndex(valueAddress);
            auto inputVector = inputVecBatch[pageIndex];
            uint32_t position = DecodePosition(valueAddress);
            SetValue<dataTypeId, false, false>(inputVector, static_cast<int32_t>(position), outputVector,
                outputIndex++);
        }
    }

    return outputVector;
}

void PagesIndex::GetSortedVecBatches(std::vector<int32_t> &outputCols, std::vector<VectorBatch *> &sortedVecBatches,
    bool canInplaceSort)
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
        if (!canInplaceSort) {
            GetOutput(outputCols.data(), outputColsCount, result, dataTypes.GetIds(), offset, rowCount);
        } else {
            GetOutputInplaceSort(outputCols.data(), outputColsCount, result, dataTypes.GetIds(), offset, rowCount);
        }
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
    if (inplaceSortColumn != nullptr) {
        delete inplaceSortColumn;
        inplaceSortColumn = nullptr;
    }
    positionCount = 0;
    totalNullCount = 0;
    VectorHelper::FreeVecBatches(inputVecBatches);
    inputVecBatches.clear();
}
}
