#pragma once

#ifdef OMNI_ENABLE_EXPERIMENTAL_SORT

#include "operator/pages_index.h"
#include "operator/timsort.h"
#include "operator/varchar_sort_policies.h"
#include "vector/vector_helper.h"

#include <algorithm>
#include <cstring>
#include <memory>
#include <vector>

#if defined(__x86_64__) || defined(_M_X64)
#include <immintrin.h>
#elif defined(__aarch64__) || defined(_M_ARM64)
#include <arm_neon.h>
#endif

namespace tipi {

using namespace omniruntime::vec;
using namespace omniruntime::type;

namespace detail {

constexpr uint32_t kSmallSize = 16;

template <typename T>
std::unique_ptr<T[]> AllocateBuffer(size_t count) {
    return std::make_unique<T[]>(count);
}

template <bool hasNull, bool nullFirst, bool asc>
ALWAYS_INLINE void PrepareDictionaryData(
    BaseVector **sortColumn, int32_t *ranks, uint64_t *addrs, int32_t &nonNullFrom,
    int32_t &nonNullTo, int32_t &maxRank) {
    SortNullAndGetDictRanks<hasNull, nullFirst, asc>(
        sortColumn, ranks, addrs, nonNullFrom, nonNullTo, maxRank);
}

ALWAYS_INLINE void PostProcessDictionaryData(
    BaseVector **sortColumn, int32_t sortColCount, int32_t currentCol, int64_t *values,
    uint32_t *varcharLength, uint64_t *addrs, int32_t nonNullFrom, int32_t nonNullTo) {
    if (currentCol >= sortColCount - 1) {
        return;
    }

    int32_t maxVecBatchIdx = -1;
    for (int32_t j = nonNullFrom; j < nonNullTo; ++j) {
        int32_t batchIdx = static_cast<int32_t>(addrs[j] >> 32);
        if (batchIdx > maxVecBatchIdx) {
            maxVecBatchIdx = batchIdx;
        }
    }

    if (maxVecBatchIdx >= 0) {
        struct ColumnMeta {
            omniruntime::vec::Encoding encoding;
            void *castedCol;
            std::string_view constValue;
        };

        std::vector<ColumnMeta> colMeta(maxVecBatchIdx + 1);

        for (int32_t b = 0; b <= maxVecBatchIdx; ++b) {
            auto *col = sortColumn[b];
            if (!col) {
                continue;
            }

            colMeta[b].encoding = col->GetEncoding();

            if (UNLIKELY(colMeta[b].encoding == omniruntime::vec::OMNI_ENCODING_CONST)) {
                colMeta[b].constValue =
                    static_cast<omniruntime::vec::ConstVector<std::string_view> *>(col)
                        ->GetConstValue();
                colMeta[b].castedCol = nullptr;
            } else if (colMeta[b].encoding == omniruntime::vec::OMNI_DICTIONARY) {
                colMeta[b].castedCol = static_cast<omniruntime::vec::Vector<
                    omniruntime::vec::DictionaryContainer<std::string_view>> *>(col);
            } else {
                colMeta[b].castedCol = static_cast<omniruntime::vec::Vector<
                    omniruntime::vec::LargeStringContainer<std::string_view>> *>(col);
            }
        }

        for (int32_t j = nonNullFrom; j < nonNullTo; ++j) {
            __builtin_prefetch(&addrs[j + 16], 0, 0);

            uint64_t encodedIndex = addrs[j];
            uint32_t vecBatchIdx = static_cast<uint32_t>(encodedIndex >> 32);
            uint32_t rowIdx = static_cast<uint32_t>(encodedIndex);

            const auto &meta = colMeta[vecBatchIdx];
            std::string_view value;

            if (meta.encoding == omniruntime::vec::OMNI_DICTIONARY) {
                value = static_cast<omniruntime::vec::Vector<
                    omniruntime::vec::DictionaryContainer<std::string_view>> *>(meta.castedCol)
                            ->GetValue(rowIdx);
            } else if (UNLIKELY(meta.encoding == omniruntime::vec::OMNI_ENCODING_CONST)) {
                value = meta.constValue;
            } else {
                value = static_cast<omniruntime::vec::Vector<
                    omniruntime::vec::LargeStringContainer<std::string_view>> *>(meta.castedCol)
                            ->GetValue(rowIdx);
            }

            values[j] = reinterpret_cast<int64_t>(const_cast<char *>(value.data()));
            varcharLength[j] = static_cast<uint32_t>(value.length());
        }
    }
}

}  // namespace detail

template <typename NormalSortPolicy, typename SmallSortPolicy, typename DictSortPolicy>
struct StringSortPipeline {
    static ALWAYS_INLINE void Sort(
        omniruntime::op::PagesIndex &pagesIndex, int64_t *values,
        std::vector<uint32_t> &varcharLength, const int32_t *sortCols,
        const int32_t *sortAscendings, const int32_t *sortNullFirsts, int32_t sortColCount,
        int32_t currentCol, int32_t &nonNullFrom, int32_t &nonNullTo) {
        const auto sortCol = sortCols[currentCol];
        const bool hasDict = pagesIndex.HasDictionary(sortCol);

        if (nonNullTo - nonNullFrom >= detail::kSmallSize && hasDict) {
            SortDictionary(
                pagesIndex, values, varcharLength, sortCols, sortAscendings, sortNullFirsts,
                sortColCount, currentCol, nonNullFrom, nonNullTo);
        } else {
            SortNormal(
                pagesIndex, values, varcharLength, sortCols, sortAscendings, sortNullFirsts,
                sortColCount, currentCol, nonNullFrom, nonNullTo);
        }
    }

private:
    static ALWAYS_INLINE void SortDictionary(
        omniruntime::op::PagesIndex &pagesIndex, int64_t *values,
        std::vector<uint32_t> &varcharLength, const int32_t *sortCols,
        const int32_t *sortAscendings, const int32_t *sortNullFirsts, int32_t sortColCount,
        int32_t currentCol, int32_t &nonNullFrom, int32_t &nonNullTo) {
        const auto sortCol = sortCols[currentCol];
        auto **sortColumn = pagesIndex.GetColumns()[sortCol];
        const bool hasNull = pagesIndex.HasNull(sortCol);
        const bool nullFirst = (sortNullFirsts[currentCol] == 1);
        const bool asc = (sortAscendings[currentCol] == 1);
        auto *addrs = pagesIndex.GetValueAddresses();

        auto ranks = detail::AllocateBuffer<int32_t>(nonNullTo - nonNullFrom);
        int32_t maxRank = 0;
        int32_t initialFrom = nonNullFrom;

        // Dispatch preparation based on null flags
        if (hasNull) {
            if (nullFirst) {
                if (asc) {
                    detail::PrepareDictionaryData<true, true, true>(
                        sortColumn, ranks.get() - initialFrom, addrs, nonNullFrom, nonNullTo,
                        maxRank);
                } else {
                    detail::PrepareDictionaryData<true, true, false>(
                        sortColumn, ranks.get() - initialFrom, addrs, nonNullFrom, nonNullTo,
                        maxRank);
                }
            } else {
                if (asc) {
                    detail::PrepareDictionaryData<true, false, true>(
                        sortColumn, ranks.get() - initialFrom, addrs, nonNullFrom, nonNullTo,
                        maxRank);
                } else {
                    detail::PrepareDictionaryData<true, false, false>(
                        sortColumn, ranks.get() - initialFrom, addrs, nonNullFrom, nonNullTo,
                        maxRank);
                }
            }
        } else {
            if (nullFirst) {
                if (asc) {
                    detail::PrepareDictionaryData<false, true, true>(
                        sortColumn, ranks.get() - initialFrom, addrs, nonNullFrom, nonNullTo,
                        maxRank);
                } else {
                    detail::PrepareDictionaryData<false, true, false>(
                        sortColumn, ranks.get() - initialFrom, addrs, nonNullFrom, nonNullTo,
                        maxRank);
                }
            } else {
                if (asc) {
                    detail::PrepareDictionaryData<false, false, true>(
                        sortColumn, ranks.get() - initialFrom, addrs, nonNullFrom, nonNullTo,
                        maxRank);
                } else {
                    detail::PrepareDictionaryData<false, false, false>(
                        sortColumn, ranks.get() - initialFrom, addrs, nonNullFrom, nonNullTo,
                        maxRank);
                }
            }
        }

        if (nonNullFrom + 1 < nonNullTo) {
            if (asc) {
                DictSortPolicy::template Sort<1>(
                    ranks.get() - initialFrom, addrs, nonNullFrom, nonNullTo, maxRank);
            } else {
                DictSortPolicy::template Sort<0>(
                    ranks.get() - initialFrom, addrs, nonNullFrom, nonNullTo, maxRank);
            }
        }

        detail::PostProcessDictionaryData(
            sortColumn, sortColCount, currentCol, values, varcharLength.data(), addrs, nonNullFrom,
            nonNullTo);
    }

    static ALWAYS_INLINE void SortNormal(
        omniruntime::op::PagesIndex &pagesIndex, int64_t *values,
        std::vector<uint32_t> &varcharLength, const int32_t *sortCols,
        const int32_t *sortAscendings, const int32_t *sortNullFirsts, int32_t sortColCount,
        int32_t currentCol, int32_t &nonNullFrom, int32_t &nonNullTo) {
        const bool asc = (sortAscendings[currentCol] == 1);
        auto *addrs = pagesIndex.GetValueAddresses();

        if (nonNullFrom + 1 < nonNullTo) {
            bool isSmall = (nonNullTo - nonNullFrom <= detail::kSmallSize);
            if (asc) {
                if (isSmall) {
                    SmallSortPolicy::template Sort<1>(
                        values, varcharLength.data(), addrs, nonNullFrom, nonNullTo);
                } else {
                    NormalSortPolicy::template Sort<1>(
                        values, varcharLength.data(), addrs, nonNullFrom, nonNullTo);
                }
            } else {
                if (isSmall) {
                    SmallSortPolicy::template Sort<0>(
                        values, varcharLength.data(), addrs, nonNullFrom, nonNullTo);
                } else {
                    NormalSortPolicy::template Sort<0>(
                        values, varcharLength.data(), addrs, nonNullFrom, nonNullTo);
                }
            }
        }
    }
};

ALWAYS_INLINE void SortWithTimSort(
    omniruntime::op::PagesIndex &pagesIndex, int64_t *values, std::vector<uint32_t> &varcharLength,
    const int32_t *sortCols, const int32_t *sortAscendings, const int32_t *sortNullFirsts,
    int32_t sortColCount, int32_t currentCol, int32_t &nonNullFrom, int32_t &nonNullTo) {
    StringSortPipeline<TimSortNormalPolicy, InsertionSortSmallPolicy, CountingSortDictPolicy>::Sort(
        pagesIndex, values, varcharLength, sortCols, sortAscendings, sortNullFirsts, sortColCount,
        currentCol, nonNullFrom, nonNullTo);
}

}  // namespace tipi

#endif  // OMNI_ENABLE_EXPERIMENTAL_SORT
