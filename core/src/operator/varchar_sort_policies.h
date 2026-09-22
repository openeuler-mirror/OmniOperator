#pragma once

#ifdef OMNI_ENABLE_EXPERIMENTAL_SORT

#include "operator/pages_index.h"
#include "operator/timsort.h"
#include "vector/vector_helper.h"

#include <algorithm>
#include <cstring>
#include <memory>
#include <vector>

namespace tipi {

using namespace omniruntime::vec;
using namespace omniruntime::type;

namespace detail {

#if defined(__BYTE_ORDER__) && defined(__ORDER_LITTLE_ENDIAN__)
constexpr bool kIsLittleEndian = (__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__);
#elif defined(_WIN32) || defined(_WIN64)
constexpr bool kIsLittleEndian = true;
#else
#error "Can't determine Endianness of the system in compile-time"
#endif

struct TimSortStringItem {
    uint64_t prefix;
    uint32_t index;
    uint32_t len;
};

ALWAYS_INLINE int32_t FallbackCompare(const char *a, const char *b, uint32_t len) {
    int res = memcmp(a, b, len);
    return res == 0 ? 0 : (res < 0 ? -1 : 1);
}

}  // namespace detail

template <bool IsAscending>
struct TimSortVarcharComparator {
    const int64_t *const values_;

    explicit TimSortVarcharComparator(const int64_t *values) : values_(values) {
    }

    ALWAYS_INLINE bool operator()(
        const detail::TimSortStringItem &a, const detail::TimSortStringItem &b) const noexcept {
        if (LIKELY(a.prefix != b.prefix)) {
            if constexpr (IsAscending) {
                return a.prefix < b.prefix;
            } else {
                return a.prefix > b.prefix;
            }
        }

        if (a.len != b.len) {
            uint32_t minLen = std::min(a.len, b.len);
            if (minLen <= 8) {
                if constexpr (IsAscending) {
                    return a.len < b.len;
                } else {
                    return a.len > b.len;
                }
            }
        } else if (a.len <= 8) {
            return false;
        }

        return SlowPath(a, b);
    }

private:
    __attribute__((cold, noinline)) bool SlowPath(
        const detail::TimSortStringItem &a, const detail::TimSortStringItem &b) const noexcept {
        const char *const aPtr = reinterpret_cast<const char *>(values_[a.index]);
        const char *const bPtr = reinterpret_cast<const char *>(values_[b.index]);

        __builtin_prefetch(aPtr, 0, 1);
        __builtin_prefetch(bPtr, 0, 1);

        uint32_t minLen = std::min(a.len, b.len);

        int32_t res;
        res = detail::FallbackCompare(aPtr + 8, bPtr + 8, minLen - 8);

        if (res == 0) {
            if (a.len == b.len) {
                return false;
            }
            res = a.len > b.len ? 1 : -1;
        }
        if constexpr (IsAscending) {
            return res < 0;
        } else {
            return res > 0;
        }
    }
};

template <int32_t sortAscending>
ALWAYS_INLINE int32_t CompareVarChar(
    int64_t leftValue, uint32_t leftLength, int64_t rightValue, uint32_t rightLength) {
    if (leftValue == rightValue && leftLength == rightLength) {
        return 0;
    }
    if constexpr (sortAscending == 1) {
        int32_t result = detail::FallbackCompare(
            reinterpret_cast<const char *>(leftValue), reinterpret_cast<const char *>(rightValue),
            std::min(leftLength, rightLength));
        if (result != 0) {
            return result;
        }
        return leftLength > rightLength ? 1 : (leftLength == rightLength ? 0 : -1);
    } else {
        int32_t result = detail::FallbackCompare(
            reinterpret_cast<const char *>(rightValue), reinterpret_cast<const char *>(leftValue),
            std::min(rightLength, leftLength));
        if (result != 0) {
            return result;
        }
        return rightLength > leftLength ? 1 : (leftLength == rightLength ? 0 : -1);
    }
}

template <int32_t sortAscending>
void InsertionSortVarChar(
    int64_t *values, uint32_t *varcharLength, uint64_t *addresses, int32_t from, int32_t to) {
    for (int32_t i = from + 1; i < to; ++i) {
        const int64_t iPtr = values[i];
        const uint32_t iLength = varcharLength[i];
        const uint64_t iAddr = addresses[i];
        int32_t j = i - 1;
        while (j >= from &&
               (CompareVarChar<sortAscending>(values[j], varcharLength[j], iPtr, iLength) > 0)) {
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
void TimSortVarChar(
    int64_t *values, uint32_t *varcharLength, uint64_t *valueAddresses, int32_t from, int32_t to) {
    int32_t count = to - from;
    if (count <= 1) {
        return;
    }

    struct OriginalDataItem {
        int64_t value;
        uint32_t length;
        uint64_t address;
    };

    size_t wordsForSortBuffer = (count * sizeof(detail::TimSortStringItem) + 7) / 8;
    size_t wordsForOriginalData = (count * sizeof(OriginalDataItem) + 7) / 8;

    std::unique_ptr<uint64_t[]> memBuffer(new uint64_t[wordsForSortBuffer + wordsForOriginalData]);

    auto sortBuffer = reinterpret_cast<detail::TimSortStringItem *>(memBuffer.get());
    auto originalData = reinterpret_cast<OriginalDataItem *>(memBuffer.get() + wordsForSortBuffer);

    for (int32_t i = 0; i < count; ++i) {
        int32_t absoluteIdx = from + i;
        const char *str = reinterpret_cast<const char *>(values[absoluteIdx]);
        uint32_t len = varcharLength[absoluteIdx];

        uint64_t raw_prefix = 0;
        if (LIKELY(len >= 8)) {
            std::copy(str, str + 8, reinterpret_cast<char *>(&raw_prefix));
        } else if (len > 0) {
            std::copy(str, str + len, reinterpret_cast<char *>(&raw_prefix));
        }
        uint64_t prefix = detail::kIsLittleEndian ? __builtin_bswap64(raw_prefix) : raw_prefix;

        sortBuffer[i] = {prefix, static_cast<uint32_t>(absoluteIdx), len};

        originalData[i] = {
          values[absoluteIdx], varcharLength[absoluteIdx], valueAddresses[absoluteIdx]};
    }

    TimSortVarcharComparator<sortAscending == 1> comp(values);
    timsort::timsort(sortBuffer, sortBuffer + count, comp);

    for (int32_t i = 0; i < count; ++i) {
        int32_t originalDataIdx = sortBuffer[i].index - from;

        const auto &item = originalData[originalDataIdx];

        values[from + i] = item.value;
        varcharLength[from + i] = item.length;
        valueAddresses[from + i] = item.address;
    }
}

struct DictStringInfo {
    std::string_view str;
    int32_t batchIdx;
    int32_t dictId;
};

ALWAYS_INLINE void SwapRank(int32_t *ranks, uint64_t *addresses, int32_t a, int32_t b) {
    std::swap(ranks[a], ranks[b]);
    std::swap(addresses[a], addresses[b]);
}

template <bool hasNull, bool sortNullFirst, bool sortAscending>
void SortNullAndGetDictRanks(
    omniruntime::vec::BaseVector **sortColumn, int32_t *ranks, uint64_t *addresses, int32_t &from,
    int32_t &to, int32_t &outMaxRank) {
    using namespace omniruntime::vec;
    int32_t nonNullFrom = from;
    int32_t nonNullTo = to;
    int32_t i = from;

    int32_t maxVecBatchIdx = -1;
    for (int32_t idx = from; idx < to; ++idx) {
        uint32_t vecBatchIdx = (uint32_t)(addresses[idx] >> 32);
        if ((int32_t)vecBatchIdx > maxVecBatchIdx) {
            maxVecBatchIdx = vecBatchIdx;
        }
    }
    if (maxVecBatchIdx == -1) {
        from = nonNullFrom;
        to = nonNullTo;
        outMaxRank = 0;
        return;
    }
    int32_t vecBatchCount = maxVecBatchIdx + 1;

    std::vector<std::vector<int32_t>> rankMap(vecBatchCount);
    std::vector<DictStringInfo> uniqueStrings;
    uniqueStrings.reserve(1024 * vecBatchCount);

    for (int32_t batchIdx = 0; batchIdx < vecBatchCount; ++batchIdx) {
        auto column = sortColumn[batchIdx];
        if (!column) {
            continue;
        }

        if (column->GetEncoding() == omniruntime::vec::OMNI_DICTIONARY) {
            auto dictCol = static_cast<omniruntime::vec::Vector<
                omniruntime::vec::DictionaryContainer<std::string_view>> *>(column);
            int32_t *dictIds = static_cast<int32_t *>(
                omniruntime::vec::VectorHelper::UnsafeGetValuesDictionary(column));
            int32_t batchSize = column->GetSize();

            int32_t maxDictId = -1;
            for (int32_t r = 0; r < batchSize; ++r) {
                if (dictIds[r] > maxDictId) {
                    maxDictId = dictIds[r];
                }
            }

            rankMap[batchIdx].assign(maxDictId + 1, -1);

            for (int32_t r = 0; r < batchSize; ++r) {
                int32_t dictId = dictIds[r];
                if (dictId >= 0 && rankMap[batchIdx][dictId] == -1) {
                    rankMap[batchIdx][dictId] = 0;
                    uniqueStrings.push_back({dictCol->GetValue(r), batchIdx, dictId});
                }
            }
        } else if (column->GetEncoding() == omniruntime::vec::OMNI_ENCODING_CONST) {
            std::string_view value =
                static_cast<omniruntime::vec::ConstVector<std::string_view> *>(column)
                    ->GetConstValue();
            uniqueStrings.push_back({value, batchIdx, 0});
            rankMap[batchIdx].assign(1, 0);
        } else {
            auto flatCol = static_cast<omniruntime::vec::Vector<
                omniruntime::vec::LargeStringContainer<std::string_view>> *>(column);
            int32_t batchSize = column->GetSize();
            rankMap[batchIdx].assign(batchSize, -1);
            for (int32_t r = 0; r < batchSize; ++r) {
                if (!column->IsNull(r)) {
                    rankMap[batchIdx][r] = 0;
                    uniqueStrings.push_back({flatCol->GetValue(r), batchIdx, r});
                }
            }
        }
    }

    // Rank 0 is always the lexicographically smallest unique string. Sort
    // direction is applied later by CountingSortDictRanks; ranking in DESC
    // order here as well would reverse the output twice.
    timsort::timsort(
        uniqueStrings.begin(), uniqueStrings.end(),
        [](const DictStringInfo &a, const DictStringInfo &b) { return a.str < b.str; });

    int32_t currentRank = 0;
    for (size_t idx = 0; idx < uniqueStrings.size(); ++idx) {
        if (idx > 0 && uniqueStrings[idx].str != uniqueStrings[idx - 1].str) {
            currentRank++;
        }
        rankMap[uniqueStrings[idx].batchIdx][uniqueStrings[idx].dictId] = currentRank;
    }

    outMaxRank = currentRank;

    while (i < nonNullTo) {
        uint64_t encodedIndex = addresses[i];
        uint32_t vecBatchIdx = (uint32_t)(encodedIndex >> 32);
        uint32_t rowIdx = (uint32_t)encodedIndex;
        auto column = sortColumn[vecBatchIdx];

        if (UNLIKELY(column->GetEncoding() == OMNI_ENCODING_CONST)) {
            ranks[i] = rankMap[vecBatchIdx][0];
            ++i;
            continue;
        }

        if constexpr (hasNull) {
            if (UNLIKELY(column->IsNull(rowIdx))) {
                if constexpr (sortNullFirst) {
                    SwapRank(ranks, addresses, i++, nonNullFrom++);
                } else {
                    SwapRank(ranks, addresses, i, --nonNullTo);
                }
                continue;
            }
        }

        int32_t dictId = 0;
        if (column->GetEncoding() == OMNI_DICTIONARY) {
            int32_t *dictIds =
                static_cast<int32_t *>(VectorHelper::UnsafeGetValuesDictionary(column));
            dictId = dictIds[rowIdx];
        } else {
            dictId = rowIdx;
        }

        ranks[i] = rankMap[vecBatchIdx][dictId];
        ++i;
    }

    from = nonNullFrom;
    to = nonNullTo;
}

template <int32_t sortAscending>
void CountingSortDictRanks(
    int32_t *ranks, uint64_t *valueAddresses, int32_t from, int32_t to, int32_t maxRank) {
    int32_t count = to - from;
    if (count <= 1) {
        return;
    }

    std::vector<int32_t> hist(maxRank + 1, 0);
    for (int32_t i = 0; i < count; ++i) {
        hist[ranks[from + i]]++;
    }

    std::vector<int32_t> offsets(maxRank + 1, 0);
    int32_t currentOffset = 0;
    if constexpr (sortAscending == 1) {
        for (int32_t i = 0; i <= maxRank; ++i) {
            offsets[i] = currentOffset;
            currentOffset += hist[i];
        }
    } else {
        for (int32_t i = maxRank; i >= 0; --i) {
            offsets[i] = currentOffset;
            currentOffset += hist[i];
        }
    }

    std::unique_ptr<uint64_t[]> sortedAddresses(new uint64_t[count]);
    for (int32_t i = 0; i < count; ++i) {
        int32_t rank = ranks[from + i];
        int32_t pos = offsets[rank]++;
        sortedAddresses[pos] = valueAddresses[from + i];
    }

    std::copy(sortedAddresses.get(), sortedAddresses.get() + count, valueAddresses + from);
}

struct TimSortNormalPolicy {
    template <int32_t sortAscending>
    static ALWAYS_INLINE void Sort(
        int64_t *values, uint32_t *varcharLength, uint64_t *valueAddresses, int32_t from,
        int32_t to) {
        TimSortVarChar<sortAscending>(values, varcharLength, valueAddresses, from, to);
    }
};

struct InsertionSortSmallPolicy {
    template <int32_t sortAscending>
    static ALWAYS_INLINE void Sort(
        int64_t *values, uint32_t *varcharLength, uint64_t *valueAddresses, int32_t from,
        int32_t to) {
        InsertionSortVarChar<sortAscending>(values, varcharLength, valueAddresses, from, to);
    }
};

struct CountingSortDictPolicy {
    template <int32_t sortAscending>
    static ALWAYS_INLINE void Sort(
        int32_t *ranks, uint64_t *valueAddresses, int32_t from, int32_t to, int32_t maxRank) {
        CountingSortDictRanks<sortAscending>(ranks, valueAddresses, from, to, maxRank);
    }
};

}  // namespace tipi

#endif  // OMNI_ENABLE_EXPERIMENTAL_SORT
