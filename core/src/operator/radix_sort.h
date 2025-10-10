/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2024. All rights reserved.
 * @Description: sort implementations
 */
#ifndef RADIX_SORT_H
#define RADIX_SORT_H
#include <iostream>
#include <cstring>
#include <cstdint>
#include <array>
#include <vector>

using Data_type = uint8_t;
using DataPtr_type = uint8_t *;
constexpr uint32_t MSD_RADIX_SORT_SIZE_THRESHOLD = (1 << 20);
constexpr uint32_t VALUES_PER_RADIX = 256;
constexpr uint32_t LONG_NBYTES = 8;
constexpr uint32_t INT_NBYTES = 4;
constexpr uint32_t SHORT_NBYTES = 2;

template <bool sortAscending>
void SortWithRemainingBytes(const DataPtr_type origPtr, const DataPtr_type tempPtr, const uint32_t sortingSize,
    bool swap, uint32_t rowWidth, std::array<uint32_t, VALUES_PER_RADIX + 1> &locations);

// Textbook LSD radix sort
template <bool sortAscending>
void RadixSortLSD(const DataPtr_type &dataPtr, const DataPtr_type &tempPtr, const uint32_t len,
    const uint32_t &sortingSize, bool swap, uint32_t rowWidth)
{
    std::array<uint32_t, VALUES_PER_RADIX> counts = {};
    for (uint32_t r = 0; r < sortingSize; ++r) {
        counts.fill(0);
        const DataPtr_type sourcePtr = swap ? tempPtr : dataPtr;
        const DataPtr_type targetPtr = swap ? dataPtr : tempPtr;
        const uint32_t offset = r;
        // starting with from-th row
        DataPtr_type offsetPtr = sourcePtr + offset;
        for (uint32_t i = 0; i < len; ++i) {
            if constexpr (sortAscending) {
                ++counts[*offsetPtr];
            } else {
                ++counts[VALUES_PER_RADIX - 1 - *offsetPtr];
            }
            offsetPtr += rowWidth;
        }
        // Compute offsets from counts
        uint32_t maxCount = counts[0];
        for (uint32_t val = 1; val < VALUES_PER_RADIX; ++val) {
            maxCount = std::max(maxCount, counts[val]);
            counts[val] += counts[val - 1];
        }

        if (maxCount == len) {
            continue;
        }

        // starting with the last row
        DataPtr_type rowPtr = sourcePtr + (len - 1) * rowWidth;
        if constexpr (sortAscending) {
            for (uint32_t i = 0; i < len; ++i) {
                uint32_t &radixOffset = --counts[*(rowPtr + offset)];
                std::copy_n(rowPtr, rowWidth, targetPtr + radixOffset * rowWidth);
                rowPtr -= rowWidth;
            }
        } else {
            for (uint32_t i = 0; i < len; ++i) {
                uint32_t &radixOffset = --counts[VALUES_PER_RADIX - 1 - *(rowPtr + offset)];
                std::copy_n(rowPtr, rowWidth, targetPtr + radixOffset * rowWidth);
                rowPtr -= rowWidth;
            }
        }
        swap = !swap;
    }
    if (swap) {
        memcpy_s(dataPtr, len * rowWidth, tempPtr, len * rowWidth);
    }
}

// MSD radix sort that switches to LSD radix sort with low bucket sizes
template <bool sortAscending>
void RadixSortMSD(const DataPtr_type origPtr, const DataPtr_type tempPtr, const uint32_t len,
    const uint32_t sortingSize, bool swap, uint32_t rowWidth)
{
    std::array<uint32_t, VALUES_PER_RADIX + 1> locations = {};
    uint32_t *counts = locations.data() + 1;

    const DataPtr_type sourcePtr = swap ? tempPtr : origPtr;
    const DataPtr_type targetPtr = swap ? origPtr : tempPtr;
    // Collect locations
    const uint32_t msdOffset = sortingSize - 1;
    DataPtr_type offsetPtr = sourcePtr + msdOffset;
    if constexpr (sortAscending) {
        for (uint32_t i = 0; i < len; ++i) {
            ++counts[*offsetPtr];
            offsetPtr += rowWidth;
        }
    } else {
        for (uint32_t i = 0; i < len; ++i) {
            ++counts[VALUES_PER_RADIX - 1 - *offsetPtr];
            offsetPtr += rowWidth;
        }
    }
    // Compute locations from locations
    uint32_t maxCount = 0;
    for (uint32_t radix = 0; radix < VALUES_PER_RADIX; ++radix) {
        maxCount = std::max(maxCount, counts[radix]);
        counts[radix] += locations[radix];
    }

    if (maxCount != len) {
        // Re-order the data in temporary array
        DataPtr_type rowPtr = sourcePtr;
        if constexpr (sortAscending) {
            for (uint32_t i = 0; i < len; ++i) {
                const uint32_t radixOffset = locations[*(rowPtr + msdOffset)]++;
                std::copy_n(rowPtr, rowWidth, targetPtr + radixOffset * rowWidth);
                rowPtr += rowWidth;
            }
        } else {
            for (uint32_t i = 0; i < len; ++i) {
                const uint32_t &radixOffset = locations[VALUES_PER_RADIX - 1 - *(rowPtr + msdOffset)]++;
                std::copy_n(rowPtr, rowWidth, targetPtr + radixOffset * rowWidth);
                rowPtr += rowWidth;
            }
        }
        swap = !swap;
    }
    // Check if it has ended
    if (msdOffset == 0) {
        if (swap) {
            memcpy_s(origPtr, len * rowWidth, tempPtr, len * rowWidth);
        }
        return;
    }
    SortWithRemainingBytes<sortAscending>(origPtr, tempPtr, sortingSize, swap, rowWidth, locations);
}

template <bool sortAscending>
void SortWithRemainingBytes(const DataPtr_type origPtr, const DataPtr_type tempPtr, const uint32_t sortingSize,
    bool swap, uint32_t rowWidth, std::array<uint32_t, VALUES_PER_RADIX + 1> &locations)
{
    // sort with remaining bytes
    uint32_t radixCount = locations[0];
    uint32_t loc = 0;
    auto tmpSortingSize = sortingSize - 1;
    for (uint32_t radix = 0; radix < VALUES_PER_RADIX; ++radix) {
        if (radixCount > MSD_RADIX_SORT_SIZE_THRESHOLD) {
            RadixSortMSD<sortAscending>(origPtr + loc, tempPtr + loc, radixCount, tmpSortingSize, swap, rowWidth);
        } else if (radixCount > 1) {
            RadixSortLSD<sortAscending>(origPtr + loc, tempPtr + loc, radixCount, tmpSortingSize, swap, rowWidth);
        } else if (radixCount != 0) {
            if (swap) {
                std::copy_n(tempPtr + loc, rowWidth, origPtr + loc);
            }
        }
        radixCount = locations[radix + 1] - locations[radix];
        loc = locations[radix] * rowWidth;
    }
}
#endif // #ifndef RADIX_SORT_H
