/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 * @Description: quick sort implementations using simd
 */

#ifndef QUICK_SORT_SIMD_H
#define QUICK_SORT_SIMD_H

#include <cstdint>

void QuickSortAscSIMD(int64_t *values, uint64_t *addresses, int32_t from, int32_t to);
void QuickSortDescSIMD(int64_t *values, uint64_t *addresses, int32_t from, int32_t to);
void QuickSortDoubleAscSIMD(int64_t *values, uint64_t *addresses, int32_t from, int32_t to);
void QuickSortDoubleDescSIMD(int64_t *values, uint64_t *addresses, int32_t from, int32_t to);

// only for ut test
template <typename RawType, int32_t sortAscending>
void QuickSortInternalSIMD(int64_t *values, uint64_t *addresses, int32_t from, int32_t to, int64_t *valueBuf,
    uint64_t *addrBuf, bool chooseAvg = false, RawType avg = 0);

#endif // QUICK_SORT_SIMD_H
