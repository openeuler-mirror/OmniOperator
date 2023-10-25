/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 * @Description: quick sort implementations using simd
 */

#ifndef QUICK_SORT_SIMD_H
#define QUICK_SORT_SIMD_H

#include <cstdint>

void QuickSortAscSIMD(int64_t *values, uint64_t *addresses, int32_t from, int32_t to);
void QuickSortDescSIMD(int64_t *values, uint64_t *addresses, int32_t from, int32_t to);

#endif // QUICK_SORT_SIMD_H
