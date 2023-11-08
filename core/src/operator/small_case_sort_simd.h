/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 * @Description: quick sort implementations using simd
 */


#ifndef SMALL_CASE_SORT_SIMD_H
#define SMALL_CASE_SORT_SIMD_H
#include <cstdint>
#include "arm_neon.h"
void SmallCaseSortAsec(int64_t *array, uint64_t *address, int32_t from, int32_t to);
void SmallCaseSortDesc(int64_t *array, uint64_t *address, int32_t from, int32_t to);
void SmallCaseSortWithoutAddressAsec(int64_t *array, int32_t from, int32_t to);
void SmallCaseSortWithoutAddressDesc(int64_t *array, int32_t from, int32_t to);
#endif // SMALL_CASE_SORT_SIMD_H
