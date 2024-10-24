/*
* Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
#ifndef OMNI_RUNTIME_QUICK_SORT_SIMD_CPP_H
#define OMNI_RUNTIME_QUICK_SORT_SIMD_CPP_H
#include <cstdint>
#include "simd/simd.h"
using namespace simd;
using namespace simd::detail;
// The address is 64 bits.
using AddrType = uint64_t;

template <class ValType> void QuickSortAscSIMD(ValType *values, AddrType *addresses, int32_t from, int32_t to);
template <class ValType> void QuickSortDescSIMD(ValType *values, AddrType *addresses, int32_t from, int32_t to);
// only for ut
template <class D, class Traits, class RawType>
void QuickSortInternalSIMD(D d, Traits st, RawType *values, AddrType *addresses, int32_t from, int32_t to,
    RawType *valueBuf, AddrType *addrBuf, bool chooseAvg = false, RawType avg = 0);
#endif // OMNI_RUNTIME_QUICK_SORT_SIMD_1_CPP_H
