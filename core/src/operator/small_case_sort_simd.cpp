/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 * @Description: quick sort implementations using simd
 */
#include "small_case_sort_simd.h"
#include <type_traits>
#include <limits>
#include <string>
#include "vector"
#include "util/omni_exception.h"
#include "util/compiler_util.h"

constexpr int64_t OMNIMAX64 = INT64_MAX;
constexpr int64_t OMNIMIN64 = INT64_MIN;
constexpr int64_t TWOOMNIMAX64[2] = {INT64_MAX, INT64_MAX};
constexpr int64_t TWOOMNIMIN64[2] = {INT64_MIN, INT64_MIN};
constexpr int32_t SORTDESCENDING = 0;
constexpr int32_t SORTASCENDING = 1;

template <class VType, int32_t SortingRule = SORTASCENDING>
static VType ALWAYS_INLINE Max64(const VType &a, const VType &b)
{
    if constexpr (SortingRule == SORTASCENDING) {
        if constexpr (std::is_same_v<VType, int64x2_t>) {
            uint64x2_t lessThan = vcltq_s64(a, b);
            return vbslq_s64(lessThan, b, a);
        } else {
            uint64x1_t lessThan = vclt_s64(a, b);
            return vbsl_s64(lessThan, b, a);
        }
    } else {
        if constexpr (std::is_same_v<VType, int64x2_t>) {
            uint64x2_t lessThan = vcltq_s64(a, b);
            return vbslq_s64(lessThan, a, b);
        } else {
            uint64x1_t lessThan = vclt_s64(a, b);
            return vbsl_s64(lessThan, a, b);
        }
    }
}
template <class VType, int32_t SortingRule = SORTASCENDING>
static VType ALWAYS_INLINE Min64(const VType &a, const VType &b)
{
    if constexpr (SortingRule == SORTASCENDING) {
        if constexpr (std::is_same_v<VType, int64x2_t>) {
            uint64x2_t lessThan = vcltq_s64(a, b);
            return vbslq_s64(lessThan, a, b);
        } else {
            uint64x1_t lessThan = vclt_s64(a, b);
            return vbsl_s64(lessThan, a, b);
        }
    } else {
        if constexpr (std::is_same_v<VType, int64x2_t>) {
            uint64x2_t lessThan = vcltq_s64(a, b);
            return vbslq_s64(lessThan, b, a);
        } else {
            uint64x1_t lessThan = vclt_s64(a, b);
            return vbsl_s64(lessThan, b, a);
        }
    }
}

template <class VType, int32_t SortRule> static void ALWAYS_INLINE Sort2(VType &a, VType &b)
{
    VType copyA = a;
    a = Min64<VType, SortRule>(a, b);
    b = Max64<VType, SortRule>(copyA, b);
}
__extension__ extern __inline uint8x16_t __attribute__((__always_inline__, __gnu_inline__, __artificial__))
RevertU8x16(uint8x16_t __a)
{
    return __builtin_shuffle(__a, (uint8x16_t){ 8, 9, 10, 11, 12, 13, 14, 15, 0, 1, 2, 3, 4, 5, 6, 7 });
}

template <class VType, class AddrType, int32_t SortRule>
static void ALWAYS_INLINE Sort2WithAddr(VType &a, VType &b, AddrType &c, AddrType &d)
{
    VType copyA = a;
    VType copyB = b;
    a = Min64<VType, SortRule>(a, b);
    b = Max64<VType, SortRule>(copyA, b);

    if constexpr (std::is_same_v<VType, int64x1_t>) {
        if (!vget_lane_u64(vceq_s64(a, copyA), 0)) {
            AddrType copyD = d;
            d = c;
            c = copyD;
        }
    } else {
        if (vgetq_lane_s64(a, 0) == vgetq_lane_s64(copyB, 0)) {
            uint64_t tempC0 = vgetq_lane_u64(c, 0);
            c = vsetq_lane_u64(vgetq_lane_u64(d, 0), c, 0);
            d = vsetq_lane_u64(tempC0, d, 0);
        }
        if (vgetq_lane_s64(a, 1) == vgetq_lane_s64(copyB, 1)) {
            uint64_t tempC1 = vgetq_lane_u64(c, 1);
            c = vsetq_lane_u64(vgetq_lane_u64(d, 1), c, 1);
            d = vsetq_lane_u64(tempC1, d, 1);
        }
    }
}

static int64x2_t ALWAYS_INLINE ReverseKeys2(int64x2_t &v)
{
    uint8x16_t copyV = vreinterpretq_u8_s64(v);
    return vreinterpretq_s64_u8(RevertU8x16(copyV));
}

static uint64x2_t ALWAYS_INLINE ReverseAddrs2(uint64x2_t &v)
{
    uint8x16_t copyV = vreinterpretq_u8_u64(v);
    return vreinterpretq_u64_u8(RevertU8x16(copyV));
}

template <int32_t SortRule> static int64x2_t ALWAYS_INLINE SortPair64(int64x2_t a, uint64x2_t &addr)
{
    int64x2_t copyA = a;
    int64x2_t swappedA = ReverseKeys2(a);
    Sort2<int64x2_t, SortRule>(a, swappedA);
    int64x2_t c = vzip1q_s64(a, swappedA);

    if (vgetq_lane_s64(a, 0) != vgetq_lane_s64(copyA, 0)) {
        addr = ReverseAddrs2(addr);
    }
    return c;
}

template <class VType> static VType ALWAYS_INLINE LoadDdata(int64_t *keys)
{
    if constexpr (std::is_same_v<VType, int64x1_t>) {
        return vld1_s64(keys);
    } else if constexpr (std::is_same_v<VType, int64x2_t>) {
        return vld1q_s64(keys);
    }
}

template <class VType> static VType ALWAYS_INLINE LoadAddrDdata(uint64_t *addr)
{
    if constexpr (std::is_same_v<VType, uint64x1_t>) {
        return vld1_u64(addr);
    } else if constexpr (std::is_same_v<VType, uint64x2_t>) {
        return vld1q_u64(addr);
    }
}

template <class VType, class AddrType, int32_t SortRule>
static void ALWAYS_INLINE Sort8(VType &v0, VType &v1, VType &v2, VType &v3, VType &v4, VType &v5, VType &v6, VType &v7,
    AddrType &vAddr0, AddrType &vAddr1, AddrType &vAddr2, AddrType &vAddr3, AddrType &vAddr4, AddrType &vAddr5,
    AddrType &vAddr6, AddrType &vAddr7)
{
    Sort2WithAddr<VType, AddrType, SortRule>(v0, v2, vAddr0, vAddr2);
    Sort2WithAddr<VType, AddrType, SortRule>(v1, v3, vAddr1, vAddr3);
    Sort2WithAddr<VType, AddrType, SortRule>(v4, v6, vAddr4, vAddr6);
    Sort2WithAddr<VType, AddrType, SortRule>(v5, v7, vAddr5, vAddr7);

    Sort2WithAddr<VType, AddrType, SortRule>(v0, v4, vAddr0, vAddr4);
    Sort2WithAddr<VType, AddrType, SortRule>(v1, v5, vAddr1, vAddr5);
    Sort2WithAddr<VType, AddrType, SortRule>(v2, v6, vAddr2, vAddr6);
    Sort2WithAddr<VType, AddrType, SortRule>(v3, v7, vAddr3, vAddr7);

    Sort2WithAddr<VType, AddrType, SortRule>(v0, v1, vAddr0, vAddr1);
    Sort2WithAddr<VType, AddrType, SortRule>(v2, v3, vAddr2, vAddr3);
    Sort2WithAddr<VType, AddrType, SortRule>(v4, v5, vAddr4, vAddr5);
    Sort2WithAddr<VType, AddrType, SortRule>(v6, v7, vAddr6, vAddr7);

    Sort2WithAddr<VType, AddrType, SortRule>(v2, v4, vAddr2, vAddr4);
    Sort2WithAddr<VType, AddrType, SortRule>(v3, v5, vAddr3, vAddr5);

    Sort2WithAddr<VType, AddrType, SortRule>(v1, v4, vAddr1, vAddr4);
    Sort2WithAddr<VType, AddrType, SortRule>(v3, v6, vAddr3, vAddr6);

    Sort2WithAddr<VType, AddrType, SortRule>(v1, v2, vAddr1, vAddr2);
    Sort2WithAddr<VType, AddrType, SortRule>(v3, v4, vAddr3, vAddr4);
    Sort2WithAddr<VType, AddrType, SortRule>(v5, v6, vAddr5, vAddr6);
}

template <int32_t SortRule>
static void ALWAYS_INLINE Merge8x2(int64x2_t &v0, int64x2_t &v1, int64x2_t &v2, int64x2_t &v3, int64x2_t &v4,
    int64x2_t &v5, int64x2_t &v6, int64x2_t &v7, uint64x2_t &vAddr0, uint64x2_t &vAddr1, uint64x2_t &vAddr2,
    uint64x2_t &vAddr3, uint64x2_t &vAddr4, uint64x2_t &vAddr5, uint64x2_t &vAddr6, uint64x2_t &vAddr7)
{
    v7 = ReverseKeys2(v7);
    vAddr7 = ReverseAddrs2(vAddr7);
    v6 = ReverseKeys2(v6);
    vAddr6 = ReverseAddrs2(vAddr6);
    v5 = ReverseKeys2(v5);
    vAddr5 = ReverseAddrs2(vAddr5);
    v4 = ReverseKeys2(v4);
    vAddr4 = ReverseAddrs2(vAddr4);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v0, v7, vAddr0, vAddr7);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v1, v6, vAddr1, vAddr6);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v2, v5, vAddr2, vAddr5);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v3, v4, vAddr3, vAddr4);

    v3 = ReverseKeys2(v3);
    vAddr3 = ReverseAddrs2(vAddr3);
    v2 = ReverseKeys2(v2);
    vAddr2 = ReverseAddrs2(vAddr2);
    v7 = ReverseKeys2(v7);
    vAddr7 = ReverseAddrs2(vAddr7);
    v6 = ReverseKeys2(v6);
    vAddr6 = ReverseAddrs2(vAddr6);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v0, v3, vAddr0, vAddr3);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v1, v2, vAddr1, vAddr2);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v4, v7, vAddr4, vAddr7);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v5, v6, vAddr5, vAddr6);

    v1 = ReverseKeys2(v1);
    vAddr1 = ReverseAddrs2(vAddr1);
    v3 = ReverseKeys2(v3);
    vAddr3 = ReverseAddrs2(vAddr3);
    v5 = ReverseKeys2(v5);
    vAddr5 = ReverseAddrs2(vAddr5);
    v7 = ReverseKeys2(v7);
    vAddr7 = ReverseAddrs2(vAddr7);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v0, v1, vAddr0, vAddr1);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v2, v3, vAddr2, vAddr3);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v4, v5, vAddr4, vAddr5);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v6, v7, vAddr6, vAddr7);

    v0 = SortPair64<SortRule>(v0, vAddr0);
    v1 = SortPair64<SortRule>(v1, vAddr1);
    v2 = SortPair64<SortRule>(v2, vAddr2);
    v3 = SortPair64<SortRule>(v3, vAddr3);
    v4 = SortPair64<SortRule>(v4, vAddr4);
    v5 = SortPair64<SortRule>(v5, vAddr5);
    v6 = SortPair64<SortRule>(v6, vAddr6);
    v7 = SortPair64<SortRule>(v7, vAddr7);
}

template <class VType, int32_t SortRule>
static void ALWAYS_INLINE Sort8Rows(int64_t *array, uint64_t *arrayAddr, int32_t dataLen)
{
    if constexpr (std::is_same_v<VType, int64x2_t>) {
        int32_t i = 0, index = 0;
        int64x2_t v[8];
        uint64x2_t vAddr[8];
        for (; index + 2 <= dataLen; i++, index += 2) {
            v[i] = vld1q_s64(array + index);
            vAddr[i] = vld1q_u64(arrayAddr + index);
        }
        //        Consider whether it is a multiple of 2
        if (dataLen % 2 != 0) {
            v[i] = vsetq_lane_s64(*(array + (index)), v[i], 0);
            vAddr[i] = vsetq_lane_u64(*(arrayAddr + (index)), vAddr[i], 0);

            if constexpr (SortRule == SORTDESCENDING) {
                v[i] = vsetq_lane_s64(OMNIMIN64, v[i], 1);
                for (int32_t p = 7; p > i; p--) {
                    v[p] = vld1q_dup_s64(TWOOMNIMIN64);
                }
            } else if constexpr (SortRule == SORTASCENDING) {
                v[i] = vsetq_lane_s64(OMNIMAX64, v[i], 1);
                for (int32_t p = 7; p > i; p--) {
                    v[p] = vld1q_dup_s64(TWOOMNIMAX64);
                }
            }
        } else {
            if constexpr (SortRule == SORTDESCENDING) {
                for (int32_t p = 7; p >= i; p--) {
                    v[p] = vld1q_dup_s64(TWOOMNIMIN64);
                }
            } else if constexpr (SortRule == SORTASCENDING) {
                for (int32_t p = 7; p >= i; p--) {
                    v[p] = vld1q_dup_s64(TWOOMNIMAX64);
                }
            }
        }
        Sort8<int64x2_t, uint64x2_t, SortRule>(v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7], vAddr[0], vAddr[1],
            vAddr[2], vAddr[3], vAddr[4], vAddr[5], vAddr[6], vAddr[7]);
        Merge8x2<SortRule>(v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7], vAddr[0], vAddr[1], vAddr[2], vAddr[3],
            vAddr[4], vAddr[5], vAddr[6], vAddr[7]);
        for (i = 0, index = 0; index + 2 <= dataLen; index += 2, i++) {
            vst1q_s64(array + index, v[i]);
            vst1q_u64(arrayAddr + index, vAddr[i]);
        }
        if (dataLen % 2 != 0) {
            *(array + index) = vgetq_lane_s64(v[i], 0);
            *(arrayAddr + index) = vgetq_lane_u64(vAddr[i], 0);
        }
    } else if constexpr (std::is_same_v<VType, int64x1_t>) {
        int64x1_t v[8];
        uint64x1_t vAddr[8];
        for (size_t i = 0; i < dataLen; i++) {
            v[i] = vld1_s64(array + i);
            vAddr[i] = LoadAddrDdata<uint64x1_t>(arrayAddr + i);
        }
        if constexpr (SortRule == SORTASCENDING) {
            for (size_t i = 7; i >= dataLen; i--) {
                v[i] = vld1_s64(&OMNIMAX64);
            }
        } else if constexpr (SortRule == SORTDESCENDING) {
            for (size_t i = 7; i >= dataLen; i--) {
                v[i] = vld1_s64(&OMNIMIN64);
            }
        }

        Sort8<int64x1_t, uint64x1_t, SortRule>(v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7], vAddr[0], vAddr[1],
            vAddr[2], vAddr[3], vAddr[4], vAddr[5], vAddr[6], vAddr[7]);
        for (int32_t i = 0; i < dataLen; i++) {
            vst1_s64(array + i, v[i]);
            vst1_u64(arrayAddr + i, vAddr[i]);
        }
    }
}
template <int32_t SortRule>
static void ALWAYS_INLINE Sort16(int64x2_t &v0, int64x2_t &v1, int64x2_t &v2, int64x2_t &v3, int64x2_t &v4,
    int64x2_t &v5, int64x2_t &v6, int64x2_t &v7, int64x2_t &v8, int64x2_t &v9, int64x2_t &va, int64x2_t &vb,
    int64x2_t &vc, int64x2_t &vd, int64x2_t &ve, int64x2_t &vf, uint64x2_t &vAddr0, uint64x2_t &vAddr1,
    uint64x2_t &vAddr2, uint64x2_t &vAddr3, uint64x2_t &vAddr4, uint64x2_t &vAddr5, uint64x2_t &vAddr6,
    uint64x2_t &vAddr7, uint64x2_t &vAddr8, uint64x2_t &vAddr9, uint64x2_t &vAddra, uint64x2_t &vAddrb,
    uint64x2_t &vAddrc, uint64x2_t &vAddrd, uint64x2_t &vAddre, uint64x2_t &vAddrf)
{
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v0, v1, vAddr0, vAddr1);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v2, v3, vAddr2, vAddr3);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v4, v5, vAddr4, vAddr5);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v6, v7, vAddr6, vAddr7);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v8, v9, vAddr8, vAddr9);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(va, vb, vAddra, vAddrb);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(vc, vd, vAddrc, vAddrd);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(ve, vf, vAddre, vAddrf);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v0, v2, vAddr0, vAddr2);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v1, v3, vAddr1, vAddr3);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v4, v6, vAddr4, vAddr6);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v5, v7, vAddr5, vAddr7);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v8, va, vAddr8, vAddra);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v9, vb, vAddr9, vAddrb);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(vc, ve, vAddrc, vAddre);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(vd, vf, vAddrd, vAddrf);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v0, v4, vAddr0, vAddr4);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v1, v5, vAddr1, vAddr5);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v2, v6, vAddr2, vAddr6);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v3, v7, vAddr3, vAddr7);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v8, vc, vAddr8, vAddrc);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v9, vd, vAddr9, vAddrd);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(va, ve, vAddra, vAddre);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(vb, vf, vAddrb, vAddrf);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v0, v8, vAddr0, vAddr8);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v1, v9, vAddr1, vAddr9);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v2, va, vAddr2, vAddra);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v3, vb, vAddr3, vAddrb);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v4, vc, vAddr4, vAddrc);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v5, vd, vAddr5, vAddrd);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v6, ve, vAddr6, vAddre);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v7, vf, vAddr7, vAddrf);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v5, va, vAddr5, vAddra);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v6, v9, vAddr6, vAddr9);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v3, vc, vAddr3, vAddrc);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v7, vb, vAddr7, vAddrb);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(vd, ve, vAddrd, vAddre);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v4, v8, vAddr4, vAddr8);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v1, v2, vAddr1, vAddr2);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v1, v4, vAddr1, vAddr4);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v7, vd, vAddr7, vAddrd);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v2, v8, vAddr2, vAddr8);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(vb, ve, vAddrb, vAddre);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v2, v4, vAddr2, vAddr4);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v5, v6, vAddr5, vAddr6);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v9, va, vAddr9, vAddra);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(vb, vd, vAddrb, vAddrd);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v3, v8, vAddr3, vAddr8);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v7, vc, vAddr7, vAddrc);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v3, v5, vAddr3, vAddr5);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v6, v8, vAddr6, vAddr8);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v7, v9, vAddr7, vAddr9);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(va, vc, vAddra, vAddrc);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v3, v4, vAddr3, vAddr4);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v5, v6, vAddr5, vAddr6);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v7, v8, vAddr7, vAddr8);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v9, va, vAddr9, vAddra);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(vb, vc, vAddrb, vAddrc);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v6, v7, vAddr6, vAddr7);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v8, v9, vAddr8, vAddr9);
}

template <int32_t SortRule>
static void ALWAYS_INLINE Merge16x2(int64x2_t &v0, int64x2_t &v1, int64x2_t &v2, int64x2_t &v3, int64x2_t &v4,
    int64x2_t &v5, int64x2_t &v6, int64x2_t &v7, int64x2_t &v8, int64x2_t &v9, int64x2_t &va, int64x2_t &vb,
    int64x2_t &vc, int64x2_t &vd, int64x2_t &ve, int64x2_t &vf, uint64x2_t &vAddr0, uint64x2_t &vAddr1,
    uint64x2_t &vAddr2, uint64x2_t &vAddr3, uint64x2_t &vAddr4, uint64x2_t &vAddr5, uint64x2_t &vAddr6,
    uint64x2_t &vAddr7, uint64x2_t &vAddr8, uint64x2_t &vAddr9, uint64x2_t &vAddra, uint64x2_t &vAddrb,
    uint64x2_t &vAddrc, uint64x2_t &vAddrd, uint64x2_t &vAddre, uint64x2_t &vAddrf)
{
    vf = ReverseKeys2(vf);
    vAddrf = ReverseAddrs2(vAddrf);
    ve = ReverseKeys2(ve);
    vAddre = ReverseAddrs2(vAddre);
    vd = ReverseKeys2(vd);
    vAddrd = ReverseAddrs2(vAddrd);
    vc = ReverseKeys2(vc);
    vAddrc = ReverseAddrs2(vAddrc);
    vb = ReverseKeys2(vb);
    vAddrb = ReverseAddrs2(vAddrb);
    va = ReverseKeys2(va);
    vAddra = ReverseAddrs2(vAddra);
    v9 = ReverseKeys2(v9);
    vAddr9 = ReverseAddrs2(vAddr9);
    v8 = ReverseKeys2(v8);
    vAddr8 = ReverseAddrs2(vAddr8);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v0, vf, vAddr0, vAddrf);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v1, ve, vAddr1, vAddre);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v2, vd, vAddr2, vAddrd);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v3, vc, vAddr3, vAddrc);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v4, vb, vAddr4, vAddrb);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v5, va, vAddr5, vAddra);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v6, v9, vAddr6, vAddr9);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v7, v8, vAddr7, vAddr8);
    v7 = ReverseKeys2(v7);
    vAddr7 = ReverseAddrs2(vAddr7);
    v6 = ReverseKeys2(v6);
    vAddr6 = ReverseAddrs2(vAddr6);
    v5 = ReverseKeys2(v5);
    vAddr5 = ReverseAddrs2(vAddr5);
    v4 = ReverseKeys2(v4);
    vAddr4 = ReverseAddrs2(vAddr4);
    vf = ReverseKeys2(vf);
    vAddrf = ReverseAddrs2(vAddrf);
    ve = ReverseKeys2(ve);
    vAddre = ReverseAddrs2(vAddre);
    vd = ReverseKeys2(vd);
    vAddrd = ReverseAddrs2(vAddrd);
    vc = ReverseKeys2(vc);
    vAddrc = ReverseAddrs2(vAddrc);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v0, v7, vAddr0, vAddr7);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v1, v6, vAddr1, vAddr6);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v2, v5, vAddr2, vAddr5);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v3, v4, vAddr3, vAddr4);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v8, vf, vAddr8, vAddrf);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v9, ve, vAddr9, vAddre);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(va, vd, vAddra, vAddrd);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(vb, vc, vAddrb, vAddrc);
    v3 = ReverseKeys2(v3);
    vAddr3 = ReverseAddrs2(vAddr3);
    v2 = ReverseKeys2(v2);
    vAddr2 = ReverseAddrs2(vAddr2);
    v7 = ReverseKeys2(v7);
    vAddr7 = ReverseAddrs2(vAddr7);
    v6 = ReverseKeys2(v6);
    vAddr6 = ReverseAddrs2(vAddr6);
    vb = ReverseKeys2(vb);
    vAddrb = ReverseAddrs2(vAddrb);
    va = ReverseKeys2(va);
    vAddra = ReverseAddrs2(vAddra);
    vf = ReverseKeys2(vf);
    vAddrf = ReverseAddrs2(vAddrf);
    ve = ReverseKeys2(ve);
    vAddre = ReverseAddrs2(vAddre);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v0, v3, vAddr0, vAddr3);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v1, v2, vAddr1, vAddr2);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v4, v7, vAddr4, vAddr7);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v5, v6, vAddr5, vAddr6);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v8, vb, vAddr8, vAddrb);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v9, va, vAddr9, vAddra);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(vc, vf, vAddrc, vAddrf);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(vd, ve, vAddrd, vAddre);
    v1 = ReverseKeys2(v1);
    vAddr1 = ReverseAddrs2(vAddr1);
    v3 = ReverseKeys2(v3);
    vAddr3 = ReverseAddrs2(vAddr3);
    v5 = ReverseKeys2(v5);
    vAddr5 = ReverseAddrs2(vAddr5);
    v7 = ReverseKeys2(v7);
    vAddr7 = ReverseAddrs2(vAddr7);
    v9 = ReverseKeys2(v9);
    vAddr9 = ReverseAddrs2(vAddr9);
    vb = ReverseKeys2(vb);
    vAddrb = ReverseAddrs2(vAddrb);
    vd = ReverseKeys2(vd);
    vAddrd = ReverseAddrs2(vAddrd);
    vf = ReverseKeys2(vf);
    vAddrf = ReverseAddrs2(vAddrf);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v0, v1, vAddr0, vAddr1);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v2, v3, vAddr2, vAddr3);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v4, v5, vAddr4, vAddr5);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v6, v7, vAddr6, vAddr7);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v8, v9, vAddr8, vAddr9);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(va, vb, vAddra, vAddrb);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(vc, vd, vAddrc, vAddrd);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(ve, vf, vAddre, vAddrf);

    v0 = SortPair64<SortRule>(v0, vAddr0);
    v1 = SortPair64<SortRule>(v1, vAddr1);
    v2 = SortPair64<SortRule>(v2, vAddr2);
    v3 = SortPair64<SortRule>(v3, vAddr3);
    v4 = SortPair64<SortRule>(v4, vAddr4);
    v5 = SortPair64<SortRule>(v5, vAddr5);
    v6 = SortPair64<SortRule>(v6, vAddr6);
    v7 = SortPair64<SortRule>(v7, vAddr7);
    v8 = SortPair64<SortRule>(v8, vAddr8);
    v9 = SortPair64<SortRule>(v9, vAddr9);
    va = SortPair64<SortRule>(va, vAddra);
    vb = SortPair64<SortRule>(vb, vAddrb);
    vc = SortPair64<SortRule>(vc, vAddrc);
    vd = SortPair64<SortRule>(vd, vAddrd);
    ve = SortPair64<SortRule>(ve, vAddre);
    vf = SortPair64<SortRule>(vf, vAddrf);
}

template <int32_t SortRule> static void ALWAYS_INLINE Sort16Rows(int64_t *array, uint64_t *arrayAddr, int32_t dataLen)
{
    int32_t i = 0, index = 0;
    int64x2_t v[16];
    uint64x2_t vAddr[16];
    for (; index + 2 <= dataLen; i++, index += 2) {
        v[i] = vld1q_s64(array + index);
        vAddr[i] = vld1q_u64(arrayAddr + index);
    }
    //        Consider whether it is a multiple of 2
    if (dataLen % 2 != 0) {
        v[i] = vsetq_lane_s64(*(array + (index)), v[i], 0);
        vAddr[i] = vsetq_lane_u64(*(arrayAddr + (index)), vAddr[i], 0);

        if constexpr (SortRule == SORTASCENDING) {
            v[i] = vsetq_lane_s64(OMNIMAX64, v[i], 1);
            for (int32_t p = 15; p > i; p--) {
                v[p] = vld1q_dup_s64(TWOOMNIMAX64);
            }
        } else if constexpr (SortRule == SORTDESCENDING) {
            v[i] = vsetq_lane_s64(OMNIMIN64, v[i], 1);
            for (int32_t p = 15; p > i; p--) {
                v[p] = vld1q_dup_s64(TWOOMNIMIN64);
            }
        }
    } else {
        if constexpr (SortRule == SORTASCENDING) {
            for (int32_t p = 15; p >= i; p--) {
                v[p] = vld1q_dup_s64(TWOOMNIMAX64);
            }
        } else if constexpr (SortRule == SORTDESCENDING) {
            for (int32_t p = 15; p >= i; p--) {
                v[p] = vld1q_dup_s64(TWOOMNIMIN64);
            }
        }
    }
    Sort16<SortRule>(v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7], v[8], v[9], v[10], v[11], v[12], v[13], v[14],
        v[15], vAddr[0], vAddr[1], vAddr[2], vAddr[3], vAddr[4], vAddr[5], vAddr[6], vAddr[7], vAddr[8], vAddr[9],
        vAddr[10], vAddr[11], vAddr[12], vAddr[13], vAddr[14], vAddr[15]);
    Merge16x2<SortRule>(v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7], v[8], v[9], v[10], v[11], v[12], v[13], v[14],
        v[15], vAddr[0], vAddr[1], vAddr[2], vAddr[3], vAddr[4], vAddr[5], vAddr[6], vAddr[7], vAddr[8], vAddr[9],
        vAddr[10], vAddr[11], vAddr[12], vAddr[13], vAddr[14], vAddr[15]);
    for (i = 0, index = 0; index + 2 <= dataLen; index += 2, i++) {
        vst1q_s64(array + index, v[i]);
        vst1q_u64(arrayAddr + index, vAddr[i]);
    }
    if (dataLen % 2 != 0) {
        *(array + index) = vgetq_lane_s64(v[i], 0);
        *(arrayAddr + index) = vgetq_lane_u64(vAddr[i], 0);
    }
}

template <int32_t SortRule> static void ALWAYS_INLINE Sort2Rows(int64_t *array, uint64_t *arrayAddr, int32_t dataLen)
{
    int64x1_t v0 = LoadDdata<int64x1_t>(array);
    int64x1_t v1 = LoadDdata<int64x1_t>(array + 1);
    uint64x1_t vAddr0 = vld1_u64(arrayAddr);
    uint64x1_t vAddr1 = vld1_u64(arrayAddr + 1);
    Sort2WithAddr<int64x1_t, uint64x1_t, SortRule>(v0, v1, vAddr0, vAddr1);
    vst1_s64(array, v0);
    vst1_s64(array + 1, v1);
    vst1_u64(arrayAddr, vAddr0);
    vst1_u64(arrayAddr + 1, vAddr1);
}

template <class VType, class AddrType, int32_t SortRule>
static void ALWAYS_INLINE Sort4(VType &v0, VType &v1, VType &v2, VType &v3, AddrType &vAddr0, AddrType &vAddr1,
    AddrType &vAddr2, AddrType &vAddr3)
{
    Sort2WithAddr<VType, AddrType, SortRule>(v0, v2, vAddr0, vAddr2);
    Sort2WithAddr<VType, AddrType, SortRule>(v1, v3, vAddr1, vAddr3);
    Sort2WithAddr<VType, AddrType, SortRule>(v0, v1, vAddr0, vAddr1);
    Sort2WithAddr<VType, AddrType, SortRule>(v2, v3, vAddr2, vAddr3);
    Sort2WithAddr<VType, AddrType, SortRule>(v1, v2, vAddr1, vAddr2);
}

template <int32_t SortRule>
static void ALWAYS_INLINE Merge4x2(int64x2_t &v0, int64x2_t &v1, int64x2_t &v2, int64x2_t &v3, uint64x2_t &vAddr0,
    uint64x2_t &vAddr1, uint64x2_t &vAddr2, uint64x2_t &vAddr3)
{
    v3 = ReverseKeys2(v3);
    vAddr3 = ReverseAddrs2(vAddr3);
    v2 = ReverseKeys2(v2);
    vAddr2 = ReverseAddrs2(vAddr2);

    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v0, v3, vAddr0, vAddr3);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v1, v2, vAddr1, vAddr2);

    v1 = ReverseKeys2(v1);
    vAddr1 = ReverseAddrs2(vAddr1);
    v3 = ReverseKeys2(v3);
    vAddr3 = ReverseAddrs2(vAddr3);

    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v0, v1, vAddr0, vAddr1);
    Sort2WithAddr<int64x2_t, uint64x2_t, SortRule>(v2, v3, vAddr2, vAddr3);

    v0 = SortPair64<SortRule>(v0, vAddr0);
    v1 = SortPair64<SortRule>(v1, vAddr1);
    v2 = SortPair64<SortRule>(v2, vAddr2);
    v3 = SortPair64<SortRule>(v3, vAddr3);
}

template <int32_t SortRule> static int64x2_t ALWAYS_INLINE SortPair64WithoutAddr(int64x2_t a)
{
    int64x2_t swappedA = ReverseKeys2(a);
    Sort2<int64x2_t, SortRule>(a, swappedA);
    int64x2_t c =vzip1q_s64(a, swappedA);
    return c;
}

template <int32_t SortRule> static void ALWAYS_INLINE Merge3x2(int64x2_t &v0, int64x2_t &v1, int64x2_t &v2)
{
    v2 = ReverseKeys2(v2);
    Sort2<int64x2_t, SortRule>(v1, v2);
    v1 = ReverseKeys2(v1);
    Sort2<int64x2_t, SortRule>(v0, v1);
    v0 = SortPair64WithoutAddr<SortRule>(v0);
    v1 = SortPair64WithoutAddr<SortRule>(v1);
    v2 = SortPair64WithoutAddr<SortRule>(v2);
}

template <int32_t SortRule> static void ALWAYS_INLINE Sort3(int64x2_t &v0, int64x2_t &v1, int64x2_t &v2)
{
    Sort2<int64x2_t, SortRule>(v0, v2);
    Sort2<int64x2_t, SortRule>(v0, v1);
    Sort2<int64x2_t, SortRule>(v1, v2);
}

template <int32_t SortRule> static void ALWAYS_INLINE Sort3Rows(int64_t *array, int32_t dataLen)
{
    int32_t i = 0, index = 0;
    int64x2_t v[3];
    for (; index + 2 <= dataLen; i++, index += 2) {
        v[i] = vld1q_s64(array + index);
    }
    Sort3<SortRule>(v[0], v[1], v[2]);
    Merge3x2<SortRule>(v[0], v[1], v[2]);
    for (i = 0, index = 0; index + 2 <= dataLen; index += 2, i++) {
        vst1q_s64(array + index, v[i]);
    }
}

template <class VType, int32_t SortRule>
static void ALWAYS_INLINE Sort4Rows(int64_t *array, uint64_t *arrayAddr, int32_t dataLen)
{
    if constexpr (std::is_same_v<VType, int64x1_t>) {
        int64x1_t v0 = LoadDdata<int64x1_t>(array);
        int64x1_t v1 = LoadDdata<int64x1_t>(array + 1);
        int64x1_t v2 = LoadDdata<int64x1_t>(array + 2);
        int64x1_t v3;
        uint64x1_t v0Addr = LoadAddrDdata<uint64x1_t>(arrayAddr);
        uint64x1_t v1Addr = LoadAddrDdata<uint64x1_t>(arrayAddr + 1);
        uint64x1_t v2Addr = LoadAddrDdata<uint64x1_t>(arrayAddr + 2);
        uint64x1_t v3Addr;
        if (dataLen == 3) {
            if constexpr (SortRule == SORTASCENDING) {
                v3 = vset_lane_s64(INT64_MAX, v3, 0);
            } else if constexpr (SortRule == SORTDESCENDING) {
                v3 = vset_lane_s64(INT64_MIN, v3, 0);
            }
        } else {
            v3 = LoadDdata<int64x1_t>(array + 3);
            v3Addr = LoadAddrDdata<uint64x1_t>(arrayAddr + 3);
        }
        Sort4<int64x1_t, uint64x1_t, SortRule>(v0, v1, v2, v3, v0Addr, v1Addr, v2Addr, v3Addr);
        vst1_s64(array, v0);
        vst1_s64(array + 1, v1);
        vst1_s64(array + 2, v2);
        vst1_u64(arrayAddr, v0Addr);
        vst1_u64(arrayAddr + 1, v1Addr);
        vst1_u64(arrayAddr + 2, v2Addr);
        if (dataLen == 4) {
            vst1_s64(array + 3, v3);
            vst1_u64(arrayAddr + 3, v3Addr);
        }
    } else if constexpr (std::is_same_v<VType, int64x2_t>) {
        int32_t i = 0, index = 0;
        int64x2_t v[4];
        uint64x2_t vAddr[4];
        for (; index + 2 <= dataLen; i++, index += 2) {
            v[i] = vld1q_s64(array + index);
            vAddr[i] = vld1q_u64(arrayAddr + index);
        }
        if (dataLen % 2 != 0) {
            v[i] = vsetq_lane_s64(*(array + (index)), v[i], 0);
            vAddr[i] = vsetq_lane_u64(*(arrayAddr + (index)), vAddr[i], 0);
            if constexpr (SortRule == SORTDESCENDING) {
                v[i] = vsetq_lane_s64(OMNIMIN64, v[i], 1);
                for (int32_t p = 3; p > i; p--) {
                    v[p] = vld1q_dup_s64(TWOOMNIMIN64);
                }
            } else if constexpr (SortRule == SORTASCENDING) {
                v[i] = vsetq_lane_s64(OMNIMAX64, v[i], 1);
                for (int32_t p = 3; p > i; p--) {
                    v[p] = vld1q_dup_s64(TWOOMNIMAX64);
                }
            }
        } else {
            if constexpr (SortRule == SORTDESCENDING) {
                for (int32_t p = 3; p >= i; p--) {
                    v[p] = vld1q_dup_s64(TWOOMNIMIN64);
                }
            } else if constexpr (SortRule == SORTASCENDING) {
                for (int32_t p = 3; p >= i; p--) {
                    v[p] = vld1q_dup_s64(TWOOMNIMAX64);
                }
            }
        }

        Sort4<int64x2_t, uint64x2_t, SortRule>(v[0], v[1], v[2], v[3], vAddr[0], vAddr[1], vAddr[2], vAddr[3]);
        Merge4x2<SortRule>(v[0], v[1], v[2], v[3], vAddr[0], vAddr[1], vAddr[2], vAddr[3]);
        for (i = 0, index = 0; index + 2 <= dataLen; index += 2, i++) {
            vst1q_s64(array + index, v[i]);
            vst1q_u64(arrayAddr + index, vAddr[i]);
        }
        if (dataLen % 2 != 0) {
            *(array + index) = vgetq_lane_s64(v[i], 0);
            *(arrayAddr + index) = vgetq_lane_u64(vAddr[i], 0);
        }
    }
}

static uint32_t ALWAYS_INLINE GenFuncId(uint32_t arrLen)
{
    return 32 - __builtin_clz(arrLen - 1);
}

using SmallCaseSortFunc = void (*)(int64_t *, uint64_t *, int32_t);

static std::vector<SmallCaseSortFunc> SmallCaseSortFuncsAsec = { nullptr,
                                                                 Sort2Rows<SORTASCENDING>,
                                                                 Sort4Rows<int64x1_t, SORTASCENDING>,
                                                                 Sort4Rows<int64x2_t, SORTASCENDING>,
                                                                 Sort8Rows<int64x2_t, SORTASCENDING>,
                                                                 Sort16Rows<SORTASCENDING> };
static std::vector<SmallCaseSortFunc> SmallCaseSortFuncsDesc = { nullptr,
                                                                 Sort2Rows<SORTDESCENDING>,
                                                                 Sort4Rows<int64x1_t, SORTDESCENDING>,
                                                                 Sort4Rows<int64x2_t, SORTDESCENDING>,
                                                                 Sort8Rows<int64x2_t, SORTDESCENDING>,
                                                                 Sort16Rows<SORTDESCENDING> };

// This interface is used when the array length is 6.
void SmallCaseSortWithoutAddressAsec(int64_t *array, int32_t from, int32_t to)
{
    Sort3Rows<1>(array + from, 6);
}

void SmallCaseSortWithoutAddressDesc(int64_t *array, int32_t from, int32_t to)
{
    Sort3Rows<0>(array + from, 6);
}

void SmallCaseSortAsec(int64_t *array, uint64_t *address, int32_t from, int32_t to)
{
    int32_t dataLen = to - from;
    if (dataLen <= 1) {
        return;
    }
    uint32_t funcId = GenFuncId(dataLen);
    SmallCaseSortFuncsAsec[funcId](array + from, address + from, dataLen);
}

void SmallCaseSortDesc(int64_t *array, uint64_t *address, int32_t from, int32_t to)
{
    int32_t dataLen = to - from;
    if (dataLen <= 1) {
        return;
    }
    uint32_t funcId = GenFuncId(dataLen);
    SmallCaseSortFuncsDesc[funcId](array + from, address + from, dataLen);
}