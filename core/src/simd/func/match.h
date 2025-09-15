/*
* Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
#ifndef OMNI_OMNI_MATCH_FUNC_H
#define OMNI_OMNI_MATCH_FUNC_H

#include "simd/simd.h"

using namespace simd;

constexpr uint64_t kMasks[] = {18446744073709551600UL, 18446744073709551360UL, 18446744073709547520UL,
                               18446744073709486080UL, 18446744073708503040UL, 18446744073692774400UL,
                               18446744073441116160UL, 18446744069414584320UL, 18446744004990074880UL,
                               18446742974197923840UL, 18446726481523507200UL, 18446462598732840960UL,
                               18442240474082181120UL, 18374686479671623680UL, 17293822569102704640UL, 0};

template <typename T>
OMNI_INLINE static unsigned FindFirstSetNonZero(T mask)
{
    if (sizeof(mask) == sizeof(unsigned)) {
        return __builtin_ctz(static_cast<unsigned>(mask));
    } else {
        return __builtin_ctzll(mask);
    }
}

class SparseMaskIter {
    uint64_t mask_;

public:
    explicit SparseMaskIter(uint64_t mask) : mask_{ mask } {}

    bool HasNext() const
    {
        return mask_ != 0;
    }

    unsigned Next()
    {
        unsigned i = FindFirstSetNonZero(mask_) >> 2;
        mask_ &= kMasks[i];
        return i;
    }
};

template <typename T, size_t N> SparseMaskIter FindMatchNibbles(T value, const T *OMNI_RESTRICT in)
{
    return SparseMaskIter{ FindMatchMask<T, N>(value, in) };
}

template <typename T, size_t N> OMNI_INLINE intptr_t FindFirstNibbles(T value, const T *OMNI_RESTRICT in)
{
    CappedTag<T, N> d;
    const auto broadCasted = Set(d, value);
    const intptr_t pos = FindFirstTrue(d, Eq(broadCasted, LoadU(d, in)));
    return static_cast<size_t>(pos);
}
#endif // OMNI_OMNI_MATCH_FUNC_H