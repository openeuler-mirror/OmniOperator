// Copyright 2021 Google LLC
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#ifndef DEMO_TRAITS_H
#define DEMO_TRAITS_H

#include <iostream>
#include "simd/simd.h"

namespace simd {
template <class D, OMNI_IF_NOT_FLOAT_NOR_SPECIAL_D(D)> Vec<D> LargestSortValue(D d)
{
    return Set(d, HighestValue<TFromD<D>>());
}

template <class D, OMNI_IF_FLOAT_OR_SPECIAL_D(D)> Vec<D> LargestSortValue(D d)
{
    return Inf(d);
}

template <class D, OMNI_IF_FLOAT_OR_SPECIAL_D(D)> Vec<D> SmallestSortValue(D d)
{
    return Neg(Inf(d));
}

template <class D, OMNI_IF_NOT_FLOAT_NOR_SPECIAL_D(D)> Vec<D> SmallestSortValue(D d)
{
    return Set(d, LowestValue<TFromD<D>>());
}

template <typename LaneTypeArg, typename KeyTypeArg> struct KeyLaneBase {
    static constexpr bool Is128()
    {
        return false;
    }

    constexpr size_t LanesPerKey() const
    {
        return 1;
    }

    // What type bench_sort should allocate for generating inputs.
    using LaneType = LaneTypeArg;
    // What type to pass to VQSort.
    using KeyType = KeyTypeArg;
};

template <typename LaneType, typename KeyType> struct KeyLane : public KeyLaneBase<LaneType, KeyType> {
    // For HeapSort
    OMNI_INLINE void Swap(LaneType *a, LaneType *b) const
    {
        const LaneType temp = *a;
        *a = *b;
        *b = temp;
    }

    template <class V, class M> OMNI_INLINE V CompressKeys(V keys, M mask) const
    {
        return CompressNot(keys, mask);
    }

    // Broadcasts one key into a vector
    template <class D> OMNI_INLINE Vec<D> SetKey(D d, const LaneType *key) const
    {
        return Set(d, *key);
    }

    template <class D> OMNI_INLINE Mask<D> EqualKeys(D /* tag */, Vec<D> a, Vec<D> b) const
    {
        return Eq(a, b);
    }

    template <class D> OMNI_INLINE Mask<D> NotEqualKeys(D /* tag */, Vec<D> a, Vec<D> b) const
    {
        return Ne(a, b);
    }

    // For keys=lanes, any difference counts.
    template <class D> OMNI_INLINE bool NoKeyDifference(D /* tag */, Vec<D> diff) const
    {
        // Must avoid floating-point comparisons (for -0)
        const RebindToUnsigned<D> du;
        return AllTrue(du, Eq(BitCast(du, diff), Zero(du)));
    }

    OMNI_INLINE bool Equal1(const LaneType *a, const LaneType *b) const
    {
        return *a == *b;
    }

    template <class D> OMNI_INLINE Vec<D> ReverseKeys(D d, Vec<D> v) const
    {
        return Reverse(d, v);
    }

    template <class D> OMNI_INLINE Vec<D> ReverseKeys2(D d, Vec<D> v) const
    {
        return Reverse2(d, v);
    }

    template <class D> OMNI_INLINE Vec<D> ReverseKeys4(D d, Vec<D> v) const
    {
        return Reverse4(d, v);
    }

    template <class D> OMNI_INLINE Vec<D> ReverseKeys8(D d, Vec<D> v) const
    {
        return Reverse8(d, v);
    }

    template <class D> OMNI_INLINE Vec<D> ReverseKeys16(D d, Vec<D> v) const
    {
        return ReverseKeys(d, v);
    }

    template <class V> OMNI_INLINE V OddEvenKeys(const V odd, const V even) const
    {
        return OddEven(odd, even);
    }

    template <class D, OMNI_IF_T_SIZE_D(D, 2)> OMNI_INLINE Vec<D> SwapAdjacentPairs(D d, const Vec<D> v) const
    {
        const Repartition<uint32_t, D> du32;
        return BitCast(d, Shuffle2301(BitCast(du32, v)));
    }

    template <class D, OMNI_IF_T_SIZE_D(D, 4)> OMNI_INLINE Vec<D> SwapAdjacentPairs(D /* tag */, const Vec<D> v) const
    {
        return Shuffle1032(v);
    }

    template <class D, OMNI_IF_T_SIZE_D(D, 8)> OMNI_INLINE Vec<D> SwapAdjacentPairs(D /* tag */, const Vec<D> v) const
    {
        return SwapAdjacentBlocks(v);
    }

    template <class D, OMNI_IF_NOT_T_SIZE_D(D, 8)> OMNI_INLINE Vec<D> SwapAdjacentQuads(D d, const Vec<D> v) const
    {
        const RepartitionToWide<D> dw;
        return BitCast(d, SwapAdjacentPairs(dw, BitCast(dw, v)));
    }

    template <class D, OMNI_IF_T_SIZE_D(D, 8)> OMNI_INLINE Vec<D> SwapAdjacentQuads(D d, const Vec<D> v) const
    {
        // Assumes max vector size = 512
        return ConcatLowerUpper(d, v, v);
    }

    template <class D, OMNI_IF_NOT_T_SIZE_D(D, 8)>
    OMNI_INLINE Vec<D> OddEvenPairs(D d, const Vec<D> odd, const Vec<D> even) const
    {
        const RepartitionToWide<D> dw;
        return BitCast(d, OddEven(BitCast(dw, odd), BitCast(dw, even)));
    }

    template <class D, OMNI_IF_T_SIZE_D(D, 8)>
    OMNI_INLINE Vec<D> OddEvenPairs(D /* tag */, Vec<D> odd, Vec<D> even) const
    {
        return OddEvenBlocks(odd, even);
    }

    template <class D, OMNI_IF_NOT_T_SIZE_D(D, 8)> OMNI_INLINE Vec<D> OddEvenQuads(D d, Vec<D> odd, Vec<D> even) const
    {
        const RepartitionToWide<D> dw;
    }

    template <class D, OMNI_IF_T_SIZE_D(D, 8)> OMNI_INLINE Vec<D> OddEvenQuads(D d, Vec<D> odd, Vec<D> even) const
    {
        return ConcatUpperLower(d, odd, even);
    }

    template <class D> OMNI_API void Reverse2First(D d, Vec<D> &a, Vec<D> &b)
    {
        using T = TFromD<D>;
        T bufA[2];
        T bufB[2];
        StoreU(a, d, bufA);
        StoreU(b, d, bufB);
        T temp = bufA[0];
        bufA[0] = bufB[0];
        bufB[0] = temp;
        a = LoadU(d, bufA);
        b = LoadU(d, bufB);
    }

    template <class D> OMNI_API void Reverse2Last(D d, Vec<D> &a, Vec<D> &b)
    {
        using T = TFromD<D>;
        T bufA[2];
        T bufB[2];
        StoreU(a, d, bufA);
        StoreU(b, d, bufB);
        T temp = bufA[1];
        bufA[1] = bufB[1];
        bufB[1] = temp;
        a = LoadU(d, bufA);
        b = LoadU(d, bufB);
    }
};

// Shared code that depends on Order.
template <class Base> struct TraitsLane : public Base {
    using TraitsForSortingNetwork = TraitsLane<typename Base::OrderForSortingNetwork>;

    // For each lane i: replaces a[i] with the first and b[i] with the second
    // according to Base.
    // Corresponds to a conditional swap, which is one "node" of a sorting
    // network. Min/Max are cheaper than compare + blend at least for integers.
    template <class D> OMNI_INLINE void Sort2(D d, Vec<D> &a, Vec<D> &b) const
    {
        const Base *base = static_cast<const Base *>(this);

        const Vec<D> a_copy = a;
        // Prior to AVX3, there is no native 64-bit Min/Max, so they compile to 4
        // instructions. We can reduce it to a compare + 2 IfThenElse.
        a = base->First(d, a, b);
        b = base->Last(d, a_copy, b);
    }

    template <class D, class A_VEC> OMNI_INLINE void Sort2(D d, Vec<D> &a, Vec<D> &b, A_VEC &c, A_VEC &e) const
    {
        const Vec<D> aCopy = a;
        Sort2(d, a, b);

        auto mask = Eq(a, aCopy);
        auto firstTrue = FindFirstTrue(d, mask);
        if constexpr (D::kPrivateLanes == 1) {
            if (firstTrue != 0) {
                A_VEC copyE = e;
                e = c;
                c = copyE;
            }
        } else {
            auto lastTrue = FindLastTrue(d, mask);
            auto diffMask = firstTrue + lastTrue;
            if (diffMask == 0) {
                A_VEC copyC = c;
                c = OddEven(e, c);
                e = OddEven(copyC, e);
            } else if (diffMask == 2) {
                A_VEC copyE = e;
                e = OddEven(e, c);
                c = OddEven(c, copyE);
            } else if (diffMask == -2) {
                A_VEC copyE = e;
                e = c;
                c = copyE;
            }
        }
    }

    // Conditionally swaps even-numbered lanes with their odd-numbered neighbor.
    template <class D> OMNI_INLINE Vec<D> SortPairsDistance1(D d, Vec<D> v) const
    {
        const Base *base = static_cast<const Base *>(this);
        Vec<D> swapped = base->ReverseKeys2(d, v);
        // Further to the above optimization, Sort2+OddEvenKeys compile to four
        // instructions; we can save one by combining two blends.
        Sort2(d, v, swapped);
        return base->OddEvenKeys(swapped, v);
    }

    // Conditionally swaps even-numbered lanes with their odd-numbered neighbor.
    template <class D, class ADDR_D = ScalableTag<uint64_t>>
    OMNI_INLINE void SortPairsDistance1(D d, Vec<D> &v, Vec<ADDR_D> &a) const
    {
        const Base *base = static_cast<const Base *>(this);
        Vec<D> swapped = base->ReverseKeys2(d, v);
        ADDR_D addrD;
        Vec<ADDR_D> swappedAddr = Reverse2(addrD, a);
        // Further to the above optimization, Sort2+OddEvenKeys compile to four
        // instructions; we can save one by combining two blends.
        Sort2(d, v, swapped, a, swappedAddr);
        v = base->OddEvenKeys(swapped, v);
        auto mask = Eq(v, swapped);
        auto firstTrue = FindFirstTrue(d, mask);
        if (firstTrue != 0) {
            a = base->OddEvenKeys(swappedAddr, a);
        }
    }

    // Swaps with the vector formed by reversing contiguous groups of 4 keys.
    template <class D, class ADDR_D = ScalableTag<uint64_t>>
    OMNI_INLINE Vec<D> SortPairsReverse4(D d, Vec<D> v, Vec<ADDR_D> &a) const
    {
        const Base *base = static_cast<const Base *>(this);
        Vec<D> swapped = base->ReverseKeys4(d, v);
        Sort2(d, v, swapped);
        return base->OddEvenPairs(d, swapped, v);
    }

    template <class D> OMNI_INLINE Vec<D> SortPairsReverse4(D d, Vec<D> v) const
    {
        const Base *base = static_cast<const Base *>(this);
        Vec<D> swapped = base->ReverseKeys4(d, v);
        Sort2(d, v, swapped);
        return base->OddEvenPairs(d, swapped, v);
    }

    // Conditionally swaps lane 0 with 4, 1 with 5 etc.
    template <class D> OMNI_INLINE Vec<D> SortPairsDistance4(D d, Vec<D> v) const
    {
        const Base *base = static_cast<const Base *>(this);
        Vec<D> swapped = base->SwapAdjacentQuads(d, v);
        // Only used in Merge16, so this will not be used on AVX2 (which only has 4
        // u64 lanes), so skip the above optimization for 64-bit AVX2.
        Sort2(d, v, swapped);
        return base->OddEvenQuads(d, swapped, v);
    }
};
}
#endif // DEMO_TRAITS_H
