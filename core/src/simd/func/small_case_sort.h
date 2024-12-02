/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
#ifndef VQSORT_H
#define VQSORT_H

#include <type_traits>
#include "traits-inl.h"

namespace simd {

#define MAX_LEVELS 50

struct SortAscending {
    static constexpr bool IsAscending()
    {
        return true;
    }
};

struct SortDescending {
    static constexpr bool IsAscending()
    {
        return false;
    }
};

template <size_t kRows, size_t kLanesPerRow, class D, class Traits, typename T = TFromD<D>>
OMNI_INLINE void CopyHalfToPaddedBuf(D d, Traits st, T *OMNI_RESTRICT keys, size_t num_lanes, T *OMNI_RESTRICT buf)
{
    constexpr size_t kMinLanes = kRows / 2 * kLanesPerRow;
    // Must cap for correctness: we will load up to the last valid lane, so
    // Lanes(dmax) must not exceed `num_lanes` (known to be at least kMinLanes).
    const CappedTag<T, kMinLanes> dmax;
    const size_t Nmax = Lanes(dmax);

    // Fill with padding - last in sort order, not copied to keys.
    const Vec<decltype(dmax)> kPadding = st.LastValue(dmax);

    // Rounding down allows aligned stores, which are typically faster.
    size_t i = num_lanes & ~(Nmax - 1);
    do {
        Store(kPadding, dmax, buf + i);
        i += Nmax;
        // Initialize enough for the last vector even if Nmax > kLanesPerRow.
    } while (i < (kRows - 1) * kLanesPerRow + Lanes(d));

    // Ensure buf contains all we will read, and perhaps more before.
    ptrdiff_t end = static_cast<ptrdiff_t>(num_lanes);
    do {
        end -= static_cast<ptrdiff_t>(Nmax);
        StoreU(LoadU(dmax, keys + end), dmax, buf + end);
    } while (end > static_cast<ptrdiff_t>(kRows / 2 * kLanesPerRow));
}

template <typename T> struct OrderAscending : public KeyLane<T, T> {
    // False indicates the entire key (i.e. lane) should be compared. KV stands
    // for key-value.
    static constexpr bool IsKV()
    {
        return false;
    }

    using Order = SortAscending;
    using OrderForSortingNetwork = OrderAscending<T>;
    detail::SortOrder currentOrder = detail::SortOrder::ASCENDING;
    OMNI_API bool Compare1(const T *a, const T *b)
    {
        return *a < *b;
    }

    template <class D> OMNI_API Mask<D> Compare(D /* tag */, Vec<D> a, Vec<D> b)
    {
        return Lt(a, b);
    }
    template <class D> OMNI_INLINE Mask<D> QuickSortCompare(D d /* tag */, Vec<D> a, Vec<D> b) const
    {
        return detail::QuickSortAscending::Compare(d, a, b);
    }
    // Two halves of Sort2, used in ScanMinMax.
    template <class D> OMNI_API Vec<D> First(D /* tag */, const Vec<D> a, const Vec<D> b)
    {
        return Min(a, b);
    }

    template <class D> OMNI_API Vec<D> Last(D /* tag */, const Vec<D> a, const Vec<D> b)
    {
        return Max(a, b);
    }

    template <class D> OMNI_API Vec<D> FirstOfLanes(D d, Vec<D> v, T *OMNI_RESTRICT /* buf */)
    {
        return MinOfLanes(d, v);
    }

    template <class D> OMNI_API Vec<D> LastOfLanes(D d, Vec<D> v, T *OMNI_RESTRICT /* buf */)
    {
        return MaxOfLanes(d, v);
    }

    template <class D> OMNI_API Vec<D> FirstValue(D d)
    {
        return SmallestSortValue(d);
    }

    template <class D> OMNI_API Vec<D> LastValue(D d)
    {
        return LargestSortValue(d);
    }

    template <class D> OMNI_API Vec<D> PrevValue(D d, Vec<D> v)
    {
        return SmallerSortValue(d, v);
    }
    template <class D> inline Vec<D> GetLargestVec(D d)
    {
        return detail::QuickSortAscending::GetLargestVec(d);
    }
    template <class D> inline Vec<D> GetSmallestVec(D d)
    {
        return detail::QuickSortAscending::GetSmallestVec(d);
    }
    template <class D, class V> static void UpdateMinMax(D d, V value, V &smallest, V &largest)
    {
        detail::QuickSortAscending::UpdateMinMax(d, value, smallest, largest);
    }
    template <class D, class V> inline TFromD<D> GetSmallest(D d, V v)
    {
        return detail::QuickSortAscending::GetSmallest(d, v);
    }

    template <class D, class V> inline TFromD<D> GetLargest(D d, V v)
    {
        return detail::QuickSortAscending::GetLargest(d, v);
    }
};

template <typename T> struct OrderDescending : public KeyLane<T, T> {
    // False indicates the entire key (i.e. lane) should be compared. KV stands
    // for key-value.
    static constexpr bool IsKV()
    {
        return false;
    }

    using Order = SortDescending;
    using OrderForSortingNetwork = OrderDescending<T>;
    detail::SortOrder currentOrder = detail::SortOrder::DESCENDING;
    OMNI_INLINE bool Compare1(const T *a, const T *b) const
    {
        return *b < *a;
    }

    template <class D> OMNI_INLINE Mask<D> Compare(D /* tag */, Vec<D> a, Vec<D> b) const
    {
        return Lt(b, a);
    }
    template <class D> OMNI_INLINE Mask<D> QuickSortCompare(D d /* tag */, Vec<D> a, Vec<D> b) const
    {
        return detail::QuickSortDescending::Compare(d, a, b);
    }
    template <class D> OMNI_INLINE Vec<D> First(D /* tag */, const Vec<D> a, const Vec<D> b) const
    {
        return Max(a, b);
    }

    template <class D> OMNI_INLINE Vec<D> Last(D /* tag */, const Vec<D> a, const Vec<D> b) const
    {
        return Min(a, b);
    }

    template <class D> OMNI_INLINE Vec<D> FirstOfLanes(D d, Vec<D> v, T *OMNI_RESTRICT /* buf */) const
    {
        return MaxOfLanes(d, v);
    }

    template <class D> OMNI_INLINE Vec<D> LastOfLanes(D d, Vec<D> v, T *OMNI_RESTRICT /* buf */) const
    {
        return MinOfLanes(d, v);
    }

    template <class D> OMNI_INLINE Vec<D> FirstValue(D d) const
    {
        return LargestSortValue(d);
    }

    template <class D> OMNI_INLINE Vec<D> LastValue(D d) const
    {
        return SmallestSortValue(d);
    }

    template <class D> OMNI_INLINE Vec<D> PrevValue(D d, Vec<D> v) const
    {
        return LargerSortValue(d, v);
    }
    template <class D> inline Vec<D> GetLargestVec(D d)
    {
        return detail::QuickSortDescending::GetLargestVec(d);
    }
    template <class D> inline Vec<D> GetSmallestVec(D d)
    {
        return detail::QuickSortDescending::GetSmallestVec(d);
    }
    template <class D, class V> static void UpdateMinMax(D d, V value, V &smallest, V &largest)
    {
        detail::QuickSortDescending::UpdateMinMax(d, value, smallest, largest);
    }
    template <class D, class V> inline TFromD<D> GetSmallest(D d, V v)
    {
        return detail::QuickSortDescending::GetSmallest(d, v);
    }

    template <class D, class V> inline TFromD<D> GetLargest(D d, V v)
    {
        return detail::QuickSortDescending::GetLargest(d, v);
    }
};

template <typename Key> struct KeyAdapter {
    template <class Order>
    using Traits = TraitsLane<If<Order::IsAscending(), OrderAscending<Key>, OrderDescending<Key>>>;
};

// ------------------------------ SharedTraits

// Code shared between all traits. It's unclear whether these can profitably be
// specialized for Lane vs Block, or optimized like SortPairsDistance1 using
// Compare/DupOdd.
template <class Base> struct SharedTraits : public Base {
    using SharedTraitsForSortingNetwork = SharedTraits<typename Base::TraitsForSortingNetwork>;

    // Conditionally swaps lane 0 with 2, 1 with 3 etc.
    template <class D> OMNI_INLINE Vec<D> SortPairsDistance2(D d, Vec<D> v) const
    {
        const Base *base = static_cast<const Base *>(this);
        Vec<D> swapped = base->SwapAdjacentPairs(d, v);
        base->Sort2(d, v, swapped);
        return base->OddEvenPairs(d, swapped, v);
    }

    template <class D, class ADDR_D = ScalableTag<uint64_t>>
    OMNI_INLINE Vec<D> SortPairsDistance2(D d, Vec<D> v, Vec<ADDR_D> a) const
    {
        const Base *base = static_cast<const Base *>(this);
        Vec<D> swapped = base->SwapAdjacentPairs(d, v);
        base->Sort2(d, v, swapped);
        return base->OddEvenPairs(d, swapped, v);
    }

    // Swaps with the vector formed by reversing contiguous groups of 8 keys.
    template <class D> OMNI_INLINE Vec<D> SortPairsReverse8(D d, Vec<D> v) const
    {
        const Base *base = static_cast<const Base *>(this);
        Vec<D> swapped = base->ReverseKeys8(d, v);
        base->Sort2(d, v, swapped);
        return base->OddEvenQuads(d, swapped, v);
    }

    template <class D, class ADDR_D = ScalableTag<uint64_t>>
    OMNI_INLINE Vec<D> SortPairsReverse8(D d, Vec<D> v, Vec<ADDR_D> a) const
    {
        const Base *base = static_cast<const Base *>(this);
        Vec<D> swapped = base->ReverseKeys8(d, v);
        base->Sort2(d, v, swapped);
        return base->OddEvenQuads(d, swapped, v);
    }

    // Swaps with the vector formed by reversing contiguous groups of 8 keys.
    template <class D> OMNI_INLINE Vec<D> SortPairsReverse16(D d, Vec<D> v) const
    {
        const Base *base = static_cast<const Base *>(this);
        Vec<D> swapped = base->ReverseKeys(d, v);
        base->Sort2(d, v, swapped);
        return ConcatUpperLower(d, swapped, v); // 8 = half of the vector
    }
};

template <typename Key, class Order> using MakeTraits = SharedTraits<typename KeyAdapter<Key>::template Traits<Order>>;

template <class D, class Traits, class V = Vec<D>, class A>
OMNI_INLINE void Sort2(D d, Traits st, V &v0, V &v1, A &a0, A &a1)
{
    st.Sort2(d, v0, v1, a0, a1);
}

template <class D, class Traits, class V = Vec<D>> OMNI_INLINE void Sort3(D d, Traits st, V &v0, V &v1, V &v2)
{
    st.Sort2(d, v0, v2);
    st.Sort2(d, v0, v1);
    st.Sort2(d, v1, v2);
}

template <class D, class Traits, class V = Vec<D>, class A = Vec<ScalableTag<uint64_t>>>
OMNI_INLINE void Sort4(D d, Traits st, V &v0, V &v1, V &v2, V &v3, A &a0, A &a1, A &a2, A &a3)
{
    st.Sort2(d, v0, v2, a0, a2);
    st.Sort2(d, v1, v3, a1, a3);
    st.Sort2(d, v0, v1, a0, a1);
    st.Sort2(d, v2, v3, a2, a3);
    st.Sort2(d, v1, v2, a1, a2);
}

template <class D, class Traits, class V = Vec<D>, class A = Vec<ScalableTag<uint64_t>>>
OMNI_INLINE void Sort8(D d, Traits st, V &v0, V &v1, V &v2, V &v3, V &v4, V &v5, V &v6, V &v7, A &a0, A &a1, A &a2,
    A &a3, A &a4, A &a5, A &a6, A &a7)
{
    st.Sort2(d, v0, v2, a0, a2);
    st.Sort2(d, v1, v3, a1, a3);
    st.Sort2(d, v4, v6, a4, a6);
    st.Sort2(d, v5, v7, a5, a7);

    st.Sort2(d, v0, v4, a0, a4);
    st.Sort2(d, v1, v5, a1, a5);
    st.Sort2(d, v2, v6, a2, a6);
    st.Sort2(d, v3, v7, a3, a7);

    st.Sort2(d, v0, v1, a0, a1);
    st.Sort2(d, v2, v3, a2, a3);
    st.Sort2(d, v4, v5, a4, a5);
    st.Sort2(d, v6, v7, a6, a7);

    st.Sort2(d, v2, v4, a2, a4);
    st.Sort2(d, v3, v5, a3, a5);

    st.Sort2(d, v1, v4, a1, a4);
    st.Sort2(d, v3, v6, a3, a6);

    st.Sort2(d, v1, v2, a1, a2);
    st.Sort2(d, v3, v4, a3, a4);
    st.Sort2(d, v5, v6, a5, a6);
}

template <class D, class Traits, class V = Vec<D>, class A = Vec<ScalableTag<uint64_t>>>
OMNI_INLINE void Sort16(D d, Traits st, V &v0, V &v1, V &v2, V &v3, V &v4, V &v5, V &v6, V &v7, V &v8, V &v9, V &va,
    V &vb, V &vc, V &vd, V &ve, V &vf, A &a0, A &a1, A &a2, A &a3, A &a4, A &a5, A &a6, A &a7, A &a8, A &a9, A &aa,
    A &ab, A &ac, A &ad, A &ae, A &af)
{
    st.Sort2(d, v0, v1, a0, a1);
    st.Sort2(d, v2, v3, a2, a3);
    st.Sort2(d, v4, v5, a4, a5);
    st.Sort2(d, v6, v7, a6, a7);
    st.Sort2(d, v8, v9, a8, a9);
    st.Sort2(d, va, vb, aa, ab);
    st.Sort2(d, vc, vd, ac, ad);
    st.Sort2(d, ve, vf, ae, af);
    st.Sort2(d, v0, v2, a0, a2);
    st.Sort2(d, v1, v3, a1, a3);
    st.Sort2(d, v4, v6, a4, a6);
    st.Sort2(d, v5, v7, a5, a7);
    st.Sort2(d, v8, va, a8, aa);
    st.Sort2(d, v9, vb, a9, ab);
    st.Sort2(d, vc, ve, ac, ae);
    st.Sort2(d, vd, vf, ad, af);
    st.Sort2(d, v0, v4, a0, a4);
    st.Sort2(d, v1, v5, a1, a5);
    st.Sort2(d, v2, v6, a2, a6);
    st.Sort2(d, v3, v7, a3, a7);
    st.Sort2(d, v8, vc, a8, ac);
    st.Sort2(d, v9, vd, a9, ad);
    st.Sort2(d, va, ve, aa, ae);
    st.Sort2(d, vb, vf, ab, af);
    st.Sort2(d, v0, v8, a0, a8);
    st.Sort2(d, v1, v9, a1, a9);
    st.Sort2(d, v2, va, a2, aa);
    st.Sort2(d, v3, vb, a3, ab);
    st.Sort2(d, v4, vc, a4, ac);
    st.Sort2(d, v5, vd, a5, ad);
    st.Sort2(d, v6, ve, a6, ae);
    st.Sort2(d, v7, vf, a7, af);
    st.Sort2(d, v5, va, a5, aa);
    st.Sort2(d, v6, v9, a6, a9);
    st.Sort2(d, v3, vc, a3, ac);
    st.Sort2(d, v7, vb, a7, ab);
    st.Sort2(d, vd, ve, ad, ae);
    st.Sort2(d, v4, v8, a4, a8);
    st.Sort2(d, v1, v2, a1, a2);
    st.Sort2(d, v1, v4, a1, a4);
    st.Sort2(d, v7, vd, a7, ad);
    st.Sort2(d, v2, v8, a2, a8);
    st.Sort2(d, vb, ve, ab, ae);
    st.Sort2(d, v2, v4, a2, a4);
    st.Sort2(d, v5, v6, a5, a6);
    st.Sort2(d, v9, va, a9, aa);
    st.Sort2(d, vb, vd, ab, ad);
    st.Sort2(d, v3, v8, a3, a8);
    st.Sort2(d, v7, vc, a7, ac);
    st.Sort2(d, v3, v5, a3, a5);
    st.Sort2(d, v6, v8, a6, a8);
    st.Sort2(d, v7, v9, a7, a9);
    st.Sort2(d, va, vc, aa, ac);
    st.Sort2(d, v3, v4, a3, a4);
    st.Sort2(d, v5, v6, a5, a6);
    st.Sort2(d, v7, v8, a7, a8);
    st.Sort2(d, v9, va, a9, aa);
    st.Sort2(d, vb, vc, ab, ac);
    st.Sort2(d, v6, v7, a6, a7);
    st.Sort2(d, v8, v9, a8, a9);
}

template <size_t kKeysPerVector, class D, class ADDR_D, class Traits, class V, class A,
    OMNI_IF_LANES_LE(kKeysPerVector, 1)>
OMNI_INLINE void Merge8x2(D, ADDR_D, Traits, V, V, V, V, V, V, V, V, A, A, A, A, A, A, A, A)
{}

template <size_t kKeysPerVector, class D, class ADDR_D, class Traits, class V, class A,
    OMNI_IF_LANES_LE(kKeysPerVector, 2)>
OMNI_INLINE void Merge8x4(D, ADDR_D, Traits, V, V, V, V, V, V, V, V, A, A, A, A, A, A, A, A)
{}

template <size_t kKeysPerVector, class D, class ADDR_D, class Traits, class V, class A,
    OMNI_IF_LANES_LE(kKeysPerVector, 1)>
OMNI_INLINE void Merge16x2(D, ADDR_D, Traits, V, V, V, V, V, V, V, V, V, V, V, V, V, V, V, V, A, A, A, A, A, A, A, A, A,
    A, A, A, A, A, A, A)
{}

template <size_t kKeysPerVector, class D, class ADDR_D, class Traits, class V, class A,
    OMNI_IF_LANES_LE(kKeysPerVector, 2)>
OMNI_INLINE void Merge16x4(D, ADDR_D, Traits, V, V, V, V, V, V, V, V, V, V, V, V, V, V, V, V, A, A, A, A, A, A, A, A, A,
    A, A, A, A, A, A, A)
{}

template <size_t kKeysPerVector, class D, class ADDR_D, class Traits, class V, class A,
    OMNI_IF_LANES_LE(kKeysPerVector, 4)>
OMNI_INLINE void Merge16x8(D, ADDR_D, Traits, V, V, V, V, V, V, V, V, V, V, V, V, V, V, V, V, A, A, A, A, A, A, A, A, A,
    A, A, A, A, A, A, A)
{}

template <class D, class Traits, class V> OMNI_INLINE void Merge3x2(D d, Traits st, V &v0, V &v1, V &v2)
{
    v2 = st.ReverseKeys2(d, v2);
    st.Sort2(d, v1, v2);
    v1 = st.ReverseKeys2(d, v1);
    st.Sort2(d, v0, v1);
    v0 = st.SortPairsDistance1(d, v0);
    v1 = st.SortPairsDistance1(d, v1);
    v2 = st.SortPairsDistance1(d, v2);
}

template <size_t kKeysPerVector, class D, class ADDR_D, class Traits, class V = Vec<D>, class A = Vec<ADDR_D>,
    OMNI_IF_LANES_GT(kKeysPerVector, 1)>
OMNI_INLINE void Merge8x2(D d, ADDR_D addrD, Traits st, V &v0, V &v1, V &v2, V &v3, V &v4, V &v5, V &v6, V &v7, A &a0,
    A &a1, A &a2, A &a3, A &a4, A &a5, A &a6, A &a7)
{
    v7 = st.ReverseKeys2(d, v7);
    v6 = st.ReverseKeys2(d, v6);
    v5 = st.ReverseKeys2(d, v5);
    v4 = st.ReverseKeys2(d, v4);
    a7 = st.ReverseKeys2(addrD, a7);
    a6 = st.ReverseKeys2(addrD, a6);
    a5 = st.ReverseKeys2(addrD, a5);
    a4 = st.ReverseKeys2(addrD, a4);
    st.Sort2(d, v0, v7, a0, a7);
    st.Sort2(d, v1, v6, a1, a6);
    st.Sort2(d, v2, v5, a2, a5);
    st.Sort2(d, v3, v4, a3, a4);

    v3 = st.ReverseKeys2(d, v3);
    v2 = st.ReverseKeys2(d, v2);
    v7 = st.ReverseKeys2(d, v7);
    v6 = st.ReverseKeys2(d, v6);
    a3 = st.ReverseKeys2(addrD, a3);
    a2 = st.ReverseKeys2(addrD, a2);
    a7 = st.ReverseKeys2(addrD, a7);
    a6 = st.ReverseKeys2(addrD, a6);
    st.Sort2(d, v0, v3, a0, a3);
    st.Sort2(d, v1, v2, a1, a2);
    st.Sort2(d, v4, v7, a4, a7);
    st.Sort2(d, v5, v6, a5, a6);

    v1 = st.ReverseKeys2(d, v1);
    v3 = st.ReverseKeys2(d, v3);
    v5 = st.ReverseKeys2(d, v5);
    v7 = st.ReverseKeys2(d, v7);
    a1 = st.ReverseKeys2(addrD, a1);
    a3 = st.ReverseKeys2(addrD, a3);
    a5 = st.ReverseKeys2(addrD, a5);
    a7 = st.ReverseKeys2(addrD, a7);
    st.Sort2(d, v0, v1, a0, a1);
    st.Sort2(d, v2, v3, a2, a3);
    st.Sort2(d, v4, v5, a4, a5);
    st.Sort2(d, v6, v7, a6, a7);

    st.SortPairsDistance1(d, v0, a0);
    st.SortPairsDistance1(d, v1, a1);
    st.SortPairsDistance1(d, v2, a2);
    st.SortPairsDistance1(d, v3, a3);
    st.SortPairsDistance1(d, v4, a4);
    st.SortPairsDistance1(d, v5, a5);
    st.SortPairsDistance1(d, v6, a6);
    st.SortPairsDistance1(d, v7, a7);
}

template <size_t kKeysPerVector, class D, class ADDR_D, class Traits, class V = Vec<D>, class A = Vec<ADDR_D>,
    OMNI_IF_LANES_GT(kKeysPerVector, 2)>
OMNI_INLINE void Merge8x4(D d, ADDR_D addrD, Traits st, V &v0, V &v1, V &v2, V &v3, V &v4, V &v5, V &v6, V &v7, A &a0,
    A &a1, A &a2, A &a3, A &a4, A &a5, A &a6, A &a7)
{
    v7 = st.ReverseKeys4(d, v7);
    v6 = st.ReverseKeys4(d, v6);
    v5 = st.ReverseKeys4(d, v5);
    v4 = st.ReverseKeys4(d, v4);

    a7 = st.ReverseKeys4(addrD, a7);
    a6 = st.ReverseKeys4(addrD, a6);
    a5 = st.ReverseKeys4(addrD, a5);
    a4 = st.ReverseKeys4(addrD, a4);

    st.Sort2(d, v0, v7, a0, a7);
    st.Sort2(d, v1, v6, a1, a6);
    st.Sort2(d, v2, v5, a2, a5);
    st.Sort2(d, v3, v4, a3, a4);

    v3 = st.ReverseKeys4(d, v3);
    v2 = st.ReverseKeys4(d, v2);
    v7 = st.ReverseKeys4(d, v7);
    v6 = st.ReverseKeys4(d, v6);

    a3 = st.ReverseKeys4(addrD, a3);
    a2 = st.ReverseKeys4(addrD, a2);
    a7 = st.ReverseKeys4(addrD, a7);
    a6 = st.ReverseKeys4(addrD, a6);

    st.Sort2(d, v0, v3, a0, a3);
    st.Sort2(d, v1, v2, a1, a2);
    st.Sort2(d, v4, v7, a4, a7);
    st.Sort2(d, v5, v6, a5, a6);

    v1 = st.ReverseKeys4(d, v1);
    v3 = st.ReverseKeys4(d, v3);
    v5 = st.ReverseKeys4(d, v5);
    v7 = st.ReverseKeys4(d, v7);

    a1 = st.ReverseKeys4(d, a1);
    a3 = st.ReverseKeys4(d, a3);
    a5 = st.ReverseKeys4(d, a5);
    a7 = st.ReverseKeys4(d, a7);

    st.Sort2(d, v0, v1, a0, a1);
    st.Sort2(d, v2, v3, a2, a3);
    st.Sort2(d, v4, v5, a4, a5);
    st.Sort2(d, v6, v7, a6, a7);

    st.SortPairsReverse4(d, v0, a0);
    st.SortPairsReverse4(d, v1, a1);
    st.SortPairsReverse4(d, v2, a2);
    st.SortPairsReverse4(d, v3, a3);
    st.SortPairsReverse4(d, v4, a4);
    st.SortPairsReverse4(d, v5, a5);
    st.SortPairsReverse4(d, v6, a6);
    st.SortPairsReverse4(d, v7, a7);

    st.SortPairsDistance1(d, v0, a0);
    st.SortPairsDistance1(d, v1, a1);
    st.SortPairsDistance1(d, v2, a2);
    st.SortPairsDistance1(d, v3, a3);
    st.SortPairsDistance1(d, v4, a4);
    st.SortPairsDistance1(d, v5, a5);
    st.SortPairsDistance1(d, v6, a6);
    st.SortPairsDistance1(d, v7, a7);
}

template <size_t kKeysPerVector, class D, class ADDR_D, class Traits, class V = Vec<D>, class A = Vec<ADDR_D>,
    OMNI_IF_LANES_GT(kKeysPerVector, 1)>
OMNI_INLINE void Merge16x2(D d, ADDR_D addrD, Traits st, V &v0, V &v1, V &v2, V &v3, V &v4, V &v5, V &v6, V &v7, V &v8,
    V &v9, V &va, V &vb, V &vc, V &vd, V &ve, V &vf, A &a0, A &a1, A &a2, A &a3, A &a4, A &a5, A &a6, A &a7, A &a8,
    A &a9, A &aa, A &ab, A &ac, A &ad, A &ae, A &af)
{
    vf = st.ReverseKeys2(d, vf);
    ve = st.ReverseKeys2(d, ve);
    vd = st.ReverseKeys2(d, vd);
    vc = st.ReverseKeys2(d, vc);
    vb = st.ReverseKeys2(d, vb);
    va = st.ReverseKeys2(d, va);
    v9 = st.ReverseKeys2(d, v9);
    v8 = st.ReverseKeys2(d, v8);

    af = st.ReverseKeys2(addrD, af);
    ae = st.ReverseKeys2(addrD, ae);
    ad = st.ReverseKeys2(addrD, ad);
    ac = st.ReverseKeys2(addrD, ac);
    ab = st.ReverseKeys2(addrD, ab);
    aa = st.ReverseKeys2(addrD, aa);
    a9 = st.ReverseKeys2(addrD, a9);
    a8 = st.ReverseKeys2(addrD, a8);

    st.Sort2(d, v0, vf, a0, af);
    st.Sort2(d, v1, ve, a1, ae);
    st.Sort2(d, v2, vd, a2, ad);
    st.Sort2(d, v3, vc, a3, ac);
    st.Sort2(d, v4, vb, a4, ab);
    st.Sort2(d, v5, va, a5, aa);
    st.Sort2(d, v6, v9, a6, a9);
    st.Sort2(d, v7, v8, a7, a8);

    v7 = st.ReverseKeys2(d, v7);
    v6 = st.ReverseKeys2(d, v6);
    v5 = st.ReverseKeys2(d, v5);
    v4 = st.ReverseKeys2(d, v4);
    vf = st.ReverseKeys2(d, vf);
    ve = st.ReverseKeys2(d, ve);
    vd = st.ReverseKeys2(d, vd);
    vc = st.ReverseKeys2(d, vc);

    a7 = st.ReverseKeys2(addrD, a7);
    a6 = st.ReverseKeys2(addrD, a6);
    a5 = st.ReverseKeys2(addrD, a5);
    a4 = st.ReverseKeys2(addrD, a4);
    af = st.ReverseKeys2(addrD, af);
    ae = st.ReverseKeys2(addrD, ae);
    ad = st.ReverseKeys2(addrD, ad);
    ac = st.ReverseKeys2(addrD, ac);

    st.Sort2(d, v0, v7, a0, a7);
    st.Sort2(d, v1, v6, a1, a6);
    st.Sort2(d, v2, v5, a2, a5);
    st.Sort2(d, v3, v4, a3, a4);
    st.Sort2(d, v8, vf, a8, af);
    st.Sort2(d, v9, ve, a9, ae);
    st.Sort2(d, va, vd, aa, ad);
    st.Sort2(d, vb, vc, ab, ac);

    v3 = st.ReverseKeys2(d, v3);
    v2 = st.ReverseKeys2(d, v2);
    v7 = st.ReverseKeys2(d, v7);
    v6 = st.ReverseKeys2(d, v6);
    vb = st.ReverseKeys2(d, vb);
    va = st.ReverseKeys2(d, va);
    vf = st.ReverseKeys2(d, vf);
    ve = st.ReverseKeys2(d, ve);

    a3 = st.ReverseKeys2(addrD, a3);
    a2 = st.ReverseKeys2(addrD, a2);
    a7 = st.ReverseKeys2(addrD, a7);
    a6 = st.ReverseKeys2(addrD, a6);
    ab = st.ReverseKeys2(addrD, ab);
    aa = st.ReverseKeys2(addrD, aa);
    af = st.ReverseKeys2(addrD, af);
    ae = st.ReverseKeys2(addrD, ae);

    st.Sort2(d, v0, v3, a0, a3);
    st.Sort2(d, v1, v2, a1, a2);
    st.Sort2(d, v4, v7, a4, a7);
    st.Sort2(d, v5, v6, a5, a6);
    st.Sort2(d, v8, vb, a8, ab);
    st.Sort2(d, v9, va, a9, aa);
    st.Sort2(d, vc, vf, ac, af);
    st.Sort2(d, vd, ve, ad, ae);

    v1 = st.ReverseKeys2(d, v1);
    v3 = st.ReverseKeys2(d, v3);
    v5 = st.ReverseKeys2(d, v5);
    v7 = st.ReverseKeys2(d, v7);
    v9 = st.ReverseKeys2(d, v9);
    vb = st.ReverseKeys2(d, vb);
    vd = st.ReverseKeys2(d, vd);
    vf = st.ReverseKeys2(d, vf);

    a1 = st.ReverseKeys2(addrD, a1);
    a3 = st.ReverseKeys2(addrD, a3);
    a5 = st.ReverseKeys2(addrD, a5);
    a7 = st.ReverseKeys2(addrD, a7);
    a9 = st.ReverseKeys2(addrD, a9);
    ab = st.ReverseKeys2(addrD, ab);
    ad = st.ReverseKeys2(addrD, ad);
    af = st.ReverseKeys2(addrD, af);

    st.Sort2(d, v0, v1, a0, a1);
    st.Sort2(d, v2, v3, a2, a3);
    st.Sort2(d, v4, v5, a4, a5);
    st.Sort2(d, v6, v7, a6, a7);
    st.Sort2(d, v8, v9, a8, a9);
    st.Sort2(d, va, vb, aa, ab);
    st.Sort2(d, vc, vd, ac, ad);
    st.Sort2(d, ve, vf, ae, af);

    st.SortPairsDistance1(d, v0, a0);
    st.SortPairsDistance1(d, v1, a1);
    st.SortPairsDistance1(d, v2, a2);
    st.SortPairsDistance1(d, v3, a3);
    st.SortPairsDistance1(d, v4, a4);
    st.SortPairsDistance1(d, v5, a5);
    st.SortPairsDistance1(d, v6, a6);
    st.SortPairsDistance1(d, v7, a7);
    st.SortPairsDistance1(d, v8, a8);
    st.SortPairsDistance1(d, v9, a9);
    st.SortPairsDistance1(d, va, aa);
    st.SortPairsDistance1(d, vb, ab);
    st.SortPairsDistance1(d, vc, ac);
    st.SortPairsDistance1(d, vd, ad);
    st.SortPairsDistance1(d, ve, ae);
    st.SortPairsDistance1(d, vf, af);
}

template <size_t kKeysPerVector, class D, class ADDR_D, class Traits, class V = Vec<D>, class A = Vec<ADDR_D>,
    OMNI_IF_LANES_GT(kKeysPerVector, 2)>
OMNI_INLINE void Merge16x4(D d, ADDR_D addrD, Traits st, V &v0, V &v1, V &v2, V &v3, V &v4, V &v5, V &v6, V &v7, V &v8,
    V &v9, V &va, V &vb, V &vc, V &vd, V &ve, V &vf, A &a0, A &a1, A &a2, A &a3, A &a4, A &a5, A &a6, A &a7, A &a8,
    A &a9, A &aa, A &ab, A &ac, A &ad, A &ae, A &af)
{
    vf = st.ReverseKeys4(d, vf);
    ve = st.ReverseKeys4(d, ve);
    vd = st.ReverseKeys4(d, vd);
    vc = st.ReverseKeys4(d, vc);
    vb = st.ReverseKeys4(d, vb);
    va = st.ReverseKeys4(d, va);
    v9 = st.ReverseKeys4(d, v9);
    v8 = st.ReverseKeys4(d, v8);

    af = st.ReverseKeys4(addrD, af);
    ae = st.ReverseKeys4(addrD, ae);
    ad = st.ReverseKeys4(addrD, ad);
    ac = st.ReverseKeys4(addrD, ac);
    ab = st.ReverseKeys4(addrD, ab);
    aa = st.ReverseKeys4(addrD, aa);
    a9 = st.ReverseKeys4(addrD, a9);
    a8 = st.ReverseKeys4(addrD, a8);

    st.Sort2(d, v0, vf, a0, af);
    st.Sort2(d, v1, ve, a1, ae);
    st.Sort2(d, v2, vd, a2, ad);
    st.Sort2(d, v3, vc, a3, ac);
    st.Sort2(d, v4, vb, a4, ab);
    st.Sort2(d, v5, va, a5, aa);
    st.Sort2(d, v6, v9, a6, a9);
    st.Sort2(d, v7, v8, a7, a8);

    v7 = st.ReverseKeys4(d, v7);
    v6 = st.ReverseKeys4(d, v6);
    v5 = st.ReverseKeys4(d, v5);
    v4 = st.ReverseKeys4(d, v4);
    vf = st.ReverseKeys4(d, vf);
    ve = st.ReverseKeys4(d, ve);
    vd = st.ReverseKeys4(d, vd);
    vc = st.ReverseKeys4(d, vc);

    a7 = st.ReverseKeys4(addrD, a7);
    a6 = st.ReverseKeys4(addrD, a6);
    a5 = st.ReverseKeys4(addrD, a5);
    a4 = st.ReverseKeys4(addrD, a4);
    af = st.ReverseKeys4(addrD, af);
    ae = st.ReverseKeys4(addrD, ae);
    ad = st.ReverseKeys4(addrD, ad);
    ac = st.ReverseKeys4(addrD, ac);

    st.Sort2(d, v0, v7, a0, a7);
    st.Sort2(d, v1, v6, a1, a6);
    st.Sort2(d, v2, v5, a2, a5);
    st.Sort2(d, v3, v4, a3, a4);
    st.Sort2(d, v8, vf, a8, af);
    st.Sort2(d, v9, ve, a9, ae);
    st.Sort2(d, va, vd, aa, ad);
    st.Sort2(d, vb, vc, ab, ac);

    v3 = st.ReverseKeys4(d, v3);
    v2 = st.ReverseKeys4(d, v2);
    v7 = st.ReverseKeys4(d, v7);
    v6 = st.ReverseKeys4(d, v6);
    vb = st.ReverseKeys4(d, vb);
    va = st.ReverseKeys4(d, va);
    vf = st.ReverseKeys4(d, vf);
    ve = st.ReverseKeys4(d, ve);

    a3 = st.ReverseKeys4(addrD, a3);
    a2 = st.ReverseKeys4(addrD, a2);
    a7 = st.ReverseKeys4(addrD, a7);
    a6 = st.ReverseKeys4(addrD, a6);
    ab = st.ReverseKeys4(addrD, ab);
    aa = st.ReverseKeys4(addrD, aa);
    af = st.ReverseKeys4(addrD, af);
    ae = st.ReverseKeys4(addrD, ae);

    st.Sort2(d, v0, v3, a0, a3);
    st.Sort2(d, v1, v2, a1, a2);
    st.Sort2(d, v4, v7, a4, a7);
    st.Sort2(d, v5, v6, a5, a6);
    st.Sort2(d, v8, vb, a8, ab);
    st.Sort2(d, v9, va, a9, aa);
    st.Sort2(d, vc, vf, ac, af);
    st.Sort2(d, vd, ve, ad, ae);

    v1 = st.ReverseKeys4(d, v1);
    v3 = st.ReverseKeys4(d, v3);
    v5 = st.ReverseKeys4(d, v5);
    v7 = st.ReverseKeys4(d, v7);
    v9 = st.ReverseKeys4(d, v9);
    vb = st.ReverseKeys4(d, vb);
    vd = st.ReverseKeys4(d, vd);
    vf = st.ReverseKeys4(d, vf);

    a1 = st.ReverseKeys4(addrD, a1);
    a3 = st.ReverseKeys4(addrD, a3);
    a5 = st.ReverseKeys4(addrD, a5);
    a7 = st.ReverseKeys4(addrD, a7);
    a9 = st.ReverseKeys4(addrD, a9);
    ab = st.ReverseKeys4(addrD, ab);
    ad = st.ReverseKeys4(addrD, ad);
    af = st.ReverseKeys4(addrD, af);

    st.Sort2(d, v0, v1, a0, a1);
    st.Sort2(d, v2, v3, a2, a3);
    st.Sort2(d, v4, v5, a4, a5);
    st.Sort2(d, v6, v7, a6, a7);
    st.Sort2(d, v8, v9, a8, a9);
    st.Sort2(d, va, vb, aa, ab);
    st.Sort2(d, vc, vd, ac, ad);
    st.Sort2(d, ve, vf, ae, af);

    v0 = st.SortPairsReverse4(d, v0, a0);
    v1 = st.SortPairsReverse4(d, v1, a1);
    v2 = st.SortPairsReverse4(d, v2, a2);
    v3 = st.SortPairsReverse4(d, v3, a3);
    v4 = st.SortPairsReverse4(d, v4, a4);
    v5 = st.SortPairsReverse4(d, v5, a5);
    v6 = st.SortPairsReverse4(d, v6, a6);
    v7 = st.SortPairsReverse4(d, v7, a7);
    v8 = st.SortPairsReverse4(d, v8, a8);
    v9 = st.SortPairsReverse4(d, v9, a9);
    va = st.SortPairsReverse4(d, va, aa);
    vb = st.SortPairsReverse4(d, vb, ab);
    vc = st.SortPairsReverse4(d, vc, ac);
    vd = st.SortPairsReverse4(d, vd, ad);
    ve = st.SortPairsReverse4(d, ve, ae);
    vf = st.SortPairsReverse4(d, vf, af);

    st.SortPairsDistance1(d, v0, a0);
    st.SortPairsDistance1(d, v1, a1);
    st.SortPairsDistance1(d, v2, a2);
    st.SortPairsDistance1(d, v3, a3);
    st.SortPairsDistance1(d, v4, a4);
    st.SortPairsDistance1(d, v5, a5);
    st.SortPairsDistance1(d, v6, a6);
    st.SortPairsDistance1(d, v7, a7);
    st.SortPairsDistance1(d, v8, a8);
    st.SortPairsDistance1(d, v9, a9);
    st.SortPairsDistance1(d, va, aa);
    st.SortPairsDistance1(d, vb, ab);
    st.SortPairsDistance1(d, vc, ac);
    st.SortPairsDistance1(d, vd, ad);
    st.SortPairsDistance1(d, ve, ae);
    st.SortPairsDistance1(d, vf, af);
}

template <size_t kKeysPerVector, class D, class ADDR_D, class Traits, class V = Vec<D>, class A = Vec<ADDR_D>,
    OMNI_IF_LANES_GT(kKeysPerVector, 4)>
OMNI_INLINE void Merge16x8(D d, ADDR_D addrD, Traits st, V &v0, V &v1, V &v2, V &v3, V &v4, V &v5, V &v6, V &v7, V &v8,
    V &v9, V &va, V &vb, V &vc, V &vd, V &ve, V &vf, A &a0, A &a1, A &a2, A &a3, A &a4, A &a5, A &a6, A &a7, A &a8,
    A &a9, A &aa, A &ab, A &ac, A &ad, A &ae, A &af)
{
    vf = st.ReverseKeys8(d, vf);
    ve = st.ReverseKeys8(d, ve);
    vd = st.ReverseKeys8(d, vd);
    vc = st.ReverseKeys8(d, vc);
    vb = st.ReverseKeys8(d, vb);
    va = st.ReverseKeys8(d, va);
    v9 = st.ReverseKeys8(d, v9);
    v8 = st.ReverseKeys8(d, v8);

    af = st.ReverseKeys8(addrD, af);
    ae = st.ReverseKeys8(addrD, ae);
    ad = st.ReverseKeys8(addrD, ad);
    ac = st.ReverseKeys8(addrD, ac);
    ab = st.ReverseKeys8(addrD, ab);
    aa = st.ReverseKeys8(addrD, aa);
    a9 = st.ReverseKeys8(addrD, a9);
    a8 = st.ReverseKeys8(addrD, a8);

    st.Sort2(d, v0, vf, a0, af);
    st.Sort2(d, v1, ve, a1, ae);
    st.Sort2(d, v2, vd, a2, ad);
    st.Sort2(d, v3, vc, a3, ac);
    st.Sort2(d, v4, vb, a4, ab);
    st.Sort2(d, v5, va, a5, aa);
    st.Sort2(d, v6, v9, a6, a9);
    st.Sort2(d, v7, v8, a7, a8);

    v7 = st.ReverseKeys8(d, v7);
    v6 = st.ReverseKeys8(d, v6);
    v5 = st.ReverseKeys8(d, v5);
    v4 = st.ReverseKeys8(d, v4);
    vf = st.ReverseKeys8(d, vf);
    ve = st.ReverseKeys8(d, ve);
    vd = st.ReverseKeys8(d, vd);
    vc = st.ReverseKeys8(d, vc);

    a7 = st.ReverseKeys8(addrD, a7);
    a6 = st.ReverseKeys8(addrD, a6);
    a5 = st.ReverseKeys8(addrD, a5);
    a4 = st.ReverseKeys8(addrD, a4);
    af = st.ReverseKeys8(addrD, af);
    ae = st.ReverseKeys8(addrD, ae);
    ad = st.ReverseKeys8(addrD, ad);
    ac = st.ReverseKeys8(addrD, ac);

    st.Sort2(d, v0, v7, a0, a7);
    st.Sort2(d, v1, v6, a1, a6);
    st.Sort2(d, v2, v5, a2, a5);
    st.Sort2(d, v3, v4, a3, a4);
    st.Sort2(d, v8, vf, a8, af);
    st.Sort2(d, v9, ve, a9, ae);
    st.Sort2(d, va, vd, aa, ad);
    st.Sort2(d, vb, vc, ab, ac);

    v3 = st.ReverseKeys8(d, v3);
    v2 = st.ReverseKeys8(d, v2);
    v7 = st.ReverseKeys8(d, v7);
    v6 = st.ReverseKeys8(d, v6);
    vb = st.ReverseKeys8(d, vb);
    va = st.ReverseKeys8(d, va);
    vf = st.ReverseKeys8(d, vf);
    ve = st.ReverseKeys8(d, ve);

    a3 = st.ReverseKeys8(addrD, a3);
    a2 = st.ReverseKeys8(addrD, a2);
    a7 = st.ReverseKeys8(addrD, a7);
    a6 = st.ReverseKeys8(addrD, a6);
    ab = st.ReverseKeys8(addrD, ab);
    aa = st.ReverseKeys8(addrD, aa);
    af = st.ReverseKeys8(addrD, af);
    ae = st.ReverseKeys8(addrD, ae);

    st.Sort2(d, v0, v3, a0, a3);
    st.Sort2(d, v1, v2, a1, a2);
    st.Sort2(d, v4, v7, a4, a7);
    st.Sort2(d, v5, v6, a5, a6);
    st.Sort2(d, v8, vb, a8, ab);
    st.Sort2(d, v9, va, a9, aa);
    st.Sort2(d, vc, vf, ac, af);
    st.Sort2(d, vd, ve, ad, ae);

    v1 = st.ReverseKeys8(d, v1);
    v3 = st.ReverseKeys8(d, v3);
    v5 = st.ReverseKeys8(d, v5);
    v7 = st.ReverseKeys8(d, v7);
    v9 = st.ReverseKeys8(d, v9);
    vb = st.ReverseKeys8(d, vb);
    vd = st.ReverseKeys8(d, vd);
    vf = st.ReverseKeys8(d, vf);

    a1 = st.ReverseKeys8(addrD, a1);
    a3 = st.ReverseKeys8(addrD, a3);
    a5 = st.ReverseKeys8(addrD, a5);
    a7 = st.ReverseKeys8(addrD, a7);
    a9 = st.ReverseKeys8(addrD, a9);
    ab = st.ReverseKeys8(addrD, ab);
    ad = st.ReverseKeys8(addrD, ad);
    af = st.ReverseKeys8(addrD, af);

    st.Sort2(d, v0, v1, a0, a1);
    st.Sort2(d, v2, v3, a2, a3);
    st.Sort2(d, v4, v5, a4, a5);
    st.Sort2(d, v6, v7, a6, a7);
    st.Sort2(d, v8, v9, a8, a9);
    st.Sort2(d, va, vb, aa, ab);
    st.Sort2(d, vc, vd, ac, ad);
    st.Sort2(d, ve, vf, ae, af);

    v0 = st.SortPairsReverse8(d, v0, a0);
    v1 = st.SortPairsReverse8(d, v1, a1);
    v2 = st.SortPairsReverse8(d, v2, a2);
    v3 = st.SortPairsReverse8(d, v3, a3);
    v4 = st.SortPairsReverse8(d, v4, a4);
    v5 = st.SortPairsReverse8(d, v5, a5);
    v6 = st.SortPairsReverse8(d, v6, a6);
    v7 = st.SortPairsReverse8(d, v7, a7);
    v8 = st.SortPairsReverse8(d, v8, a8);
    v9 = st.SortPairsReverse8(d, v9, a9);
    va = st.SortPairsReverse8(d, va, aa);
    vb = st.SortPairsReverse8(d, vb, ab);
    vc = st.SortPairsReverse8(d, vc, ac);
    vd = st.SortPairsReverse8(d, vd, ad);
    ve = st.SortPairsReverse8(d, ve, ae);
    vf = st.SortPairsReverse8(d, vf, af);

    v0 = st.SortPairsDistance2(d, v0, a0);
    v1 = st.SortPairsDistance2(d, v1, a1);
    v2 = st.SortPairsDistance2(d, v2, a2);
    v3 = st.SortPairsDistance2(d, v3, a3);
    v4 = st.SortPairsDistance2(d, v4, a4);
    v5 = st.SortPairsDistance2(d, v5, a5);
    v6 = st.SortPairsDistance2(d, v6, a6);
    v7 = st.SortPairsDistance2(d, v7, a7);
    v8 = st.SortPairsDistance2(d, v8, a8);
    v9 = st.SortPairsDistance2(d, v9, a9);
    va = st.SortPairsDistance2(d, va, aa);
    vb = st.SortPairsDistance2(d, vb, ab);
    vc = st.SortPairsDistance2(d, vc, ac);
    vd = st.SortPairsDistance2(d, vd, ad);
    ve = st.SortPairsDistance2(d, ve, ae);
    vf = st.SortPairsDistance2(d, vf, af);

    st.SortPairsDistance1(d, v0, a0);
    st.SortPairsDistance1(d, v1, a1);
    st.SortPairsDistance1(d, v2, a2);
    st.SortPairsDistance1(d, v3, a3);
    st.SortPairsDistance1(d, v4, a4);
    st.SortPairsDistance1(d, v5, a5);
    st.SortPairsDistance1(d, v6, a6);
    st.SortPairsDistance1(d, v7, a7);
    st.SortPairsDistance1(d, v8, a8);
    st.SortPairsDistance1(d, v9, a9);
    st.SortPairsDistance1(d, va, aa);
    st.SortPairsDistance1(d, vb, ab);
    st.SortPairsDistance1(d, vc, ac);
    st.SortPairsDistance1(d, vd, ad);
    st.SortPairsDistance1(d, ve, ae);
    st.SortPairsDistance1(d, vf, af);
}

template <class Traits, typename T>
OMNI_API void Sort2To2(Traits st, T *OMNI_RESTRICT keys, uint64_t *OMNI_RESTRICT addr, size_t num_lanes,
    T *OMNI_RESTRICT /* valueBuf */, uint64_t *OMNI_RESTRICT /* addrBuf */)
{
    constexpr size_t kLPK = 1;

    // One key per vector, required to avoid reading past the end of `keys`.
    const CappedTag<T, kLPK> d;
    using V = Vec<decltype(d)>;
    const CappedTag<uint64_t, kLPK> addrD;
    using A = Vec<decltype(addrD)>;

    V v0 = LoadU(d, keys + 0x0 * kLPK);
    V v1 = LoadU(d, keys + 0x1 * kLPK);

    A a0 = LoadU(addrD, addr + 0x0 * kLPK);
    A a1 = LoadU(addrD, addr + 0x1 * kLPK);

    Sort2(d, st, v0, v1, a0, a1);

    StoreU(v0, d, keys + 0x0 * kLPK);
    StoreU(v1, d, keys + 0x1 * kLPK);

    StoreU(a0, addrD, addr + 0x0 * kLPK);
    StoreU(a1, addrD, addr + 0x1 * kLPK);
}

template <size_t kKeysPerRow = 2, class Traits, typename T>
OMNI_INLINE void Sort3Rows(Traits st, T *OMNI_RESTRICT keys, size_t num_lanes)
{
    constexpr size_t kLPK = st.LanesPerKey() * kKeysPerRow;
    // One key per vector, required to avoid reading past the end of `keys`.
    const CappedTag<T, kLPK> d;
    using V = Vec<decltype(d)>;

    V v0 = LoadU(d, keys + 0x0 * kLPK);
    V v1 = LoadU(d, keys + 0x1 * kLPK);
    V v2 = LoadU(d, keys + 0x2 * kLPK);

    Sort3(d, st, v0, v1, v2);
    Merge3x2(d, st, v0, v1, v2);

    StoreU(v0, d, keys + 0x0 * kLPK);
    StoreU(v1, d, keys + 0x1 * kLPK);
    StoreU(v2, d, keys + 0x2 * kLPK);
}

template <class Traits, typename T>
OMNI_INLINE void Sort3To4(Traits st, T *OMNI_RESTRICT keys, uint64_t *OMNI_RESTRICT addr, size_t num_lanes,
    T *OMNI_RESTRICT valueBuf, uint64_t *OMNI_RESTRICT addrBuf)
{
    constexpr size_t kLPK = st.LanesPerKey();
    const size_t num_keys = num_lanes / kLPK;

    // One key per vector, required to avoid reading past the end of `keys`.
    const CappedTag<T, kLPK> d;
    using V = Vec<decltype(d)>;
    const CappedTag<uint64_t, kLPK> addrD;
    using A = Vec<decltype(addrD)>;

    // If num_keys == 3, initialize padding for the last sorting network element
    // so that it does not influence the other elements.
    Store(st.LastValue(d), d, valueBuf);

    // Points to a valid key, or padding. This avoids special-casing
    // OMNI_MEM_OPS_MIGHT_FAULT because there is only a single key per vector.
    T *in_out3 = num_keys == 3 ? valueBuf : keys + 0x3 * kLPK;
    uint64_t *in_out3_addr = num_keys == 3 ? addrBuf : addr + 0x3 * kLPK;

    V v0 = LoadU(d, keys + 0x0 * kLPK);
    V v1 = LoadU(d, keys + 0x1 * kLPK);
    V v2 = LoadU(d, keys + 0x2 * kLPK);
    V v3 = LoadU(d, in_out3);

    A a0 = LoadU(addrD, addr + 0x0 * kLPK);
    A a1 = LoadU(addrD, addr + 0x1 * kLPK);
    A a2 = LoadU(addrD, addr + 0x2 * kLPK);
    A a3 = LoadU(addrD, in_out3_addr);

    Sort4(d, st, v0, v1, v2, v3, a0, a1, a2, a3);

    StoreU(v0, d, keys + 0x0 * kLPK);
    StoreU(v1, d, keys + 0x1 * kLPK);
    StoreU(v2, d, keys + 0x2 * kLPK);
    StoreU(v3, d, in_out3);

    StoreU(a0, addrD, addr + 0x0 * kLPK);
    StoreU(a1, addrD, addr + 0x1 * kLPK);
    StoreU(a2, addrD, addr + 0x2 * kLPK);
    StoreU(a3, addrD, in_out3_addr);
}

template <size_t kKeysPerRow, class Traits, typename T>
void Sort8Rows(Traits st, T *OMNI_RESTRICT keys, uint64_t *OMNI_RESTRICT addresses, size_t num_lanes,
    T *OMNI_RESTRICT valueBuf, uint64_t *OMNI_RESTRICT addrBuf)
{
    // kKeysPerRow <= 4 because 8 64-bit keys implies 512-bit vectors, which
    // are likely slower than 16x4, so 8x4 is the largest we handle here.
    static_assert(kKeysPerRow <= 4, "error");

    constexpr size_t kLPK = 1;
    // We reshape the 1D keys into kRows x kKeysPerRow.
    constexpr size_t kRows = 8;
    constexpr size_t kLanesPerRow = kKeysPerRow * kLPK;
    constexpr size_t kMinLanes = kRows / 2 * kLanesPerRow;

    const CappedTag<T, kLanesPerRow> d;
    using V = Vec<decltype(d)>;

    // At least half the kRows are valid, otherwise a different function would
    // have been called to handle this num_lanes.
    V v0 = LoadU(d, keys + 0x0 * kLanesPerRow);
    V v1 = LoadU(d, keys + 0x1 * kLanesPerRow);
    V v2 = LoadU(d, keys + 0x2 * kLanesPerRow);
    V v3 = LoadU(d, keys + 0x3 * kLanesPerRow);
    CopyHalfToPaddedBuf<kRows, kLanesPerRow>(d, st, keys, num_lanes, valueBuf);
    V v4 = LoadU(d, valueBuf + 0x4 * kLanesPerRow);
    V v5 = LoadU(d, valueBuf + 0x5 * kLanesPerRow);
    V v6 = LoadU(d, valueBuf + 0x6 * kLanesPerRow);
    V v7 = LoadU(d, valueBuf + 0x7 * kLanesPerRow);

    const CappedTag<uint64_t, kLanesPerRow> addrD;
    using A = Vec<decltype(addrD)>;
    A a0 = LoadU(addrD, addresses + 0x0 * kLanesPerRow);
    A a1 = LoadU(addrD, addresses + 0x1 * kLanesPerRow);
    A a2 = LoadU(addrD, addresses + 0x2 * kLanesPerRow);
    A a3 = LoadU(addrD, addresses + 0x3 * kLanesPerRow);
    CopyHalfToPaddedBuf<kRows, kLanesPerRow>(addrD, st, addresses, num_lanes, addrBuf);
    A a4 = LoadU(addrD, addrBuf + 0x4 * kLanesPerRow);
    A a5 = LoadU(addrD, addrBuf + 0x5 * kLanesPerRow);
    A a6 = LoadU(addrD, addrBuf + 0x6 * kLanesPerRow);
    A a7 = LoadU(addrD, addrBuf + 0x7 * kLanesPerRow);


    Sort8(d, st, v0, v1, v2, v3, v4, v5, v6, v7, a0, a1, a2, a3, a4, a5, a6, a7);

    // Merge8x2t is a no-op if kKeysPerRow < 2 etc.
    Merge8x2<kKeysPerRow>(d, addrD, st, v0, v1, v2, v3, v4, v5, v6, v7, a0, a1, a2, a3, a4, a5, a6, a7);
    Merge8x4<kKeysPerRow>(d, addrD, st, v0, v1, v2, v3, v4, v5, v6, v7, a0, a1, a2, a3, a4, a5, a6, a7);

    StoreU(v0, d, keys + 0x0 * kLanesPerRow);
    StoreU(v1, d, keys + 0x1 * kLanesPerRow);
    StoreU(v2, d, keys + 0x2 * kLanesPerRow);
    StoreU(v3, d, keys + 0x3 * kLanesPerRow);

    // Store remaining vectors into buf and safely copy them into keys.
    StoreU(v4, d, valueBuf + 0x4 * kLanesPerRow);
    StoreU(v5, d, valueBuf + 0x5 * kLanesPerRow);
    StoreU(v6, d, valueBuf + 0x6 * kLanesPerRow);
    StoreU(v7, d, valueBuf + 0x7 * kLanesPerRow);

    const ScalableTag<T> dMax;
    const size_t nMax = Lanes(dMax);

    // The first half of vectors have already been stored unconditionally into
    // `keys`, so we do not copy them.
    size_t i = kMinLanes;
    for (; i + nMax <= num_lanes; i += nMax) {
        StoreU(LoadU(dMax, valueBuf + i), dMax, keys + i);
    }

    // Last iteration: copy partial vector
    const size_t remaining = num_lanes - i;
    SafeCopyN(remaining, dMax, valueBuf + i, keys + i);

    StoreU(a0, addrD, addresses + 0x0 * kLanesPerRow);
    StoreU(a1, addrD, addresses + 0x1 * kLanesPerRow);
    StoreU(a2, addrD, addresses + 0x2 * kLanesPerRow);
    StoreU(a3, addrD, addresses + 0x3 * kLanesPerRow);

    // Store remaining vectors into buf and safely copy them into keys.
    StoreU(a4, addrD, addrBuf + 0x4 * kLanesPerRow);
    StoreU(a5, addrD, addrBuf + 0x5 * kLanesPerRow);
    StoreU(a6, addrD, addrBuf + 0x6 * kLanesPerRow);
    StoreU(a7, addrD, addrBuf + 0x7 * kLanesPerRow);

    const ScalableTag<uint64_t> addrMax;
    const size_t addrNMax = Lanes(addrMax);

    i = kMinLanes;
    for (; i + addrNMax <= num_lanes; i += addrNMax) {
        StoreU(LoadU(addrMax, addrBuf + i), addrMax, addresses + i);
    }

    // Last iteration: copy partial vector
    const size_t addrRemaining = num_lanes - i;
    SafeCopyN(addrRemaining, addrMax, addrBuf + i, addresses + i);
}

template <size_t kKeysPerRow, class Traits, typename T>
void Sort16Rows(Traits st, T *OMNI_RESTRICT keys, uint64_t *OMNI_RESTRICT addr, size_t num_lanes,
    T *OMNI_RESTRICT valueBuf, uint64_t *OMNI_RESTRICT addrBuf)
{
    constexpr size_t kLPK = st.LanesPerKey();

    // We reshape the 1D keys into kRows x kKeysPerRow.
    constexpr size_t kRows = 16;
    constexpr size_t kLanesPerRow = kKeysPerRow * kLPK;
    constexpr size_t kMinLanes = kRows / 2 * kLanesPerRow;

    const CappedTag<T, kLanesPerRow> d;
    using V = Vec<decltype(d)>;
    const CappedTag<uint64_t, kLanesPerRow> addrD;
    using A = Vec<decltype(addrD)>;
    V v8, v9, va, vb, vc, vd, ve, vf;
    A a8, a9, aa, ab, ac, ad, ae, af;
    // At least half the kRows are valid, otherwise a different function would
    // have been called to handle this num_lanes.
    V v0 = LoadU(d, keys + 0x0 * kLanesPerRow);
    V v1 = LoadU(d, keys + 0x1 * kLanesPerRow);
    V v2 = LoadU(d, keys + 0x2 * kLanesPerRow);
    V v3 = LoadU(d, keys + 0x3 * kLanesPerRow);
    V v4 = LoadU(d, keys + 0x4 * kLanesPerRow);
    V v5 = LoadU(d, keys + 0x5 * kLanesPerRow);
    V v6 = LoadU(d, keys + 0x6 * kLanesPerRow);
    V v7 = LoadU(d, keys + 0x7 * kLanesPerRow);

    CopyHalfToPaddedBuf<kRows, kLanesPerRow>(d, st, keys, num_lanes, valueBuf);
    v8 = LoadU(d, valueBuf + 0x8 * kLanesPerRow);
    v9 = LoadU(d, valueBuf + 0x9 * kLanesPerRow);
    va = LoadU(d, valueBuf + 0xa * kLanesPerRow);
    vb = LoadU(d, valueBuf + 0xb * kLanesPerRow);
    vc = LoadU(d, valueBuf + 0xc * kLanesPerRow);
    vd = LoadU(d, valueBuf + 0xd * kLanesPerRow);
    ve = LoadU(d, valueBuf + 0xe * kLanesPerRow);
    vf = LoadU(d, valueBuf + 0xf * kLanesPerRow);

    A a0 = LoadU(addrD, addr + 0x0 * kLanesPerRow);
    A a1 = LoadU(addrD, addr + 0x1 * kLanesPerRow);
    A a2 = LoadU(addrD, addr + 0x2 * kLanesPerRow);
    A a3 = LoadU(addrD, addr + 0x3 * kLanesPerRow);
    A a4 = LoadU(addrD, addr + 0x4 * kLanesPerRow);
    A a5 = LoadU(addrD, addr + 0x5 * kLanesPerRow);
    A a6 = LoadU(addrD, addr + 0x6 * kLanesPerRow);
    A a7 = LoadU(addrD, addr + 0x7 * kLanesPerRow);

    CopyHalfToPaddedBuf<kRows, kLanesPerRow>(addrD, st, addr, num_lanes, addrBuf);
    a8 = LoadU(addrD, addrBuf + 0x8 * kLanesPerRow);
    a9 = LoadU(addrD, addrBuf + 0x9 * kLanesPerRow);
    aa = LoadU(addrD, addrBuf + 0xa * kLanesPerRow);
    ab = LoadU(addrD, addrBuf + 0xb * kLanesPerRow);
    ac = LoadU(addrD, addrBuf + 0xc * kLanesPerRow);
    ad = LoadU(addrD, addrBuf + 0xd * kLanesPerRow);
    ae = LoadU(addrD, addrBuf + 0xe * kLanesPerRow);
    af = LoadU(addrD, addrBuf + 0xf * kLanesPerRow);

    Sort16(d, st, v0, v1, v2, v3, v4, v5, v6, v7, v8, v9, va, vb, vc, vd, ve, vf, a0, a1, a2, a3, a4, a5, a6, a7, a8,
        a9, aa, ab, ac, ad, ae, af);

    // Merge16x4 is a no-op if kKeysPerRow < 4 etc.
    Merge16x2<kKeysPerRow>(d, addrD, st, v0, v1, v2, v3, v4, v5, v6, v7, v8, v9, va, vb, vc, vd, ve, vf, a0, a1, a2, a3,
        a4, a5, a6, a7, a8, a9, aa, ab, ac, ad, ae, af);
    Merge16x4<kKeysPerRow>(d, addrD, st, v0, v1, v2, v3, v4, v5, v6, v7, v8, v9, va, vb, vc, vd, ve, vf, a0, a1, a2, a3,
        a4, a5, a6, a7, a8, a9, aa, ab, ac, ad, ae, af);
    Merge16x8<kKeysPerRow>(d, addrD, st, v0, v1, v2, v3, v4, v5, v6, v7, v8, v9, va, vb, vc, vd, ve, vf, a0, a1, a2, a3,
        a4, a5, a6, a7, a8, a9, aa, ab, ac, ad, ae, af);


    StoreU(v0, d, keys + 0x0 * kLanesPerRow);
    StoreU(v1, d, keys + 0x1 * kLanesPerRow);
    StoreU(v2, d, keys + 0x2 * kLanesPerRow);
    StoreU(v3, d, keys + 0x3 * kLanesPerRow);
    StoreU(v4, d, keys + 0x4 * kLanesPerRow);
    StoreU(v5, d, keys + 0x5 * kLanesPerRow);
    StoreU(v6, d, keys + 0x6 * kLanesPerRow);
    StoreU(v7, d, keys + 0x7 * kLanesPerRow);

    // Store remaining vectors into buf and safely copy them into keys.
    StoreU(v8, d, valueBuf + 0x8 * kLanesPerRow);
    StoreU(v9, d, valueBuf + 0x9 * kLanesPerRow);
    StoreU(va, d, valueBuf + 0xa * kLanesPerRow);
    StoreU(vb, d, valueBuf + 0xb * kLanesPerRow);
    StoreU(vc, d, valueBuf + 0xc * kLanesPerRow);
    StoreU(vd, d, valueBuf + 0xd * kLanesPerRow);
    StoreU(ve, d, valueBuf + 0xe * kLanesPerRow);
    StoreU(vf, d, valueBuf + 0xf * kLanesPerRow);

    StoreU(a0, addrD, addr + 0x0 * kLanesPerRow);
    StoreU(a1, addrD, addr + 0x1 * kLanesPerRow);
    StoreU(a2, addrD, addr + 0x2 * kLanesPerRow);
    StoreU(a3, addrD, addr + 0x3 * kLanesPerRow);
    StoreU(a4, addrD, addr + 0x4 * kLanesPerRow);
    StoreU(a5, addrD, addr + 0x5 * kLanesPerRow);
    StoreU(a6, addrD, addr + 0x6 * kLanesPerRow);
    StoreU(a7, addrD, addr + 0x7 * kLanesPerRow);

    // Store remaining vectors into buf and safely copy them into keys.
    StoreU(a8, addrD, addrBuf + 0x8 * kLanesPerRow);
    StoreU(a9, addrD, addrBuf + 0x9 * kLanesPerRow);
    StoreU(aa, addrD, addrBuf + 0xa * kLanesPerRow);
    StoreU(ab, addrD, addrBuf + 0xb * kLanesPerRow);
    StoreU(ac, addrD, addrBuf + 0xc * kLanesPerRow);
    StoreU(ad, addrD, addrBuf + 0xd * kLanesPerRow);
    StoreU(ae, addrD, addrBuf + 0xe * kLanesPerRow);
    StoreU(af, addrD, addrBuf + 0xf * kLanesPerRow);

    const ScalableTag<T> dmax;
    const size_t Nmax = Lanes(dmax);

    // The first half of vectors have already been stored unconditionally into
    // `keys`, so we do not copy them.
    size_t i = kMinLanes;
    for (; i + Nmax <= num_lanes; i += Nmax) {
        StoreU(LoadU(dmax, valueBuf + i), dmax, keys + i);
    }
    i = kMinLanes;
    for (; i + Nmax <= num_lanes; i += Nmax) {
        StoreU(LoadU(addrD, addrBuf + i), addrD, addr + i);
    }

    // Last iteration: copy partial vector
    const size_t remaining = num_lanes - i;
    SafeCopyN(remaining, dmax, valueBuf + i, keys + i);
    const size_t addrRemaining = num_lanes - i;
    SafeCopyN(addrRemaining, addrD, addrBuf + i, addr + i);
}

template <class D, class TraitsKV, typename T>
void SmallCaseSort(D d, TraitsKV, T *OMNI_RESTRICT keys, uint64_t *addresses, size_t num_lanes, T *valueBuf,
    uint64_t *addrBuf)
{
    static_assert(std::is_same<T, int64_t>::value || std::is_same<T, double>::value,
        "Only long and double are allowed.");
    using Traits = typename TraitsKV::SharedTraitsForSortingNetwork;
    Traits st;
    constexpr size_t kLPK = 1;
    const size_t num_keys = num_lanes / kLPK;

    // Can be zero when called through HandleSpecialCases, but also 1 (in which
    // case the array is already sorted). Also ensures num_lanes - 1 != 0.
    if (OMNI_UNLIKELY(num_keys <= 1))
        return;

    const size_t ceilLog2 = 32 - __builtin_clz(static_cast<uint32_t>(num_keys - 1));

    // Checking kMaxKeysPerVector avoids generating unreachable codepaths.
    constexpr size_t kMaxKeysPerVector = MaxLanes(d) / kLPK;

    using FuncPtr = decltype(&Sort2To2<Traits, T>);
    const FuncPtr funcs[6] = {nullptr, &Sort2To2<Traits, T>, &Sort3To4<Traits, T>, &Sort8Rows<1, Traits, T>,
                              kMaxKeysPerVector >= 2 ? &Sort8Rows<2, Traits, T> : nullptr};
    funcs[ceilLog2](st, keys, addresses, num_lanes, valueBuf, addrBuf);
}

template <class RawDataType> void SmallCaseSortAsec(int64_t *array, uint64_t *address, int32_t from, int32_t to)
{
    int32_t dataLen = to - from;
    if (dataLen <= 1) {
        return;
    }

    OMNI_ALIGN RawDataType valueBuf[MAX_LEVELS];
    OMNI_ALIGN uint64_t addrBuf[MAX_LEVELS];
    const simd::MakeTraits<RawDataType, simd::SortAscending> st;
    using LaneType = typename decltype(st)::LaneType;
    const simd::ScalableTag<LaneType> d;
    SmallCaseSort(d, st, reinterpret_cast<RawDataType *>(array + from), address + from, dataLen, valueBuf, addrBuf);
}

template <class RawDataType> void SmallCaseSortDesc(int64_t *array, uint64_t *address, int32_t from, int32_t to)
{
    int32_t dataLen = to - from;
    if (dataLen <= 1) {
        return;
    }

    OMNI_ALIGN RawDataType valueBuf[MAX_LEVELS];
    OMNI_ALIGN uint64_t addrBuf[MAX_LEVELS];
    const simd::MakeTraits<RawDataType, simd::SortDescending> st;
    using LaneType = typename decltype(st)::LaneType;
    const simd::ScalableTag<LaneType> d;
    SmallCaseSort(d, st, reinterpret_cast<RawDataType *>(array + from), address + from, dataLen, valueBuf, addrBuf);
}
}
#endif // VQSORT_H
