/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
#ifndef OMNI_GENERIC_OPS_H
#define OMNI_GENERIC_OPS_H

#include "simd/base.h"

namespace simd {
// The lane type of a vector type, e.g. float for Vec<ScalableTag<float>>.
template <class V> using LaneType = decltype(GetLane(V()));

// Vector type, e.g. Vec128<float> for CappedTag<float, 4>. Useful as the return
// type of functions that do not take a vector argument, or as an argument type
// if the function only has a template argument for D, or for explicit type
// names instead of auto. This may be a built-in type.
template <class D> using Vec = decltype(Zero(D()));

// Mask type. Useful as the return type of functions that do not take a mask
// argument, or as an argument type if the function only has a template argument
// for D, or for explicit type names instead of auto.
template <class D> using Mask = decltype(MaskFromVec(Zero(D())));

// Returns the closest value to v within [lo, hi].
template <class V> OMNI_API V Clamp(const V v, const V lo, const V hi)
{
    return Min(Max(lo, v), hi);
}

template <size_t kLanes, class D> OMNI_API VFromD<D> CombineShiftRightLanes(D d, VFromD<D> hi, VFromD<D> lo)
{
    constexpr size_t kBytes = kLanes * sizeof(TFromD<D>);
    static_assert(kBytes < 16, "Shift count is per-block");
    return CombineShiftRightBytes<kBytes>(d, hi, lo);
}

// Returns lanes with the most significant bit set and all other bits zero.
template <class D> OMNI_API Vec<D> SignBit(D d)
{
    const RebindToUnsigned<decltype(d)> du;
    return BitCast(d, Set(du, SignMask<TFromD<D>>()));
}

// Returns quiet NaN.
template <class D> OMNI_API Vec<D> NaN(D d)
{
    const RebindToSigned<D> di;
    // LimitsMax sets all exponent and mantissa bits to 1. The exponent plus
    // mantissa MSB (to indicate quiet) would be sufficient.
    return BitCast(d, Set(di, LimitsMax<TFromD<decltype(di)>>()));
}

// Returns positive infinity.
template <class D> OMNI_API Vec<D> Inf(D d)
{
    const RebindToUnsigned<D> du;
    using T = TFromD<D>;
    using TU = TFromD<decltype(du)>;
    const TU max_x2 = static_cast<TU>(MaxExponentTimes2<T>());
    return BitCast(d, Set(du, max_x2 >> 1));
}

// ------------------------------ ZeroExtendResizeBitCast

// The implementation of detail::ZeroExtendResizeBitCast for the OMNI_EMU128
// target is in emu128-inl.h, and the implementation of
// detail::ZeroExtendResizeBitCast for the OMNI_SCALAR target is in scalar-inl.h
namespace detail {
#if OMNI_HAVE_SCALABLE
template <size_t kFromVectSize, size_t kToVectSize, class DTo, class DFrom>
OMNI_INLINE VFromD<DTo> ZeroExtendResizeBitCast(simd::SizeTag<kFromVectSize> /* from_size_tag */,
    simd::SizeTag<kToVectSize> /* to_size_tag */, DTo d_to, DFrom d_from, VFromD<DFrom> v)
{
    const Repartition<uint8_t, DTo> d_to_u8;
    const auto resized = ResizeBitCast(d_to_u8, v);
    // Zero the upper bytes which were not present/valid in d_from.
    const size_t num_bytes = Lanes(Repartition<uint8_t, decltype(d_from)>());
    return BitCast(d_to, IfThenElseZero(FirstN(d_to_u8, num_bytes), resized));
}
#else // target that uses fixed-size vectors

// Truncating or same-size resizing cast: same as ResizeBitCast
template <size_t kFromVectSize, size_t kToVectSize, class DTo, class DFrom,
    OMNI_IF_LANES_LE(kToVectSize, kFromVectSize)>
OMNI_INLINE VFromD<DTo> ZeroExtendResizeBitCast(simd::SizeTag<kFromVectSize> /* from_size_tag */,
    simd::SizeTag<kToVectSize> /* to_size_tag */, DTo d_to, DFrom /* d_from */, VFromD<DFrom> v)
{
    return ResizeBitCast(d_to, v);
}

// Resizing cast to vector that has twice the number of lanes of the source
// vector
template <size_t kFromVectSize, size_t kToVectSize, class DTo, class DFrom,
    OMNI_IF_LANES(kToVectSize, kFromVectSize * 2)>
OMNI_INLINE VFromD<DTo> ZeroExtendResizeBitCast(simd::SizeTag<kFromVectSize> /* from_size_tag */,
    simd::SizeTag<kToVectSize> /* to_size_tag */, DTo d_to, DFrom d_from, VFromD<DFrom> v)
{
    const Twice<decltype(d_from)> dt_from;
    return BitCast(d_to, ZeroExtendVector(dt_from, v));
}

// Resizing cast to vector that has more than twice the number of lanes of the
// source vector
template <size_t kFromVectSize, size_t kToVectSize, class DTo, class DFrom,
    OMNI_IF_LANES_GT(kToVectSize, kFromVectSize * 2)>
OMNI_INLINE VFromD<DTo> ZeroExtendResizeBitCast(simd::SizeTag<kFromVectSize> /* from_size_tag */,
    simd::SizeTag<kToVectSize> /* to_size_tag */, DTo d_to, DFrom /* d_from */, VFromD<DFrom> v)
{
    using TFrom = TFromD<DFrom>;
    constexpr size_t kNumOfFromLanes = kFromVectSize / sizeof(TFrom);
    const Repartition<TFrom, decltype(d_to)> d_resize_to;
    return BitCast(d_to, IfThenElseZero(FirstN(d_resize_to, kNumOfFromLanes), ResizeBitCast(d_resize_to, v)));
}

#endif // OMNI_HAVE_SCALABLE
} // namespace detail

template <class DTo, class DFrom> OMNI_API VFromD<DTo> ZeroExtendResizeBitCast(DTo d_to, DFrom d_from, VFromD<DFrom> v)
{
    return detail::ZeroExtendResizeBitCast(simd::SizeTag<d_from.MaxBytes()>(), simd::SizeTag<d_to.MaxBytes()>(), d_to,
        d_from, v);
}

// ------------------------------ SafeFillN

template <class D, typename T = TFromD<D>>
OMNI_API void SafeFillN(const size_t num, const T value, D d, T *OMNI_RESTRICT to)
{
#if OMNI_MEM_OPS_MIGHT_FAULT
    (void)d;
    for (size_t i = 0; i < num; ++i) {
        to[i] = value;
    }
#else
    BlendedStore(Set(d, value), FirstN(d, num), d, to);
#endif
}

// ------------------------------ SafeCopyN

template <class D, typename T = TFromD<D>>
OMNI_API void SafeCopyN(const size_t num, D d, const T *OMNI_RESTRICT from, T *OMNI_RESTRICT to)
{
#if OMNI_MEM_OPS_MIGHT_FAULT
    (void)d;
    for (size_t i = 0; i < num; ++i) {
        to[i] = from[i];
    }
#else
    const Mask<D> mask = FirstN(d, num);
    BlendedStore(MaskedLoad(mask, d, from), mask, d, to);
#endif
}

// ------------------------------ IsNegative
#ifndef OMNI_NATIVE_IS_NEGATIVE
#define OMNI_NATIVE_IS_NEGATIVE

template <class V, OMNI_IF_NOT_UNSIGNED_V(V)> OMNI_API Mask<DFromV<V>> IsNegative(V v)
{
    const DFromV<decltype(v)> d;
    const RebindToSigned<decltype(d)> di;
    return RebindMask(d, MaskFromVec(BroadcastSignBit(BitCast(di, v))));
}

#endif // OMNI_NATIVE_IS_NEGATIVE

// ------------------------------ MaskFalse
#ifndef OMNI_NATIVE_MASK_FALSE
#define OMNI_NATIVE_MASK_FALSE

template <class D> OMNI_API Mask<D> MaskFalse(D d)
{
    return MaskFromVec(Zero(d));
}

#endif // OMNI_NATIVE_MASK_FALSE

// ------------------------------ IfNegativeThenElseZero
#ifndef OMNI_NATIVE_IF_NEG_THEN_ELSE_ZERO
#define OMNI_NATIVE_IF_NEG_THEN_ELSE_ZERO

template <class V, OMNI_IF_NOT_UNSIGNED_V(V)> OMNI_API V IfNegativeThenElseZero(V v, V yes)
{
    return IfThenElseZero(IsNegative(v), yes);
}

#endif // OMNI_NATIVE_IF_NEG_THEN_ELSE_ZERO

// ------------------------------ IfNegativeThenZeroElse
#ifndef OMNI_NATIVE_IF_NEG_THEN_ZERO_ELSE
#define OMNI_NATIVE_IF_NEG_THEN_ZERO_ELSE

template <class V, OMNI_IF_NOT_UNSIGNED_V(V)> OMNI_API V IfNegativeThenZeroElse(V v, V no)
{
    return IfThenZeroElse(IsNegative(v), no);
}

#endif // OMNI_NATIVE_IF_NEG_THEN_ZERO_ELSE

// ------------------------------ ZeroIfNegative (IfNegativeThenZeroElse)

// ZeroIfNegative is generic for all vector lengths
template <class V, OMNI_IF_NOT_UNSIGNED_V(V)> OMNI_API V ZeroIfNegative(V v)
{
    return IfNegativeThenZeroElse(v, v);
}

// ------------------------------ BitwiseIfThenElse
#ifndef OMNI_NATIVE_BITWISE_IF_THEN_ELSE
#define OMNI_NATIVE_BITWISE_IF_THEN_ELSE

template <class V> OMNI_API V BitwiseIfThenElse(V mask, V yes, V no)
{
    return Or(And(mask, yes), AndNot(mask, no));
}

#endif // OMNI_NATIVE_BITWISE_IF_THEN_ELSE

// ------------------------------ PromoteMaskTo

#ifndef OMNI_NATIVE_PROMOTE_MASK_TO
#define OMNI_NATIVE_PROMOTE_MASK_TO

template <class DTo, class DFrom> OMNI_API Mask<DTo> PromoteMaskTo(DTo d_to, DFrom d_from, Mask<DFrom> m)
{
    static_assert(sizeof(TFromD<DTo>) > sizeof(TFromD<DFrom>),
        "sizeof(TFromD<DTo>) must be greater than sizeof(TFromD<DFrom>)");
    static_assert(IsSame<Mask<DFrom>, Mask<Rebind<TFromD<DFrom>, DTo>>>(),
        "Mask<DFrom> must be the same type as Mask<Rebind<TFromD<DFrom>, DTo>>");

    const RebindToSigned<decltype(d_to)> di_to;
    const RebindToSigned<decltype(d_from)> di_from;

    return MaskFromVec(BitCast(d_to, PromoteTo(di_to, BitCast(di_from, VecFromMask(d_from, m)))));
}

#endif // OMNI_NATIVE_PROMOTE_MASK_TO

// ------------------------------ DemoteMaskTo

#ifndef OMNI_NATIVE_DEMOTE_MASK_TO
#define OMNI_NATIVE_DEMOTE_MASK_TO

template <class DTo, class DFrom> OMNI_API Mask<DTo> DemoteMaskTo(DTo d_to, DFrom d_from, Mask<DFrom> m)
{
    static_assert(sizeof(TFromD<DTo>) < sizeof(TFromD<DFrom>),
        "sizeof(TFromD<DTo>) must be less than sizeof(TFromD<DFrom>)");
    static_assert(IsSame<Mask<DFrom>, Mask<Rebind<TFromD<DFrom>, DTo>>>(),
        "Mask<DFrom> must be the same type as Mask<Rebind<TFromD<DFrom>, DTo>>");

    const RebindToSigned<decltype(d_to)> di_to;
    const RebindToSigned<decltype(d_from)> di_from;

    return MaskFromVec(BitCast(d_to, DemoteTo(di_to, BitCast(di_from, VecFromMask(d_from, m)))));
}

#endif // OMNI_NATIVE_DEMOTE_MASK_TO

// ------------------------------ CombineMasks

#ifndef OMNI_NATIVE_COMBINE_MASKS
#define OMNI_NATIVE_COMBINE_MASKS

template <class D> OMNI_API Mask<D> CombineMasks(D d, Mask<Half<D>> hi, Mask<Half<D>> lo)
{
    const Half<decltype(d)> dh;
    return MaskFromVec(Combine(d, VecFromMask(dh, hi), VecFromMask(dh, lo)));
}

#endif // OMNI_NATIVE_COMBINE_MASKS

// ------------------------------ LowerHalfOfMask

#ifndef OMNI_NATIVE_LOWER_HALF_OF_MASK
#define OMNI_NATIVE_LOWER_HALF_OF_MASK

template <class D> OMNI_API Mask<D> LowerHalfOfMask(D d, Mask<Twice<D>> m)
{
    const Twice<decltype(d)> dt;
    return MaskFromVec(LowerHalf(d, VecFromMask(dt, m)));
}

#endif // OMNI_NATIVE_LOWER_HALF_OF_MASK

// ------------------------------ UpperHalfOfMask

#ifndef OMNI_NATIVE_UPPER_HALF_OF_MASK
#define OMNI_NATIVE_UPPER_HALF_OF_MASK

template <class D> OMNI_API Mask<D> UpperHalfOfMask(D d, Mask<Twice<D>> m)
{
    const Twice<decltype(d)> dt;
    return MaskFromVec(UpperHalf(d, VecFromMask(dt, m)));
}

#endif // OMNI_NATIVE_UPPER_HALF_OF_MASK

// ------------------------------ OrderedDemote2MasksTo

#ifndef OMNI_NATIVE_ORDERED_DEMOTE_2_MASKS_TO) == defined(OMNI_TARGET_TOGGLE))
#ifdef OMNI_NATIVE_ORDERED_DEMOTE_2_MASKS_TO
#undef OMNI_NATIVE_ORDERED_DEMOTE_2_MASKS_TO
#else
#define OMNI_NATIVE_ORDERED_DEMOTE_2_MASKS_TO
#endif

template <class DTo, class DFrom>
OMNI_API Mask<DTo> OrderedDemote2MasksTo(DTo d_to, DFrom d_from, Mask<DFrom> a, Mask<DFrom> b)
{
    static_assert(sizeof(TFromD<DTo>) == sizeof(TFromD<DFrom>) / 2,
        "sizeof(TFromD<DTo>) must be equal to sizeof(TFromD<DFrom>) / 2");
    static_assert(IsSame<Mask<DTo>, Mask<Repartition<TFromD<DTo>, DFrom>>>(), "Mask<DTo> must be the same type as "
        "Mask<Repartition<TFromD<DTo>, DFrom>>>()");

    const RebindToSigned<decltype(d_from)> di_from;
    const RebindToSigned<decltype(d_to)> di_to;

    const auto va = BitCast(di_from, VecFromMask(d_from, a));
    const auto vb = BitCast(di_from, VecFromMask(d_from, b));
    return MaskFromVec(BitCast(d_to, OrderedDemote2To(di_to, va, vb)));
}

#endif // OMNI_NATIVE_ORDERED_DEMOTE_2_MASKS_TO

// ------------------------------ RotateLeft
template <int kBits, class V, OMNI_IF_NOT_FLOAT_NOR_SPECIAL_V(V)> OMNI_API V RotateLeft(V v)
{
    constexpr size_t kSizeInBits = sizeof(TFromV<V>) * 8;
    static_assert(0 <= kBits && kBits < kSizeInBits, "Invalid shift count");

    constexpr int kRotateRightAmt = (kBits == 0) ? 0 : static_cast<int>(kSizeInBits) - kBits;
    return RotateRight<kRotateRightAmt>(v);
}

// ------------------------------ InterleaveWholeLower/InterleaveWholeUpper
#ifndef OMNI_NATIVE_INTERLEAVE_WHOLE
#define OMNI_NATIVE_INTERLEAVE_WHOLE

template <class D, OMNI_IF_V_SIZE_LE_D(D, 16)> OMNI_API VFromD<D> InterleaveWholeLower(D d, VFromD<D> a, VFromD<D> b)
{
    // InterleaveWholeLower(d, a, b) is equivalent to InterleaveLower(a, b) if
    // D().MaxBytes() <= 16 is true
    return InterleaveLower(d, a, b);
}
template <class D, OMNI_IF_V_SIZE_LE_D(D, 16)> OMNI_API VFromD<D> InterleaveWholeUpper(D d, VFromD<D> a, VFromD<D> b)
{
    // InterleaveWholeUpper(d, a, b) is equivalent to InterleaveUpper(a, b) if
    // D().MaxBytes() <= 16 is true
    return InterleaveUpper(d, a, b);
}
#endif // OMNI_NATIVE_INTERLEAVE_WHOLE

// The InterleaveWholeLower without the optional D parameter is generic for all
// vector lengths.
template <class V> OMNI_API V InterleaveWholeLower(V a, V b)
{
    return InterleaveWholeLower(DFromV<V>(), a, b);
}

// ------------------------------ InterleaveEven

// InterleaveEven without the optional D parameter is generic for all vector
// lengths
template <class V> OMNI_API V InterleaveEven(V a, V b)
{
    return InterleaveEven(DFromV<V>(), a, b);
}

// ------------------------------ AddSub

template <class V, OMNI_IF_LANES_D(DFromV<V>, 1)> OMNI_API V AddSub(V a, V b)
{
    // AddSub(a, b) for a one-lane vector is equivalent to Sub(a, b)
    return Sub(a, b);
}

// AddSub for integer vectors on SVE2 is implemented in arm_sve-inl.h
template <class V, OMNI_IF_ADDSUB_V(V)> OMNI_API V AddSub(V a, V b)
{
    using D = DFromV<decltype(a)>;
    using T = TFromD<D>;
    using TNegate = If<!simd::IsSigned<T>(), MakeSigned<T>, T>;

    const D d;
    const Rebind<TNegate, D> d_negate;

    // Negate the even lanes of b
    const auto negated_even_b = OddEven(b, BitCast(d, Neg(BitCast(d_negate, b))));

    return Add(a, negated_even_b);
}

// ------------------------------ MaskedAddOr etc.
#ifndef OMNI_NATIVE_MASKED_ARITH
#define OMNI_NATIVE_MASKED_ARITH

template <class V, class M> OMNI_API V MaskedMinOr(V no, M m, V a, V b)
{
    return IfThenElse(m, Min(a, b), no);
}

template <class V, class M> OMNI_API V MaskedMaxOr(V no, M m, V a, V b)
{
    return IfThenElse(m, Max(a, b), no);
}

template <class V, class M> OMNI_API V MaskedAddOr(V no, M m, V a, V b)
{
    return IfThenElse(m, Add(a, b), no);
}

template <class V, class M> OMNI_API V MaskedSubOr(V no, M m, V a, V b)
{
    return IfThenElse(m, Sub(a, b), no);
}

template <class V, class M> OMNI_API V MaskedMulOr(V no, M m, V a, V b)
{
    return IfThenElse(m, Mul(a, b), no);
}

template <class V, class M> OMNI_API V MaskedDivOr(V no, M m, V a, V b)
{
    return IfThenElse(m, Div(a, b), no);
}

template <class V, class M> OMNI_API V MaskedModOr(V no, M m, V a, V b)
{
    return IfThenElse(m, Mod(a, b), no);
}

template <class V, class M> OMNI_API V MaskedSatAddOr(V no, M m, V a, V b)
{
    return IfThenElse(m, SaturatedAdd(a, b), no);
}

template <class V, class M> OMNI_API V MaskedSatSubOr(V no, M m, V a, V b)
{
    return IfThenElse(m, SaturatedSub(a, b), no);
}
#endif // OMNI_NATIVE_MASKED_ARITH

// ------------------------------ IfNegativeThenNegOrUndefIfZero

#ifndef OMNI_NATIVE_INTEGER_IF_NEGATIVE_THEN_NEG) == defined(OMNI_TARGET_TOGGLE))
#ifdef OMNI_NATIVE_INTEGER_IF_NEGATIVE_THEN_NEG
#undef OMNI_NATIVE_INTEGER_IF_NEGATIVE_THEN_NEG
#else
#define OMNI_NATIVE_INTEGER_IF_NEGATIVE_THEN_NEG
#endif

template <class V, OMNI_IF_NOT_FLOAT_NOR_SPECIAL_V(V)> OMNI_API V IfNegativeThenNegOrUndefIfZero(V mask, V v)
{
#if OMNI_HAVE_SCALABLE || OMNI_TARGET_IS_SVE
    // MaskedSubOr is more efficient than IfNegativeThenElse on RVV/SVE
    const auto zero = Zero(DFromV<V>());
    return MaskedSubOr(v, Lt(mask, zero), zero, v);
#else
    return IfNegativeThenElse(mask, Neg(v), v);
#endif
}

#endif // OMNI_NATIVE_INTEGER_IF_NEGATIVE_THEN_NEG

template <class V, OMNI_IF_FLOAT_V(V)> OMNI_API V IfNegativeThenNegOrUndefIfZero(V mask, V v)
{
    return CopySign(v, Xor(mask, v));
}

// ------------------------------ SaturatedNeg

#ifndef OMNI_NATIVE_SATURATED_NEG_8_16_32
#define OMNI_NATIVE_SATURATED_NEG_8_16_32

template <class V, OMNI_IF_T_SIZE_ONE_OF_V(V, (1 << 1) | (1 << 2)), OMNI_IF_SIGNED_V(V)> OMNI_API V SaturatedNeg(V v)
{
    const DFromV<decltype(v)> d;
    return SaturatedSub(Zero(d), v);
}

template <class V, OMNI_IF_I32(TFromV<V>)> OMNI_API V SaturatedNeg(V v)
{
    const DFromV<decltype(v)> d;
    return SaturatedSub(Zero(d), v);
}

#endif // OMNI_NATIVE_SATURATED_NEG_8_16_32

#ifndef OMNI_NATIVE_SATURATED_NEG_64
#define OMNI_NATIVE_SATURATED_NEG_64

template <class V, OMNI_IF_I64(TFromV<V>)> OMNI_API V SaturatedNeg(V v)
{
#if OMNI_TARGET == OMNI_RVV || OMNI_TARGET_IS_SVE || OMNI_TARGET_IS_NEON
    // RVV/SVE/NEON have native I64 SaturatedSub instructions
    const DFromV<decltype(v)> d;
    return SaturatedSub(Zero(d), v);
#else
    const auto neg_v = Neg(v);
    return Add(neg_v, BroadcastSignBit(And(v, neg_v)));
#endif
}

#endif // OMNI_NATIVE_SATURATED_NEG_64

// ------------------------------ SaturatedAbs

#ifndef OMNI_NATIVE_SATURATED_ABS
#define OMNI_NATIVE_SATURATED_ABS

template <class V, OMNI_IF_SIGNED_V(V)> OMNI_API V SaturatedAbs(V v)
{
    return Max(v, SaturatedNeg(v));
}

#endif

// ------------------------------ Reductions

// Targets follow one of two strategies. If OMNI_NATIVE_REDUCE_SCALAR is toggled,
// they SVE implement ReduceSum and SumOfLanes via Set.
// Otherwise, they (Armv7) define zero to most of the
// SumOfLanes overloads. For the latter group, we here define the remaining
// overloads, plus ReduceSum which uses them plus GetLane.
#ifndef OMNI_NATIVE_REDUCE_SCALAR
#define OMNI_NATIVE_REDUCE_SCALAR

namespace detail {
// Allows reusing the same shuffle code for SumOfLanes/MinOfLanes/MaxOfLanes.
struct AddFunc {
    template <class V> V operator () (V a, V b) const
    {
        return Add(a, b);
    }
};

struct MinFunc {
    template <class V> V operator () (V a, V b) const
    {
        return Min(a, b);
    }
};

struct MaxFunc {
    template <class V> V operator () (V a, V b) const
    {
        return Max(a, b);
    }
};

// No-op for vectors of at most one block.
template <class D, class Func, OMNI_IF_V_SIZE_LE_D(D, 16)>
OMNI_INLINE VFromD<D> ReduceAcrossBlocks(D, Func, VFromD<D> v)
{
    return v;
}

// Reduces a lane with its counterpart in other block(s). Shared by AVX2 and
// WASM_EMU256. AVX3 has its own overload.
template <class D, class Func, OMNI_IF_V_SIZE_D(D, 32)>
OMNI_INLINE VFromD<D> ReduceAcrossBlocks(D /* d */, Func f, VFromD<D> v)
{
    return f(v, SwapAdjacentBlocks(v));
}

// These return the reduction result broadcasted across all lanes. They assume
// the caller has already reduced across blocks.

template <class D, class Func, OMNI_IF_LANES_PER_BLOCK_D(D, 2)>
OMNI_INLINE VFromD<D> ReduceWithinBlocks(D d, Func f, VFromD<D> v10)
{
    return f(v10, Reverse2(d, v10));
}

template <class D, class Func, OMNI_IF_LANES_PER_BLOCK_D(D, 4)>
OMNI_INLINE VFromD<D> ReduceWithinBlocks(D d, Func f, VFromD<D> v3210)
{
    const VFromD<D> v0123 = Reverse4(d, v3210);
    const VFromD<D> v03_12_12_03 = f(v3210, v0123);
    const VFromD<D> v12_03_03_12 = Reverse2(d, v03_12_12_03);
    return f(v03_12_12_03, v12_03_03_12);
}

template <class D, class Func, OMNI_IF_LANES_PER_BLOCK_D(D, 8)>
OMNI_INLINE VFromD<D> ReduceWithinBlocks(D d, Func f, VFromD<D> v76543210)
{
    // The upper half is reversed from the lower half; omit for brevity.
    const VFromD<D> v34_25_16_07 = f(v76543210, Reverse8(d, v76543210));
    const VFromD<D> v0347_1625_1625_0347 = f(v34_25_16_07, Reverse4(d, v34_25_16_07));
    return f(v0347_1625_1625_0347, Reverse2(d, v0347_1625_1625_0347));
}

template <class D, class Func, OMNI_IF_LANES_PER_BLOCK_D(D, 16), OMNI_IF_U8_D(D)>
OMNI_INLINE VFromD<D> ReduceWithinBlocks(D d, Func f, VFromD<D> v)
{
    const RepartitionToWide<decltype(d)> dw;
    using VW = VFromD<decltype(dw)>;
    const VW vw = BitCast(dw, v);
    // f is commutative, so no need to adapt for OMNI_IS_LITTLE_ENDIAN.
    const VW even = And(vw, Set(dw, 0xFF));
    const VW odd = ShiftRight<8>(vw);
    const VW reduced = ReduceWithinBlocks(dw, f, f(even, odd));
#if OMNI_IS_LITTLE_ENDIAN
    return DupEven(BitCast(d, reduced));
#else
    return DupOdd(BitCast(d, reduced));
#endif
}

template <class D, class Func, OMNI_IF_LANES_PER_BLOCK_D(D, 16), OMNI_IF_I8_D(D)>
OMNI_INLINE VFromD<D> ReduceWithinBlocks(D d, Func f, VFromD<D> v)
{
    const RepartitionToWide<decltype(d)> dw;
    using VW = VFromD<decltype(dw)>;
    const VW vw = BitCast(dw, v);
    // Sign-extend
    // f is commutative, so no need to adapt for OMNI_IS_LITTLE_ENDIAN.
    const VW even = ShiftRight<8>(ShiftLeft<8>(vw));
    const VW odd = ShiftRight<8>(vw);
    const VW reduced = ReduceWithinBlocks(dw, f, f(even, odd));
#if OMNI_IS_LITTLE_ENDIAN
    return DupEven(BitCast(d, reduced));
#else
    return DupOdd(BitCast(d, reduced));
#endif
}
} // namespace detail

template <class D, OMNI_IF_SUM_OF_LANES_D(D)> OMNI_API VFromD<D> SumOfLanes(D d, VFromD<D> v)
{
    const detail::AddFunc f;
    v = detail::ReduceAcrossBlocks(d, f, v);
    return detail::ReduceWithinBlocks(d, f, v);
}
template <class D, OMNI_IF_MINMAX_OF_LANES_D(D)> OMNI_API VFromD<D> MinOfLanes(D d, VFromD<D> v)
{
    const detail::MinFunc f;
    v = detail::ReduceAcrossBlocks(d, f, v);
    return detail::ReduceWithinBlocks(d, f, v);
}
template <class D, OMNI_IF_MINMAX_OF_LANES_D(D)> OMNI_API VFromD<D> MaxOfLanes(D d, VFromD<D> v)
{
    const detail::MaxFunc f;
    v = detail::ReduceAcrossBlocks(d, f, v);
    return detail::ReduceWithinBlocks(d, f, v);
}

template <class D, OMNI_IF_REDUCE_D(D)> OMNI_API TFromD<D> ReduceSum(D d, VFromD<D> v)
{
    return GetLane(SumOfLanes(d, v));
}
template <class D, OMNI_IF_REDUCE_D(D)> OMNI_API TFromD<D> ReduceMin(D d, VFromD<D> v)
{
    return GetLane(MinOfLanes(d, v));
}
template <class D, OMNI_IF_REDUCE_D(D)> OMNI_API TFromD<D> ReduceMax(D d, VFromD<D> v)
{
    return GetLane(MaxOfLanes(d, v));
}

#endif // OMNI_NATIVE_REDUCE_SCALAR

// Corner cases for both generic and native implementations:
// N=1 (native covers N=2 e.g. for u64x2 and even u32x2 on Arm)
template <class D, OMNI_IF_LANES_D(D, 1)> OMNI_API TFromD<D> ReduceSum(D /* d */, VFromD<D> v)
{
    return GetLane(v);
}

template <class D, OMNI_IF_LANES_D(D, 1)> OMNI_API TFromD<D> ReduceMin(D /* d */, VFromD<D> v)
{
    return GetLane(v);
}

template <class D, OMNI_IF_LANES_D(D, 1)> OMNI_API TFromD<D> ReduceMax(D /* d */, VFromD<D> v)
{
    return GetLane(v);
}

template <class D, OMNI_IF_LANES_D(D, 1)> OMNI_API VFromD<D> SumOfLanes(D /* tag */, VFromD<D> v)
{
    return v;
}

template <class D, OMNI_IF_LANES_D(D, 1)> OMNI_API VFromD<D> MinOfLanes(D /* tag */, VFromD<D> v)
{
    return v;
}

template <class D, OMNI_IF_LANES_D(D, 1)> OMNI_API VFromD<D> MaxOfLanes(D /* tag */, VFromD<D> v)
{
    return v;
}

// N=4 for 8-bit is still less than the minimum native size.

// ARMv7 NEON/PPC/RVV/SVE have target-specific implementations of the N=4 I8/U8
// ReduceSum operations
#ifndef OMNI_NATIVE_REDUCE_SUM_4_UI8
#define OMNI_NATIVE_REDUCE_SUM_4_UI8

template <class D, OMNI_IF_V_SIZE_D(D, 4), OMNI_IF_UI8_D(D)> OMNI_API TFromD<D> ReduceSum(D d, VFromD<D> v)
{
    const Twice<RepartitionToWide<decltype(d)>> dw;
    return static_cast<TFromD<D>>(ReduceSum(dw, PromoteTo(dw, v)));
}
#endif // OMNI_NATIVE_REDUCE_SUM_4_UI8

// RVV/SVE have target-specific implementations of the N=4 I8/U8
// ReduceMin/ReduceMax operations
#ifndef OMNI_NATIVE_REDUCE_MINMAX_4_UI8
#define OMNI_NATIVE_REDUCE_MINMAX_4_UI8

template <class D, OMNI_IF_V_SIZE_D(D, 4), OMNI_IF_UI8_D(D)> OMNI_API TFromD<D> ReduceMin(D d, VFromD<D> v)
{
    const Twice<RepartitionToWide<decltype(d)>> dw;
    return static_cast<TFromD<D>>(ReduceMin(dw, PromoteTo(dw, v)));
}
template <class D, OMNI_IF_V_SIZE_D(D, 4), OMNI_IF_UI8_D(D)> OMNI_API TFromD<D> ReduceMax(D d, VFromD<D> v)
{
    const Twice<RepartitionToWide<decltype(d)>> dw;
    return static_cast<TFromD<D>>(ReduceMax(dw, PromoteTo(dw, v)));
}
#endif // OMNI_NATIVE_REDUCE_MINMAX_4_UI8

// ------------------------------ IsEitherNaN
#ifndef OMNI_NATIVE_IS_EITHER_NAN
#define OMNI_NATIVE_IS_EITHER_NAN

template <class V, OMNI_IF_FLOAT_V(V)> OMNI_API MFromD<DFromV<V>> IsEitherNaN(V a, V b)
{
    return Or(IsNaN(a), IsNaN(b));
}

#endif // OMNI_NATIVE_IS_EITHER_NAN

// ------------------------------ IsInf, IsFinite

// AVX3 has target-specific implementations of these.
#ifndef OMNI_NATIVE_ISINF
#define OMNI_NATIVE_ISINF

template <class V, class D = DFromV<V>> OMNI_API MFromD<D> IsInf(const V v)
{
    using T = TFromD<D>;
    const D d;
    const RebindToUnsigned<decltype(d)> du;
    const VFromD<decltype(du)> vu = BitCast(du, v);
    // 'Shift left' to clear the sign bit, check for exponent=max and mantissa=0.
    return RebindMask(d, Eq(Add(vu, vu), Set(du, static_cast<MakeUnsigned<T>>(simd::MaxExponentTimes2<T>()))));
}

// Returns whether normal/subnormal/zero.
template <class V, class D = DFromV<V>> OMNI_API MFromD<D> IsFinite(const V v)
{
    using T = TFromD<D>;
    const D d;
    const RebindToUnsigned<decltype(d)> du;
    const RebindToSigned<decltype(d)> di; // cheaper than unsigned comparison
    const VFromD<decltype(du)> vu = BitCast(du, v);
// 'Shift left' to clear the sign bit. MSVC seems to generate incorrect code
// for AVX2 if we instead add vu + vu.
#if OMNI_COMPILER_MSVC
    const VFromD<decltype(du)> shl = ShiftLeft<1>(vu);
#else
    const VFromD<decltype(du)> shl = Add(vu, vu);
#endif

    // Then shift right so we can compare with the max exponent (cannot compare
    // with MaxExponentTimes2 directly because it is negative and non-negative
    // floats would be greater).
    const VFromD<decltype(di)> exp = BitCast(di, ShiftRight<simd::MantissaBits<T>() + 1>(shl));
    return RebindMask(d, Lt(exp, Set(di, simd::MaxExponentField<T>())));
}

#endif // OMNI_NATIVE_ISINF

// ------------------------------ CeilInt/FloorInt
#ifndef OMNI_NATIVE_CEIL_FLOOR_INT
#define OMNI_NATIVE_CEIL_FLOOR_INT

template <class V, OMNI_IF_FLOAT_V(V)> OMNI_API VFromD<RebindToSigned<DFromV<V>>> CeilInt(V v)
{
    const DFromV<decltype(v)> d;
    const RebindToSigned<decltype(d)> di;
    return ConvertTo(di, Ceil(v));
}

template <class V, OMNI_IF_FLOAT_V(V)> OMNI_API VFromD<RebindToSigned<DFromV<V>>> FloorInt(V v)
{
    const DFromV<decltype(v)> d;
    const RebindToSigned<decltype(d)> di;
    return ConvertTo(di, Floor(v));
}

#endif // OMNI_NATIVE_CEIL_FLOOR_INT

// ------------------------------ MulByPow2/MulByFloorPow2

#ifndef OMNI_NATIVE_MUL_BY_POW2
#define OMNI_NATIVE_MUL_BY_POW2

template <class V, OMNI_IF_FLOAT_V(V)> OMNI_API V MulByPow2(V v, VFromD<RebindToSigned<DFromV<V>>> exp)
{
    const DFromV<decltype(v)> df;
    const RebindToUnsigned<decltype(df)> du;
    const RebindToSigned<decltype(df)> di;

    using TF = TFromD<decltype(df)>;
    using TI = TFromD<decltype(di)>;
    using TU = TFromD<decltype(du)>;

    using VF = VFromD<decltype(df)>;
    using VI = VFromD<decltype(di)>;

    constexpr TI kMaxBiasedExp = MaxExponentField<TF>();
    static_assert(kMaxBiasedExp > 0, "kMaxBiasedExp > 0 must be true");

    constexpr TI kExpBias = static_cast<TI>(kMaxBiasedExp >> 1);
    static_assert(kExpBias > 0, "kExpBias > 0 must be true");
    static_assert(kExpBias <= LimitsMax<TI>() / 3, "kExpBias <= LimitsMax<TI>() / 3 must be true");

    using TExpMinMax = TI;

    using TExpSatSub = If<(sizeof(TF) == 4), uint8_t, TU>;

    static_assert(kExpBias <= static_cast<TI>(LimitsMax<TExpMinMax>() / 3),
        "kExpBias <= LimitsMax<TExpMinMax>() / 3 must be true");

    const Repartition<TExpMinMax, decltype(df)> d_exp_min_max;
    const Repartition<TExpSatSub, decltype(df)> d_sat_exp_sub;

    constexpr int kNumOfExpBits = ExponentBits<TF>();
    constexpr int kNumOfMantBits = MantissaBits<TF>();

    // The sign bit of BitCastScalar<TU>(a[i]) >> kNumOfMantBits can be zeroed out
    // using SaturatedSub if kZeroOutSignUsingSatSub is true.

    // If kZeroOutSignUsingSatSub is true, then val_for_exp_sub will be bitcasted
    // to a vector that has a smaller lane size than TU for the SaturatedSub
    // operation below.
    constexpr bool kZeroOutSignUsingSatSub = ((sizeof(TExpSatSub) * 8) == static_cast<size_t>(kNumOfExpBits));

    // If kZeroOutSignUsingSatSub is true, then the upper
    // (sizeof(TU) - sizeof(TExpSatSub)) * 8 bits of kExpDecrBy1Bits will be all
    // ones and the lower sizeof(TExpSatSub) * 8 bits of kExpDecrBy1Bits will be
    // equal to 1.

    // Otherwise, if kZeroOutSignUsingSatSub is false, kExpDecrBy1Bits will be
    // equal to 1.
    constexpr TU kExpDecrBy1Bits =
        static_cast<TU>(TU{ 1 } - (static_cast<TU>(kZeroOutSignUsingSatSub) << kNumOfExpBits));

    VF val_for_exp_sub = v;
    OMNI_IF_CONSTEXPR(!kZeroOutSignUsingSatSub)
    {
        // If kZeroOutSignUsingSatSub is not true, zero out the sign bit of
        // val_for_exp_sub[i] using Abs
        val_for_exp_sub = Abs(val_for_exp_sub);
    }

    // min_exp1_plus_min_exp2[i] is the smallest exponent such that
    // min_exp1_plus_min_exp2[i] >= 2 - kExpBias * 2 and
    // std::ldexp(v[i], min_exp1_plus_min_exp2[i]) is a normal floating-point
    // number if v[i] is a normal number
    const VI min_exp1_plus_min_exp2 = BitCast(di, Max(BitCast(d_exp_min_max,
        Neg(BitCast(di, SaturatedSub(BitCast(d_sat_exp_sub, ShiftRight<kNumOfMantBits>(BitCast(du, val_for_exp_sub))),
        BitCast(d_sat_exp_sub, Set(du, kExpDecrBy1Bits)))))),
        BitCast(d_exp_min_max, Set(di, static_cast<TI>(2 - kExpBias - kExpBias)))));

    const VI clamped_exp = Max(Min(exp, Set(di, static_cast<TI>(kExpBias * 3))),
        Add(min_exp1_plus_min_exp2, Set(di, static_cast<TI>(1 - kExpBias))));

    const VI exp1_plus_exp2 = BitCast(di, Max(Min(BitCast(d_exp_min_max, Sub(clamped_exp, ShiftRight<2>(clamped_exp))),
        BitCast(d_exp_min_max, Set(di, static_cast<TI>(kExpBias + kExpBias)))),
        BitCast(d_exp_min_max, min_exp1_plus_min_exp2)));

    const VI exp1 = ShiftRight<1>(exp1_plus_exp2);
    const VI exp2 = Sub(exp1_plus_exp2, exp1);
    const VI exp3 = Sub(clamped_exp, exp1_plus_exp2);

    const VI exp_bias = Set(di, kExpBias);

    const VF factor1 = BitCast(df, ShiftLeft<kNumOfMantBits>(Add(exp1, exp_bias)));
    const VF factor2 = BitCast(df, ShiftLeft<kNumOfMantBits>(Add(exp2, exp_bias)));
    const VF factor3 = BitCast(df, ShiftLeft<kNumOfMantBits>(Add(exp3, exp_bias)));

    return Mul(Mul(Mul(v, factor1), factor2), factor3);
}

template <class V, OMNI_IF_FLOAT_V(V)> OMNI_API V MulByFloorPow2(V v, V exp)
{
    const DFromV<decltype(v)> df;

    // MulByFloorPow2 special cases:
    // MulByFloorPow2(v, NaN) => NaN
    // MulByFloorPow2(0, inf) => NaN
    // MulByFloorPow2(inf, -inf) => NaN
    // MulByFloorPow2(-inf, -inf) => NaN
    const auto is_special_case_with_nan_result =
        Or(IsNaN(exp), And(Eq(Abs(v), IfNegativeThenElseZero(exp, Inf(df))), IsInf(exp)));

    return IfThenElse(is_special_case_with_nan_result, NaN(df), MulByPow2(v, FloorInt(exp)));
}

#endif // OMNI_NATIVE_MUL_BY_POW2


// Load/StoreInterleaved for special floats. Requires OMNI_GENERIC_IF_EMULATED_D
// is defined such that it is true only for types that actually require these
// generic implementations.
#if defined(OMNI_NATIVE_LOAD_STORE_SPECIAL_FLOAT_INTERLEAVED) && defined(OMNI_GENERIC_IF_EMULATED_D)
#ifdef OMNI_NATIVE_LOAD_STORE_SPECIAL_FLOAT_INTERLEAVED
#undef OMNI_NATIVE_LOAD_STORE_SPECIAL_FLOAT_INTERLEAVED
#else
#define OMNI_NATIVE_LOAD_STORE_SPECIAL_FLOAT_INTERLEAVED
#endif

template <class D, OMNI_GENERIC_IF_EMULATED_D(D), typename T = TFromD<D>>
OMNI_API void LoadInterleaved2(D d, const T *OMNI_RESTRICT unaligned, VFromD<D> &v0, VFromD<D> &v1)
{
    const RebindToUnsigned<decltype(d)> du;
    VFromD<decltype(du)> vu0, vu1;
    LoadInterleaved2(du, detail::U16LanePointer(unaligned), vu0, vu1);
    v0 = BitCast(d, vu0);
    v1 = BitCast(d, vu1);
}

template <class D, OMNI_GENERIC_IF_EMULATED_D(D), typename T = TFromD<D>>
OMNI_API void LoadInterleaved3(D d, const T *OMNI_RESTRICT unaligned, VFromD<D> &v0, VFromD<D> &v1, VFromD<D> &v2)
{
    const RebindToUnsigned<decltype(d)> du;
    VFromD<decltype(du)> vu0, vu1, vu2;
    LoadInterleaved3(du, detail::U16LanePointer(unaligned), vu0, vu1, vu2);
    v0 = BitCast(d, vu0);
    v1 = BitCast(d, vu1);
    v2 = BitCast(d, vu2);
}

template <class D, OMNI_GENERIC_IF_EMULATED_D(D), typename T = TFromD<D>>
OMNI_API void LoadInterleaved4(D d, const T *OMNI_RESTRICT unaligned, VFromD<D> &v0, VFromD<D> &v1, VFromD<D> &v2,
    VFromD<D> &v3)
{
    const RebindToUnsigned<decltype(d)> du;
    VFromD<decltype(du)> vu0, vu1, vu2, vu3;
    LoadInterleaved4(du, detail::U16LanePointer(unaligned), vu0, vu1, vu2, vu3);
    v0 = BitCast(d, vu0);
    v1 = BitCast(d, vu1);
    v2 = BitCast(d, vu2);
    v3 = BitCast(d, vu3);
}

template <class D, OMNI_GENERIC_IF_EMULATED_D(D), typename T = TFromD<D>>
OMNI_API void StoreInterleaved2(VFromD<D> v0, VFromD<D> v1, D d, T *OMNI_RESTRICT unaligned)
{
    const RebindToUnsigned<decltype(d)> du;
    StoreInterleaved2(BitCast(du, v0), BitCast(du, v1), du, detail::U16LanePointer(unaligned));
}

template <class D, OMNI_GENERIC_IF_EMULATED_D(D), typename T = TFromD<D>>
OMNI_API void StoreInterleaved3(VFromD<D> v0, VFromD<D> v1, VFromD<D> v2, D d, T *OMNI_RESTRICT unaligned)
{
    const RebindToUnsigned<decltype(d)> du;
    StoreInterleaved3(BitCast(du, v0), BitCast(du, v1), BitCast(du, v2), du, detail::U16LanePointer(unaligned));
}

template <class D, OMNI_GENERIC_IF_EMULATED_D(D), typename T = TFromD<D>>
OMNI_API void StoreInterleaved4(VFromD<D> v0, VFromD<D> v1, VFromD<D> v2, VFromD<D> v3, D d, T *OMNI_RESTRICT unaligned)
{
    const RebindToUnsigned<decltype(d)> du;
    StoreInterleaved4(BitCast(du, v0), BitCast(du, v1), BitCast(du, v2), BitCast(du, v3), du,
        detail::U16LanePointer(unaligned));
}
#endif // OMNI_NATIVE_LOAD_STORE_SPECIAL_FLOAT_INTERLEAVED

// ------------------------------ LoadN
#ifndef OMNI_NATIVE_LOAD_N
#define OMNI_NATIVE_LOAD_N

#if OMNI_MEM_OPS_MIGHT_FAULT && !OMNI_HAVE_SCALABLE
namespace detail {
template <class DTo, class DFrom> OMNI_INLINE VFromD<DTo> LoadNResizeBitCast(DTo d_to, DFrom d_from, VFromD<DFrom> v)
{
    // On other targets such as PPC/NEON, the contents of any lanes past the first
    // (lowest-index) Lanes(d_from) lanes of v.raw might be non-zero if
    // sizeof(decltype(v.raw)) > d_from.MaxBytes() is true.
    return ZeroExtendResizeBitCast(d_to, d_from, v);
}
} // namespace detail

template <class D, OMNI_IF_V_SIZE_LE_D(D, 16), OMNI_IF_LANES_D(D, 1), OMNI_IF_NOT_BF16_D(D)>
OMNI_API VFromD<D> LoadN(D d, const TFromD<D> *OMNI_RESTRICT p, size_t num_lanes)
{
    return (num_lanes > 0) ? LoadU(d, p) : Zero(d);
}

template <class D, OMNI_IF_V_SIZE_LE_D(D, 16), OMNI_IF_LANES_D(D, 1), OMNI_IF_NOT_BF16_D(D)>
OMNI_API VFromD<D> LoadNOr(VFromD<D> no, D d, const TFromD<D> *OMNI_RESTRICT p, size_t num_lanes)
{
    return (num_lanes > 0) ? LoadU(d, p) : no;
}

template <class D, OMNI_IF_V_SIZE_LE_D(D, 16), OMNI_IF_LANES_D(D, 2), OMNI_IF_NOT_BF16_D(D)>
OMNI_API VFromD<D> LoadN(D d, const TFromD<D> *OMNI_RESTRICT p, size_t num_lanes)
{
    const FixedTag<TFromD<D>, 1> d1;

    if (num_lanes >= 2)
        return LoadU(d, p);
    if (num_lanes == 0)
        return Zero(d);
    return detail::LoadNResizeBitCast(d, d1, LoadU(d1, p));
}

template <class D, OMNI_IF_V_SIZE_LE_D(D, 16), OMNI_IF_LANES_D(D, 2), OMNI_IF_NOT_BF16_D(D)>
OMNI_API VFromD<D> LoadNOr(VFromD<D> no, D d, const TFromD<D> *OMNI_RESTRICT p, size_t num_lanes)
{
    const FixedTag<TFromD<D>, 1> d1;

    if (num_lanes >= 2)
        return LoadU(d, p);
    if (num_lanes == 0)
        return no;
    return InterleaveLower(ResizeBitCast(d, LoadU(d1, p)), no);
}

template <class D, OMNI_IF_V_SIZE_LE_D(D, 16), OMNI_IF_LANES_D(D, 4), OMNI_IF_NOT_BF16_D(D)>
OMNI_API VFromD<D> LoadN(D d, const TFromD<D> *OMNI_RESTRICT p, size_t num_lanes)
{
    const FixedTag<TFromD<D>, 2> d2;
    const Half<decltype(d2)> d1;

    if (num_lanes >= 4)
        return LoadU(d, p);
    if (num_lanes == 0)
        return Zero(d);
    if (num_lanes == 1)
        return detail::LoadNResizeBitCast(d, d1, LoadU(d1, p));

    // Two or three lanes.
    const VFromD<D> v_lo = detail::LoadNResizeBitCast(d, d2, LoadU(d2, p));
    return (num_lanes == 2) ? v_lo : InsertLane(v_lo, 2, p[2]);
}

template <class D, OMNI_IF_V_SIZE_LE_D(D, 16), OMNI_IF_LANES_D(D, 4), OMNI_IF_NOT_BF16_D(D)>
OMNI_API VFromD<D> LoadNOr(VFromD<D> no, D d, const TFromD<D> *OMNI_RESTRICT p, size_t num_lanes)
{
    const FixedTag<TFromD<D>, 2> d2;

    if (num_lanes >= 4)
        return LoadU(d, p);
    if (num_lanes == 0)
        return no;
    if (num_lanes == 1)
        return InsertLane(no, 0, p[0]);

    // Two or three lanes.
    const VFromD<D> v_lo = ConcatUpperLower(d, no, ResizeBitCast(d, LoadU(d2, p)));
    return (num_lanes == 2) ? v_lo : InsertLane(v_lo, 2, p[2]);
}

template <class D, OMNI_IF_V_SIZE_LE_D(D, 16), OMNI_IF_LANES_D(D, 8), OMNI_IF_NOT_BF16_D(D)>
OMNI_API VFromD<D> LoadN(D d, const TFromD<D> *OMNI_RESTRICT p, size_t num_lanes)
{
    const FixedTag<TFromD<D>, 4> d4;
    const Half<decltype(d4)> d2;
    const Half<decltype(d2)> d1;

    if (num_lanes >= 8)
        return LoadU(d, p);
    if (num_lanes == 0)
        return Zero(d);
    if (num_lanes == 1)
        return detail::LoadNResizeBitCast(d, d1, LoadU(d1, p));

    const size_t leading_len = num_lanes & 4;
    VFromD<decltype(d4)> v_trailing = Zero(d4);

    if ((num_lanes & 2) != 0) {
        const VFromD<decltype(d2)> v_trailing_lo2 = LoadU(d2, p + leading_len);
        if ((num_lanes & 1) != 0) {
            v_trailing =
                Combine(d4, detail::LoadNResizeBitCast(d2, d1, LoadU(d1, p + leading_len + 2)), v_trailing_lo2);
        } else {
            v_trailing = detail::LoadNResizeBitCast(d4, d2, v_trailing_lo2);
        }
    } else if ((num_lanes & 1) != 0) {
        v_trailing = detail::LoadNResizeBitCast(d4, d1, LoadU(d1, p + leading_len));
    }

    if (leading_len != 0) {
        return Combine(d, v_trailing, LoadU(d4, p));
    } else {
        return detail::LoadNResizeBitCast(d, d4, v_trailing);
    }
}

template <class D, OMNI_IF_V_SIZE_LE_D(D, 16), OMNI_IF_LANES_D(D, 8), OMNI_IF_NOT_BF16_D(D)>
OMNI_API VFromD<D> LoadNOr(VFromD<D> no, D d, const TFromD<D> *OMNI_RESTRICT p, size_t num_lanes)
{
    const FixedTag<TFromD<D>, 4> d4;
    const Half<decltype(d4)> d2;
    const Half<decltype(d2)> d1;

    if (num_lanes >= 8)
        return LoadU(d, p);
    if (num_lanes == 0)
        return no;
    if (num_lanes == 1)
        return InsertLane(no, 0, p[0]);

    const size_t leading_len = num_lanes & 4;
    VFromD<decltype(d4)> v_trailing = ResizeBitCast(d4, no);

    if ((num_lanes & 2) != 0) {
        const VFromD<decltype(d2)> v_trailing_lo2 = LoadU(d2, p + leading_len);
        if ((num_lanes & 1) != 0) {
            v_trailing =
                Combine(d4, InterleaveLower(ResizeBitCast(d2, LoadU(d1, p + leading_len + 2)), ResizeBitCast(d2, no)),
                v_trailing_lo2);
        } else {
            v_trailing = ConcatUpperLower(d4, ResizeBitCast(d4, no), ResizeBitCast(d4, v_trailing_lo2));
        }
    } else if ((num_lanes & 1) != 0) {
        v_trailing = InsertLane(ResizeBitCast(d4, no), 0, p[leading_len]);
    }

    if (leading_len != 0) {
        return Combine(d, v_trailing, LoadU(d4, p));
    } else {
        return ConcatUpperLower(d, no, ResizeBitCast(d, v_trailing));
    }
}

template <class D, OMNI_IF_V_SIZE_LE_D(D, 16), OMNI_IF_LANES_D(D, 16), OMNI_IF_NOT_BF16_D(D)>
OMNI_API VFromD<D> LoadN(D d, const TFromD<D> *OMNI_RESTRICT p, size_t num_lanes)
{
    const FixedTag<TFromD<D>, 8> d8;
    const Half<decltype(d8)> d4;
    const Half<decltype(d4)> d2;
    const Half<decltype(d2)> d1;

    if (num_lanes >= 16)
        return LoadU(d, p);
    if (num_lanes == 0)
        return Zero(d);
    if (num_lanes == 1)
        return detail::LoadNResizeBitCast(d, d1, LoadU(d1, p));

    const size_t leading_len = num_lanes & 12;
    VFromD<decltype(d4)> v_trailing = Zero(d4);

    if ((num_lanes & 2) != 0) {
        const VFromD<decltype(d2)> v_trailing_lo2 = LoadU(d2, p + leading_len);
        if ((num_lanes & 1) != 0) {
            v_trailing =
                Combine(d4, detail::LoadNResizeBitCast(d2, d1, LoadU(d1, p + leading_len + 2)), v_trailing_lo2);
        } else {
            v_trailing = detail::LoadNResizeBitCast(d4, d2, v_trailing_lo2);
        }
    } else if ((num_lanes & 1) != 0) {
        v_trailing = detail::LoadNResizeBitCast(d4, d1, LoadU(d1, p + leading_len));
    }

    if (leading_len != 0) {
        if (leading_len >= 8) {
            const VFromD<decltype(d8)> v_hi7 = ((leading_len & 4) != 0) ?
                Combine(d8, v_trailing, LoadU(d4, p + 8)) :
                detail::LoadNResizeBitCast(d8, d4, v_trailing);
            return Combine(d, v_hi7, LoadU(d8, p));
        } else {
            return detail::LoadNResizeBitCast(d, d8, Combine(d8, v_trailing, LoadU(d4, p)));
        }
    } else {
        return detail::LoadNResizeBitCast(d, d4, v_trailing);
    }
}

template <class D, OMNI_IF_V_SIZE_LE_D(D, 16), OMNI_IF_LANES_D(D, 16), OMNI_IF_NOT_BF16_D(D)>
OMNI_API VFromD<D> LoadNOr(VFromD<D> no, D d, const TFromD<D> *OMNI_RESTRICT p, size_t num_lanes)
{
    const FixedTag<TFromD<D>, 8> d8;
    const Half<decltype(d8)> d4;
    const Half<decltype(d4)> d2;
    const Half<decltype(d2)> d1;

    if (num_lanes >= 16)
        return LoadU(d, p);
    if (num_lanes == 0)
        return no;
    if (num_lanes == 1)
        return InsertLane(no, 0, p[0]);

    const size_t leading_len = num_lanes & 12;
    VFromD<decltype(d4)> v_trailing = ResizeBitCast(d4, no);

    if ((num_lanes & 2) != 0) {
        const VFromD<decltype(d2)> v_trailing_lo2 = LoadU(d2, p + leading_len);
        if ((num_lanes & 1) != 0) {
            v_trailing =
                Combine(d4, InterleaveLower(ResizeBitCast(d2, LoadU(d1, p + leading_len + 2)), ResizeBitCast(d2, no)),
                v_trailing_lo2);
        } else {
            v_trailing = ConcatUpperLower(d4, ResizeBitCast(d4, no), ResizeBitCast(d4, v_trailing_lo2));
        }
    } else if ((num_lanes & 1) != 0) {
        v_trailing = InsertLane(ResizeBitCast(d4, no), 0, p[leading_len]);
    }

    if (leading_len != 0) {
        if (leading_len >= 8) {
            const VFromD<decltype(d8)> v_hi7 = ((leading_len & 4) != 0) ?
                Combine(d8, v_trailing, LoadU(d4, p + 8)) :
                ConcatUpperLower(d8, ResizeBitCast(d8, no), ResizeBitCast(d8, v_trailing));
            return Combine(d, v_hi7, LoadU(d8, p));
        } else {
            return ConcatUpperLower(d, ResizeBitCast(d, no), ResizeBitCast(d, Combine(d8, v_trailing, LoadU(d4, p))));
        }
    } else {
        const Repartition<uint32_t, D> du32;
        // lowest 4 bytes from v_trailing, next 4 from no.
        const VFromD<decltype(du32)> lo8 = InterleaveLower(ResizeBitCast(du32, v_trailing), BitCast(du32, no));
        return ConcatUpperLower(d, ResizeBitCast(d, no), ResizeBitCast(d, lo8));
    }
}

#if OMNI_MAX_BYTES >= 32

template <class D, OMNI_IF_V_SIZE_GT_D(D, 16), OMNI_IF_NOT_BF16_D(D)>
OMNI_API VFromD<D> LoadN(D d, const TFromD<D> *OMNI_RESTRICT p, size_t num_lanes)
{
    if (num_lanes >= Lanes(d))
        return LoadU(d, p);

    const Half<decltype(d)> dh;
    const size_t half_N = Lanes(dh);
    if (num_lanes <= half_N) {
        return ZeroExtendVector(d, LoadN(dh, p, num_lanes));
    } else {
        const VFromD<decltype(dh)> v_lo = LoadU(dh, p);
        const VFromD<decltype(dh)> v_hi = LoadN(dh, p + half_N, num_lanes - half_N);
        return Combine(d, v_hi, v_lo);
    }
}

template <class D, OMNI_IF_V_SIZE_GT_D(D, 16), OMNI_IF_NOT_BF16_D(D)>
OMNI_API VFromD<D> LoadNOr(VFromD<D> no, D d, const TFromD<D> *OMNI_RESTRICT p, size_t num_lanes)
{
    if (num_lanes >= Lanes(d))
        return LoadU(d, p);

    const Half<decltype(d)> dh;
    const size_t half_N = Lanes(dh);
    const VFromD<decltype(dh)> no_h = LowerHalf(no);
    if (num_lanes <= half_N) {
        return ConcatUpperLower(d, no, ResizeBitCast(d, LoadNOr(no_h, dh, p, num_lanes)));
    } else {
        const VFromD<decltype(dh)> v_lo = LoadU(dh, p);
        const VFromD<decltype(dh)> v_hi = LoadNOr(no_h, dh, p + half_N, num_lanes - half_N);
        return Combine(d, v_hi, v_lo);
    }
}

#endif // OMNI_MAX_BYTES >= 32

template <class D, OMNI_IF_BF16_D(D)> OMNI_API VFromD<D> LoadN(D d, const TFromD<D> *OMNI_RESTRICT p, size_t num_lanes)
{
    const RebindToUnsigned<D> du;
    return BitCast(d, LoadN(du, detail::U16LanePointer(p), num_lanes));
}

template <class D, OMNI_IF_BF16_D(D)>
OMNI_API VFromD<D> LoadNOr(VFromD<D> no, D d, const TFromD<D> *OMNI_RESTRICT p, size_t num_lanes)
{
    const RebindToUnsigned<D> du;
    return BitCast(d, LoadNOr(BitCast(du, no), du, detail::U16LanePointer(p), num_lanes));
}

#else // !OMNI_MEM_OPS_MIGHT_FAULT || OMNI_HAVE_SCALABLE

// For SVE and non-sanitizer AVX-512; RVV has its own specialization.
template <class D> OMNI_API VFromD<D> LoadN(D d, const TFromD<D> *OMNI_RESTRICT p, size_t num_lanes)
{
#if OMNI_MEM_OPS_MIGHT_FAULT
    if (num_lanes <= 0)
        return Zero(d);
#endif

    return MaskedLoad(FirstN(d, num_lanes), d, p);
}

template <class D> OMNI_API VFromD<D> LoadNOr(VFromD<D> no, D d, const TFromD<D> *OMNI_RESTRICT p, size_t num_lanes)
{
#if OMNI_MEM_OPS_MIGHT_FAULT
    if (num_lanes <= 0)
        return no;
#endif

    return MaskedLoadOr(no, FirstN(d, num_lanes), d, p);
}

#endif // OMNI_MEM_OPS_MIGHT_FAULT && !OMNI_HAVE_SCALABLE
#endif // OMNI_NATIVE_LOAD_N

// ------------------------------ StoreN
#ifndef OMNI_NATIVE_STORE_N
#define OMNI_NATIVE_STORE_N

#if OMNI_MEM_OPS_MIGHT_FAULT && !OMNI_HAVE_SCALABLE
namespace detail {
template <class DH, OMNI_IF_V_SIZE_LE_D(DH, 4)> OMNI_INLINE VFromD<DH> StoreNGetUpperHalf(DH dh, VFromD<Twice<DH>> v)
{
    constexpr size_t kMinShrVectBytes = OMNI_TARGET_IS_NEON ? 8 : 16;
    const FixedTag<uint8_t, kMinShrVectBytes> d_shift;
    return ResizeBitCast(dh, ShiftRightBytes<dh.MaxBytes()>(d_shift, ResizeBitCast(d_shift, v)));
}

template <class DH, OMNI_IF_V_SIZE_GT_D(DH, 4)> OMNI_INLINE VFromD<DH> StoreNGetUpperHalf(DH dh, VFromD<Twice<DH>> v)
{
    return UpperHalf(dh, v);
}
} // namespace detail

template <class D, OMNI_IF_V_SIZE_LE_D(D, 16), OMNI_IF_LANES_D(D, 1), typename T = TFromD<D>>
OMNI_API void StoreN(VFromD<D> v, D d, T *OMNI_RESTRICT p, size_t max_lanes_to_store)
{
    if (max_lanes_to_store > 0) {
        StoreU(v, d, p);
    }
}

template <class D, OMNI_IF_V_SIZE_LE_D(D, 16), OMNI_IF_LANES_D(D, 2), typename T = TFromD<D>>
OMNI_API void StoreN(VFromD<D> v, D d, T *OMNI_RESTRICT p, size_t max_lanes_to_store)
{
    if (max_lanes_to_store > 1) {
        StoreU(v, d, p);
    } else if (max_lanes_to_store == 1) {
        const FixedTag<TFromD<D>, 1> d1;
        StoreU(LowerHalf(d1, v), d1, p);
    }
}

template <class D, OMNI_IF_V_SIZE_LE_D(D, 16), OMNI_IF_LANES_D(D, 4), typename T = TFromD<D>>
OMNI_API void StoreN(VFromD<D> v, D d, T *OMNI_RESTRICT p, size_t max_lanes_to_store)
{
    const FixedTag<TFromD<D>, 2> d2;
    const Half<decltype(d2)> d1;

    if (max_lanes_to_store > 1) {
        if (max_lanes_to_store >= 4) {
            StoreU(v, d, p);
        } else {
            StoreU(ResizeBitCast(d2, v), d2, p);
            if (max_lanes_to_store == 3) {
                StoreU(ResizeBitCast(d1, detail::StoreNGetUpperHalf(d2, v)), d1, p + 2);
            }
        }
    } else if (max_lanes_to_store == 1) {
        StoreU(ResizeBitCast(d1, v), d1, p);
    }
}

template <class D, OMNI_IF_V_SIZE_LE_D(D, 16), OMNI_IF_LANES_D(D, 8), typename T = TFromD<D>>
OMNI_API void StoreN(VFromD<D> v, D d, T *OMNI_RESTRICT p, size_t max_lanes_to_store)
{
    const FixedTag<TFromD<D>, 4> d4;
    const Half<decltype(d4)> d2;
    const Half<decltype(d2)> d1;

    if (max_lanes_to_store <= 1) {
        if (max_lanes_to_store == 1) {
            StoreU(ResizeBitCast(d1, v), d1, p);
        }
    } else if (max_lanes_to_store >= 8) {
        StoreU(v, d, p);
    } else if (max_lanes_to_store >= 4) {
        StoreU(LowerHalf(d4, v), d4, p);
        StoreN(detail::StoreNGetUpperHalf(d4, v), d4, p + 4, max_lanes_to_store - 4);
    } else {
        StoreN(LowerHalf(d4, v), d4, p, max_lanes_to_store);
    }
}

template <class D, OMNI_IF_V_SIZE_LE_D(D, 16), OMNI_IF_LANES_D(D, 16), typename T = TFromD<D>>
OMNI_API void StoreN(VFromD<D> v, D d, T *OMNI_RESTRICT p, size_t max_lanes_to_store)
{
    const FixedTag<TFromD<D>, 8> d8;
    const Half<decltype(d8)> d4;
    const Half<decltype(d4)> d2;
    const Half<decltype(d2)> d1;

    if (max_lanes_to_store <= 1) {
        if (max_lanes_to_store == 1) {
            StoreU(ResizeBitCast(d1, v), d1, p);
        }
    } else if (max_lanes_to_store >= 16) {
        StoreU(v, d, p);
    } else if (max_lanes_to_store >= 8) {
        StoreU(LowerHalf(d8, v), d8, p);
        StoreN(detail::StoreNGetUpperHalf(d8, v), d8, p + 8, max_lanes_to_store - 8);
    } else {
        StoreN(LowerHalf(d8, v), d8, p, max_lanes_to_store);
    }
}

#if OMNI_MAX_BYTES >= 32
template <class D, OMNI_IF_V_SIZE_GT_D(D, 16), typename T = TFromD<D>>
OMNI_API void StoreN(VFromD<D> v, D d, T *OMNI_RESTRICT p, size_t max_lanes_to_store)
{
    const size_t N = Lanes(d);
    if (max_lanes_to_store >= N) {
        StoreU(v, d, p);
        return;
    }

    const Half<decltype(d)> dh;
    const size_t half_N = Lanes(dh);
    if (max_lanes_to_store <= half_N) {
        StoreN(LowerHalf(dh, v), dh, p, max_lanes_to_store);
    } else {
        StoreU(LowerHalf(dh, v), dh, p);
        StoreN(UpperHalf(dh, v), dh, p + half_N, max_lanes_to_store - half_N);
    }
}
#endif // OMNI_MAX_BYTES >= 32

#else // !OMNI_MEM_OPS_MIGHT_FAULT || OMNI_HAVE_SCALABLE

template <class D, typename T = TFromD<D>>
OMNI_API void StoreN(VFromD<D> v, D d, T *OMNI_RESTRICT p, size_t max_lanes_to_store)
{
    const size_t N = Lanes(d);
    const size_t clamped_max_lanes_to_store = OMNI_MIN(max_lanes_to_store, N);
#if OMNI_MEM_OPS_MIGHT_FAULT
    if (clamped_max_lanes_to_store == 0)
        return;
#endif

    BlendedStore(v, FirstN(d, clamped_max_lanes_to_store), d, p);

    detail::MaybeUnpoison(p, clamped_max_lanes_to_store);
}

#endif // OMNI_MEM_OPS_MIGHT_FAULT && !OMNI_HAVE_SCALABLE

#endif // OMNI_NATIVE_STORE_N

// ------------------------------ Scatter

#ifndef OMNI_NATIVE_SCATTER
#define OMNI_NATIVE_SCATTER

template <class D, typename T = TFromD<D>>
OMNI_API void ScatterOffset(VFromD<D> v, D d, T *OMNI_RESTRICT base, VFromD<RebindToSigned<D>> offset)
{
    const RebindToSigned<decltype(d)> di;
    using TI = TFromD<decltype(di)>;
    static_assert(sizeof(T) == sizeof(TI), "Index/lane size must match");

    OMNI_ALIGN T lanes[MaxLanes(d)];
    Store(v, d, lanes);

    OMNI_ALIGN TI offset_lanes[MaxLanes(d)];
    Store(offset, di, offset_lanes);

    uint8_t *base_bytes = reinterpret_cast<uint8_t *>(base);
    for (size_t i = 0; i < MaxLanes(d); ++i) {
        CopyBytes<sizeof(T)>(&lanes[i], base_bytes + offset_lanes[i]);
    }
}

template <class D, typename T = TFromD<D>>
OMNI_API void ScatterIndex(VFromD<D> v, D d, T *OMNI_RESTRICT base, VFromD<RebindToSigned<D>> index)
{
    const RebindToSigned<decltype(d)> di;
    using TI = TFromD<decltype(di)>;
    static_assert(sizeof(T) == sizeof(TI), "Index/lane size must match");

    OMNI_ALIGN T lanes[MaxLanes(d)];
    Store(v, d, lanes);

    OMNI_ALIGN TI index_lanes[MaxLanes(d)];
    Store(index, di, index_lanes);

    for (size_t i = 0; i < MaxLanes(d); ++i) {
        base[index_lanes[i]] = lanes[i];
    }
}

template <class D, typename T = TFromD<D>>
OMNI_API void MaskedScatterIndex(VFromD<D> v, MFromD<D> m, D d, T *OMNI_RESTRICT base, VFromD<RebindToSigned<D>> index)
{
    const RebindToSigned<decltype(d)> di;
    using TI = TFromD<decltype(di)>;
    static_assert(sizeof(T) == sizeof(TI), "Index/lane size must match");

    OMNI_ALIGN T lanes[MaxLanes(d)];
    Store(v, d, lanes);

    OMNI_ALIGN TI index_lanes[MaxLanes(d)];
    Store(index, di, index_lanes);

    OMNI_ALIGN TI mask_lanes[MaxLanes(di)];
    Store(BitCast(di, VecFromMask(d, m)), di, mask_lanes);

    for (size_t i = 0; i < MaxLanes(d); ++i) {
        if (mask_lanes[i])
            base[index_lanes[i]] = lanes[i];
    }
}

template <class D, typename T = TFromD<D>>
OMNI_API void ScatterIndexN(VFromD<D> v, D d, T *OMNI_RESTRICT base, VFromD<RebindToSigned<D>> index,
    const size_t max_lanes_to_store)
{
    const RebindToSigned<decltype(d)> di;
    using TI = TFromD<decltype(di)>;
    static_assert(sizeof(T) == sizeof(TI), "Index/lane size must match");

    for (size_t i = 0; i < MaxLanes(d); ++i) {
        if (i < max_lanes_to_store)
            base[ExtractLane(index, i)] = ExtractLane(v, i);
    }
}
#else

template <class D, typename T = TFromD<D>>
OMNI_API void ScatterIndexN(VFromD<D> v, D d, T *OMNI_RESTRICT base, VFromD<RebindToSigned<D>> index,
    const size_t max_lanes_to_store)
{
    MaskedScatterIndex(v, FirstN(d, max_lanes_to_store), d, base, index);
}

#endif // OMNI_NATIVE_SCATTER

// ------------------------------ Gather

#ifndef OMNI_NATIVE_GATHER
#define OMNI_NATIVE_GATHER

template <class D, typename T = TFromD<D>>
OMNI_API VFromD<D> GatherOffset(D d, const T *OMNI_RESTRICT base, VFromD<RebindToSigned<D>> offset)
{
    const RebindToSigned<D> di;
    using TI = TFromD<decltype(di)>;
    static_assert(sizeof(T) == sizeof(TI), "Index/lane size must match");

    OMNI_ALIGN TI offset_lanes[MaxLanes(d)];
    Store(offset, di, offset_lanes);

    OMNI_ALIGN T lanes[MaxLanes(d)];
    const uint8_t *base_bytes = reinterpret_cast<const uint8_t *>(base);
    for (size_t i = 0; i < MaxLanes(d); ++i) {
        CopyBytes<sizeof(T)>(base_bytes + offset_lanes[i], &lanes[i]);
    }
    return Load(d, lanes);
}

template <class D, typename T = TFromD<D>>
OMNI_API VFromD<D> GatherIndex(D d, const T *OMNI_RESTRICT base, VFromD<RebindToSigned<D>> index)
{
    const RebindToSigned<D> di;
    using TI = TFromD<decltype(di)>;
    static_assert(sizeof(T) == sizeof(TI), "Index/lane size must match");

    OMNI_ALIGN TI index_lanes[MaxLanes(d)];
    Store(index, di, index_lanes);

    OMNI_ALIGN T lanes[MaxLanes(d)];
    for (size_t i = 0; i < MaxLanes(d); ++i) {
        lanes[i] = base[index_lanes[i]];
    }
    return Load(d, lanes);
}

template <class D, typename T = TFromD<D>>
OMNI_API VFromD<D> MaskedGatherIndex(MFromD<D> m, D d, const T *OMNI_RESTRICT base, VFromD<RebindToSigned<D>> index)
{
    const RebindToSigned<D> di;
    using TI = TFromD<decltype(di)>;
    static_assert(sizeof(T) == sizeof(TI), "Index/lane size must match");

    OMNI_ALIGN TI index_lanes[MaxLanes(di)];
    Store(index, di, index_lanes);

    OMNI_ALIGN TI mask_lanes[MaxLanes(di)];
    Store(BitCast(di, VecFromMask(d, m)), di, mask_lanes);

    OMNI_ALIGN T lanes[MaxLanes(d)];
    for (size_t i = 0; i < MaxLanes(d); ++i) {
        lanes[i] = mask_lanes[i] ? base[index_lanes[i]] : T{ 0 };
    }
    return Load(d, lanes);
}

template <class D, typename T = TFromD<D>>
OMNI_API VFromD<D> MaskedGatherIndexOr(VFromD<D> no, MFromD<D> m, D d, const T *OMNI_RESTRICT base,
    VFromD<RebindToSigned<D>> index)
{
    const RebindToSigned<D> di;
    using TI = TFromD<decltype(di)>;
    static_assert(sizeof(T) == sizeof(TI), "Index/lane size must match");

    OMNI_ALIGN TI index_lanes[MaxLanes(di)];
    Store(index, di, index_lanes);

    OMNI_ALIGN TI mask_lanes[MaxLanes(di)];
    Store(BitCast(di, VecFromMask(d, m)), di, mask_lanes);

    OMNI_ALIGN T no_lanes[MaxLanes(d)];
    Store(no, d, no_lanes);

    OMNI_ALIGN T lanes[MaxLanes(d)];
    for (size_t i = 0; i < MaxLanes(d); ++i) {
        lanes[i] = mask_lanes[i] ? base[index_lanes[i]] : no_lanes[i];
    }
    return Load(d, lanes);
}

template <class D, typename T = TFromD<D>>
OMNI_API VFromD<D> GatherIndexN(D d, const T *OMNI_RESTRICT base, VFromD<RebindToSigned<D>> index,
    const size_t max_lanes_to_load)
{
    const RebindToSigned<D> di;
    using TI = TFromD<decltype(di)>;
    static_assert(sizeof(T) == sizeof(TI), "Index/lane size must match");

    VFromD<D> v = Zero(d);
    for (size_t i = 0; i < OMNI_MIN(MaxLanes(d), max_lanes_to_load); ++i) {
        v = InsertLane(v, i, base[ExtractLane(index, i)]);
    }
    return v;
}

template <class D, typename T = TFromD<D>>
OMNI_API VFromD<D> GatherIndexNOr(VFromD<D> no, D d, const T *OMNI_RESTRICT base, VFromD<RebindToSigned<D>> index,
    const size_t max_lanes_to_load)
{
    const RebindToSigned<D> di;
    using TI = TFromD<decltype(di)>;
    static_assert(sizeof(T) == sizeof(TI), "Index/lane size must match");

    VFromD<D> v = no;
    for (size_t i = 0; i < OMNI_MIN(MaxLanes(d), max_lanes_to_load); ++i) {
        v = InsertLane(v, i, base[ExtractLane(index, i)]);
    }
    return v;
}
#else

template <class D, typename T = TFromD<D>>
OMNI_API VFromD<D> GatherIndexN(D d, const T *OMNI_RESTRICT base, VFromD<RebindToSigned<D>> index,
    const size_t max_lanes_to_load)
{
    return MaskedGatherIndex(FirstN(d, max_lanes_to_load), d, base, index);
}

template <class D, typename T = TFromD<D>>
OMNI_API VFromD<D> GatherIndexNOr(VFromD<D> no, D d, const T *OMNI_RESTRICT base, VFromD<RebindToSigned<D>> index,
    const size_t max_lanes_to_load)
{
    return MaskedGatherIndexOr(no, FirstN(d, max_lanes_to_load), d, base, index);
}

#endif // OMNI_NATIVE_GATHER

// ------------------------------ Integer AbsDiff and SumsOf8AbsDiff
#ifndef OMNI_NATIVE_INTEGER_ABS_DIFF
#define OMNI_NATIVE_INTEGER_ABS_DIFF
template <class V, OMNI_IF_NOT_FLOAT_NOR_SPECIAL_V(V)> OMNI_API V AbsDiff(V a, V b)
{
    return Sub(Max(a, b), Min(a, b));
}
#endif // OMNI_NATIVE_INTEGER_ABS_DIFF

#ifndef OMNI_NATIVE_SUMS_OF_8_ABS_DIFF
#define OMNI_NATIVE_SUMS_OF_8_ABS_DIFF

template <class V, OMNI_IF_UI8_D(DFromV<V>), OMNI_IF_V_SIZE_GT_D(DFromV<V>, 4)>
OMNI_API Vec<RepartitionToWideX3<DFromV<V>>> SumsOf8AbsDiff(V a, V b)
{
    const DFromV<decltype(a)> d;
    const RebindToUnsigned<decltype(d)> du;
    const RepartitionToWideX3<decltype(d)> dw;

    return BitCast(dw, SumsOf8(BitCast(du, AbsDiff(a, b))));
}

#endif // OMNI_NATIVE_SUMS_OF_8_ABS_DIFF

// ------------------------------ Unsigned to signed demotions
template <class DN, OMNI_IF_SIGNED_D(DN), class V, OMNI_IF_UNSIGNED_V(V), OMNI_IF_U2I_DEMOTE_FROM_LANE_SIZE_V(V),
    class V2 = VFromD<Rebind<TFromV<V>, DN>>, simd::EnableIf<(sizeof(TFromD<DN>) < sizeof(TFromV<V>))> * = nullptr,
    OMNI_IF_LANES_D(DFromV<V>, OMNI_MAX_LANES_D(DFromV<V2>))>
OMNI_API VFromD<DN> DemoteTo(DN dn, V v)
{
    const DFromV<decltype(v)> d;
    const RebindToSigned<decltype(d)> di;
    const RebindToUnsigned<decltype(dn)> dn_u;

    // First, do a signed to signed demotion. This will convert any values
    // that are greater than simd::HighestValue<MakeSigned<TFromV<V>>>() to a
    // negative value.
    const auto i2i_demote_result = DemoteTo(dn, BitCast(di, v));

    // Second, convert any negative values to simd::HighestValue<TFromD<DN>>()
    // using an unsigned Min operation.
    const auto max_signed_val = Set(dn, simd::HighestValue<TFromD<DN>>());

    return BitCast(dn, Min(BitCast(dn_u, i2i_demote_result), BitCast(dn_u, max_signed_val)));
}

template <class DN, OMNI_IF_SIGNED_D(DN), class V, OMNI_IF_UNSIGNED_V(V), OMNI_IF_U2I_DEMOTE_FROM_LANE_SIZE_V(V),
    class V2 = VFromD<Repartition<TFromV<V>, DN>>, OMNI_IF_T_SIZE_V(V, sizeof(TFromD<DN>) * 2),
    OMNI_IF_LANES_D(DFromV<V>, OMNI_MAX_LANES_D(DFromV<V2>))>
OMNI_API VFromD<DN> ReorderDemote2To(DN dn, V a, V b)
{
    const DFromV<decltype(a)> d;
    const RebindToSigned<decltype(d)> di;
    const RebindToUnsigned<decltype(dn)> dn_u;

    // First, do a signed to signed demotion. This will convert any values
    // that are greater than simd::HighestValue<MakeSigned<TFromV<V>>>() to a
    // negative value.
    const auto i2i_demote_result = ReorderDemote2To(dn, BitCast(di, a), BitCast(di, b));

    // Second, convert any negative values to simd::HighestValue<TFromD<DN>>()
    // using an unsigned Min operation.
    const auto max_signed_val = Set(dn, simd::HighestValue<TFromD<DN>>());

    return BitCast(dn, Min(BitCast(dn_u, i2i_demote_result), BitCast(dn_u, max_signed_val)));
}

// ------------------------------ PromoteLowerTo

// There is no codegen advantage for a native version of this. It is provided
// only for convenience.
template <class D, class V> OMNI_API VFromD<D> PromoteLowerTo(D d, V v)
{
    // Lanes(d) may differ from Lanes(DFromV<V>()). Use the lane type from V
    // because it cannot be deduced from D (could be either bf16 or f16).
    const Rebind<TFromV<V>, decltype(d)> dh;
    return PromoteTo(d, LowerHalf(dh, v));
}

// ------------------------------ F64->F16 DemoteTo
#ifndef OMNI_NATIVE_DEMOTE_F64_TO_F16
#define OMNI_NATIVE_DEMOTE_F64_TO_F16

template <class D, OMNI_IF_F16_D(D)> OMNI_API VFromD<D> DemoteTo(D df16, VFromD<Rebind<double, D>> v)
{
    const Rebind<double, D> df64;
    const Rebind<uint64_t, D> du64;
    const Rebind<float, D> df32;

    // The mantissa bits of v[i] are first rounded using round-to-odd rounding to
    // the nearest F64 value that has the lower 29 bits zeroed out to ensure that
    // the result is correctly rounded to a F16.

    const auto vf64_rounded = OrAnd(And(v, BitCast(df64, Set(du64, static_cast<uint64_t>(0xFFFFFFFFE0000000u)))),
        BitCast(df64, Add(BitCast(du64, v), Set(du64, static_cast<uint64_t>(0x000000001FFFFFFFu)))),
        BitCast(df64, Set(du64, static_cast<uint64_t>(0x0000000020000000ULL))));

    return DemoteTo(df16, DemoteTo(df32, vf64_rounded));
}

#endif // OMNI_NATIVE_DEMOTE_F64_TO_F16

// ------------------------------ F16->F64 PromoteTo
#ifndef OMNI_NATIVE_PROMOTE_F16_TO_F64
#define OMNI_NATIVE_PROMOTE_F16_TO_F64

template <class D, OMNI_IF_F64_D(D)> OMNI_API VFromD<D> PromoteTo(D df64, VFromD<Rebind<float16_t, D>> v)
{
    return PromoteTo(df64, PromoteTo(Rebind<float, D>(), v));
}

#endif // OMNI_NATIVE_PROMOTE_F16_TO_F64

// ------------------------------ F32 to BF16 DemoteTo
#ifndef OMNI_NATIVE_DEMOTE_F32_TO_BF16
#define OMNI_NATIVE_DEMOTE_F32_TO_BF16

namespace detail {
// Round a F32 value to the nearest BF16 value, with the result returned as the
// rounded F32 value bitcasted to an U32

// RoundF32ForDemoteToBF16 also converts NaN values to QNaN values to prevent
// NaN F32 values from being converted to an infinity
template <class V, OMNI_IF_F32(TFromV<V>)> OMNI_INLINE VFromD<RebindToUnsigned<DFromV<V>>> RoundF32ForDemoteToBF16(V v)
{
    const DFromV<decltype(v)> d;
    const RebindToUnsigned<decltype(d)> du32;

    const auto is_non_nan = Not(IsNaN(v));
    const auto bits32 = BitCast(du32, v);

    const auto round_incr = Add(And(ShiftRight<16>(bits32), Set(du32, uint32_t{ 1 })), Set(du32, uint32_t{ 0x7FFFu }));
    return MaskedAddOr(Or(bits32, Set(du32, uint32_t{ 0x00400000u })), RebindMask(du32, is_non_nan), bits32,
        round_incr);
}
} // namespace detail

template <class D, OMNI_IF_BF16_D(D)> OMNI_API VFromD<D> DemoteTo(D dbf16, VFromD<Rebind<float, D>> v)
{
    const RebindToUnsigned<decltype(dbf16)> du16;
    const Twice<decltype(du16)> dt_u16;

    const auto rounded_bits = BitCast(dt_u16, detail::RoundF32ForDemoteToBF16(v));
#if OMNI_IS_LITTLE_ENDIAN
    return BitCast(dbf16, LowerHalf(du16, ConcatOdd(dt_u16, rounded_bits, rounded_bits)));
#else
    return BitCast(dbf16, LowerHalf(du16, ConcatEven(dt_u16, rounded_bits, rounded_bits)));
#endif
}

template <class D, OMNI_IF_BF16_D(D)>
OMNI_API VFromD<D> OrderedDemote2To(D dbf16, VFromD<Repartition<float, D>> a, VFromD<Repartition<float, D>> b)
{
    const RebindToUnsigned<decltype(dbf16)> du16;

    const auto rounded_a_bits32 = BitCast(du16, detail::RoundF32ForDemoteToBF16(a));
    const auto rounded_b_bits32 = BitCast(du16, detail::RoundF32ForDemoteToBF16(b));
#if OMNI_IS_LITTLE_ENDIAN
    return BitCast(dbf16, ConcatOdd(du16, BitCast(du16, rounded_b_bits32), BitCast(du16, rounded_a_bits32)));
#else
    return BitCast(dbf16, ConcatEven(du16, BitCast(du16, rounded_b_bits32), BitCast(du16, rounded_a_bits32)));
#endif
}

template <class D, OMNI_IF_BF16_D(D)>
OMNI_API VFromD<D> ReorderDemote2To(D dbf16, VFromD<Repartition<float, D>> a, VFromD<Repartition<float, D>> b)
{
    const RebindToUnsigned<decltype(dbf16)> du16;

#if OMNI_IS_LITTLE_ENDIAN
    const auto a_in_odd = detail::RoundF32ForDemoteToBF16(a);
    const auto b_in_even = ShiftRight<16>(detail::RoundF32ForDemoteToBF16(b));
#else
    const auto a_in_odd = ShiftRight<16>(detail::RoundF32ForDemoteToBF16(a));
    const auto b_in_even = detail::RoundF32ForDemoteToBF16(b);
#endif

    return BitCast(dbf16, OddEven(BitCast(du16, a_in_odd), BitCast(du16, b_in_even)));
}

#endif // OMNI_NATIVE_DEMOTE_F32_TO_BF16

// ------------------------------ PromoteInRangeTo
#ifndef OMNI_NATIVE_F32_TO_UI64_PROMOTE_IN_RANGE_TO
#define OMNI_NATIVE_F32_TO_UI64_PROMOTE_IN_RANGE_TO

#if OMNI_HAVE_INTEGER64

template <class D64, OMNI_IF_UI64_D(D64)> OMNI_API VFromD<D64> PromoteInRangeTo(D64 d64, VFromD<Rebind<float, D64>> v)
{
    return PromoteTo(d64, v);
}

#endif

#endif // OMNI_NATIVE_F32_TO_UI64_PROMOTE_IN_RANGE_TO

// ------------------------------ ConvertInRangeTo
#ifndef OMNI_NATIVE_F2I_CONVERT_IN_RANGE_TO
#define OMNI_NATIVE_F2I_CONVERT_IN_RANGE_TO

template <class DI, OMNI_IF_NOT_FLOAT_NOR_SPECIAL_D(DI),
    OMNI_IF_T_SIZE_ONE_OF_D(DI, (OMNI_HAVE_FLOAT16 ? (1 << 2) : 0) | (1 << 4) | (OMNI_HAVE_FLOAT64 ? (1 << 8) : 0))>
OMNI_API VFromD<DI> ConvertInRangeTo(DI di, VFromD<RebindToFloat<DI>> v)
{
    return ConvertTo(di, v);
}

#endif // OMNI_NATIVE_F2I_CONVERT_IN_RANGE_TO

// ------------------------------ DemoteInRangeTo
#ifndef OMNI_NATIVE_F64_TO_UI32_DEMOTE_IN_RANGE_TO) == defined(OMNI_TARGET_TOGGLE))
#ifdef OMNI_NATIVE_F64_TO_UI32_DEMOTE_IN_RANGE_TO
#undef OMNI_NATIVE_F64_TO_UI32_DEMOTE_IN_RANGE_TO
#else
#define OMNI_NATIVE_F64_TO_UI32_DEMOTE_IN_RANGE_TO
#endif

template <class D32, OMNI_IF_UI32_D(D32)> OMNI_API VFromD<D32> DemoteInRangeTo(D32 d32, VFromD<Rebind<double, D32>> v)
{
    return DemoteTo(d32, v);
}

#endif // OMNI_NATIVE_F64_TO_UI32_DEMOTE_IN_RANGE_TO

// ------------------------------ PromoteInRangeLowerTo/PromoteInRangeUpperTo

template <class D, OMNI_IF_UI64_D(D), class V, OMNI_IF_F32(TFromV<V>)>
OMNI_API VFromD<D> PromoteInRangeLowerTo(D d, V v)
{
    // Lanes(d) may differ from Lanes(DFromV<V>()). Use the lane type from V
    // because it cannot be deduced from D (could be either bf16 or f16).
    const Rebind<TFromV<V>, decltype(d)> dh;
    return PromoteInRangeTo(d, LowerHalf(dh, v));
}

template <class D, OMNI_IF_UI64_D(D), class V, OMNI_IF_F32(TFromV<V>)>
OMNI_API VFromD<D> PromoteInRangeUpperTo(D d, V v)
{
    // Otherwise, on targets where F32->UI64 PromoteInRangeTo is simply a wrapper
    // around F32->UI64 PromoteTo, promote the upper half of v to TFromD<D> using
    // PromoteUpperTo
    return PromoteUpperTo(d, v);
}

// ------------------------------ PromoteInRangeEvenTo/PromoteInRangeOddTo

template <class D, OMNI_IF_UI64_D(D), class V, OMNI_IF_F32(TFromV<V>)> OMNI_API VFromD<D> PromoteInRangeEvenTo(D d, V v)
{
    // Otherwise, on targets where F32->UI64 PromoteInRangeTo is simply a wrapper
    // around F32->UI64 PromoteTo, promote the even lanes of v to TFromD<D> using
    // PromoteEvenTo
    return PromoteEvenTo(d, v);
}

template <class D, OMNI_IF_UI64_D(D), class V, OMNI_IF_F32(TFromV<V>)> OMNI_API VFromD<D> PromoteInRangeOddTo(D d, V v)
{
    // Otherwise, on targets where F32->UI64 PromoteInRangeTo is simply a wrapper
    // around F32->UI64 PromoteTo, promote the odd lanes of v to TFromD<D> using
    // PromoteOddTo
    return PromoteOddTo(d, v);
}

// ------------------------------ SumsOf2
namespace detail {
template <class TypeTag, size_t kLaneSize, class V>
OMNI_INLINE VFromD<RepartitionToWide<DFromV<V>>> SumsOf2(TypeTag /* type_tag */,
    simd::SizeTag<kLaneSize> /* lane_size_tag */, V v)
{
    const DFromV<decltype(v)> d;
    const RepartitionToWide<decltype(d)> dw;
    return Add(PromoteEvenTo(dw, v), PromoteOddTo(dw, v));
}
} // namespace detail

template <class V> OMNI_API VFromD<RepartitionToWide<DFromV<V>>> SumsOf2(V v)
{
    return detail::SumsOf2(simd::TypeTag<TFromV<V>>(), simd::SizeTag<sizeof(TFromV<V>)>(), v);
}

// ------------------------------ SumsOf4

namespace detail {
template <class TypeTag, size_t kLaneSize, class V>
OMNI_INLINE VFromD<RepartitionToWideX2<DFromV<V>>> SumsOf4(TypeTag /* type_tag */,
    simd::SizeTag<kLaneSize> /* lane_size_tag */, V v)
{
    using simd::SumsOf2;
    return SumsOf2(SumsOf2(v));
}
} // namespace detail

template <class V> OMNI_API VFromD<RepartitionToWideX2<DFromV<V>>> SumsOf4(V v)
{
    return detail::SumsOf4(simd::TypeTag<TFromV<V>>(), simd::SizeTag<sizeof(TFromV<V>)>(), v);
}

// ------------------------------ OrderedTruncate2To
#ifdef OMNI_NATIVE_ORDERED_TRUNCATE_2_TO
#undef OMNI_NATIVE_ORDERED_TRUNCATE_2_TO
#else
#define OMNI_NATIVE_ORDERED_TRUNCATE_2_TO
#endif

// (Must come after OMNI_TARGET_TOGGLE, else we don't reset it for scalar)
template <class DN, OMNI_IF_UNSIGNED_D(DN), class V, OMNI_IF_UNSIGNED_V(V), OMNI_IF_T_SIZE_V(V, sizeof(TFromD<DN>) * 2),
    OMNI_IF_LANES_D(DFromV<VFromD<DN>>, OMNI_MAX_LANES_D(DFromV<V>) * 2)>
OMNI_API VFromD<DN> OrderedTruncate2To(DN dn, V a, V b)
{
    return ConcatEven(dn, BitCast(dn, b), BitCast(dn, a));
}

// ------------------------------ AESRound

// Cannot implement on scalar: need at least 16 bytes for TableLookupBytes.
#if OMNI_TARGET != OMNI_SCALAR || OMNI_IDE

// Define for white-box testing, even if native instructions are available.
namespace detail {
template <class V>
// u8
OMNI_INLINE V SubBytesMulInverseAndAffineLookup(V state, V affine_tblL, V affine_tblU)
{
    const DFromV<V> du;
    const auto mask = Set(du, uint8_t{ 0xF });

    // Change polynomial basis to GF(2^4)
    {
        const VFromD<decltype(du)> basisL = Dup128VecFromValues(du, 0x00, 0x70, 0x2A, 0x5A, 0x98, 0xE8, 0xB2, 0xC2,
            0x08, 0x78, 0x22, 0x52, 0x90, 0xE0, 0xBA, 0xCA);
        const VFromD<decltype(du)> basisU = Dup128VecFromValues(du, 0x00, 0x4D, 0x7C, 0x31, 0x7D, 0x30, 0x01, 0x4C,
            0x81, 0xCC, 0xFD, 0xB0, 0xFC, 0xB1, 0x80, 0xCD);
        const auto sL = And(state, mask);
        const auto sU = ShiftRight<4>(state); // byte shift => upper bits are zero
        const auto gf4L = TableLookupBytes(basisL, sL);
        const auto gf4U = TableLookupBytes(basisU, sU);
        state = Xor(gf4L, gf4U);
    }

    // Inversion in GF(2^4). Elements 0 represent "infinity" (division by 0) and
    // cause TableLookupBytesOr0 to return 0.
    const VFromD<decltype(du)> zetaInv =
        Dup128VecFromValues(du, 0x80, 7, 11, 15, 6, 10, 4, 1, 9, 8, 5, 2, 12, 14, 13, 3);
    const VFromD<decltype(du)> tbl = Dup128VecFromValues(du, 0x80, 1, 8, 13, 15, 6, 5, 14, 2, 12, 11, 10, 9, 3, 7, 4);
    const auto sL = And(state, mask);     // L=low nibble, U=upper
    const auto sU = ShiftRight<4>(state); // byte shift => upper bits are zero
    const auto sX = Xor(sU, sL);
    const auto invL = TableLookupBytes(zetaInv, sL);
    const auto invU = TableLookupBytes(tbl, sU);
    const auto invX = TableLookupBytes(tbl, sX);
    const auto outL = Xor(sX, TableLookupBytesOr0(tbl, Xor(invL, invU)));
    const auto outU = Xor(sU, TableLookupBytesOr0(tbl, Xor(invL, invX)));

    const auto affL = TableLookupBytesOr0(affine_tblL, outL);
    const auto affU = TableLookupBytesOr0(affine_tblU, outU);
    return Xor(affL, affU);
}

template <class V>
// u8
OMNI_INLINE V SubBytes(V state)
{
    const DFromV<V> du;
    // Linear skew (cannot bake 0x63 bias into the table because out* indices
    // may have the infinity flag set).
    const VFromD<decltype(du)> affineL = Dup128VecFromValues(du, 0x00, 0xC7, 0xBD, 0x6F, 0x17, 0x6D, 0xD2, 0xD0, 0x78,
        0xA8, 0x02, 0xC5, 0x7A, 0xBF, 0xAA, 0x15);
    const VFromD<decltype(du)> affineU = Dup128VecFromValues(du, 0x00, 0x6A, 0xBB, 0x5F, 0xA5, 0x74, 0xE4, 0xCF, 0xFA,
        0x35, 0x2B, 0x41, 0xD1, 0x90, 0x1E, 0x8E);
    return Xor(SubBytesMulInverseAndAffineLookup(state, affineL, affineU), Set(du, uint8_t{ 0x63 }));
}

template <class V>
// u8
OMNI_INLINE V InvSubBytes(V state)
{
    const DFromV<V> du;
    const VFromD<decltype(du)> gF2P4InvToGF2P8InvL = Dup128VecFromValues(du, 0x00, 0x40, 0xF9, 0x7E, 0x53, 0xEA, 0x87,
        0x13, 0x2D, 0x3E, 0x94, 0xD4, 0xB9, 0x6D, 0xAA, 0xC7);
    const VFromD<decltype(du)> gF2P4InvToGF2P8InvU = Dup128VecFromValues(du, 0x00, 0x1D, 0x44, 0x93, 0x0F, 0x56, 0xD7,
        0x12, 0x9C, 0x8E, 0xC5, 0xD8, 0x59, 0x81, 0x4B, 0xCA);

    // Apply the inverse affine transformation
    const auto b = Xor(Xor3(Or(ShiftLeft<1>(state), ShiftRight<7>(state)),
        Or(ShiftLeft<3>(state), ShiftRight<5>(state)), Or(ShiftLeft<6>(state), ShiftRight<2>(state))),
        Set(du, uint8_t{ 0x05 }));

    // The GF(2^8) multiplicative inverse is computed as follows:
    // - Changing the polynomial basis to GF(2^4)
    // - Computing the GF(2^4) multiplicative inverse
    // - Converting the GF(2^4) multiplicative inverse to the GF(2^8)
    //   multiplicative inverse through table lookups using the
    //   kGF2P4InvToGF2P8InvL and kGF2P4InvToGF2P8InvU tables
    return SubBytesMulInverseAndAffineLookup(b, gF2P4InvToGF2P8InvL, gF2P4InvToGF2P8InvU);
}
} // namespace detail

#endif // OMNI_TARGET != OMNI_SCALAR

#ifdef OMNI_NATIVE_AES
#undef OMNI_NATIVE_AES
#else
#define OMNI_NATIVE_AES
#endif
namespace detail {
template <class V>
// u8
OMNI_INLINE V ShiftRows(const V state)
{
    const DFromV<V> du;
    // transposed: state is column major
    const VFromD<decltype(du)> shift_row =
        Dup128VecFromValues(du, 0, 5, 10, 15, 4, 9, 14, 3, 8, 13, 2, 7, 12, 1, 6, 11);
    return TableLookupBytes(state, shift_row);
}

template <class V>
// u8
OMNI_INLINE V InvShiftRows(const V state)
{
    const DFromV<V> du;
    // transposed: state is column major
    const VFromD<decltype(du)> shift_row =
        Dup128VecFromValues(du, 0, 13, 10, 7, 4, 1, 14, 11, 8, 5, 2, 15, 12, 9, 6, 3);
    return TableLookupBytes(state, shift_row);
}

template <class V>
// u8
OMNI_INLINE V GF2P8Mod11BMulBy2(V v)
{
    const DFromV<V> du;
    const RebindToSigned<decltype(du)> di; // can only do signed comparisons
    const auto msb = Lt(BitCast(di, v), Zero(di));
    const auto overflow = BitCast(du, IfThenElseZero(msb, Set(di, int8_t{ 0x1B })));
    return Xor(Add(v, v), overflow); // = v*2 in GF(2^8).
}

template <class V>
// u8
OMNI_INLINE V MixColumns(const V state)
{
    const DFromV<V> du;
    // For each column, the rows are the sum of GF(2^8) matrix multiplication by:
    // 2 3 1 1  // Let s := state*1, d := state*2, t := state*3.
    // 1 2 3 1  // d are on diagonal, no permutation needed.
    // 1 1 2 3  // t1230 indicates column indices of threes for the 4 rows.
    // 3 1 1 2  // We also need to compute s2301 and s3012 (=1230 o 2301).
    const VFromD<decltype(du)> v2301 = Dup128VecFromValues(du, 2, 3, 0, 1, 6, 7, 4, 5, 10, 11, 8, 9, 14, 15, 12, 13);
    const VFromD<decltype(du)> v1230 = Dup128VecFromValues(du, 1, 2, 3, 0, 5, 6, 7, 4, 9, 10, 11, 8, 13, 14, 15, 12);
    const auto d = GF2P8Mod11BMulBy2(state); // = state*2 in GF(2^8).
    const auto s2301 = TableLookupBytes(state, v2301);
    const auto d_s2301 = Xor(d, s2301);
    const auto t_s2301 = Xor(state, d_s2301); // t(s*3) = XOR-sum {s, d(s*2)}
    const auto t1230_s3012 = TableLookupBytes(t_s2301, v1230);
    return Xor(d_s2301, t1230_s3012); // XOR-sum of 4 terms
}

template <class V>
// u8
OMNI_INLINE V InvMixColumns(const V state)
{
    const DFromV<V> du;
    // For each column, the rows are the sum of GF(2^8) matrix multiplication by:
    // 14 11 13  9
    //  9 14 11 13
    // 13  9 14 11
    // 11 13  9 14
    const VFromD<decltype(du)> v2301 = Dup128VecFromValues(du, 2, 3, 0, 1, 6, 7, 4, 5, 10, 11, 8, 9, 14, 15, 12, 13);
    const VFromD<decltype(du)> v1230 = Dup128VecFromValues(du, 1, 2, 3, 0, 5, 6, 7, 4, 9, 10, 11, 8, 13, 14, 15, 12);

    const auto sx2 = GF2P8Mod11BMulBy2(state); /* = state*2 in GF(2^8) */
    const auto sx4 = GF2P8Mod11BMulBy2(sx2);   /* = state*4 in GF(2^8) */
    const auto sx8 = GF2P8Mod11BMulBy2(sx4);   /* = state*8 in GF(2^8) */
    const auto sx9 = Xor(sx8, state);          /* = state*9 in GF(2^8) */
    const auto sx11 = Xor(sx9, sx2);           /* = state*11 in GF(2^8) */
    const auto sx13 = Xor(sx9, sx4);           /* = state*13 in GF(2^8) */
    const auto sx14 = Xor3(sx8, sx4, sx2);     /* = state*14 in GF(2^8) */

    const auto sx13_0123_sx9_1230 = Xor(sx13, TableLookupBytes(sx9, v1230));
    const auto sx14_0123_sx11_1230 = Xor(sx14, TableLookupBytes(sx11, v1230));
    const auto sx13_2301_sx9_3012 = TableLookupBytes(sx13_0123_sx9_1230, v2301);
    return Xor(sx14_0123_sx11_1230, sx13_2301_sx9_3012);
}
} // namespace detail

template <class V>
// u8
OMNI_API V AESRound(V state, const V round_key)
{
    // Intel docs swap the first two steps, but it does not matter because
    // ShiftRows is a permutation and SubBytes is independent of lane index.
    state = detail::SubBytes(state);
    state = detail::ShiftRows(state);
    state = detail::MixColumns(state);
    state = Xor(state, round_key); // AddRoundKey
    return state;
}

template <class V>
// u8
OMNI_API V AESLastRound(V state, const V round_key)
{
    // LIke AESRound, but without MixColumns.
    state = detail::SubBytes(state);
    state = detail::ShiftRows(state);
    state = Xor(state, round_key); // AddRoundKey
    return state;
}

template <class V> OMNI_API V AESInvMixColumns(V state)
{
    return detail::InvMixColumns(state);
}

template <class V>
// u8
OMNI_API V AESRoundInv(V state, const V round_key)
{
    state = detail::InvSubBytes(state);
    state = detail::InvShiftRows(state);
    state = detail::InvMixColumns(state);
    state = Xor(state, round_key); // AddRoundKey
    return state;
}

template <class V>
// u8
OMNI_API V AESLastRoundInv(V state, const V round_key)
{
    // Like AESRoundInv, but without InvMixColumns.
    state = detail::InvSubBytes(state);
    state = detail::InvShiftRows(state);
    state = Xor(state, round_key); // AddRoundKey
    return state;
}

template <uint8_t kRcon, class V, OMNI_IF_U8_D(DFromV<V>)> OMNI_API V AESKeyGenAssist(V v)
{
    const DFromV<decltype(v)> d;
    const V rconXorMask = Dup128VecFromValues(d, 0, 0, 0, 0, kRcon, 0, 0, 0, 0, 0, 0, 0, kRcon, 0, 0, 0);
    const V rotWordShuffle = Dup128VecFromValues(d, 4, 5, 6, 7, 5, 6, 7, 4, 12, 13, 14, 15, 13, 14, 15, 12);
    const auto sub_word_result = detail::SubBytes(v);
    const auto rot_word_result = TableLookupBytes(sub_word_result, rotWordShuffle);
    return Xor(rot_word_result, rconXorMask);
}

template <class V> OMNI_API V CLMulLower(V a, V b)
{
    const DFromV<V> d;
    static_assert(IsSame<TFromD<decltype(d)>, uint64_t>(), "V must be u64");
    const auto k1 = Set(d, 0x1111111111111111ULL);
    const auto k2 = Set(d, 0x2222222222222222ULL);
    const auto k4 = Set(d, 0x4444444444444444ULL);
    const auto k8 = Set(d, 0x8888888888888888ULL);
    const auto a0 = And(a, k1);
    const auto a1 = And(a, k2);
    const auto a2 = And(a, k4);
    const auto a3 = And(a, k8);
    const auto b0 = And(b, k1);
    const auto b1 = And(b, k2);
    const auto b2 = And(b, k4);
    const auto b3 = And(b, k8);

    auto m0 = Xor(MulEven(a0, b0), MulEven(a1, b3));
    auto m1 = Xor(MulEven(a0, b1), MulEven(a1, b0));
    auto m2 = Xor(MulEven(a0, b2), MulEven(a1, b1));
    auto m3 = Xor(MulEven(a0, b3), MulEven(a1, b2));
    m0 = Xor(m0, Xor(MulEven(a2, b2), MulEven(a3, b1)));
    m1 = Xor(m1, Xor(MulEven(a2, b3), MulEven(a3, b2)));
    m2 = Xor(m2, Xor(MulEven(a2, b0), MulEven(a3, b3)));
    m3 = Xor(m3, Xor(MulEven(a2, b1), MulEven(a3, b0)));
    return Or(Or(And(m0, k1), And(m1, k2)), Or(And(m2, k4), And(m3, k8)));
}

template <class V> OMNI_API V CLMulUpper(V a, V b)
{
    const DFromV<V> d;
    static_assert(IsSame<TFromD<decltype(d)>, uint64_t>(), "V must be u64");
    const auto k1 = Set(d, 0x1111111111111111ULL);
    const auto k2 = Set(d, 0x2222222222222222ULL);
    const auto k4 = Set(d, 0x4444444444444444ULL);
    const auto k8 = Set(d, 0x8888888888888888ULL);
    const auto a0 = And(a, k1);
    const auto a1 = And(a, k2);
    const auto a2 = And(a, k4);
    const auto a3 = And(a, k8);
    const auto b0 = And(b, k1);
    const auto b1 = And(b, k2);
    const auto b2 = And(b, k4);
    const auto b3 = And(b, k8);

    auto m0 = Xor(MulOdd(a0, b0), MulOdd(a1, b3));
    auto m1 = Xor(MulOdd(a0, b1), MulOdd(a1, b0));
    auto m2 = Xor(MulOdd(a0, b2), MulOdd(a1, b1));
    auto m3 = Xor(MulOdd(a0, b3), MulOdd(a1, b2));
    m0 = Xor(m0, Xor(MulOdd(a2, b2), MulOdd(a3, b1)));
    m1 = Xor(m1, Xor(MulOdd(a2, b3), MulOdd(a3, b2)));
    m2 = Xor(m2, Xor(MulOdd(a2, b0), MulOdd(a3, b3)));
    m3 = Xor(m3, Xor(MulOdd(a2, b1), MulOdd(a3, b0)));
    return Or(Or(And(m0, k1), And(m1, k2)), Or(And(m2, k4), And(m3, k8)));
}

// ------------------------------ 64-bit multiplication
#ifndef OMNI_NATIVE_MUL_64
#define OMNI_NATIVE_MUL_64

// Single-lane i64 or u64
template <class V, OMNI_IF_T_SIZE_V(V, 8), OMNI_IF_V_SIZE_V(V, 8), OMNI_IF_NOT_FLOAT_V(V)>
OMNI_API V operator*(V x, V y)
{
    const DFromV<V> d;
    using T = TFromD<decltype(d)>;
    using TU = MakeUnsigned<T>;
    const TU xu = static_cast<TU>(GetLane(x));
    const TU yu = static_cast<TU>(GetLane(y));
    return Set(d, static_cast<T>(xu * yu));
}

template <class V, class D64 = DFromV<V>, OMNI_IF_U64_D(D64), OMNI_IF_V_SIZE_GT_D(D64, 8)>
OMNI_API V operator*(V x, V y)
{
    RepartitionToNarrow<D64> d32;
    auto x32 = BitCast(d32, x);
    auto y32 = BitCast(d32, y);
    auto lolo = BitCast(d32, MulEven(x32, y32));
    auto lohi = BitCast(d32, MulEven(x32, BitCast(d32, ShiftRight<32>(y))));
    auto hilo = BitCast(d32, MulEven(BitCast(d32, ShiftRight<32>(x)), y32));
    auto hi = BitCast(d32, ShiftLeft<32>(BitCast(D64{}, lohi + hilo)));
    return BitCast(D64{}, lolo + hi);
}
template <class V, class DI64 = DFromV<V>, OMNI_IF_I64_D(DI64), OMNI_IF_V_SIZE_GT_D(DI64, 8)>
OMNI_API V operator*(V x, V y)
{
    RebindToUnsigned<DI64> du64;
    return BitCast(DI64{}, BitCast(du64, x) * BitCast(du64, y));
}

#endif // OMNI_NATIVE_MUL_64

// ------------------------------ Integer MulSub / NegMulSub
#ifdef OMNI_NATIVE_INT_FMSUB
#undef OMNI_NATIVE_INT_FMSUB
#else
#define OMNI_NATIVE_INT_FMSUB
#endif

template <class V, OMNI_IF_NOT_FLOAT_NOR_SPECIAL_V(V)> OMNI_API V MulSub(V mul, V x, V sub)
{
    const DFromV<decltype(mul)> d;
    const RebindToSigned<decltype(d)> di;
    return MulAdd(mul, x, BitCast(d, Neg(BitCast(di, sub))));
}

template <class V, OMNI_IF_NOT_FLOAT_NOR_SPECIAL_V(V)> OMNI_API V NegMulSub(V mul, V x, V sub)
{
    const DFromV<decltype(mul)> d;
    const RebindToSigned<decltype(d)> di;

    return BitCast(d, Neg(BitCast(di, MulAdd(mul, x, sub))));
}

// ------------------------------ MulAddSub

// MulAddSub(mul, x, sub_or_add) for a 1-lane vector is equivalent to
// MulSub(mul, x, sub_or_add)
template <class V, OMNI_IF_LANES_D(DFromV<V>, 1)> OMNI_API V MulAddSub(V mul, V x, V sub_or_add)
{
    return MulSub(mul, x, sub_or_add);
}

// MulAddSub for integer vectors on SVE2 is implemented in arm_sve-inl.h
template <class V, OMNI_IF_MULADDSUB_V(V)> OMNI_API V MulAddSub(V mul, V x, V sub_or_add)
{
    using D = DFromV<V>;
    using T = TFromD<D>;
    using TNegate = If<!IsSigned<T>(), MakeSigned<T>, T>;

    const D d;
    const Rebind<TNegate, D> d_negate;

    const auto add = OddEven(sub_or_add, BitCast(d, Neg(BitCast(d_negate, sub_or_add))));
    return MulAdd(mul, x, add);
}

// ------------------------------ Integer division
#ifndef OMNI_NATIVE_INT_DIV
#define OMNI_NATIVE_INT_DIV

namespace detail {
// DemoteInRangeTo, PromoteInRangeTo, and ConvertInRangeTo are okay to use in
// the implementation of detail::IntDiv in generic_ops-inl.h as the current
// implementations of DemoteInRangeTo, PromoteInRangeTo, and ConvertInRangeTo
// will convert values that are outside of the range of TFromD<DI> by either
// saturation, truncation, or converting values that are outside of the
// destination range to LimitsMin<TFromD<DI>>() (which is equal to
// static_cast<TFromD<DI>>(LimitsMax<TFromD<DI>>() + 1))

template <class D, class V, OMNI_IF_T_SIZE_D(D, sizeof(TFromV<V>))> OMNI_INLINE Vec<D> IntDivConvFloatToInt(D di, V vf)
{
    return ConvertInRangeTo(di, vf);
}

template <class D, class V, OMNI_IF_T_SIZE_D(D, sizeof(TFromV<V>))> OMNI_INLINE Vec<D> IntDivConvIntToFloat(D df, V vi)
{
    return ConvertTo(df, vi);
}

template <size_t kOrigLaneSize, class V, OMNI_IF_NOT_FLOAT_NOR_SPECIAL_V(V),
    OMNI_IF_T_SIZE_GT(TFromV<V>, kOrigLaneSize)>
OMNI_INLINE V IntDivUsingFloatDiv(V a, V b)
{
    const DFromV<decltype(a)> d;
    const RebindToFloat<decltype(d)> df;

    // If kOrigLaneSize < sizeof(T) is true, then a[i] and b[i] are both in the
    // [LimitsMin<SignedFromSize<kOrigLaneSize>>(),
    // LimitsMax<UnsignedFromSize<kOrigLaneSize>>()] range.

    // floor(|a[i] / b[i]|) <= |flt_q| < floor(|a[i] / b[i]|) + 1 is also
    // guaranteed to be true if MakeFloat<T> has at least kOrigLaneSize*8 + 1
    // mantissa bits (including the implied one bit), where flt_q is equal to
    // static_cast<MakeFloat<T>>(a[i]) / static_cast<MakeFloat<T>>(b[i]),
    // even in the case where the magnitude of an inexact floating point division
    // result is rounded up.

    // In other words, floor(flt_q) < flt_q < ceil(flt_q) is guaranteed to be true
    // if (a[i] % b[i]) != 0 is true and MakeFloat<T> has at least
    // kOrigLaneSize*8 + 1 mantissa bits (including the implied one bit), even in
    // the case where the magnitude of an inexact floating point division result
    // is rounded up.

    // It is okay to do conversions from MakeFloat<TFromV<V>> to TFromV<V> using
    // ConvertInRangeTo if sizeof(TFromV<V>) > kOrigLaneSize as the result of the
    // floating point division is always greater than LimitsMin<TFromV<V>>() and
    // less than LimitsMax<TFromV<V>>() if sizeof(TFromV<V>) > kOrigLaneSize and
    // b[i] != 0.

    // On targets other than Armv7 NEON, use F16 or F32 division as most targets
    // other than Armv7 NEON have native F32 divide instructions
    return ConvertInRangeTo(d, Div(ConvertTo(df, a), ConvertTo(df, b)));
}

template <size_t kOrigLaneSize, class V, OMNI_IF_NOT_FLOAT_NOR_SPECIAL_V(V), OMNI_IF_T_SIZE(TFromV<V>, kOrigLaneSize),
    OMNI_IF_T_SIZE_ONE_OF_V(V, (1 << 4) | (1 << 8))>
OMNI_INLINE V IntDivUsingFloatDiv(V a, V b)
{
    // If kOrigLaneSize == sizeof(T) is true, at least two reciprocal
    // multiplication steps are needed as the mantissa of MakeFloat<T> has fewer
    // than kOrigLaneSize*8 + 1 bits

    using T = TFromV<V>;
    using TF = MakeFloat<T>;

    const DFromV<decltype(a)> d;
    const RebindToSigned<decltype(d)> di;
    const RebindToUnsigned<decltype(d)> du;
    const Rebind<TF, decltype(d)> df;

    if (!IsSigned<T>()) {
        // If T is unsigned, set a[i] to (a[i] >= b[i] ? 1 : 0) and set b[i] to 1 if
        // b[i] > LimitsMax<MakeSigned<T>>() is true

        const auto one = Set(di, MakeSigned<T>{ 1 });
        a = BitCast(d,
            IfNegativeThenElse(BitCast(di, b), IfThenElseZero(RebindMask(di, Ge(a, b)), one), BitCast(di, a)));
        b = BitCast(d, IfNegativeThenElse(BitCast(di, b), one, BitCast(di, b)));
    }

    // LimitsMin<T>() <= b[i] <= LimitsMax<MakeSigned<T>>() is now true

    const auto flt_b = IntDivConvIntToFloat(df, b);

    const auto flt_recip_b = Div(Set(df, TF(1.0)), flt_b);

    // It is okay if the conversion of a[i] * flt_recip_b[i] to T using
    // IntDivConvFloatToInt returns incorrect results in any lanes where b[i] == 0
    // as the result of IntDivUsingFloatDiv(a, b) is implementation-defined in any
    // lanes where b[i] == 0.

    // If ScalarAbs(b[i]) == 1 is true, then it is possible for
    // a[i] * flt_recip_b[i] to be rounded up to a value that is outside of the
    // range of T. If a[i] * flt_recip_b[i] is outside of the range of T,
    // IntDivConvFloatToInt will convert any values that are out of the range of T
    // by either saturation, truncation, or wrapping around to LimitsMin<T>().

    // It is okay if the conversion of a[i] * flt_recip_b[i] to T using
    // IntDivConvFloatToInt wraps around if ScalarAbs(b[i]) == 1 as r0 will have
    // the correct sign if ScalarAbs(b[i]) == 1, even in the cases where the
    // conversion of a[i] * flt_recip_b[i] to T using IntDivConvFloatToInt is
    // truncated or wraps around.

    // If ScalarAbs(b[i]) >= 2 is true, a[i] * flt_recip_b[i] will be within the
    // range of T, even in the cases where the conversion of a[i] to TF is
    // rounded up or the result of multiplying a[i] by flt_recip_b[i] is rounded
    // up.

    // ScalarAbs(r0[i]) will also always be less than (LimitsMax<T>() / 2) if
    // b[i] != 0, even in the cases where the conversion of a[i] * flt_recip_b[i]
    // to T using IntDivConvFloatToInt is truncated or is wrapped around.

    auto q0 = IntDivConvFloatToInt(d, Mul(IntDivConvIntToFloat(df, a), flt_recip_b));
    const auto r0 = BitCast(di, simd::NegMulAdd(q0, b, a));

    // If b[i] != 0 is true, r0[i] * flt_recip_b[i] is always within the range of
    // T, even in the cases where the conversion of r0[i] to TF is rounded up or
    // the multiplication of r0[i] by flt_recip_b[i] is rounded up.

    auto q1 = IntDivConvFloatToInt(di, Mul(IntDivConvIntToFloat(df, r0), flt_recip_b));
    const auto r1 = simd::NegMulAdd(q1, BitCast(di, b), r0);

    auto r3 = r1;

    auto r4 = r3;

    // Need to negate r4[i] if a[i] < 0 is true
    if (IsSigned<TFromV<V>>()) {
        r4 = IfNegativeThenNegOrUndefIfZero(BitCast(di, a), r4);
    }

    // r4[i] is now equal to (a[i] < 0) ? (-r3[i]) : r3[i]

    auto abs_b = BitCast(du, b);
    if (IsSigned<TFromV<V>>()) {
        abs_b = BitCast(du, Abs(BitCast(di, abs_b)));
    }

    // If (r4[i] < 0 || r4[i] >= abs_b[i]) is true, then set q4[i] to -1.
    // Otherwise, set r4[i] to 0.

    // (r4[i] < 0 || r4[i] >= abs_b[i]) can be carried out using a single unsigned
    // comparison as static_cast<TU>(r4[i]) >= TU(LimitsMax<TI>() + 1) >= abs_b[i]
    // will be true if r4[i] < 0 is true.
    auto q4 = BitCast(di, VecFromMask(du, Ge(BitCast(du, r4), abs_b)));

    // q4[i] is now equal to (r4[i] < 0 || r4[i] >= abs_b[i]) ? -1 : 0

    // Need to negate q4[i] if r3[i] and b[i] do not have the same sign
    auto q4_negate_mask = r3;
    if (IsSigned<TFromV<V>>()) {
        q4_negate_mask = Xor(q4_negate_mask, BitCast(di, b));
    }
    q4 = IfNegativeThenElse(q4_negate_mask, Neg(q4), q4);

    // q4[i] is now equal to (r4[i] < 0 || r4[i] >= abs_b[i]) ?
    //                       (((r3[i] ^ b[i]) < 0) ? 1 : -1)

    // The final result is equal to q0[i] + q1[i] - q4[i]
    return Sub(Add(q0, BitCast(d, q1)), BitCast(d, q4));
}

template <size_t kOrigLaneSize, class V, OMNI_IF_T_SIZE_ONE_OF_V(V, (1 << 1) | (1 << 2)),
    OMNI_IF_V_SIZE_LE_V(V, OMNI_MAX_BYTES / ((!OMNI_HAVE_FLOAT16 && sizeof(TFromV<V>) == 1) ? 4 : 2))>
OMNI_INLINE V IntDiv(V a, V b)
{
    using T = TFromV<V>;

    // If OMNI_HAVE_FLOAT16 is 0, need to promote I8 to I32 and U8 to U32
    using TW = MakeWide<If<(!OMNI_HAVE_FLOAT16 && sizeof(TFromV<V>) == 1), MakeWide<T>, T>>;

    const DFromV<decltype(a)> d;
    const Rebind<TW, decltype(d)> dw;

    // On other targets, promote to TW and demote to T
    const decltype(dw)dw_i;
    const decltype(d)d_demote_to;

    return BitCast(d,
        DemoteTo(d_demote_to, IntDivUsingFloatDiv<kOrigLaneSize>(PromoteTo(dw_i, a), PromoteTo(dw_i, b))));
}

template <size_t kOrigLaneSize, class V, OMNI_IF_T_SIZE_ONE_OF_V(V, (OMNI_HAVE_FLOAT16 ? (1 << 1) : 0) | (1 << 2)),
    OMNI_IF_V_SIZE_GT_V(V, OMNI_MAX_BYTES / 2)>
OMNI_INLINE V IntDiv(V a, V b)
{
    const DFromV<decltype(a)> d;
    const RepartitionToWide<decltype(d)> dw;

    // On other targets, promote to MakeWide<TFromV<V>> and demote to TFromV<V>
    const decltype(dw)dw_i;
    const decltype(d)d_demote_to;

    return BitCast(d, OrderedDemote2To(d_demote_to,
        IntDivUsingFloatDiv<kOrigLaneSize>(PromoteLowerTo(dw_i, a), PromoteLowerTo(dw_i, b)),
        IntDivUsingFloatDiv<kOrigLaneSize>(PromoteUpperTo(dw_i, a), PromoteUpperTo(dw_i, b))));
}

#if !OMNI_HAVE_FLOAT16
template <size_t kOrigLaneSize, class V, OMNI_IF_UI8(TFromV<V>), OMNI_IF_V_SIZE_V(V, OMNI_MAX_BYTES / 2)>
OMNI_INLINE V IntDiv(V a, V b)
{
    const DFromV<decltype(a)> d;
    const Rebind<MakeWide<TFromV<V>>, decltype(d)> dw;

    // On other targets, demote from MakeWide<TFromV<V>> to TFromV<V>
    const decltype(dw)dw_i;

    return DemoteTo(d, BitCast(dw_i, IntDiv<1>(PromoteTo(dw, a), PromoteTo(dw, b))));
}
template <size_t kOrigLaneSize, class V, OMNI_IF_UI8(TFromV<V>), OMNI_IF_V_SIZE_GT_V(V, OMNI_MAX_BYTES / 2)>
OMNI_INLINE V IntDiv(V a, V b)
{
    const DFromV<decltype(a)> d;
    const RepartitionToWide<decltype(d)> dw;

    // On other targets, demote from MakeWide<TFromV<V>> to TFromV<V>
    const decltype(dw)dw_i;

    return OrderedDemote2To(d, BitCast(dw_i, IntDiv<1>(PromoteLowerTo(dw, a), PromoteLowerTo(dw, b))),
        BitCast(dw_i, IntDiv<1>(PromoteUpperTo(dw, a), PromoteUpperTo(dw, b))));
}
#endif // !OMNI_HAVE_FLOAT16

template <size_t kOrigLaneSize, class V, OMNI_IF_T_SIZE_ONE_OF_V(V, (OMNI_HAVE_FLOAT64 ? 0 : (1 << 4)) | (1 << 8))>
OMNI_INLINE V IntDiv(V a, V b)
{
    return IntDivUsingFloatDiv<kOrigLaneSize>(a, b);
}

template <size_t kOrigLaneSize, class V, OMNI_IF_UI32(TFromV<V>), OMNI_IF_V_SIZE_LE_V(V, OMNI_MAX_BYTES / 2)>
OMNI_INLINE V IntDiv(V a, V b)
{
    const DFromV<decltype(a)> d;
    const Rebind<double, decltype(d)> df64;

    // It is okay to demote the F64 Div result to int32_t or uint32_t using
    // DemoteInRangeTo as static_cast<double>(a[i]) / static_cast<double>(b[i])
    // will always be within the range of TFromV<V> if b[i] != 0 and
    // sizeof(TFromV<V>) <= 4.

    return DemoteInRangeTo(d, Div(PromoteTo(df64, a), PromoteTo(df64, b)));
}
template <size_t kOrigLaneSize, class V, OMNI_IF_UI32(TFromV<V>), OMNI_IF_V_SIZE_GT_V(V, OMNI_MAX_BYTES / 2)>
OMNI_INLINE V IntDiv(V a, V b)
{
    const DFromV<decltype(a)> d;
    const Half<decltype(d)> dh;
    const Repartition<double, decltype(d)> df64;

    // It is okay to demote the F64 Div result to int32_t or uint32_t using
    // DemoteInRangeTo as static_cast<double>(a[i]) / static_cast<double>(b[i])
    // will always be within the range of TFromV<V> if b[i] != 0 and
    // sizeof(TFromV<V>) <= 4.

    const VFromD<decltype(df64)> div1 = Div(PromoteUpperTo(df64, a), PromoteUpperTo(df64, b));
    const VFromD<decltype(df64)> div0 = Div(PromoteLowerTo(df64, a), PromoteLowerTo(df64, b));
    return Combine(d, DemoteInRangeTo(dh, div1), DemoteInRangeTo(dh, div0));
}

template <size_t kOrigLaneSize, class V, OMNI_IF_NOT_FLOAT_NOR_SPECIAL_V(V),
    OMNI_IF_T_SIZE_ONE_OF_V(V, (1 << 1) | (1 << 2) | (1 << 4) | (1 << 8))>
OMNI_INLINE V IntMod(V a, V b)
{
    return simd::NegMulAdd(IntDiv<kOrigLaneSize>(a, b), b, a);
}
} // namespace detail

template <class T, size_t N, OMNI_IF_NOT_FLOAT_NOR_SPECIAL(T)>
OMNI_API Vec128<T, N> operator / (Vec128<T, N> a, Vec128<T, N> b)
{
    return detail::IntDiv<sizeof(T)>(a, b);
}

template <class T, size_t N, OMNI_IF_NOT_FLOAT_NOR_SPECIAL(T)>
OMNI_API Vec128<T, N> operator % (Vec128<T, N> a, Vec128<T, N> b)
{
    return detail::IntMod<sizeof(T)>(a, b);
}

#endif // OMNI_NATIVE_INT_DIV

// ------------------------------ AverageRound
#ifndef OMNI_NATIVE_AVERAGE_ROUND_UI64
#define OMNI_NATIVE_AVERAGE_ROUND_UI64

template <class V, OMNI_IF_UI64(TFromV<V>)> OMNI_API V AverageRound(V a, V b)
{
    using T = TFromV<V>;
    const DFromV<decltype(a)> d;
    return Add(Add(ShiftRight<1>(a), ShiftRight<1>(b)), And(Or(a, b), Set(d, T{ 1 })));
}

#endif // OMNI_NATIVE_AVERAGE_ROUND_UI64

// ------------------------------ RoundingShiftRight (AverageRound)

#ifndef OMNI_NATIVE_ROUNDING_SHR
#define OMNI_NATIVE_ROUNDING_SHR

template <int kShiftAmt, class V, OMNI_IF_NOT_FLOAT_NOR_SPECIAL_V(V)> OMNI_API V RoundingShiftRight(V v)
{
    const DFromV<V> d;
    using T = TFromD<decltype(d)>;

    static_assert(0 <= kShiftAmt && kShiftAmt <= static_cast<int>(sizeof(T) * 8 - 1), "kShiftAmt is out of range");

    constexpr int kScaleDownShrAmt = OMNI_MAX(kShiftAmt - 1, 0);

    auto scaled_down_v = v;
    OMNI_IF_CONSTEXPR(kScaleDownShrAmt > 0)
    {
        scaled_down_v = ShiftRight<kScaleDownShrAmt>(v);
    }

    OMNI_IF_CONSTEXPR(kShiftAmt == 0)
    {
        return scaled_down_v;
    }

    return AverageRound(scaled_down_v, Zero(d));
}

template <class V, OMNI_IF_NOT_FLOAT_NOR_SPECIAL_V(V)> OMNI_API V RoundingShiftRightSame(V v, int shift_amt)
{
    const DFromV<V> d;
    using T = TFromD<decltype(d)>;

    const int shift_amt_is_zero_mask = -static_cast<int>(shift_amt == 0);

    const auto scaled_down_v = ShiftRightSame(v,
        static_cast<int>(static_cast<unsigned>(shift_amt) + static_cast<unsigned>(~shift_amt_is_zero_mask)));

    return AverageRound(scaled_down_v, And(scaled_down_v, Set(d, static_cast<T>(shift_amt_is_zero_mask))));
}

template <class V, OMNI_IF_NOT_FLOAT_NOR_SPECIAL_V(V)> OMNI_API V RoundingShr(V v, V amt)
{
    const DFromV<V> d;
    const RebindToUnsigned<decltype(d)> du;
    using T = TFromD<decltype(d)>;
    using TU = MakeUnsigned<T>;

    const auto unsigned_amt = BitCast(du, amt);
    const auto scale_down_shr_amt = BitCast(d, SaturatedSub(unsigned_amt, Set(du, TU{ 1 })));

    const auto scaled_down_v = Shr(v, scale_down_shr_amt);
    return AverageRound(scaled_down_v, IfThenElseZero(Eq(amt, Zero(d)), scaled_down_v));
}

#endif // OMNI_NATIVE_ROUNDING_SHR

// ------------------------------ MulEvenAdd (PromoteEvenTo)

// SVE with bf16 and NEON with bf16 override this.
#ifndef OMNI_NATIVE_MUL_EVEN_BF16
#define OMNI_NATIVE_MUL_EVEN_BF16

template <class DF, OMNI_IF_F32_D(DF), class VBF = VFromD<Repartition<bfloat16_t, DF>>>
OMNI_API VFromD<DF> MulEvenAdd(DF df, VBF a, VBF b, VFromD<DF> c)
{
    return MulAdd(PromoteEvenTo(df, a), PromoteEvenTo(df, b), c);
}

template <class DF, OMNI_IF_F32_D(DF), class VBF = VFromD<Repartition<bfloat16_t, DF>>>
OMNI_API VFromD<DF> MulOddAdd(DF df, VBF a, VBF b, VFromD<DF> c)
{
    return MulAdd(PromoteOddTo(df, a), PromoteOddTo(df, b), c);
}

#endif // OMNI_NATIVE_MUL_EVEN_BF16

// ------------------------------ ReorderWidenMulAccumulate (MulEvenAdd)

// AVX3_SPR/ZEN4, and NEON with bf16 but not(!) SVE override this.
#ifndef OMNI_NATIVE_REORDER_WIDEN_MUL_ACC_BF16) == defined(OMNI_TARGET_TOGGLE))
#ifdef OMNI_NATIVE_REORDER_WIDEN_MUL_ACC_BF16
#undef OMNI_NATIVE_REORDER_WIDEN_MUL_ACC_BF16
#else
#define OMNI_NATIVE_REORDER_WIDEN_MUL_ACC_BF16
#endif

template <class DF, OMNI_IF_F32_D(DF), class VBF = VFromD<Repartition<bfloat16_t, DF>>>
OMNI_API VFromD<DF> ReorderWidenMulAccumulate(DF df, VBF a, VBF b, VFromD<DF> sum0, VFromD<DF> &sum1)
{
    // Lane order within sum0/1 is undefined, hence we can avoid the
    // longer-latency lane-crossing PromoteTo by using PromoteEvenTo.
    sum1 = MulOddAdd(df, a, b, sum1);
    return MulEvenAdd(df, a, b, sum0);
}

#endif // OMNI_NATIVE_REORDER_WIDEN_MUL_ACC_BF16

// ------------------------------ WidenMulAccumulate

#ifndef OMNI_NATIVE_WIDEN_MUL_ACCUMULATE
#define OMNI_NATIVE_WIDEN_MUL_ACCUMULATE

template <class D, OMNI_IF_INTEGER(TFromD<D>), class DN = RepartitionToNarrow<D>>
OMNI_API VFromD<D> WidenMulAccumulate(D d, VFromD<DN> mul, VFromD<DN> x, VFromD<D> low, VFromD<D> &high)
{
    high = MulAdd(PromoteUpperTo(d, mul), PromoteUpperTo(d, x), high);
    return MulAdd(PromoteLowerTo(d, mul), PromoteLowerTo(d, x), low);
}

#endif // OMNI_NATIVE_WIDEN_MUL_ACCUMULATE

// ------------------------------ SatWidenMulPairwiseAdd
#ifndef OMNI_NATIVE_U8_I8_SATWIDENMULPAIRWISEADD
#define OMNI_NATIVE_U8_I8_SATWIDENMULPAIRWISEADD

template <class DI16, class VU8, class VI8, class VU8_2 = Vec<Repartition<uint8_t, DI16>>, OMNI_IF_I16_D(DI16),
    OMNI_IF_U8_D(DFromV<VU8>), OMNI_IF_I8_D(DFromV<VI8>), OMNI_IF_LANES_D(DFromV<VU8>, OMNI_MAX_LANES_V(VI8)),
    OMNI_IF_LANES_D(DFromV<VU8>, OMNI_MAX_LANES_V(VU8_2))>
OMNI_API Vec<DI16> SatWidenMulPairwiseAdd(DI16 di16, VU8 a, VI8 b)
{
    const RebindToUnsigned<decltype(di16)> du16;

    const auto a0 = BitCast(di16, PromoteEvenTo(du16, a));
    const auto b0 = PromoteEvenTo(di16, b);

    const auto a1 = BitCast(di16, PromoteOddTo(du16, a));
    const auto b1 = PromoteOddTo(di16, b);

    return SaturatedAdd(Mul(a0, b0), Mul(a1, b1));
}

#endif

// ------------------------------ SatWidenMulPairwiseAccumulate

#ifndef OMNI_NATIVE_I16_I16_SATWIDENMULPAIRWISEACCUM
#define OMNI_NATIVE_I16_I16_SATWIDENMULPAIRWISEACCUM

template <class DI32, OMNI_IF_I32_D(DI32)>
OMNI_API VFromD<DI32> SatWidenMulPairwiseAccumulate(DI32 di32, VFromD<Repartition<int16_t, DI32>> a,
    VFromD<Repartition<int16_t, DI32>> b, VFromD<DI32> sum)
{
    // WidenMulPairwiseAdd(di32, a, b) is okay here as
    // a[0]*b[0]+a[1]*b[1] is between -2147418112 and 2147483648 and as
    // a[0]*b[0]+a[1]*b[1] can only overflow an int32_t if
    // a[0], b[0], a[1], and b[1] are all equal to -32768.

    const auto product = WidenMulPairwiseAdd(di32, a, b);

    const auto mul_overflow = VecFromMask(di32, Eq(product, Set(di32, LimitsMin<int32_t>())));

    return SaturatedAdd(Sub(sum, And(BroadcastSignBit(sum), mul_overflow)), Add(product, mul_overflow));
}

#endif // OMNI_NATIVE_I16_I16_SATWIDENMULPAIRWISEACCUM

// ------------------------------ SatWidenMulAccumFixedPoint

#ifndef OMNI_NATIVE_I16_SATWIDENMULACCUMFIXEDPOINT
#define OMNI_NATIVE_I16_SATWIDENMULACCUMFIXEDPOINT

template <class DI32, OMNI_IF_I32_D(DI32)>
OMNI_API VFromD<DI32> SatWidenMulAccumFixedPoint(DI32 di32, VFromD<Rebind<int16_t, DI32>> a,
    VFromD<Rebind<int16_t, DI32>> b, VFromD<DI32> sum)
{
    const Repartition<int16_t, DI32> dt_i16;

    const auto vt_a = ResizeBitCast(dt_i16, a);
    const auto vt_b = ResizeBitCast(dt_i16, b);

    const auto dup_a = InterleaveWholeLower(dt_i16, vt_a, vt_a);
    const auto dup_b = InterleaveWholeLower(dt_i16, vt_b, vt_b);

    return SatWidenMulPairwiseAccumulate(di32, dup_a, dup_b, sum);
}

#endif // OMNI_NATIVE_I16_SATWIDENMULACCUMFIXEDPOINT

// ------------------------------ SumOfMulQuadAccumulate

#ifndef OMNI_NATIVE_I8_I8_SUMOFMULQUADACCUMULATE) == defined(OMNI_TARGET_TOGGLE))

#ifdef OMNI_NATIVE_I8_I8_SUMOFMULQUADACCUMULATE
#undef OMNI_NATIVE_I8_I8_SUMOFMULQUADACCUMULATE
#else
#define OMNI_NATIVE_I8_I8_SUMOFMULQUADACCUMULATE
#endif

template <class DI32, OMNI_IF_I32_D(DI32)>
OMNI_API VFromD<DI32> SumOfMulQuadAccumulate(DI32 di32, VFromD<Repartition<int8_t, DI32>> a,
    VFromD<Repartition<int8_t, DI32>> b, VFromD<DI32> sum)
{
    const Repartition<int16_t, decltype(di32)> di16;

    const auto a0 = PromoteEvenTo(di16, a);
    const auto b0 = PromoteEvenTo(di16, b);

    const auto a1 = PromoteOddTo(di16, a);
    const auto b1 = PromoteOddTo(di16, b);

    return Add(sum, Add(WidenMulPairwiseAdd(di32, a0, b0), WidenMulPairwiseAdd(di32, a1, b1)));
}

#endif

#ifndef OMNI_NATIVE_U8_U8_SUMOFMULQUADACCUMULATE) == defined(OMNI_TARGET_TOGGLE))

#ifdef OMNI_NATIVE_U8_U8_SUMOFMULQUADACCUMULATE
#undef OMNI_NATIVE_U8_U8_SUMOFMULQUADACCUMULATE
#else
#define OMNI_NATIVE_U8_U8_SUMOFMULQUADACCUMULATE
#endif

template <class DU32, OMNI_IF_U32_D(DU32)>
OMNI_API VFromD<DU32> SumOfMulQuadAccumulate(DU32 du32, VFromD<Repartition<uint8_t, DU32>> a,
    VFromD<Repartition<uint8_t, DU32>> b, VFromD<DU32> sum)
{
    const Repartition<uint16_t, decltype(du32)> du16;
    const RebindToSigned<decltype(du16)> di16;
    const RebindToSigned<decltype(du32)> di32;

    const auto lo8_mask = Set(di16, int16_t{ 0x00FF });
    const auto a0 = And(BitCast(di16, a), lo8_mask);
    const auto b0 = And(BitCast(di16, b), lo8_mask);

    const auto a1 = BitCast(di16, ShiftRight<8>(BitCast(du16, a)));
    const auto b1 = BitCast(di16, ShiftRight<8>(BitCast(du16, b)));

    return Add(sum,
        Add(BitCast(du32, WidenMulPairwiseAdd(di32, a0, b0)), BitCast(du32, WidenMulPairwiseAdd(di32, a1, b1))));
}

#endif

#ifndef OMNI_NATIVE_U8_I8_SUMOFMULQUADACCUMULATE) == defined(OMNI_TARGET_TOGGLE))

#ifdef OMNI_NATIVE_U8_I8_SUMOFMULQUADACCUMULATE
#undef OMNI_NATIVE_U8_I8_SUMOFMULQUADACCUMULATE
#else
#define OMNI_NATIVE_U8_I8_SUMOFMULQUADACCUMULATE
#endif

template <class DI32, OMNI_IF_I32_D(DI32)>
OMNI_API VFromD<DI32> SumOfMulQuadAccumulate(DI32 di32, VFromD<Repartition<uint8_t, DI32>> a_u,
    VFromD<Repartition<int8_t, DI32>> b_i, VFromD<DI32> sum)
{
    const Repartition<int16_t, decltype(di32)> di16;
    const RebindToUnsigned<decltype(di16)> du16;

    const auto a0 = And(BitCast(di16, a_u), Set(di16, int16_t{ 0x00FF }));
    const auto b0 = ShiftRight<8>(ShiftLeft<8>(BitCast(di16, b_i)));

    const auto a1 = BitCast(di16, ShiftRight<8>(BitCast(du16, a_u)));
    const auto b1 = ShiftRight<8>(BitCast(di16, b_i));

    // NOTE: SatWidenMulPairwiseAdd(di16, a_u, b_i) cannot be used in
    // SumOfMulQuadAccumulate as it is possible for
    // a_u[0]*b_i[0]+a_u[1]*b_i[1] to overflow an int16_t if a_u[0], b_i[0],
    // a_u[1], and b_i[1] are all non-zero and b_i[0] and b_i[1] have the same
    // sign.

    return Add(sum, Add(WidenMulPairwiseAdd(di32, a0, b0), WidenMulPairwiseAdd(di32, a1, b1)));
}

#endif

#ifndef OMNI_NATIVE_I16_I16_SUMOFMULQUADACCUMULATE
#define OMNI_NATIVE_I16_I16_SUMOFMULQUADACCUMULATE

template <class DI64, OMNI_IF_I64_D(DI64)>
OMNI_API VFromD<DI64> SumOfMulQuadAccumulate(DI64 di64, VFromD<Repartition<int16_t, DI64>> a,
    VFromD<Repartition<int16_t, DI64>> b, VFromD<DI64> sum)
{
    const Repartition<int32_t, decltype(di64)> di32;

    // WidenMulPairwiseAdd(di32, a, b) is okay here as
    // a[0]*b[0]+a[1]*b[1] is between -2147418112 and 2147483648 and as
    // a[0]*b[0]+a[1]*b[1] can only overflow an int32_t if
    // a[0], b[0], a[1], and b[1] are all equal to -32768.

    const auto i32_pairwise_sum = WidenMulPairwiseAdd(di32, a, b);
    const auto i32_pairwise_sum_overflow = VecFromMask(di32, Eq(i32_pairwise_sum, Set(di32, LimitsMin<int32_t>())));

    // The upper 32 bits of sum0 and sum1 need to be zeroed out in the case of
    // overflow.
    const auto hi32_mask = Set(di64, static_cast<int64_t>(~int64_t{ 0xFFFFFFFF }));
    const auto p0_zero_out_mask = ShiftLeft<32>(BitCast(di64, i32_pairwise_sum_overflow));
    const auto p1_zero_out_mask = And(BitCast(di64, i32_pairwise_sum_overflow), hi32_mask);

    const auto p0 = AndNot(p0_zero_out_mask, ShiftRight<32>(ShiftLeft<32>(BitCast(di64, i32_pairwise_sum))));
    const auto p1 = AndNot(p1_zero_out_mask, ShiftRight<32>(BitCast(di64, i32_pairwise_sum)));

    return Add(sum, Add(p0, p1));
}
#endif // OMNI_NATIVE_I16_I16_SUMOFMULQUADACCUMULATE

#ifndef OMNI_NATIVE_U16_U16_SUMOFMULQUADACCUMULATE
#define OMNI_NATIVE_U16_U16_SUMOFMULQUADACCUMULATE

template <class DU64, OMNI_IF_U64_D(DU64)>
OMNI_API VFromD<DU64> SumOfMulQuadAccumulate(DU64 du64, VFromD<Repartition<uint16_t, DU64>> a,
    VFromD<Repartition<uint16_t, DU64>> b, VFromD<DU64> sum)
{
    const auto u32_even_prod = MulEven(a, b);
    const auto u32_odd_prod = MulOdd(a, b);

    const auto p0 = Add(PromoteEvenTo(du64, u32_even_prod), PromoteEvenTo(du64, u32_odd_prod));
    const auto p1 = Add(PromoteOddTo(du64, u32_even_prod), PromoteOddTo(du64, u32_odd_prod));

    return Add(sum, Add(p0, p1));
}
#endif // OMNI_NATIVE_U16_U16_SUMOFMULQUADACCUMULATE

// ------------------------------ Compress*

#ifndef OMNI_NATIVE_COMPRESS8
#define OMNI_NATIVE_COMPRESS8

template <class V, class D, typename T, OMNI_IF_T_SIZE(T, 1)>
OMNI_API size_t CompressBitsStore(V v, const uint8_t *OMNI_RESTRICT bits, D d, T *unaligned)
{
    OMNI_ALIGN T lanes[MaxLanes(d)];
    Store(v, d, lanes);

    const Simd<T, OMNI_MIN(MaxLanes(d), 8), 0> d8;
    T *OMNI_RESTRICT pos = unaligned;

    OMNI_ALIGN constexpr T table[2048] = { 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7,
            1, 0, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7,
            2, 0, 1, 3, 4, 5, 6, 7, 0, 2, 1, 3, 4, 5, 6, 7,
            1, 2, 0, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7,
            3, 0, 1, 2, 4, 5, 6, 7, 0, 3, 1, 2, 4, 5, 6, 7,
            1, 3, 0, 2, 4, 5, 6, 7, 0, 1, 3, 2, 4, 5, 6, 7,
            2, 3, 0, 1, 4, 5, 6, 7, 0, 2, 3, 1, 4, 5, 6, 7,
            1, 2, 3, 0, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7,
            4, 0, 1, 2, 3, 5, 6, 7, 0, 4, 1, 2, 3, 5, 6, 7,
            1, 4, 0, 2, 3, 5, 6, 7, 0, 1, 4, 2, 3, 5, 6, 7,
            2, 4, 0, 1, 3, 5, 6, 7, 0, 2, 4, 1, 3, 5, 6, 7,
            1, 2, 4, 0, 3, 5, 6, 7, 0, 1, 2, 4, 3, 5, 6, 7,
            3, 4, 0, 1, 2, 5, 6, 7, 0, 3, 4, 1, 2, 5, 6, 7,
            1, 3, 4, 0, 2, 5, 6, 7, 0, 1, 3, 4, 2, 5, 6, 7,
            2, 3, 4, 0, 1, 5, 6, 7, 0, 2, 3, 4, 1, 5, 6, 7,
            1, 2, 3, 4, 0, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7,
            5, 0, 1, 2, 3, 4, 6, 7, 0, 5, 1, 2, 3, 4, 6, 7,
            1, 5, 0, 2, 3, 4, 6, 7, 0, 1, 5, 2, 3, 4, 6, 7,
            2, 5, 0, 1, 3, 4, 6, 7, 0, 2, 5, 1, 3, 4, 6, 7,
            1, 2, 5, 0, 3, 4, 6, 7, 0, 1, 2, 5, 3, 4, 6, 7,
            3, 5, 0, 1, 2, 4, 6, 7, 0, 3, 5, 1, 2, 4, 6, 7,
            1, 3, 5, 0, 2, 4, 6, 7, 0, 1, 3, 5, 2, 4, 6, 7,
            2, 3, 5, 0, 1, 4, 6, 7, 0, 2, 3, 5, 1, 4, 6, 7,
            1, 2, 3, 5, 0, 4, 6, 7, 0, 1, 2, 3, 5, 4, 6, 7,
            4, 5, 0, 1, 2, 3, 6, 7, 0, 4, 5, 1, 2, 3, 6, 7,
            1, 4, 5, 0, 2, 3, 6, 7, 0, 1, 4, 5, 2, 3, 6, 7,
            2, 4, 5, 0, 1, 3, 6, 7, 0, 2, 4, 5, 1, 3, 6, 7,
            1, 2, 4, 5, 0, 3, 6, 7, 0, 1, 2, 4, 5, 3, 6, 7,
            3, 4, 5, 0, 1, 2, 6, 7, 0, 3, 4, 5, 1, 2, 6, 7,
            1, 3, 4, 5, 0, 2, 6, 7, 0, 1, 3, 4, 5, 2, 6, 7,
            2, 3, 4, 5, 0, 1, 6, 7, 0, 2, 3, 4, 5, 1, 6, 7,
            1, 2, 3, 4, 5, 0, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7,
            6, 0, 1, 2, 3, 4, 5, 7, 0, 6, 1, 2, 3, 4, 5, 7,
            1, 6, 0, 2, 3, 4, 5, 7, 0, 1, 6, 2, 3, 4, 5, 7,
            2, 6, 0, 1, 3, 4, 5, 7, 0, 2, 6, 1, 3, 4, 5, 7,
            1, 2, 6, 0, 3, 4, 5, 7, 0, 1, 2, 6, 3, 4, 5, 7,
            3, 6, 0, 1, 2, 4, 5, 7, 0, 3, 6, 1, 2, 4, 5, 7,
            1, 3, 6, 0, 2, 4, 5, 7, 0, 1, 3, 6, 2, 4, 5, 7,
            2, 3, 6, 0, 1, 4, 5, 7, 0, 2, 3, 6, 1, 4, 5, 7,
            1, 2, 3, 6, 0, 4, 5, 7, 0, 1, 2, 3, 6, 4, 5, 7,
            4, 6, 0, 1, 2, 3, 5, 7, 0, 4, 6, 1, 2, 3, 5, 7,
            1, 4, 6, 0, 2, 3, 5, 7, 0, 1, 4, 6, 2, 3, 5, 7,
            2, 4, 6, 0, 1, 3, 5, 7, 0, 2, 4, 6, 1, 3, 5, 7,
            1, 2, 4, 6, 0, 3, 5, 7, 0, 1, 2, 4, 6, 3, 5, 7,
            3, 4, 6, 0, 1, 2, 5, 7, 0, 3, 4, 6, 1, 2, 5, 7,
            1, 3, 4, 6, 0, 2, 5, 7, 0, 1, 3, 4, 6, 2, 5, 7,
            2, 3, 4, 6, 0, 1, 5, 7, 0, 2, 3, 4, 6, 1, 5, 7,
            1, 2, 3, 4, 6, 0, 5, 7, 0, 1, 2, 3, 4, 6, 5, 7,
            5, 6, 0, 1, 2, 3, 4, 7, 0, 5, 6, 1, 2, 3, 4, 7,
            1, 5, 6, 0, 2, 3, 4, 7, 0, 1, 5, 6, 2, 3, 4, 7,
            2, 5, 6, 0, 1, 3, 4, 7, 0, 2, 5, 6, 1, 3, 4, 7,
            1, 2, 5, 6, 0, 3, 4, 7, 0, 1, 2, 5, 6, 3, 4, 7,
            3, 5, 6, 0, 1, 2, 4, 7, 0, 3, 5, 6, 1, 2, 4, 7,
            1, 3, 5, 6, 0, 2, 4, 7, 0, 1, 3, 5, 6, 2, 4, 7,
            2, 3, 5, 6, 0, 1, 4, 7, 0, 2, 3, 5, 6, 1, 4, 7,
            1, 2, 3, 5, 6, 0, 4, 7, 0, 1, 2, 3, 5, 6, 4, 7,
            4, 5, 6, 0, 1, 2, 3, 7, 0, 4, 5, 6, 1, 2, 3, 7,
            1, 4, 5, 6, 0, 2, 3, 7, 0, 1, 4, 5, 6, 2, 3, 7,
            2, 4, 5, 6, 0, 1, 3, 7, 0, 2, 4, 5, 6, 1, 3, 7,
            1, 2, 4, 5, 6, 0, 3, 7, 0, 1, 2, 4, 5, 6, 3, 7,
            3, 4, 5, 6, 0, 1, 2, 7, 0, 3, 4, 5, 6, 1, 2, 7,
            1, 3, 4, 5, 6, 0, 2, 7, 0, 1, 3, 4, 5, 6, 2, 7,
            2, 3, 4, 5, 6, 0, 1, 7, 0, 2, 3, 4, 5, 6, 1, 7,
            1, 2, 3, 4, 5, 6, 0, 7, 0, 1, 2, 3, 4, 5, 6, 7,
            7, 0, 1, 2, 3, 4, 5, 6, 0, 7, 1, 2, 3, 4, 5, 6,
            1, 7, 0, 2, 3, 4, 5, 6, 0, 1, 7, 2, 3, 4, 5, 6,
            2, 7, 0, 1, 3, 4, 5, 6, 0, 2, 7, 1, 3, 4, 5, 6,
            1, 2, 7, 0, 3, 4, 5, 6, 0, 1, 2, 7, 3, 4, 5, 6,
            3, 7, 0, 1, 2, 4, 5, 6, 0, 3, 7, 1, 2, 4, 5, 6,
            1, 3, 7, 0, 2, 4, 5, 6, 0, 1, 3, 7, 2, 4, 5, 6,
            2, 3, 7, 0, 1, 4, 5, 6, 0, 2, 3, 7, 1, 4, 5, 6,
            1, 2, 3, 7, 0, 4, 5, 6, 0, 1, 2, 3, 7, 4, 5, 6,
            4, 7, 0, 1, 2, 3, 5, 6, 0, 4, 7, 1, 2, 3, 5, 6,
            1, 4, 7, 0, 2, 3, 5, 6, 0, 1, 4, 7, 2, 3, 5, 6,
            2, 4, 7, 0, 1, 3, 5, 6, 0, 2, 4, 7, 1, 3, 5, 6,
            1, 2, 4, 7, 0, 3, 5, 6, 0, 1, 2, 4, 7, 3, 5, 6,
            3, 4, 7, 0, 1, 2, 5, 6, 0, 3, 4, 7, 1, 2, 5, 6,
            1, 3, 4, 7, 0, 2, 5, 6, 0, 1, 3, 4, 7, 2, 5, 6,
            2, 3, 4, 7, 0, 1, 5, 6, 0, 2, 3, 4, 7, 1, 5, 6,
            1, 2, 3, 4, 7, 0, 5, 6, 0, 1, 2, 3, 4, 7, 5, 6,
            5, 7, 0, 1, 2, 3, 4, 6, 0, 5, 7, 1, 2, 3, 4, 6,
            1, 5, 7, 0, 2, 3, 4, 6, 0, 1, 5, 7, 2, 3, 4, 6,
            2, 5, 7, 0, 1, 3, 4, 6, 0, 2, 5, 7, 1, 3, 4, 6,
            1, 2, 5, 7, 0, 3, 4, 6, 0, 1, 2, 5, 7, 3, 4, 6,
            3, 5, 7, 0, 1, 2, 4, 6, 0, 3, 5, 7, 1, 2, 4, 6,
            1, 3, 5, 7, 0, 2, 4, 6, 0, 1, 3, 5, 7, 2, 4, 6,
            2, 3, 5, 7, 0, 1, 4, 6, 0, 2, 3, 5, 7, 1, 4, 6,
            1, 2, 3, 5, 7, 0, 4, 6, 0, 1, 2, 3, 5, 7, 4, 6,
            4, 5, 7, 0, 1, 2, 3, 6, 0, 4, 5, 7, 1, 2, 3, 6,
            1, 4, 5, 7, 0, 2, 3, 6, 0, 1, 4, 5, 7, 2, 3, 6,
            2, 4, 5, 7, 0, 1, 3, 6, 0, 2, 4, 5, 7, 1, 3, 6,
            1, 2, 4, 5, 7, 0, 3, 6, 0, 1, 2, 4, 5, 7, 3, 6,
            3, 4, 5, 7, 0, 1, 2, 6, 0, 3, 4, 5, 7, 1, 2, 6,
            1, 3, 4, 5, 7, 0, 2, 6, 0, 1, 3, 4, 5, 7, 2, 6,
            2, 3, 4, 5, 7, 0, 1, 6, 0, 2, 3, 4, 5, 7, 1, 6,
            1, 2, 3, 4, 5, 7, 0, 6, 0, 1, 2, 3, 4, 5, 7, 6,
            6, 7, 0, 1, 2, 3, 4, 5, 0, 6, 7, 1, 2, 3, 4, 5,
            1, 6, 7, 0, 2, 3, 4, 5, 0, 1, 6, 7, 2, 3, 4, 5,
            2, 6, 7, 0, 1, 3, 4, 5, 0, 2, 6, 7, 1, 3, 4, 5,
            1, 2, 6, 7, 0, 3, 4, 5, 0, 1, 2, 6, 7, 3, 4, 5,
            3, 6, 7, 0, 1, 2, 4, 5, 0, 3, 6, 7, 1, 2, 4, 5,
            1, 3, 6, 7, 0, 2, 4, 5, 0, 1, 3, 6, 7, 2, 4, 5,
            2, 3, 6, 7, 0, 1, 4, 5, 0, 2, 3, 6, 7, 1, 4, 5,
            1, 2, 3, 6, 7, 0, 4, 5, 0, 1, 2, 3, 6, 7, 4, 5,
            4, 6, 7, 0, 1, 2, 3, 5, 0, 4, 6, 7, 1, 2, 3, 5,
            1, 4, 6, 7, 0, 2, 3, 5, 0, 1, 4, 6, 7, 2, 3, 5,
            2, 4, 6, 7, 0, 1, 3, 5, 0, 2, 4, 6, 7, 1, 3, 5,
            1, 2, 4, 6, 7, 0, 3, 5, 0, 1, 2, 4, 6, 7, 3, 5,
            3, 4, 6, 7, 0, 1, 2, 5, 0, 3, 4, 6, 7, 1, 2, 5,
            1, 3, 4, 6, 7, 0, 2, 5, 0, 1, 3, 4, 6, 7, 2, 5,
            2, 3, 4, 6, 7, 0, 1, 5, 0, 2, 3, 4, 6, 7, 1, 5,
            1, 2, 3, 4, 6, 7, 0, 5, 0, 1, 2, 3, 4, 6, 7, 5,
            5, 6, 7, 0, 1, 2, 3, 4, 0, 5, 6, 7, 1, 2, 3, 4,
            1, 5, 6, 7, 0, 2, 3, 4, 0, 1, 5, 6, 7, 2, 3, 4,
            2, 5, 6, 7, 0, 1, 3, 4, 0, 2, 5, 6, 7, 1, 3, 4,
            1, 2, 5, 6, 7, 0, 3, 4, 0, 1, 2, 5, 6, 7, 3, 4,
            3, 5, 6, 7, 0, 1, 2, 4, 0, 3, 5, 6, 7, 1, 2, 4,
            1, 3, 5, 6, 7, 0, 2, 4, 0, 1, 3, 5, 6, 7, 2, 4,
            2, 3, 5, 6, 7, 0, 1, 4, 0, 2, 3, 5, 6, 7, 1, 4,
            1, 2, 3, 5, 6, 7, 0, 4, 0, 1, 2, 3, 5, 6, 7, 4,
            4, 5, 6, 7, 0, 1, 2, 3, 0, 4, 5, 6, 7, 1, 2, 3,
            1, 4, 5, 6, 7, 0, 2, 3, 0, 1, 4, 5, 6, 7, 2, 3,
            2, 4, 5, 6, 7, 0, 1, 3, 0, 2, 4, 5, 6, 7, 1, 3,
            1, 2, 4, 5, 6, 7, 0, 3, 0, 1, 2, 4, 5, 6, 7, 3,
            3, 4, 5, 6, 7, 0, 1, 2, 0, 3, 4, 5, 6, 7, 1, 2,
            1, 3, 4, 5, 6, 7, 0, 2, 0, 1, 3, 4, 5, 6, 7, 2,
            2, 3, 4, 5, 6, 7, 0, 1, 0, 2, 3, 4, 5, 6, 7, 1,
            1, 2, 3, 4, 5, 6, 7, 0, 0, 1, 2, 3, 4, 5, 6, 7 };

    for (size_t i = 0; i < Lanes(d); i += 8) {
        // Each byte worth of bits is the index of one of 256 8-byte ranges, and its
        // population count determines how far to advance the write position.
        const size_t bits8 = bits[i / 8];
        const auto indices = Load(d8, table + bits8 * 8);
        const auto compressed = TableLookupBytes(LoadU(d8, lanes + i), indices);
        StoreU(compressed, d8, pos);
        pos += PopCount(bits8);
    }
    return static_cast<size_t>(pos - unaligned);
}

template <class V, class M, class D, typename T, OMNI_IF_T_SIZE(T, 1)>
OMNI_API size_t CompressStore(V v, M mask, D d, T *OMNI_RESTRICT unaligned)
{
    uint8_t bits[OMNI_MAX(size_t{ 8 }, MaxLanes(d) / 8)];
    (void)StoreMaskBits(d, mask, bits);
    return CompressBitsStore(v, bits, d, unaligned);
}

template <class V, class M, class D, typename T, OMNI_IF_T_SIZE(T, 1)>
OMNI_API size_t CompressBlendedStore(V v, M mask, D d, T *OMNI_RESTRICT unaligned)
{
    OMNI_ALIGN T buf[MaxLanes(d)];
    const size_t bytes = CompressStore(v, mask, d, buf);
    BlendedStore(Load(d, buf), FirstN(d, bytes), d, unaligned);
    return bytes;
}

// For reasons unknown, OMNI_IF_T_SIZE_V is a compile error in SVE.
template <class V, class M, typename T = TFromV<V>, OMNI_IF_T_SIZE(T, 1)> OMNI_API V Compress(V v, const M mask)
{
    const DFromV<V> d;
    OMNI_ALIGN T lanes[MaxLanes(d)];
    (void)CompressStore(v, mask, d, lanes);
    return Load(d, lanes);
}

template <class V, typename T = TFromV<V>, OMNI_IF_T_SIZE(T, 1)>
OMNI_API V CompressBits(V v, const uint8_t *OMNI_RESTRICT bits)
{
    const DFromV<V> d;
    OMNI_ALIGN T lanes[MaxLanes(d)];
    (void)CompressBitsStore(v, bits, d, lanes);
    return Load(d, lanes);
}

template <class V, class M, typename T = TFromV<V>, OMNI_IF_T_SIZE(T, 1)> OMNI_API V CompressNot(V v, M mask)
{
    return Compress(v, Not(mask));
}

#endif // OMNI_NATIVE_COMPRESS8

// ------------------------------ Expand

// Note that this generic implementation assumes <= 128 bit fixed vectors;
// the SVE and RVV targets provide their own native implementations.
#ifndef OMNI_NATIVE_EXPAND
#define OMNI_NATIVE_EXPAND

namespace detail {
template <size_t N> OMNI_INLINE Vec128<uint8_t, N> IndicesForExpandFromBits(uint64_t mask_bits)
{
    static_assert(N <= 8, "Should only be called for half-vectors");
    const Simd<uint8_t, N, 0> du8;
    alignas(16) static constexpr uint8_t table[2048] = {                                         // PrintExpand8x8Tables
        128, 128, 128, 128, 128, 128, 128, 128,
        0,   128, 128, 128, 128, 128, 128, 128,
        128, 0,   128, 128, 128, 128, 128, 128,
        0,   1,   128, 128, 128, 128, 128, 128,
        128, 128, 0,   128, 128, 128, 128, 128,
        0,   128, 1,   128, 128, 128, 128, 128,
        128, 0,   1,   128, 128, 128, 128, 128,
        0,   1,   2,   128, 128, 128, 128, 128,
        128, 128, 128, 0,   128, 128, 128, 128,
        0,   128, 128, 1,   128, 128, 128, 128,
        128, 0,   128, 1,   128, 128, 128, 128,
        0,   1,   128, 2,   128, 128, 128, 128,
        128, 128, 0,   1,   128, 128, 128, 128,
        0,   128, 1,   2,   128, 128, 128, 128,
        128, 0,   1,   2,   128, 128, 128, 128,
        0,   1,   2,   3,   128, 128, 128, 128,
        128, 128, 128, 128, 0,   128, 128, 128,
        0,   128, 128, 128, 1,   128, 128, 128,
        128, 0,   128, 128, 1,   128, 128, 128,
        0,   1,   128, 128, 2,   128, 128, 128,
        128, 128, 0,   128, 1,   128, 128, 128,
        0,   128, 1,   128, 2,   128, 128, 128,
        128, 0,   1,   128, 2,   128, 128, 128,
        0,   1,   2,   128, 3,   128, 128, 128,
        128, 128, 128, 0,   1,   128, 128, 128,
        0,   128, 128, 1,   2,   128, 128, 128,
        128, 0,   128, 1,   2,   128, 128, 128,
        0,   1,   128, 2,   3,   128, 128, 128,
        128, 128, 0,   1,   2,   128, 128, 128,
        0,   128, 1,   2,   3,   128, 128, 128,
        128, 0,   1,   2,   3,   128, 128, 128,
        0,   1,   2,   3,   4,   128, 128, 128,
        128, 128, 128, 128, 128, 0,   128, 128,
        0,   128, 128, 128, 128, 1,   128, 128,
        128, 0,   128, 128, 128, 1,   128, 128,
        0,   1,   128, 128, 128, 2,   128, 128,
        128, 128, 0,   128, 128, 1,   128, 128,
        0,   128, 1,   128, 128, 2,   128, 128,
        128, 0,   1,   128, 128, 2,   128, 128,
        0,   1,   2,   128, 128, 3,   128, 128,
        128, 128, 128, 0,   128, 1,   128, 128,
        0,   128, 128, 1,   128, 2,   128, 128,
        128, 0,   128, 1,   128, 2,   128, 128,
        0,   1,   128, 2,   128, 3,   128, 128,
        128, 128, 0,   1,   128, 2,   128, 128,
        0,   128, 1,   2,   128, 3,   128, 128,
        128, 0,   1,   2,   128, 3,   128, 128,
        0,   1,   2,   3,   128, 4,   128, 128,
        128, 128, 128, 128, 0,   1,   128, 128,
        0,   128, 128, 128, 1,   2,   128, 128,
        128, 0,   128, 128, 1,   2,   128, 128,
        0,   1,   128, 128, 2,   3,   128, 128,
        128, 128, 0,   128, 1,   2,   128, 128,
        0,   128, 1,   128, 2,   3,   128, 128,
        128, 0,   1,   128, 2,   3,   128, 128,
        0,   1,   2,   128, 3,   4,   128, 128,
        128, 128, 128, 0,   1,   2,   128, 128,
        0,   128, 128, 1,   2,   3,   128, 128,
        128, 0,   128, 1,   2,   3,   128, 128,
        0,   1,   128, 2,   3,   4,   128, 128,
        128, 128, 0,   1,   2,   3,   128, 128,
        0,   128, 1,   2,   3,   4,   128, 128,
        128, 0,   1,   2,   3,   4,   128, 128,
        0,   1,   2,   3,   4,   5,   128, 128,
        128, 128, 128, 128, 128, 128, 0,   128,
        0,   128, 128, 128, 128, 128, 1,   128,
        128, 0,   128, 128, 128, 128, 1,   128,
        0,   1,   128, 128, 128, 128, 2,   128,
        128, 128, 0,   128, 128, 128, 1,   128,
        0,   128, 1,   128, 128, 128, 2,   128,
        128, 0,   1,   128, 128, 128, 2,   128,
        0,   1,   2,   128, 128, 128, 3,   128,
        128, 128, 128, 0,   128, 128, 1,   128,
        0,   128, 128, 1,   128, 128, 2,   128,
        128, 0,   128, 1,   128, 128, 2,   128,
        0,   1,   128, 2,   128, 128, 3,   128,
        128, 128, 0,   1,   128, 128, 2,   128,
        0,   128, 1,   2,   128, 128, 3,   128,
        128, 0,   1,   2,   128, 128, 3,   128,
        0,   1,   2,   3,   128, 128, 4,   128,
        128, 128, 128, 128, 0,   128, 1,   128,
        0,   128, 128, 128, 1,   128, 2,   128,
        128, 0,   128, 128, 1,   128, 2,   128,
        0,   1,   128, 128, 2,   128, 3,   128,
        128, 128, 0,   128, 1,   128, 2,   128,
        0,   128, 1,   128, 2,   128, 3,   128,
        128, 0,   1,   128, 2,   128, 3,   128,
        0,   1,   2,   128, 3,   128, 4,   128,
        128, 128, 128, 0,   1,   128, 2,   128,
        0,   128, 128, 1,   2,   128, 3,   128,
        128, 0,   128, 1,   2,   128, 3,   128,
        0,   1,   128, 2,   3,   128, 4,   128,
        128, 128, 0,   1,   2,   128, 3,   128,
        0,   128, 1,   2,   3,   128, 4,   128,
        128, 0,   1,   2,   3,   128, 4,   128,
        0,   1,   2,   3,   4,   128, 5,   128,
        128, 128, 128, 128, 128, 0,   1,   128,
        0,   128, 128, 128, 128, 1,   2,   128,
        128, 0,   128, 128, 128, 1,   2,   128,
        0,   1,   128, 128, 128, 2,   3,   128,
        128, 128, 0,   128, 128, 1,   2,   128,
        0,   128, 1,   128, 128, 2,   3,   128,
        128, 0,   1,   128, 128, 2,   3,   128,
        0,   1,   2,   128, 128, 3,   4,   128,
        128, 128, 128, 0,   128, 1,   2,   128,
        0,   128, 128, 1,   128, 2,   3,   128,
        128, 0,   128, 1,   128, 2,   3,   128,
        0,   1,   128, 2,   128, 3,   4,   128,
        128, 128, 0,   1,   128, 2,   3,   128,
        0,   128, 1,   2,   128, 3,   4,   128,
        128, 0,   1,   2,   128, 3,   4,   128,
        0,   1,   2,   3,   128, 4,   5,   128,
        128, 128, 128, 128, 0,   1,   2,   128,
        0,   128, 128, 128, 1,   2,   3,   128,
        128, 0,   128, 128, 1,   2,   3,   128,
        0,   1,   128, 128, 2,   3,   4,   128,
        128, 128, 0,   128, 1,   2,   3,   128,
        0,   128, 1,   128, 2,   3,   4,   128,
        128, 0,   1,   128, 2,   3,   4,   128,
        0,   1,   2,   128, 3,   4,   5,   128,
        128, 128, 128, 0,   1,   2,   3,   128,
        0,   128, 128, 1,   2,   3,   4,   128,
        128, 0,   128, 1,   2,   3,   4,   128,
        0,   1,   128, 2,   3,   4,   5,   128,
        128, 128, 0,   1,   2,   3,   4,   128,
        0,   128, 1,   2,   3,   4,   5,   128,
        128, 0,   1,   2,   3,   4,   5,   128,
        0,   1,   2,   3,   4,   5,   6,   128,
        128, 128, 128, 128, 128, 128, 128, 0,
        0,   128, 128, 128, 128, 128, 128, 1,
        128, 0,   128, 128, 128, 128, 128, 1,
        0,   1,   128, 128, 128, 128, 128, 2,
        128, 128, 0,   128, 128, 128, 128, 1,
        0,   128, 1,   128, 128, 128, 128, 2,
        128, 0,   1,   128, 128, 128, 128, 2,
        0,   1,   2,   128, 128, 128, 128, 3,
        128, 128, 128, 0,   128, 128, 128, 1,
        0,   128, 128, 1,   128, 128, 128, 2,
        128, 0,   128, 1,   128, 128, 128, 2,
        0,   1,   128, 2,   128, 128, 128, 3,
        128, 128, 0,   1,   128, 128, 128, 2,
        0,   128, 1,   2,   128, 128, 128, 3,
        128, 0,   1,   2,   128, 128, 128, 3,
        0,   1,   2,   3,   128, 128, 128, 4,
        128, 128, 128, 128, 0,   128, 128, 1,
        0,   128, 128, 128, 1,   128, 128, 2,
        128, 0,   128, 128, 1,   128, 128, 2,
        0,   1,   128, 128, 2,   128, 128, 3,
        128, 128, 0,   128, 1,   128, 128, 2,
        0,   128, 1,   128, 2,   128, 128, 3,
        128, 0,   1,   128, 2,   128, 128, 3,
        0,   1,   2,   128, 3,   128, 128, 4,
        128, 128, 128, 0,   1,   128, 128, 2,
        0,   128, 128, 1,   2,   128, 128, 3,
        128, 0,   128, 1,   2,   128, 128, 3,
        0,   1,   128, 2,   3,   128, 128, 4,
        128, 128, 0,   1,   2,   128, 128, 3,
        0,   128, 1,   2,   3,   128, 128, 4,
        128, 0,   1,   2,   3,   128, 128, 4,
        0,   1,   2,   3,   4,   128, 128, 5,
        128, 128, 128, 128, 128, 0,   128, 1,
        0,   128, 128, 128, 128, 1,   128, 2,
        128, 0,   128, 128, 128, 1,   128, 2,
        0,   1,   128, 128, 128, 2,   128, 3,
        128, 128, 0,   128, 128, 1,   128, 2,
        0,   128, 1,   128, 128, 2,   128, 3,
        128, 0,   1,   128, 128, 2,   128, 3,
        0,   1,   2,   128, 128, 3,   128, 4,
        128, 128, 128, 0,   128, 1,   128, 2,
        0,   128, 128, 1,   128, 2,   128, 3,
        128, 0,   128, 1,   128, 2,   128, 3,
        0,   1,   128, 2,   128, 3,   128, 4,
        128, 128, 0,   1,   128, 2,   128, 3,
        0,   128, 1,   2,   128, 3,   128, 4,
        128, 0,   1,   2,   128, 3,   128, 4,
        0,   1,   2,   3,   128, 4,   128, 5,
        128, 128, 128, 128, 0,   1,   128, 2,
        0,   128, 128, 128, 1,   2,   128, 3,
        128, 0,   128, 128, 1,   2,   128, 3,
        0,   1,   128, 128, 2,   3,   128, 4,
        128, 128, 0,   128, 1,   2,   128, 3,
        0,   128, 1,   128, 2,   3,   128, 4,
        128, 0,   1,   128, 2,   3,   128, 4,
        0,   1,   2,   128, 3,   4,   128, 5,
        128, 128, 128, 0,   1,   2,   128, 3,
        0,   128, 128, 1,   2,   3,   128, 4,
        128, 0,   128, 1,   2,   3,   128, 4,
        0,   1,   128, 2,   3,   4,   128, 5,
        128, 128, 0,   1,   2,   3,   128, 4,
        0,   128, 1,   2,   3,   4,   128, 5,
        128, 0,   1,   2,   3,   4,   128, 5,
        0,   1,   2,   3,   4,   5,   128, 6,
        128, 128, 128, 128, 128, 128, 0,   1,
        0,   128, 128, 128, 128, 128, 1,   2,
        128, 0,   128, 128, 128, 128, 1,   2,
        0,   1,   128, 128, 128, 128, 2,   3,
        128, 128, 0,   128, 128, 128, 1,   2,
        0,   128, 1,   128, 128, 128, 2,   3,
        128, 0,   1,   128, 128, 128, 2,   3,
        0,   1,   2,   128, 128, 128, 3,   4,
        128, 128, 128, 0,   128, 128, 1,   2,
        0,   128, 128, 1,   128, 128, 2,   3,
        128, 0,   128, 1,   128, 128, 2,   3,
        0,   1,   128, 2,   128, 128, 3,   4,
        128, 128, 0,   1,   128, 128, 2,   3,
        0,   128, 1,   2,   128, 128, 3,   4,
        128, 0,   1,   2,   128, 128, 3,   4,
        0,   1,   2,   3,   128, 128, 4,   5,
        128, 128, 128, 128, 0,   128, 1,   2,
        0,   128, 128, 128, 1,   128, 2,   3,
        128, 0,   128, 128, 1,   128, 2,   3,
        0,   1,   128, 128, 2,   128, 3,   4,
        128, 128, 0,   128, 1,   128, 2,   3,
        0,   128, 1,   128, 2,   128, 3,   4,
        128, 0,   1,   128, 2,   128, 3,   4,
        0,   1,   2,   128, 3,   128, 4,   5,
        128, 128, 128, 0,   1,   128, 2,   3,
        0,   128, 128, 1,   2,   128, 3,   4,
        128, 0,   128, 1,   2,   128, 3,   4,
        0,   1,   128, 2,   3,   128, 4,   5,
        128, 128, 0,   1,   2,   128, 3,   4,
        0,   128, 1,   2,   3,   128, 4,   5,
        128, 0,   1,   2,   3,   128, 4,   5,
        0,   1,   2,   3,   4,   128, 5,   6,
        128, 128, 128, 128, 128, 0,   1,   2,
        0,   128, 128, 128, 128, 1,   2,   3,
        128, 0,   128, 128, 128, 1,   2,   3,
        0,   1,   128, 128, 128, 2,   3,   4,
        128, 128, 0,   128, 128, 1,   2,   3,
        0,   128, 1,   128, 128, 2,   3,   4,
        128, 0,   1,   128, 128, 2,   3,   4,
        0,   1,   2,   128, 128, 3,   4,   5,
        128, 128, 128, 0,   128, 1,   2,   3,
        0,   128, 128, 1,   128, 2,   3,   4,
        128, 0,   128, 1,   128, 2,   3,   4,
        0,   1,   128, 2,   128, 3,   4,   5,
        128, 128, 0,   1,   128, 2,   3,   4,
        0,   128, 1,   2,   128, 3,   4,   5,
        128, 0,   1,   2,   128, 3,   4,   5,
        0,   1,   2,   3,   128, 4,   5,   6,
        128, 128, 128, 128, 0,   1,   2,   3,
        0,   128, 128, 128, 1,   2,   3,   4,
        128, 0,   128, 128, 1,   2,   3,   4,
        0,   1,   128, 128, 2,   3,   4,   5,
        128, 128, 0,   128, 1,   2,   3,   4,
        0,   128, 1,   128, 2,   3,   4,   5,
        128, 0,   1,   128, 2,   3,   4,   5,
        0,   1,   2,   128, 3,   4,   5,   6,
        128, 128, 128, 0,   1,   2,   3,   4,
        0,   128, 128, 1,   2,   3,   4,   5,
        128, 0,   128, 1,   2,   3,   4,   5,
        0,   1,   128, 2,   3,   4,   5,   6,
        128, 128, 0,   1,   2,   3,   4,   5,
        0,   128, 1,   2,   3,   4,   5,   6,
        128, 0,   1,   2,   3,   4,   5,   6,
        0,   1,   2,   3,   4,   5,   6,   7};
    return LoadU(du8, table + mask_bits * 8);
}
} // namespace detail

// Half vector of bytes: one table lookup
template <typename T, size_t N, OMNI_IF_T_SIZE(T, 1), OMNI_IF_V_SIZE_LE(T, N, 8)>
OMNI_API Vec128<T, N> Expand(Vec128<T, N> v, Mask128<T, N> mask)
{
    const DFromV<decltype(v)> d;

    const uint64_t mask_bits = detail::BitsFromMask(mask);
    const Vec128<uint8_t, N> indices = detail::IndicesForExpandFromBits<N>(mask_bits);
    return BitCast(d, TableLookupBytesOr0(v, indices));
}

// Full vector of bytes: two table lookups
template <typename T, OMNI_IF_T_SIZE(T, 1)> OMNI_API Vec128<T> Expand(Vec128<T> v, Mask128<T> mask)
{
    const Full128<T> d;
    const RebindToUnsigned<decltype(d)> du;
    const Half<decltype(du)> duh;
    const Vec128<uint8_t> vu = BitCast(du, v);

    const uint64_t mask_bits = detail::BitsFromMask(mask);
    const uint64_t maskL = mask_bits & 0xFF;
    const uint64_t maskH = mask_bits >> 8;

    // We want to skip past the v bytes already consumed by idxL. There is no
    // instruction for shift-reg by variable bytes. Storing v itself would work
    // but would involve a store-load forwarding stall. We instead shuffle using
    // loaded indices. multishift_epi64_epi8 would also help, but if we have that,
    // we probably also have native 8-bit Expand.
    alignas(16) static constexpr uint8_t iota[32] = {
        0,   1,   2,   3,   4,   5,   6,   7,   8,   9,   10,
        11,  12,  13,  14,  15,  128, 128, 128, 128, 128, 128,
        128, 128, 128, 128, 128, 128, 128, 128, 128, 128};
    const VFromD<decltype(du)> shift = LoadU(du, iota + PopCount(maskL));
    const VFromD<decltype(duh)> vL = LowerHalf(duh, vu);
    const VFromD<decltype(duh)> vH = LowerHalf(duh, TableLookupBytesOr0(vu, shift));

    const VFromD<decltype(duh)> idxL = detail::IndicesForExpandFromBits<8>(maskL);
    const VFromD<decltype(duh)> idxH = detail::IndicesForExpandFromBits<8>(maskH);

    const VFromD<decltype(duh)> expandL = TableLookupBytesOr0(vL, idxL);
    const VFromD<decltype(duh)> expandH = TableLookupBytesOr0(vH, idxH);
    return BitCast(d, Combine(du, expandH, expandL));
}

template <typename T, size_t N, OMNI_IF_T_SIZE(T, 2)> OMNI_API Vec128<T, N> Expand(Vec128<T, N> v, Mask128<T, N> mask)
{
    const DFromV<decltype(v)> d;
    const RebindToUnsigned<decltype(d)> du;

    const Rebind<uint8_t, decltype(d)> du8;
    const uint64_t mask_bits = detail::BitsFromMask(mask);

    // Storing as 8-bit reduces table size from 4 KiB to 2 KiB. We cannot apply
    // the nibble trick used below because not all indices fit within one lane.
    alignas(16) static constexpr uint8_t table[2048] = { // PrintExpand16x8ByteTables
        128, 128, 128, 128, 128, 128, 128, 128, //
        0,   128, 128, 128, 128, 128, 128, 128, //
        128, 0,   128, 128, 128, 128, 128, 128, //
        0,   2,   128, 128, 128, 128, 128, 128, //
        128, 128, 0,   128, 128, 128, 128, 128, //
        0,   128, 2,   128, 128, 128, 128, 128, //
        128, 0,   2,   128, 128, 128, 128, 128, //
        0,   2,   4,   128, 128, 128, 128, 128, //
        128, 128, 128, 0,   128, 128, 128, 128, //
        0,   128, 128, 2,   128, 128, 128, 128, //
        128, 0,   128, 2,   128, 128, 128, 128, //
        0,   2,   128, 4,   128, 128, 128, 128, //
        128, 128, 0,   2,   128, 128, 128, 128, //
        0,   128, 2,   4,   128, 128, 128, 128, //
        128, 0,   2,   4,   128, 128, 128, 128, //
        0,   2,   4,   6,   128, 128, 128, 128, //
        128, 128, 128, 128, 0,   128, 128, 128, //
        0,   128, 128, 128, 2,   128, 128, 128, //
        128, 0,   128, 128, 2,   128, 128, 128, //
        0,   2,   128, 128, 4,   128, 128, 128, //
        128, 128, 0,   128, 2,   128, 128, 128, //
        0,   128, 2,   128, 4,   128, 128, 128, //
        128, 0,   2,   128, 4,   128, 128, 128, //
        0,   2,   4,   128, 6,   128, 128, 128, //
        128, 128, 128, 0,   2,   128, 128, 128, //
        0,   128, 128, 2,   4,   128, 128, 128, //
        128, 0,   128, 2,   4,   128, 128, 128, //
        0,   2,   128, 4,   6,   128, 128, 128, //
        128, 128, 0,   2,   4,   128, 128, 128, //
        0,   128, 2,   4,   6,   128, 128, 128, //
        128, 0,   2,   4,   6,   128, 128, 128, //
        0,   2,   4,   6,   8,   128, 128, 128, //
        128, 128, 128, 128, 128, 0,   128, 128, //
        0,   128, 128, 128, 128, 2,   128, 128, //
        128, 0,   128, 128, 128, 2,   128, 128, //
        0,   2,   128, 128, 128, 4,   128, 128, //
        128, 128, 0,   128, 128, 2,   128, 128, //
        0,   128, 2,   128, 128, 4,   128, 128, //
        128, 0,   2,   128, 128, 4,   128, 128, //
        0,   2,   4,   128, 128, 6,   128, 128, //
        128, 128, 128, 0,   128, 2,   128, 128, //
        0,   128, 128, 2,   128, 4,   128, 128, //
        128, 0,   128, 2,   128, 4,   128, 128, //
        0,   2,   128, 4,   128, 6,   128, 128, //
        128, 128, 0,   2,   128, 4,   128, 128, //
        0,   128, 2,   4,   128, 6,   128, 128, //
        128, 0,   2,   4,   128, 6,   128, 128, //
        0,   2,   4,   6,   128, 8,   128, 128, //
        128, 128, 128, 128, 0,   2,   128, 128, //
        0,   128, 128, 128, 2,   4,   128, 128, //
        128, 0,   128, 128, 2,   4,   128, 128, //
        0,   2,   128, 128, 4,   6,   128, 128, //
        128, 128, 0,   128, 2,   4,   128, 128, //
        0,   128, 2,   128, 4,   6,   128, 128, //
        128, 0,   2,   128, 4,   6,   128, 128, //
        0,   2,   4,   128, 6,   8,   128, 128, //
        128, 128, 128, 0,   2,   4,   128, 128, //
        0,   128, 128, 2,   4,   6,   128, 128, //
        128, 0,   128, 2,   4,   6,   128, 128, //
        0,   2,   128, 4,   6,   8,   128, 128, //
        128, 128, 0,   2,   4,   6,   128, 128, //
        0,   128, 2,   4,   6,   8,   128, 128, //
        128, 0,   2,   4,   6,   8,   128, 128, //
        0,   2,   4,   6,   8,   10,  128, 128, //
        128, 128, 128, 128, 128, 128, 0,   128, //
        0,   128, 128, 128, 128, 128, 2,   128, //
        128, 0,   128, 128, 128, 128, 2,   128, //
        0,   2,   128, 128, 128, 128, 4,   128, //
        128, 128, 0,   128, 128, 128, 2,   128, //
        0,   128, 2,   128, 128, 128, 4,   128, //
        128, 0,   2,   128, 128, 128, 4,   128, //
        0,   2,   4,   128, 128, 128, 6,   128, //
        128, 128, 128, 0,   128, 128, 2,   128, //
        0,   128, 128, 2,   128, 128, 4,   128, //
        128, 0,   128, 2,   128, 128, 4,   128, //
        0,   2,   128, 4,   128, 128, 6,   128, //
        128, 128, 0,   2,   128, 128, 4,   128, //
        0,   128, 2,   4,   128, 128, 6,   128, //
        128, 0,   2,   4,   128, 128, 6,   128, //
        0,   2,   4,   6,   128, 128, 8,   128, //
        128, 128, 128, 128, 0,   128, 2,   128, //
        0,   128, 128, 128, 2,   128, 4,   128, //
        128, 0,   128, 128, 2,   128, 4,   128, //
        0,   2,   128, 128, 4,   128, 6,   128, //
        128, 128, 0,   128, 2,   128, 4,   128, //
        0,   128, 2,   128, 4,   128, 6,   128, //
        128, 0,   2,   128, 4,   128, 6,   128, //
        0,   2,   4,   128, 6,   128, 8,   128, //
        128, 128, 128, 0,   2,   128, 4,   128, //
        0,   128, 128, 2,   4,   128, 6,   128, //
        128, 0,   128, 2,   4,   128, 6,   128, //
        0,   2,   128, 4,   6,   128, 8,   128, //
        128, 128, 0,   2,   4,   128, 6,   128, //
        0,   128, 2,   4,   6,   128, 8,   128, //
        128, 0,   2,   4,   6,   128, 8,   128, //
        0,   2,   4,   6,   8,   128, 10,  128, //
        128, 128, 128, 128, 128, 0,   2,   128, //
        0,   128, 128, 128, 128, 2,   4,   128, //
        128, 0,   128, 128, 128, 2,   4,   128, //
        0,   2,   128, 128, 128, 4,   6,   128, //
        128, 128, 0,   128, 128, 2,   4,   128, //
        0,   128, 2,   128, 128, 4,   6,   128, //
        128, 0,   2,   128, 128, 4,   6,   128, //
        0,   2,   4,   128, 128, 6,   8,   128, //
        128, 128, 128, 0,   128, 2,   4,   128, //
        0,   128, 128, 2,   128, 4,   6,   128, //
        128, 0,   128, 2,   128, 4,   6,   128, //
        0,   2,   128, 4,   128, 6,   8,   128, //
        128, 128, 0,   2,   128, 4,   6,   128, //
        0,   128, 2,   4,   128, 6,   8,   128, //
        128, 0,   2,   4,   128, 6,   8,   128, //
        0,   2,   4,   6,   128, 8,   10,  128, //
        128, 128, 128, 128, 0,   2,   4,   128, //
        0,   128, 128, 128, 2,   4,   6,   128, //
        128, 0,   128, 128, 2,   4,   6,   128, //
        0,   2,   128, 128, 4,   6,   8,   128, //
        128, 128, 0,   128, 2,   4,   6,   128, //
        0,   128, 2,   128, 4,   6,   8,   128, //
        128, 0,   2,   128, 4,   6,   8,   128, //
        0,   2,   4,   128, 6,   8,   10,  128, //
        128, 128, 128, 0,   2,   4,   6,   128, //
        0,   128, 128, 2,   4,   6,   8,   128, //
        128, 0,   128, 2,   4,   6,   8,   128, //
        0,   2,   128, 4,   6,   8,   10,  128, //
        128, 128, 0,   2,   4,   6,   8,   128, //
        0,   128, 2,   4,   6,   8,   10,  128, //
        128, 0,   2,   4,   6,   8,   10,  128, //
        0,   2,   4,   6,   8,   10,  12,  128, //
        128, 128, 128, 128, 128, 128, 128, 0,   //
        0,   128, 128, 128, 128, 128, 128, 2,   //
        128, 0,   128, 128, 128, 128, 128, 2,   //
        0,   2,   128, 128, 128, 128, 128, 4,   //
        128, 128, 0,   128, 128, 128, 128, 2,   //
        0,   128, 2,   128, 128, 128, 128, 4,   //
        128, 0,   2,   128, 128, 128, 128, 4,   //
        0,   2,   4,   128, 128, 128, 128, 6,   //
        128, 128, 128, 0,   128, 128, 128, 2,   //
        0,   128, 128, 2,   128, 128, 128, 4,   //
        128, 0,   128, 2,   128, 128, 128, 4,   //
        0,   2,   128, 4,   128, 128, 128, 6,   //
        128, 128, 0,   2,   128, 128, 128, 4,   //
        0,   128, 2,   4,   128, 128, 128, 6,   //
        128, 0,   2,   4,   128, 128, 128, 6,   //
        0,   2,   4,   6,   128, 128, 128, 8,   //
        128, 128, 128, 128, 0,   128, 128, 2,   //
        0,   128, 128, 128, 2,   128, 128, 4,   //
        128, 0,   128, 128, 2,   128, 128, 4,   //
        0,   2,   128, 128, 4,   128, 128, 6,   //
        128, 128, 0,   128, 2,   128, 128, 4,   //
        0,   128, 2,   128, 4,   128, 128, 6,   //
        128, 0,   2,   128, 4,   128, 128, 6,   //
        0,   2,   4,   128, 6,   128, 128, 8,   //
        128, 128, 128, 0,   2,   128, 128, 4,   //
        0,   128, 128, 2,   4,   128, 128, 6,   //
        128, 0,   128, 2,   4,   128, 128, 6,   //
        0,   2,   128, 4,   6,   128, 128, 8,   //
        128, 128, 0,   2,   4,   128, 128, 6,   //
        0,   128, 2,   4,   6,   128, 128, 8,   //
        128, 0,   2,   4,   6,   128, 128, 8,   //
        0,   2,   4,   6,   8,   128, 128, 10,  //
        128, 128, 128, 128, 128, 0,   128, 2,   //
        0,   128, 128, 128, 128, 2,   128, 4,   //
        128, 0,   128, 128, 128, 2,   128, 4,   //
        0,   2,   128, 128, 128, 4,   128, 6,   //
        128, 128, 0,   128, 128, 2,   128, 4,   //
        0,   128, 2,   128, 128, 4,   128, 6,   //
        128, 0,   2,   128, 128, 4,   128, 6,   //
        0,   2,   4,   128, 128, 6,   128, 8,   //
        128, 128, 128, 0,   128, 2,   128, 4,   //
        0,   128, 128, 2,   128, 4,   128, 6,   //
        128, 0,   128, 2,   128, 4,   128, 6,   //
        0,   2,   128, 4,   128, 6,   128, 8,   //
        128, 128, 0,   2,   128, 4,   128, 6,   //
        0,   128, 2,   4,   128, 6,   128, 8,   //
        128, 0,   2,   4,   128, 6,   128, 8,   //
        0,   2,   4,   6,   128, 8,   128, 10,  //
        128, 128, 128, 128, 0,   2,   128, 4,   //
        0,   128, 128, 128, 2,   4,   128, 6,   //
        128, 0,   128, 128, 2,   4,   128, 6,   //
        0,   2,   128, 128, 4,   6,   128, 8,   //
        128, 128, 0,   128, 2,   4,   128, 6,   //
        0,   128, 2,   128, 4,   6,   128, 8,   //
        128, 0,   2,   128, 4,   6,   128, 8,   //
        0,   2,   4,   128, 6,   8,   128, 10,  //
        128, 128, 128, 0,   2,   4,   128, 6,   //
        0,   128, 128, 2,   4,   6,   128, 8,   //
        128, 0,   128, 2,   4,   6,   128, 8,   //
        0,   2,   128, 4,   6,   8,   128, 10,  //
        128, 128, 0,   2,   4,   6,   128, 8,   //
        0,   128, 2,   4,   6,   8,   128, 10,  //
        128, 0,   2,   4,   6,   8,   128, 10,  //
        0,   2,   4,   6,   8,   10,  128, 12,  //
        128, 128, 128, 128, 128, 128, 0,   2,   //
        0,   128, 128, 128, 128, 128, 2,   4,   //
        128, 0,   128, 128, 128, 128, 2,   4,   //
        0,   2,   128, 128, 128, 128, 4,   6,   //
        128, 128, 0,   128, 128, 128, 2,   4,   //
        0,   128, 2,   128, 128, 128, 4,   6,   //
        128, 0,   2,   128, 128, 128, 4,   6,   //
        0,   2,   4,   128, 128, 128, 6,   8,   //
        128, 128, 128, 0,   128, 128, 2,   4,   //
        0,   128, 128, 2,   128, 128, 4,   6,   //
        128, 0,   128, 2,   128, 128, 4,   6,   //
        0,   2,   128, 4,   128, 128, 6,   8,   //
        128, 128, 0,   2,   128, 128, 4,   6,   //
        0,   128, 2,   4,   128, 128, 6,   8,   //
        128, 0,   2,   4,   128, 128, 6,   8,   //
        0,   2,   4,   6,   128, 128, 8,   10,  //
        128, 128, 128, 128, 0,   128, 2,   4,   //
        0,   128, 128, 128, 2,   128, 4,   6,   //
        128, 0,   128, 128, 2,   128, 4,   6,   //
        0,   2,   128, 128, 4,   128, 6,   8,   //
        128, 128, 0,   128, 2,   128, 4,   6,   //
        0,   128, 2,   128, 4,   128, 6,   8,   //
        128, 0,   2,   128, 4,   128, 6,   8,   //
        0,   2,   4,   128, 6,   128, 8,   10,  //
        128, 128, 128, 0,   2,   128, 4,   6,   //
        0,   128, 128, 2,   4,   128, 6,   8,   //
        128, 0,   128, 2,   4,   128, 6,   8,   //
        0,   2,   128, 4,   6,   128, 8,   10,  //
        128, 128, 0,   2,   4,   128, 6,   8,   //
        0,   128, 2,   4,   6,   128, 8,   10,  //
        128, 0,   2,   4,   6,   128, 8,   10,  //
        0,   2,   4,   6,   8,   128, 10,  12,  //
        128, 128, 128, 128, 128, 0,   2,   4,   //
        0,   128, 128, 128, 128, 2,   4,   6,   //
        128, 0,   128, 128, 128, 2,   4,   6,   //
        0,   2,   128, 128, 128, 4,   6,   8,   //
        128, 128, 0,   128, 128, 2,   4,   6,   //
        0,   128, 2,   128, 128, 4,   6,   8,   //
        128, 0,   2,   128, 128, 4,   6,   8,   //
        0,   2,   4,   128, 128, 6,   8,   10,  //
        128, 128, 128, 0,   128, 2,   4,   6,   //
        0,   128, 128, 2,   128, 4,   6,   8,   //
        128, 0,   128, 2,   128, 4,   6,   8,   //
        0,   2,   128, 4,   128, 6,   8,   10,  //
        128, 128, 0,   2,   128, 4,   6,   8,   //
        0,   128, 2,   4,   128, 6,   8,   10,  //
        128, 0,   2,   4,   128, 6,   8,   10,  //
        0,   2,   4,   6,   128, 8,   10,  12,  //
        128, 128, 128, 128, 0,   2,   4,   6,   //
        0,   128, 128, 128, 2,   4,   6,   8,   //
        128, 0,   128, 128, 2,   4,   6,   8,   //
        0,   2,   128, 128, 4,   6,   8,   10,  //
        128, 128, 0,   128, 2,   4,   6,   8,   //
        0,   128, 2,   128, 4,   6,   8,   10,  //
        128, 0,   2,   128, 4,   6,   8,   10,  //
        0,   2,   4,   128, 6,   8,   10,  12,  //
        128, 128, 128, 0,   2,   4,   6,   8,   //
        0,   128, 128, 2,   4,   6,   8,   10,  //
        128, 0,   128, 2,   4,   6,   8,   10,  //
        0,   2,   128, 4,   6,   8,   10,  12,  //
        128, 128, 0,   2,   4,   6,   8,   10,  //
        0,   128, 2,   4,   6,   8,   10,  12,  //
        128, 0,   2,   4,   6,   8,   10,  12,  //
        0,   2,   4,   6,   8,   10,  12,  14};
    // Extend to double length because InterleaveLower will only use the (valid)
    // lower half, and we want N u16.
    const Twice<decltype(du8)> du8x2;
    const Vec128<uint8_t, 2 *N> indices8 = ZeroExtendVector(du8x2, Load(du8, table + mask_bits * 8));
    const Vec128<uint16_t, N> indices16 = BitCast(du, InterleaveLower(du8x2, indices8, indices8));
    // TableLookupBytesOr0 operates on bytes. To convert u16 lane indices to byte
    // indices, add 0 to even and 1 to odd byte lanes.
    const Vec128<uint16_t, N> byte_indices =
        Add(indices16, Set(du, static_cast<uint16_t>(OMNI_IS_LITTLE_ENDIAN ? 0x0100 : 0x0001)));
    return BitCast(d, TableLookupBytesOr0(v, byte_indices));
}

template <typename T, size_t N, OMNI_IF_T_SIZE(T, 4)> OMNI_API Vec128<T, N> Expand(Vec128<T, N> v, Mask128<T, N> mask)
{
    const DFromV<decltype(v)> d;
    const RebindToUnsigned<decltype(d)> du;

    const uint64_t mask_bits = detail::BitsFromMask(mask);

    alignas(16) static constexpr uint32_t packed_array[16] = {
        // PrintExpand64x4Nibble - same for 32x4.
        0x0000ffff, 0x0000fff0, 0x0000ff0f, 0x0000ff10, 0x0000f0ff, 0x0000f1f0,
        0x0000f10f, 0x0000f210, 0x00000fff, 0x00001ff0, 0x00001f0f, 0x00002f10,
        0x000010ff, 0x000021f0, 0x0000210f, 0x00003210};

    // For lane i, shift the i-th 4-bit index down to bits [0, 2).
    const Vec128<uint32_t, N> packed = Set(du, packed_array[mask_bits]);
    alignas(16) static constexpr uint32_t shifts[4] = {0, 4, 8, 12};
    Vec128<uint32_t, N> indices = packed >> Load(du, shifts);
    // AVX2 _mm256_permutexvar_epi32 will ignore upper bits, but IndicesFromVec
    // checks bounds, so clear the upper bits.
    indices = And(indices, Set(du, N - 1));
    const Vec128<uint32_t, N> expand = TableLookupLanes(BitCast(du, v), IndicesFromVec(du, indices));
    // TableLookupLanes cannot also zero masked-off lanes, so do that now.
    return IfThenElseZero(mask, BitCast(d, expand));
}

template <typename T, OMNI_IF_T_SIZE(T, 8)> OMNI_API Vec128<T> Expand(Vec128<T> v, Mask128<T> mask)
{
    // Same as Compress, just zero out the mask=false lanes.
    return IfThenElseZero(mask, Compress(v, mask));
}

// For single-element vectors, this is at least as fast as native.
template <typename T> OMNI_API Vec128<T, 1> Expand(Vec128<T, 1> v, Mask128<T, 1> mask)
{
    return IfThenElseZero(mask, v);
}

// ------------------------------ LoadExpand
template <class D, OMNI_IF_V_SIZE_LE_D(D, 16)>
OMNI_API VFromD<D> LoadExpand(MFromD<D> mask, D d, const TFromD<D> *OMNI_RESTRICT unaligned)
{
    return Expand(LoadU(d, unaligned), mask);
}

#endif // OMNI_NATIVE_EXPAND

// ------------------------------ TwoTablesLookupLanes

template <class D> using IndicesFromD = decltype(IndicesFromVec(D(), Zero(RebindToUnsigned<D>())));

// RVV/SVE have their own implementations of
// TwoTablesLookupLanes(D d, VFromD<D> a, VFromD<D> b, IndicesFromD<D> idx)
#if OMNI_TARGET != OMNI_RVV && !OMNI_TARGET_IS_SVE
template <class D> OMNI_API VFromD<D> TwoTablesLookupLanes(D /* d */, VFromD<D> a, VFromD<D> b, IndicesFromD<D> idx)
{
    return TwoTablesLookupLanes(a, b, idx);
}
#endif

// ------------------------------ ReverseLaneBytes

#ifndef OMNI_NATIVE_REVERSE_LANE_BYTES
#define OMNI_NATIVE_REVERSE_LANE_BYTES

template <class V, OMNI_IF_T_SIZE_V(V, 2)> OMNI_API V ReverseLaneBytes(V v)
{
    const DFromV<V> d;
    const Repartition<uint8_t, decltype(d)> du8;
    return BitCast(d, Reverse2(du8, BitCast(du8, v)));
}

template <class V, OMNI_IF_T_SIZE_V(V, 4)> OMNI_API V ReverseLaneBytes(V v)
{
    const DFromV<V> d;
    const Repartition<uint8_t, decltype(d)> du8;
    return BitCast(d, Reverse4(du8, BitCast(du8, v)));
}

template <class V, OMNI_IF_T_SIZE_V(V, 8)> OMNI_API V ReverseLaneBytes(V v)
{
    const DFromV<V> d;
    const Repartition<uint8_t, decltype(d)> du8;
    return BitCast(d, Reverse8(du8, BitCast(du8, v)));
}

#endif // OMNI_NATIVE_REVERSE_LANE_BYTES

// ------------------------------ ReverseBits

// On these targets, we emulate 8-bit shifts using 16-bit shifts and therefore
// require at least two lanes to BitCast to 16-bit. We avoid Simd's 8-bit
// shifts because those would add extra masking already taken care of by
// UI8ReverseBitsStep. Note that AVX3_DL/AVX3_ZEN4 support GFNI and use it to
// implement ReverseBits, so this code is not used there.
#undef OMNI_REVERSE_BITS_MIN_BYTES
#define OMNI_REVERSE_BITS_MIN_BYTES 1

#ifndef OMNI_NATIVE_REVERSE_BITS_UI16_32_64
#define OMNI_NATIVE_REVERSE_BITS_UI16_32_64

template <class V, OMNI_IF_T_SIZE_ONE_OF_V(V, (1 << 2) | (1 << 4) | (1 << 8)), OMNI_IF_NOT_FLOAT_NOR_SPECIAL_V(V)>
OMNI_API V ReverseBits(V v)
{
    const DFromV<decltype(v)> d;
    const Repartition<uint8_t, decltype(d)> du8;
    return ReverseLaneBytes(BitCast(d, ReverseBits(BitCast(du8, v))));
}
#endif // OMNI_NATIVE_REVERSE_BITS_UI16_32_64

// ------------------------------ Per4LaneBlockShuffle

#ifndef OMNI_NATIVE_PER4LANEBLKSHUF_DUP32
#define OMNI_NATIVE_PER4LANEBLKSHUF_DUP32

#if OMNI_TARGET != OMNI_SCALAR || OMNI_IDE
namespace detail {
template <class D>
OMNI_INLINE Vec<D> Per4LaneBlkShufDupSet4xU32(D d, const uint32_t x3, const uint32_t x2, const uint32_t x1,
    const uint32_t x0)
{
#if OMNI_TARGET == OMNI_RVV
    constexpr int kPow2 = d.Pow2();
    constexpr int kLoadPow2 = OMNI_MAX(kPow2, -1);
    const ScalableTag<uint32_t, kLoadPow2> d_load;
#else
    constexpr size_t kMaxBytes = d.MaxBytes();
#if OMNI_TARGET_IS_NEON
    constexpr size_t kMinLanesToLoad = 2;
#else
    constexpr size_t kMinLanesToLoad = 4;
#endif
    constexpr size_t kNumToLoad = OMNI_MAX(kMaxBytes / sizeof(uint32_t), kMinLanesToLoad);
    const CappedTag<uint32_t, kNumToLoad> d_load;
#endif
    return ResizeBitCast(d, Dup128VecFromValues(d_load, x0, x1, x2, x3));
}
} // namespace detail
#endif

#endif // OMNI_NATIVE_PER4LANEBLKSHUF_DUP32

namespace detail {
template <class V> OMNI_INLINE V Per2LaneBlockShuffle(simd::SizeTag<0> /* idx_10_tag */, V v)
{
    return DupEven(v);
}

template <class V> OMNI_INLINE V Per2LaneBlockShuffle(simd::SizeTag<1> /* idx_10_tag */, V v)
{
    const DFromV<decltype(v)> d;
    return Reverse2(d, v);
}

template <class V> OMNI_INLINE V Per2LaneBlockShuffle(simd::SizeTag<2> /* idx_10_tag */, V v)
{
    return v;
}

template <class V> OMNI_INLINE V Per2LaneBlockShuffle(simd::SizeTag<3> /* idx_10_tag */, V v)
{
    return DupOdd(v);
}

OMNI_INLINE uint32_t U8x4Per4LaneBlkIndices(const uint32_t idx3, const uint32_t idx2, const uint32_t idx1,
    const uint32_t idx0)
{
#if OMNI_IS_LITTLE_ENDIAN
    return static_cast<uint32_t>((idx3 << 24) | (idx2 << 16) | (idx1 << 8) | idx0);
#else
    return static_cast<uint32_t>(idx3 | (idx2 << 8) | (idx1 << 16) | (idx0 << 24));
#endif
}

template <class D>
OMNI_INLINE Vec<D> TblLookupPer4LaneBlkU8IdxInBlk(D d, const uint32_t idx3, const uint32_t idx2, const uint32_t idx1,
    const uint32_t idx0)
{
    const Repartition<uint32_t, D> du32;
    return ResizeBitCast(d, Set(du32, U8x4Per4LaneBlkIndices(idx3, idx2, idx1, idx0)));
}

#if OMNI_TARGET_IS_SVE
#define OMNI_PER_4_BLK_TBL_LOOKUP_LANES_ENABLE(D) void * = nullptr
#else
#define OMNI_PER_4_BLK_TBL_LOOKUP_LANES_ENABLE(D) OMNI_IF_T_SIZE_D(D, 8)

template <class V, OMNI_IF_T_SIZE_ONE_OF_V(V, (1 << 1) | (1 << 2) | (1 << 4))>
OMNI_INLINE V Per4LaneBlkShufDoTblLookup(V v, V idx)
{
    const DFromV<decltype(v)> d;
    const Repartition<uint8_t, decltype(d)> du8;
    return BitCast(d, TableLookupBytes(BitCast(du8, v), BitCast(du8, idx)));
}

template <class D, OMNI_IF_T_SIZE_D(D, 1)>
OMNI_INLINE Vec<D> TblLookupPer4LaneBlkShufIdx(D d, const uint32_t idx3, const uint32_t idx2, const uint32_t idx1,
    const uint32_t idx0)
{
    const Repartition<uint32_t, decltype(d)> du32;
    const uint32_t idx3210 = U8x4Per4LaneBlkIndices(idx3, idx2, idx1, idx0);
    const auto v_byte_idx = Per4LaneBlkShufDupSet4xU32(du32, static_cast<uint32_t>(idx3210 + 0x0C0C0C0C),
        static_cast<uint32_t>(idx3210 + 0x08080808), static_cast<uint32_t>(idx3210 + 0x04040404),
        static_cast<uint32_t>(idx3210));
    return ResizeBitCast(d, v_byte_idx);
}

template <class D, OMNI_IF_T_SIZE_D(D, 2)>
OMNI_INLINE Vec<D> TblLookupPer4LaneBlkShufIdx(D d, const uint32_t idx3, const uint32_t idx2, const uint32_t idx1,
    const uint32_t idx0)
{
    const Repartition<uint32_t, decltype(d)> du32;
#if OMNI_IS_LITTLE_ENDIAN
    const uint32_t idx10 = static_cast<uint32_t>((idx1 << 16) | idx0);
    const uint32_t idx32 = static_cast<uint32_t>((idx3 << 16) | idx2);
    constexpr uint32_t kLaneByteOffsets{ 0x01000100 };
#else
    const uint32_t idx10 = static_cast<uint32_t>(idx1 | (idx0 << 16));
    const uint32_t idx32 = static_cast<uint32_t>(idx3 | (idx2 << 16));
    constexpr uint32_t kLaneByteOffsets{ 0x00010001 };
#endif
    constexpr uint32_t kHiLaneByteOffsets{ kLaneByteOffsets + 0x08080808u };

    const auto v_byte_idx =
        Per4LaneBlkShufDupSet4xU32(du32, static_cast<uint32_t>(idx32 * 0x0202u + kHiLaneByteOffsets),
        static_cast<uint32_t>(idx10 * 0x0202u + kHiLaneByteOffsets),
        static_cast<uint32_t>(idx32 * 0x0202u + kLaneByteOffsets),
        static_cast<uint32_t>(idx10 * 0x0202u + kLaneByteOffsets));
    return ResizeBitCast(d, v_byte_idx);
}

template <class D, OMNI_IF_T_SIZE_D(D, 4)>
OMNI_INLINE Vec<D> TblLookupPer4LaneBlkShufIdx(D d, const uint32_t idx3, const uint32_t idx2, const uint32_t idx1,
    const uint32_t idx0)
{
    const Repartition<uint32_t, decltype(d)> du32;
#if OMNI_IS_LITTLE_ENDIAN
    constexpr uint32_t kLaneByteOffsets{ 0x03020100 };
#else
    constexpr uint32_t kLaneByteOffsets{ 0x00010203 };
#endif

    const auto v_byte_idx =
        Per4LaneBlkShufDupSet4xU32(du32, static_cast<uint32_t>(idx3 * 0x04040404u + kLaneByteOffsets),
        static_cast<uint32_t>(idx2 * 0x04040404u + kLaneByteOffsets),
        static_cast<uint32_t>(idx1 * 0x04040404u + kLaneByteOffsets),
        static_cast<uint32_t>(idx0 * 0x04040404u + kLaneByteOffsets));
    return ResizeBitCast(d, v_byte_idx);
}
#endif

template <class D, OMNI_IF_T_SIZE_D(D, 1)>
OMNI_INLINE VFromD<D> TblLookupPer4LaneBlkIdxInBlk(D d, const uint32_t idx3, const uint32_t idx2, const uint32_t idx1,
    const uint32_t idx0)
{
    return TblLookupPer4LaneBlkU8IdxInBlk(d, idx3, idx2, idx1, idx0);
}

template <class D, OMNI_IF_T_SIZE_D(D, 2)>
OMNI_INLINE VFromD<D> TblLookupPer4LaneBlkIdxInBlk(D d, const uint32_t idx3, const uint32_t idx2, const uint32_t idx1,
    const uint32_t idx0)
{
    const uint16_t u16_idx0 = static_cast<uint16_t>(idx0);
    const uint16_t u16_idx1 = static_cast<uint16_t>(idx1);
    const uint16_t u16_idx2 = static_cast<uint16_t>(idx2);
    const uint16_t u16_idx3 = static_cast<uint16_t>(idx3);
#if OMNI_TARGET_IS_NEON
    constexpr size_t kMinLanesToLoad = 4;
#else
    constexpr size_t kMinLanesToLoad = 8;
#endif
    constexpr size_t kNumToLoad = OMNI_MAX(OMNI_MAX_LANES_D(D), kMinLanesToLoad);
    const CappedTag<uint16_t, kNumToLoad> d_load;
    return ResizeBitCast(d,
        Dup128VecFromValues(d_load, u16_idx0, u16_idx1, u16_idx2, u16_idx3, u16_idx0, u16_idx1, u16_idx2, u16_idx3));
}

template <class D, OMNI_IF_T_SIZE_D(D, 4)>
OMNI_INLINE VFromD<D> TblLookupPer4LaneBlkIdxInBlk(D d, const uint32_t idx3, const uint32_t idx2, const uint32_t idx1,
    const uint32_t idx0)
{
    return Per4LaneBlkShufDupSet4xU32(d, idx3, idx2, idx1, idx0);
}

template <class D, OMNI_IF_T_SIZE_D(D, 8)>
OMNI_INLINE VFromD<D> TblLookupPer4LaneBlkIdxInBlk(D d, const uint32_t idx3, const uint32_t idx2, const uint32_t idx1,
    const uint32_t idx0)
{
    const RebindToUnsigned<decltype(d)> du;
    const Rebind<uint32_t, decltype(d)> du32;
    return BitCast(d, PromoteTo(du, Per4LaneBlkShufDupSet4xU32(du32, idx3, idx2, idx1, idx0)));
}

template <class D, OMNI_PER_4_BLK_TBL_LOOKUP_LANES_ENABLE(D)>
OMNI_INLINE IndicesFromD<D> TblLookupPer4LaneBlkShufIdx(D d, const uint32_t idx3, const uint32_t idx2,
    const uint32_t idx1, const uint32_t idx0)
{
    const RebindToUnsigned<decltype(d)> du;
    using TU = TFromD<decltype(du)>;
    auto idx_in_blk = TblLookupPer4LaneBlkIdxInBlk(du, idx3, idx2, idx1, idx0);

    constexpr size_t kN = OMNI_MAX_LANES_D(D);
    if (kN < 4) {
        idx_in_blk = And(idx_in_blk, Set(du, static_cast<TU>(kN - 1)));
    }

    const auto blk_offsets = And(Iota(du, TU{ 0 }), Set(du, static_cast<TU>(~TU{ 3 })));
    return IndicesFromVec(d, Add(idx_in_blk, blk_offsets));
}

template <class V, OMNI_PER_4_BLK_TBL_LOOKUP_LANES_ENABLE(DFromV<V>)>
OMNI_INLINE V Per4LaneBlkShufDoTblLookup(V v, IndicesFromD<DFromV<V>> idx)
{
    return TableLookupLanes(v, idx);
}

#undef OMNI_PER_4_BLK_TBL_LOOKUP_LANES_ENABLE

template <class V> OMNI_INLINE V TblLookupPer4LaneBlkShuf(V v, size_t idx3210)
{
    const DFromV<decltype(v)> d;
    const uint32_t idx3 = static_cast<uint32_t>((idx3210 >> 6) & 3);
    const uint32_t idx2 = static_cast<uint32_t>((idx3210 >> 4) & 3);
    const uint32_t idx1 = static_cast<uint32_t>((idx3210 >> 2) & 3);
    const uint32_t idx0 = static_cast<uint32_t>(idx3210 & 3);
    const auto idx = TblLookupPer4LaneBlkShufIdx(d, idx3, idx2, idx1, idx0);
    return Per4LaneBlkShufDoTblLookup(v, idx);
}

// The detail::Per4LaneBlockShuffle overloads that have the extra lane_size_tag
// and vect_size_tag parameters are only called for vectors that have at
// least 4 lanes (or scalable vectors that might possibly have 4 or more lanes)
template <size_t kIdx3210, size_t kLaneSize, size_t kVectSize, class V>
OMNI_INLINE V Per4LaneBlockShuffle(simd::SizeTag<kIdx3210> /* idx_3210_tag */,
    simd::SizeTag<kLaneSize> /* lane_size_tag */, simd::SizeTag<kVectSize> /* vect_size_tag */, V v)
{
    return TblLookupPer4LaneBlkShuf(v, kIdx3210);
}

template <class V>
OMNI_INLINE VFromD<RepartitionToWide<DFromV<V>>> Per4LaneBlockShufCastToWide(simd::FloatTag /* type_tag */,
    simd::SizeTag<4> /* lane_size_tag */, V v)
{
    const DFromV<decltype(v)> d;
    const RepartitionToWide<decltype(d)> dw;
    return BitCast(dw, v);
}

template <size_t kLaneSize, class V>
OMNI_INLINE VFromD<RepartitionToWide<RebindToUnsigned<DFromV<V>>>> Per4LaneBlockShufCastToWide(
    simd::FloatTag /* type_tag */, simd::SizeTag<kLaneSize> /* lane_size_tag */, V v)
{
    const DFromV<decltype(v)> d;
    const RebindToUnsigned<decltype(d)> du;
    const RepartitionToWide<decltype(du)> dw;
    return BitCast(dw, v);
}

template <size_t kLaneSize, class V>
OMNI_INLINE VFromD<RepartitionToWide<DFromV<V>>> Per4LaneBlockShufCastToWide(simd::NonFloatTag /* type_tag */,
    simd::SizeTag<kLaneSize> /* lane_size_tag */, V v)
{
    const DFromV<decltype(v)> d;
    const RepartitionToWide<decltype(d)> dw;
    return BitCast(dw, v);
}

template <class V> OMNI_INLINE V Per4LaneBlockShuffle(simd::SizeTag<0x1B> /* idx_3210_tag */, V v)
{
    const DFromV<decltype(v)> d;
    return Reverse4(d, v);
}

template <class V, OMNI_IF_T_SIZE_ONE_OF_V(V, (1 << 1) | (1 << 2) | (OMNI_HAVE_INTEGER64 ? (1 << 4) : 0))>
OMNI_INLINE V Per4LaneBlockShuffle(simd::SizeTag<0x44> /* idx_3210_tag */, V v)
{
    const DFromV<decltype(v)> d;
    const auto vw = Per4LaneBlockShufCastToWide(simd::IsFloatTag<TFromV<V>>(), simd::SizeTag<sizeof(TFromV<V>)>(), v);
    return BitCast(d, DupEven(vw));
}

template <class V, OMNI_IF_T_SIZE_ONE_OF_V(V, (1 << 1) | (1 << 2) | (OMNI_HAVE_INTEGER64 ? (1 << 4) : 0))>
OMNI_INLINE V Per4LaneBlockShuffle(simd::SizeTag<0x4E> /* idx_3210_tag */, V v)
{
    const DFromV<decltype(v)> d;
    const auto vw = Per4LaneBlockShufCastToWide(simd::IsFloatTag<TFromV<V>>(), simd::SizeTag<sizeof(TFromV<V>)>(), v);
    const DFromV<decltype(vw)> dw;
    return BitCast(d, Reverse2(dw, vw));
}

#if OMNI_MAX_BYTES >= 32

template <class V, OMNI_IF_T_SIZE_V(V, 8)>
OMNI_INLINE V Per4LaneBlockShuffle(simd::SizeTag<0x4E> /* idx_3210_tag */, V v)
{
    return SwapAdjacentBlocks(v);
}

#endif

template <class V, OMNI_IF_LANES_D(DFromV<V>, 4), OMNI_IF_T_SIZE_ONE_OF_V(V, (1 << 1) | (1 << 2))>
OMNI_INLINE V Per4LaneBlockShuffle(simd::SizeTag<0x50> /* idx_3210_tag */, V v)
{
    const DFromV<decltype(v)> d;
    return InterleaveLower(d, v, v);
}

template <class V, OMNI_IF_T_SIZE_V(V, 4)>
OMNI_INLINE V Per4LaneBlockShuffle(simd::SizeTag<0x50> /* idx_3210_tag */, V v)
{
    const DFromV<decltype(v)> d;
    return InterleaveLower(d, v, v);
}

template <class V, OMNI_IF_LANES_D(DFromV<V>, 4)>
OMNI_INLINE V Per4LaneBlockShuffle(simd::SizeTag<0x88> /* idx_3210_tag */, V v)
{
    const DFromV<decltype(v)> d;
    return ConcatEven(d, v, v);
}

template <class V> OMNI_INLINE V Per4LaneBlockShuffle(simd::SizeTag<0xA0> /* idx_3210_tag */, V v)
{
    return DupEven(v);
}

template <class V> OMNI_INLINE V Per4LaneBlockShuffle(simd::SizeTag<0xB1> /* idx_3210_tag */, V v)
{
    const DFromV<decltype(v)> d;
    return Reverse2(d, v);
}

template <class V, OMNI_IF_LANES_D(DFromV<V>, 4)>
OMNI_INLINE V Per4LaneBlockShuffle(simd::SizeTag<0xDD> /* idx_3210_tag */, V v)
{
    const DFromV<decltype(v)> d;
    return ConcatOdd(d, v, v);
}

template <class V> OMNI_INLINE V Per4LaneBlockShuffle(simd::SizeTag<0xE4> /* idx_3210_tag */, V v)
{
    return v;
}

template <class V, OMNI_IF_T_SIZE_ONE_OF_V(V, (1 << 1) | (1 << 2) | (OMNI_HAVE_INTEGER64 ? (1 << 4) : 0))>
OMNI_INLINE V Per4LaneBlockShuffle(simd::SizeTag<0xEE> /* idx_3210_tag */, V v)
{
    const DFromV<decltype(v)> d;
    const auto vw = Per4LaneBlockShufCastToWide(simd::IsFloatTag<TFromV<V>>(), simd::SizeTag<sizeof(TFromV<V>)>(), v);
    return BitCast(d, DupOdd(vw));
}

template <class V> OMNI_INLINE V Per4LaneBlockShuffle(simd::SizeTag<0xF5> /* idx_3210_tag */, V v)
{
    return DupOdd(v);
}

template <class V, OMNI_IF_T_SIZE_V(V, 4)>
OMNI_INLINE V Per4LaneBlockShuffle(simd::SizeTag<0xFA> /* idx_3210_tag */, V v)
{
    const DFromV<decltype(v)> d;
    return InterleaveUpper(d, v, v);
}

template <size_t kIdx3210, class V> OMNI_INLINE V Per4LaneBlockShuffle(simd::SizeTag<kIdx3210> idx_3210_tag, V v)
{
    const DFromV<decltype(v)> d;
    return Per4LaneBlockShuffle(idx_3210_tag, simd::SizeTag<sizeof(TFromV<V>)>(), simd::SizeTag<d.MaxBytes()>(), v);
}
} // namespace detail

template <size_t kIdx3, size_t kIdx2, size_t kIdx1, size_t kIdx0, class V, OMNI_IF_LANES_D(DFromV<V>, 1)>
OMNI_API V Per4LaneBlockShuffle(V v)
{
    static_assert(kIdx0 <= 3, "kIdx0 <= 3 must be true");
    static_assert(kIdx1 <= 3, "kIdx1 <= 3 must be true");
    static_assert(kIdx2 <= 3, "kIdx2 <= 3 must be true");
    static_assert(kIdx3 <= 3, "kIdx3 <= 3 must be true");

    return v;
}

template <size_t kIdx3, size_t kIdx2, size_t kIdx1, size_t kIdx0, class V, OMNI_IF_LANES_D(DFromV<V>, 2)>
OMNI_API V Per4LaneBlockShuffle(V v)
{
    static_assert(kIdx0 <= 3, "kIdx0 <= 3 must be true");
    static_assert(kIdx1 <= 3, "kIdx1 <= 3 must be true");
    static_assert(kIdx2 <= 3, "kIdx2 <= 3 must be true");
    static_assert(kIdx3 <= 3, "kIdx3 <= 3 must be true");

    constexpr bool isReverse2 = (kIdx0 == 1 || kIdx1 == 0) && (kIdx0 != kIdx1);
    constexpr size_t kPer2BlkIdx0 = (kIdx0 <= 1) ? kIdx0 : (isReverse2 ? 1 : 0);
    constexpr size_t kPer2BlkIdx1 = (kIdx1 <= 1) ? kIdx1 : (isReverse2 ? 0 : 1);

    constexpr size_t kIdx10 = (kPer2BlkIdx1 << 1) | kPer2BlkIdx0;
    static_assert(kIdx10 <= 3, "kIdx10 <= 3 must be true");
    return detail::Per2LaneBlockShuffle(simd::SizeTag<kIdx10>(), v);
}

template <size_t kIdx3, size_t kIdx2, size_t kIdx1, size_t kIdx0, class V, OMNI_IF_LANES_GT_D(DFromV<V>, 2)>
OMNI_API V Per4LaneBlockShuffle(V v)
{
    static_assert(kIdx0 <= 3, "kIdx0 <= 3 must be true");
    static_assert(kIdx1 <= 3, "kIdx1 <= 3 must be true");
    static_assert(kIdx2 <= 3, "kIdx2 <= 3 must be true");
    static_assert(kIdx3 <= 3, "kIdx3 <= 3 must be true");

    constexpr size_t kIdx3210 = (kIdx3 << 6) | (kIdx2 << 4) | (kIdx1 << 2) | kIdx0;
    return detail::Per4LaneBlockShuffle(simd::SizeTag<kIdx3210>(), v);
}

// ------------------------------ Blocks

template <class D> OMNI_API size_t Blocks(D d)
{
    return (d.MaxBytes() <= 16) ? 1 : ((Lanes(d) * sizeof(TFromD<D>) + 15) / 16);
}

// ------------------------------ Block insert/extract/broadcast ops
#ifndef OMNI_NATIVE_BLK_INSERT_EXTRACT
#define OMNI_NATIVE_BLK_INSERT_EXTRACT

template <int kBlockIdx, class V, OMNI_IF_V_SIZE_LE_V(V, 16)> OMNI_API V InsertBlock(V /* v */, V blk_to_insert)
{
    static_assert(kBlockIdx == 0, "Invalid block index");
    return blk_to_insert;
}

template <int kBlockIdx, class V, OMNI_IF_V_SIZE_LE_V(V, 16)> OMNI_API V ExtractBlock(V v)
{
    static_assert(kBlockIdx == 0, "Invalid block index");
    return v;
}

template <int kBlockIdx, class V, OMNI_IF_V_SIZE_LE_V(V, 16)> OMNI_API V BroadcastBlock(V v)
{
    static_assert(kBlockIdx == 0, "Invalid block index");
    return v;
}

#endif // OMNI_NATIVE_BLK_INSERT_EXTRACT

// ------------------------------ BroadcastLane
#ifndef OMNI_NATIVE_BROADCASTLANE
#define OMNI_NATIVE_BROADCASTLANE

template <int kLane, class V, OMNI_IF_V_SIZE_LE_V(V, 16)> OMNI_API V BroadcastLane(V v)
{
    return Broadcast<kLane>(v);
}

#endif // OMNI_NATIVE_BROADCASTLANE

// ------------------------------ Slide1Up and Slide1Down
#ifndef OMNI_NATIVE_SLIDE1_UP_DOWN
#define OMNI_NATIVE_SLIDE1_UP_DOWN

template <class D, OMNI_IF_LANES_D(D, 1)> OMNI_API VFromD<D> Slide1Up(D d, VFromD<D> /* v */)
{
    return Zero(d);
}
template <class D, OMNI_IF_LANES_D(D, 1)> OMNI_API VFromD<D> Slide1Down(D d, VFromD<D> /* v */)
{
    return Zero(d);
}

#if OMNI_TARGET != OMNI_SCALAR || OMNI_IDE
template <class D, OMNI_IF_V_SIZE_LE_D(D, 16), OMNI_IF_LANES_GT_D(D, 1)> OMNI_API VFromD<D> Slide1Up(D d, VFromD<D> v)
{
    return ShiftLeftLanes<1>(d, v);
}
template <class D, OMNI_IF_V_SIZE_LE_D(D, 16), OMNI_IF_LANES_GT_D(D, 1)> OMNI_API VFromD<D> Slide1Down(D d, VFromD<D> v)
{
    return ShiftRightLanes<1>(d, v);
}
#endif // OMNI_TARGET != OMNI_SCALAR

#endif // OMNI_NATIVE_SLIDE1_UP_DOWN

// ------------------------------ SlideUpBlocks

template <int kBlocks, class D, OMNI_IF_V_SIZE_LE_D(D, 16)> OMNI_API VFromD<D> SlideUpBlocks(D /* d */, VFromD<D> v)
{
    static_assert(kBlocks == 0, "kBlocks == 0 must be true");
    return v;
}

#if OMNI_HAVE_SCALABLE || OMNI_TARGET == OMNI_SVE_256

template <int kBlocks, class D, OMNI_IF_V_SIZE_GT_D(D, 16)> OMNI_API VFromD<D> SlideUpBlocks(D d, VFromD<D> v)
{
    static_assert(0 <= kBlocks && static_cast<size_t>(kBlocks) < d.MaxBlocks(),
        "kBlocks must be between 0 and d.MaxBlocks() - 1");
    constexpr size_t kLanesPerBlock = 16 / sizeof(TFromD<D>);
    return SlideUpLanes(d, v, static_cast<size_t>(kBlocks) * kLanesPerBlock);
}

#endif

// ------------------------------ SlideDownBlocks

template <int kBlocks, class D, OMNI_IF_V_SIZE_LE_D(D, 16)> OMNI_API VFromD<D> SlideDownBlocks(D /* d */, VFromD<D> v)
{
    static_assert(kBlocks == 0, "kBlocks == 0 must be true");
    return v;
}

#if OMNI_HAVE_SCALABLE || OMNI_TARGET == OMNI_SVE_256

template <int kBlocks, class D, OMNI_IF_V_SIZE_GT_D(D, 16)> OMNI_API VFromD<D> SlideDownBlocks(D d, VFromD<D> v)
{
    static_assert(0 <= kBlocks && static_cast<size_t>(kBlocks) < d.MaxBlocks(),
        "kBlocks must be between 0 and d.MaxBlocks() - 1");
    constexpr size_t kLanesPerBlock = 16 / sizeof(TFromD<D>);
    return SlideDownLanes(d, v, static_cast<size_t>(kBlocks) * kLanesPerBlock);
}

#endif

// ------------------------------ Slide mask up/down
#ifndef OMNI_NATIVE_SLIDE_MASK
#define OMNI_NATIVE_SLIDE_MASK

template <class D> OMNI_API Mask<D> SlideMask1Up(D d, Mask<D> m)
{
    return MaskFromVec(Slide1Up(d, VecFromMask(d, m)));
}

template <class D> OMNI_API Mask<D> SlideMask1Down(D d, Mask<D> m)
{
    return MaskFromVec(Slide1Down(d, VecFromMask(d, m)));
}

template <class D> OMNI_API Mask<D> SlideMaskUpLanes(D d, Mask<D> m, size_t amt)
{
    return MaskFromVec(SlideUpLanes(d, VecFromMask(d, m), amt));
}

template <class D> OMNI_API Mask<D> SlideMaskDownLanes(D d, Mask<D> m, size_t amt)
{
    return MaskFromVec(SlideDownLanes(d, VecFromMask(d, m), amt));
}

#endif // OMNI_NATIVE_SLIDE_MASK

// ------------------------------ SumsOfAdjQuadAbsDiff

#ifndef OMNI_NATIVE_SUMS_OF_ADJ_QUAD_ABS_DIFF) == defined(OMNI_TARGET_TOGGLE))
#ifdef OMNI_NATIVE_SUMS_OF_ADJ_QUAD_ABS_DIFF
#undef OMNI_NATIVE_SUMS_OF_ADJ_QUAD_ABS_DIFF
#else
#define OMNI_NATIVE_SUMS_OF_ADJ_QUAD_ABS_DIFF
#endif

#if OMNI_TARGET != OMNI_SCALAR || OMNI_IDE

template <int kAOffset, int kBOffset, class V8, OMNI_IF_UI8_D(DFromV<V8>)>
OMNI_API Vec<RepartitionToWide<DFromV<V8>>> SumsOfAdjQuadAbsDiff(V8 a, V8 b)
{
    static_assert(0 <= kAOffset && kAOffset <= 1, "kAOffset must be between 0 and 1");
    static_assert(0 <= kBOffset && kBOffset <= 3, "kBOffset must be between 0 and 3");
    using D8 = DFromV<V8>;
    const D8 d8;
    const RebindToUnsigned<decltype(d8)> du8;
    const RepartitionToWide<decltype(d8)> d16;
    const RepartitionToWide<decltype(du8)> du16;

    // Ensure that a is resized to a vector that has at least
    // OMNI_MAX(Lanes(d8), size_t{8} << kAOffset) lanes for the interleave and
    // CombineShiftRightBytes operations below.
#if OMNI_TARGET_IS_SVE
    // On SVE targets, Lanes(d8_interleave) >= 16 and
    // Lanes(d8_interleave) >= Lanes(d8) are both already true as d8 is a SIMD
    // tag for a full u8/i8 vector on SVE.
    const D8 d8_interleave;
#else
    // On targets that use non-scalable vector types, Lanes(d8_interleave) is
    // equal to OMNI_MAX(Lanes(d8), size_t{8} << kAOffset).
    constexpr size_t kInterleaveLanes = OMNI_MAX(OMNI_MAX_LANES_D(D8), size_t{ 8 } << kAOffset);
    const FixedTag<TFromD<D8>, kInterleaveLanes> d8_interleave;
#endif

    // The ResizeBitCast operation below will resize a to a vector that has
    // at least OMNI_MAX(Lanes(d8), size_t{8} << kAOffset) lanes for the
    // InterleaveLower, InterleaveUpper, and CombineShiftRightBytes operations
    // below.
    const auto a_to_interleave = ResizeBitCast(d8_interleave, a);

    const auto a_interleaved_lo = InterleaveLower(d8_interleave, a_to_interleave, a_to_interleave);
    const auto a_interleaved_hi = InterleaveUpper(d8_interleave, a_to_interleave, a_to_interleave);

    /* a01: { a[kAOffset*4+0], a[kAOffset*4+1], a[kAOffset*4+1], a[kAOffset*4+2],
              a[kAOffset*4+2], a[kAOffset*4+3], a[kAOffset*4+3], a[kAOffset*4+4],
              a[kAOffset*4+4], a[kAOffset*4+5], a[kAOffset*4+5], a[kAOffset*4+6],
              a[kAOffset*4+6], a[kAOffset*4+7], a[kAOffset*4+7], a[kAOffset*4+8] }
     */
    /* a23: { a[kAOffset*4+2], a[kAOffset*4+3], a[kAOffset*4+3], a[kAOffset*4+4],
              a[kAOffset*4+4], a[kAOffset*4+5], a[kAOffset*4+5], a[kAOffset*4+6],
              a[kAOffset*4+6], a[kAOffset*4+7], a[kAOffset*4+7], a[kAOffset*4+8],
              a[kAOffset*4+8], a[kAOffset*4+9], a[kAOffset*4+9], a[kAOffset*4+10]
       } */

    // a01 and a23 are resized back to V8 as only the first Lanes(d8) lanes of
    // the CombineShiftRightBytes are needed for the subsequent AbsDiff operations
    // and as a01 and a23 need to be the same vector type as b01 and b23 for the
    // AbsDiff operations below.
    const V8 a01 =
        ResizeBitCast(d8, CombineShiftRightBytes<kAOffset * 8 + 1>(d8_interleave, a_interleaved_hi, a_interleaved_lo));
    const V8 a23 =
        ResizeBitCast(d8, CombineShiftRightBytes<kAOffset * 8 + 5>(d8_interleave, a_interleaved_hi, a_interleaved_lo));

    /* b01: { b[kBOffset*4+0], b[kBOffset*4+1], b[kBOffset*4+0], b[kBOffset*4+1],
              b[kBOffset*4+0], b[kBOffset*4+1], b[kBOffset*4+0], b[kBOffset*4+1],
              b[kBOffset*4+0], b[kBOffset*4+1], b[kBOffset*4+0], b[kBOffset*4+1],
              b[kBOffset*4+0], b[kBOffset*4+1], b[kBOffset*4+0], b[kBOffset*4+1] }
     */
    /* b23: { b[kBOffset*4+2], b[kBOffset*4+3], b[kBOffset*4+2], b[kBOffset*4+3],
              b[kBOffset*4+2], b[kBOffset*4+3], b[kBOffset*4+2], b[kBOffset*4+3],
              b[kBOffset*4+2], b[kBOffset*4+3], b[kBOffset*4+2], b[kBOffset*4+3],
              b[kBOffset*4+2], b[kBOffset*4+3], b[kBOffset*4+2], b[kBOffset*4+3] }
     */
    const V8 b01 = BitCast(d8, Broadcast<kBOffset * 2>(BitCast(d16, b)));
    const V8 b23 = BitCast(d8, Broadcast<kBOffset * 2 + 1>(BitCast(d16, b)));

    const VFromD<decltype(du16)> absdiff_sum_01 = SumsOf2(BitCast(du8, AbsDiff(a01, b01)));
    const VFromD<decltype(du16)> absdiff_sum_23 = SumsOf2(BitCast(du8, AbsDiff(a23, b23)));
    return BitCast(d16, Add(absdiff_sum_01, absdiff_sum_23));
}

#endif // OMNI_TARGET != OMNI_SCALAR

#endif // OMNI_NATIVE_SUMS_OF_ADJ_QUAD_ABS_DIFF

// ------------------------------ SumsOfShuffledQuadAbsDiff

#ifndef OMNI_NATIVE_SUMS_OF_SHUFFLED_QUAD_ABS_DIFF) == defined(OMNI_TARGET_TOGGLE))
#ifdef OMNI_NATIVE_SUMS_OF_SHUFFLED_QUAD_ABS_DIFF
#undef OMNI_NATIVE_SUMS_OF_SHUFFLED_QUAD_ABS_DIFF
#else
#define OMNI_NATIVE_SUMS_OF_SHUFFLED_QUAD_ABS_DIFF
#endif

template <int kIdx3, int kIdx2, int kIdx1, int kIdx0, class V8, OMNI_IF_UI8_D(DFromV<V8>)>
OMNI_API Vec<RepartitionToWide<DFromV<V8>>> SumsOfShuffledQuadAbsDiff(V8 a, V8 b)
{
    static_assert(0 <= kIdx0 && kIdx0 <= 3, "kIdx0 must be between 0 and 3");
    static_assert(0 <= kIdx1 && kIdx1 <= 3, "kIdx1 must be between 0 and 3");
    static_assert(0 <= kIdx2 && kIdx2 <= 3, "kIdx2 must be between 0 and 3");
    static_assert(0 <= kIdx3 && kIdx3 <= 3, "kIdx3 must be between 0 and 3");

    const DFromV<decltype(a)> d8;
    const RepartitionToWide<decltype(d8)> d16;
    const RepartitionToWide<decltype(d16)> d32;

    const auto vA = a;
    const auto vB = b;

    const RebindToUnsigned<decltype(d8)> du8;

    const auto a_shuf = Per4LaneBlockShuffle<kIdx3, kIdx2, kIdx1, kIdx0>(BitCast(d32, vA));
    /* a0123_2345: { a_shuf[0], a_shuf[1], a_shuf[2], a_shuf[3],
                     a_shuf[2], a_shuf[3], a_shuf[4], a_shuf[5],
                     a_shuf[8], a_shuf[9], a_shuf[10], a_shuf[11],
                     a_shuf[10], a_shuf[11], a_shuf[12], a_shuf[13] } */
    /* a1234_3456: { a_shuf[1], a_shuf[2], a_shuf[3], a_shuf[4],
                     a_shuf[3], a_shuf[4], a_shuf[5], a_shuf[6],
                     a_shuf[9], a_shuf[10], a_shuf[11], a_shuf[12],
                     a_shuf[11], a_shuf[12], a_shuf[13], a_shuf[14] } */
#if OMNI_TARGET_IS_SVE
    // On RVV/SVE targets, use Slide1Up/Slide1Down instead of
    // ShiftLeftBytes/ShiftRightBytes to avoid unnecessary zeroing out of any
    // lanes that are shifted into an adjacent 16-byte block as any lanes that are
    // shifted into an adjacent 16-byte block by Slide1Up/Slide1Down will be
    // replaced by the OddEven operation.
    const auto a_0123_2345 = BitCast(d8, OddEven(BitCast(d32, Slide1Up(d16, BitCast(d16, a_shuf))), a_shuf));
    const auto a_1234_3456 = BitCast(d8,
        OddEven(BitCast(d32, Slide1Up(d8, BitCast(d8, a_shuf))), BitCast(d32, Slide1Down(d8, BitCast(d8, a_shuf)))));
#else
    const auto a_0123_2345 = BitCast(d8, OddEven(ShiftLeftBytes<2>(d32, a_shuf), a_shuf));
    const auto a_1234_3456 = BitCast(d8, OddEven(ShiftLeftBytes<1>(d32, a_shuf), ShiftRightBytes<1>(d32, a_shuf)));
#endif

    auto even_sums = SumsOf4(BitCast(du8, AbsDiff(a_0123_2345, vB)));
    auto odd_sums = SumsOf4(BitCast(du8, AbsDiff(a_1234_3456, vB)));

#if OMNI_IS_LITTLE_ENDIAN
    odd_sums = ShiftLeft<16>(odd_sums);
#else
    even_sums = ShiftLeft<16>(even_sums);
#endif

    const auto sums = OddEven(BitCast(d16, odd_sums), BitCast(d16, even_sums));
    return sums;
}

#endif // OMNI_NATIVE_SUMS_OF_SHUFFLED_QUAD_ABS_DIFF

// ------------------------------ BitShuffle (Rol)
#ifndef OMNI_NATIVE_BITSHUFFLE
#define OMNI_NATIVE_BITSHUFFLE

template <class V, class VI, OMNI_IF_UI64(TFromV<V>), OMNI_IF_UI8(TFromV<VI>)> OMNI_API V BitShuffle(V v, VI idx)
{
    const DFromV<decltype(v)> d64;
    const RebindToUnsigned<decltype(d64)> du64;
    const Repartition<uint8_t, decltype(d64)> du8;
    const Repartition<uint8_t, decltype(d64)> d_idx_shr;
    constexpr uint64_t kExtractedBitsMask = static_cast<uint64_t>(0x8040201008040201u);

    const auto k7 = Set(du8, uint8_t{ 0x07 });

    auto unmasked_byte_idx = BitCast(du8, ShiftRight<3>(BitCast(d_idx_shr, idx)));

    const auto byte_idx = BitwiseIfThenElse(k7, unmasked_byte_idx,
        BitCast(du8, Dup128VecFromValues(du64, uint64_t{ 0 }, uint64_t{ 0x0808080808080808u })));
    // We want to shift right by idx & 7 to extract the desired bit in `bytes`,
    // and left by iota & 7 to put it in the correct output bit. To correctly
    // handle shift counts from -7 to 7, we rotate.
    const auto rotate_left_bits = Sub(Iota(du8, uint8_t{ 0 }), BitCast(du8, idx));

    const auto extracted_bits =
        And(Rol(TableLookupBytes(v, byte_idx), rotate_left_bits), BitCast(du8, Set(du64, kExtractedBitsMask)));
    // Combine bit-sliced (one bit per byte) into one 64-bit sum.
    return BitCast(d64, SumsOf8(extracted_bits));
}

#endif // OMNI_NATIVE_BITSHUFFLE

// ================================================== Operator wrapper

// SVE* and RVV currently cannot define operators and have already defined
// (only) the corresponding functions such as Add.
#ifndef OMNI_NATIVE_OPERATOR_REPLACEMENTS
#define OMNI_NATIVE_OPERATOR_REPLACEMENTS

template <class V> OMNI_API V Add(V a, V b)
{
    return a + b;
}
template <class V> OMNI_API V Sub(V a, V b)
{
    return a - b;
}

template <class V> OMNI_API V Mul(V a, V b)
{
    return a * b;
}
template <class V> OMNI_API V Div(V a, V b)
{
    return a / b;
}
template <class V> OMNI_API V Mod(V a, V b)
{
    return a % b;
}

template <class V> V Shl(V a, V b)
{
    return a << b;
}
template <class V> V Shr(V a, V b)
{
    return a >> b;
}

template <class V> OMNI_API auto Eq(V a, V b) -> decltype(a == b)
{
    return a == b;
}
template <class V> OMNI_API auto Ne(V a, V b) -> decltype(a == b)
{
    return a != b;
}
template <class V> OMNI_API auto Lt(V a, V b) -> decltype(a == b)
{
    return a < b;
}

template <class V> OMNI_API auto Gt(V a, V b) -> decltype(a == b)
{
    return a > b;
}
template <class V> OMNI_API auto Ge(V a, V b) -> decltype(a == b)
{
    return a >= b;
}

template <class V> OMNI_API auto Le(V a, V b) -> decltype(a == b)
{
    return a <= b;
}

#endif // OMNI_NATIVE_OPERATOR_REPLACEMENTS

#undef OMNI_GENERIC_IF_EMULATED_D

} // namespace omni

#endif // OMNI_GENERIC_OPS_H