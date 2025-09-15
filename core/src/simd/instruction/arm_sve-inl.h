/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
#ifndef ARM_SVE_INL_H
#define ARM_SVE_INL_H
#include <arm_sve.h>
#include <float.h>
#include <math.h>
#include "simd/instruction/shared-inl.h"

namespace simd {
template <class V> struct DFromV_t {}; // specialized in macros
template <class V> using DFromV = typename DFromV_t<RemoveConst<V>>::type;
template <class V> using TFromV = TFromD<DFromV<V>>;

// ================================================== MACROS

// Generate specializations and function definitions using X macros. Although
// harder to read and debug, writing everything manually is too bulky.

namespace detail { // for code folding

// Args: BASE, CHAR, BITS, HALF, NAME, OP

// Unsigned:
#define OMNI_SVE_FOREACH_U08(X_MACRO, NAME, OP) X_MACRO(uint, u, 8, 8, NAME, OP)
#define OMNI_SVE_FOREACH_U16(X_MACRO, NAME, OP) X_MACRO(uint, u, 16, 8, NAME, OP)
#define OMNI_SVE_FOREACH_U32(X_MACRO, NAME, OP) X_MACRO(uint, u, 32, 16, NAME, OP)
#define OMNI_SVE_FOREACH_U64(X_MACRO, NAME, OP) X_MACRO(uint, u, 64, 32, NAME, OP)

// Signed:
#define OMNI_SVE_FOREACH_I08(X_MACRO, NAME, OP) X_MACRO(int, s, 8, 8, NAME, OP)
#define OMNI_SVE_FOREACH_I16(X_MACRO, NAME, OP) X_MACRO(int, s, 16, 8, NAME, OP)
#define OMNI_SVE_FOREACH_I32(X_MACRO, NAME, OP) X_MACRO(int, s, 32, 16, NAME, OP)
#define OMNI_SVE_FOREACH_I64(X_MACRO, NAME, OP) X_MACRO(int, s, 64, 32, NAME, OP)

// Float:
#define OMNI_SVE_FOREACH_F16(X_MACRO, NAME, OP) X_MACRO(float, f, 16, 16, NAME, OP)
#define OMNI_SVE_FOREACH_F32(X_MACRO, NAME, OP) X_MACRO(float, f, 32, 16, NAME, OP)
#define OMNI_SVE_FOREACH_F64(X_MACRO, NAME, OP) X_MACRO(float, f, 64, 32, NAME, OP)

#define OMNI_SVE_FOREACH_BF16_UNCONDITIONAL(X_MACRO, NAME, OP) X_MACRO(bfloat, bf, 16, 16, NAME, OP)

#define OMNI_SVE_FOREACH_BF16(X_MACRO, NAME, OP)
#define OMNI_SVE_IF_EMULATED_D(D) OMNI_IF_BF16_D(D)
#define OMNI_GENERIC_IF_EMULATED_D(D) OMNI_IF_BF16_D(D)
#define OMNI_SVE_IF_NOT_EMULATED_D(D) OMNI_IF_NOT_BF16_D(D)

// For all element sizes:
#define OMNI_SVE_FOREACH_U(X_MACRO, NAME, OP) \
    OMNI_SVE_FOREACH_U08(X_MACRO, NAME, OP)   \
    OMNI_SVE_FOREACH_U16(X_MACRO, NAME, OP)   \
    OMNI_SVE_FOREACH_U32(X_MACRO, NAME, OP)   \
    OMNI_SVE_FOREACH_U64(X_MACRO, NAME, OP)

#define OMNI_SVE_FOREACH_I(X_MACRO, NAME, OP) \
    OMNI_SVE_FOREACH_I08(X_MACRO, NAME, OP)   \
    OMNI_SVE_FOREACH_I16(X_MACRO, NAME, OP)   \
    OMNI_SVE_FOREACH_I32(X_MACRO, NAME, OP)   \
    OMNI_SVE_FOREACH_I64(X_MACRO, NAME, OP)

#define OMNI_SVE_FOREACH_F3264(X_MACRO, NAME, OP) \
    OMNI_SVE_FOREACH_F32(X_MACRO, NAME, OP)       \
    OMNI_SVE_FOREACH_F64(X_MACRO, NAME, OP)

// OMNI_SVE_FOREACH_F does not include OMNI_SVE_FOREACH_BF16 because SVE lacks
// bf16 overloads for some intrinsics (especially less-common arithmetic).
// However, this does include f16 because SVE supports it unconditionally.
#define OMNI_SVE_FOREACH_F(X_MACRO, NAME, OP) \
    OMNI_SVE_FOREACH_F3264(X_MACRO, NAME, OP)

// Commonly used type categories for a given element size:
#define OMNI_SVE_FOREACH_UI08(X_MACRO, NAME, OP) \
    OMNI_SVE_FOREACH_U08(X_MACRO, NAME, OP)      \
    OMNI_SVE_FOREACH_I08(X_MACRO, NAME, OP)

#define OMNI_SVE_FOREACH_UI16(X_MACRO, NAME, OP) \
    OMNI_SVE_FOREACH_U16(X_MACRO, NAME, OP)      \
    OMNI_SVE_FOREACH_I16(X_MACRO, NAME, OP)

#define OMNI_SVE_FOREACH_UI32(X_MACRO, NAME, OP) \
    OMNI_SVE_FOREACH_U32(X_MACRO, NAME, OP)      \
    OMNI_SVE_FOREACH_I32(X_MACRO, NAME, OP)

#define OMNI_SVE_FOREACH_UI64(X_MACRO, NAME, OP) \
    OMNI_SVE_FOREACH_U64(X_MACRO, NAME, OP)      \
    OMNI_SVE_FOREACH_I64(X_MACRO, NAME, OP)

#define OMNI_SVE_FOREACH_UIF3264(X_MACRO, NAME, OP) \
    OMNI_SVE_FOREACH_UI32(X_MACRO, NAME, OP)        \
    OMNI_SVE_FOREACH_UI64(X_MACRO, NAME, OP)        \
    OMNI_SVE_FOREACH_F3264(X_MACRO, NAME, OP)

// Commonly used type categories:
#define OMNI_SVE_FOREACH_UI(X_MACRO, NAME, OP) \
    OMNI_SVE_FOREACH_U(X_MACRO, NAME, OP)      \
    OMNI_SVE_FOREACH_I(X_MACRO, NAME, OP)

#define OMNI_SVE_FOREACH_IF(X_MACRO, NAME, OP) \
    OMNI_SVE_FOREACH_I(X_MACRO, NAME, OP)      \
    OMNI_SVE_FOREACH_F(X_MACRO, NAME, OP)

#define OMNI_SVE_FOREACH(X_MACRO, NAME, OP) \
    OMNI_SVE_FOREACH_U(X_MACRO, NAME, OP)   \
    OMNI_SVE_FOREACH_I(X_MACRO, NAME, OP)   \
    OMNI_SVE_FOREACH_F(X_MACRO, NAME, OP)

// Assemble types for use in x-macros
#define OMNI_SVE_T(BASE, BITS) BASE##BITS##_t
#define OMNI_SVE_D(BASE, BITS, N, POW2) Simd<OMNI_SVE_T(BASE, BITS), N, POW2>
#define OMNI_SVE_V(BASE, BITS) sv##BASE##BITS##_t
#define OMNI_SVE_TUPLE(BASE, BITS, MUL) sv##BASE##BITS##x##MUL##_t
} // namespace detail

#define OMNI_SPECIALIZE(BASE, CHAR, BITS, HALF, NAME, OP) \
    template <> struct DFromV_t<OMNI_SVE_V(BASE, BITS)> { \
        using type = ScalableTag<OMNI_SVE_T(BASE, BITS)>; \
    };

OMNI_SVE_FOREACH(OMNI_SPECIALIZE, _, _)

OMNI_SVE_FOREACH_BF16_UNCONDITIONAL(OMNI_SPECIALIZE, _, _)

#undef OMNI_SPECIALIZE

// Note: _x (don't-care value for inactive lanes) avoids additional MOVPRFX
// instructions, and we anyway only use it when the predicate is ptrue.

// vector = f(vector), e.g. Not
#define OMNI_SVE_RETV_ARGPV(BASE, CHAR, BITS, HALF, NAME, OP)      \
    OMNI_API OMNI_SVE_V(BASE, BITS) NAME(OMNI_SVE_V(BASE, BITS) v) \
    {                                                              \
        return sv##OP##_##CHAR##BITS##_x(OMNI_SVE_PTRUE(BITS), v); \
    }
#define OMNI_SVE_RETV_ARGV(BASE, CHAR, BITS, HALF, NAME, OP)       \
    OMNI_API OMNI_SVE_V(BASE, BITS) NAME(OMNI_SVE_V(BASE, BITS) v) \
    {                                                              \
        return sv##OP##_##CHAR##BITS(v);                           \
    }

// vector = f(vector, scalar), e.g. detail::AddN
#define OMNI_SVE_RETV_ARGPVN(BASE, CHAR, BITS, HALF, NAME, OP)                               \
    OMNI_API OMNI_SVE_V(BASE, BITS) NAME(OMNI_SVE_V(BASE, BITS) a, OMNI_SVE_T(BASE, BITS) b) \
    {                                                                                        \
        return sv##OP##_##CHAR##BITS##_x(OMNI_SVE_PTRUE(BITS), a, b);                        \
    }
#define OMNI_SVE_RETV_ARGVN(BASE, CHAR, BITS, HALF, NAME, OP)                                \
    OMNI_API OMNI_SVE_V(BASE, BITS) NAME(OMNI_SVE_V(BASE, BITS) a, OMNI_SVE_T(BASE, BITS) b) \
    {                                                                                        \
        return sv##OP##_##CHAR##BITS(a, b);                                                  \
    }

// vector = f(vector, vector), e.g. Add
#define OMNI_SVE_RETV_ARGVV(BASE, CHAR, BITS, HALF, NAME, OP)                                \
    OMNI_API OMNI_SVE_V(BASE, BITS) NAME(OMNI_SVE_V(BASE, BITS) a, OMNI_SVE_V(BASE, BITS) b) \
    {                                                                                        \
        return sv##OP##_##CHAR##BITS(a, b);                                                  \
    }
// All-true mask
#define OMNI_SVE_RETV_ARGPVV(BASE, CHAR, BITS, HALF, NAME, OP)                               \
    OMNI_API OMNI_SVE_V(BASE, BITS) NAME(OMNI_SVE_V(BASE, BITS) a, OMNI_SVE_V(BASE, BITS) b) \
    {                                                                                        \
        return sv##OP##_##CHAR##BITS##_x(OMNI_SVE_PTRUE(BITS), a, b);                        \
    }
// User-specified mask. Mask=false value is undefined and must be set by caller
// because SVE instructions take it from one of the two inputs, whereas
// AVX-512, RVV and Simd allow a third argument.
#define OMNI_SVE_RETV_ARGMVV(BASE, CHAR, BITS, HALF, NAME, OP)                                           \
    OMNI_API OMNI_SVE_V(BASE, BITS) NAME(svbool_t m, OMNI_SVE_V(BASE, BITS) a, OMNI_SVE_V(BASE, BITS) b) \
    {                                                                                                    \
        return sv##OP##_##CHAR##BITS##_x(m, a, b);                                                       \
    }

#define OMNI_SVE_RETV_ARGVVV(BASE, CHAR, BITS, HALF, NAME, OP)                                                         \
    OMNI_API OMNI_SVE_V(BASE, BITS) NAME(OMNI_SVE_V(BASE, BITS) a, OMNI_SVE_V(BASE, BITS) b, OMNI_SVE_V(BASE, BITS) c) \
    {                                                                                                                  \
        return sv##OP##_##CHAR##BITS(a, b, c);                                                                         \
    }

// ------------------------------ Lanes
namespace detail {
// Returns actual lanes of a hardware vector without rounding to a power of two.
template <typename T, OMNI_IF_T_SIZE(T, 1)> OMNI_INLINE size_t AllHardwareLanes()
{
    return svcntb_pat(SV_ALL);
}

template <typename T, OMNI_IF_T_SIZE(T, 2)> OMNI_INLINE size_t AllHardwareLanes()
{
    return svcnth_pat(SV_ALL);
}

template <typename T, OMNI_IF_T_SIZE(T, 4)> OMNI_INLINE size_t AllHardwareLanes()
{
    return svcntw_pat(SV_ALL);
}

template <typename T, OMNI_IF_T_SIZE(T, 8)> OMNI_INLINE size_t AllHardwareLanes()
{
    return svcntd_pat(SV_ALL);
}

// All-true mask from a macro
#define OMNI_SVE_ALL_PTRUE(BITS) svptrue_b##BITS()
#define OMNI_SVE_PTRUE(BITS) svptrue_b##BITS()
} // namespace detail

// ================================================== MASK INIT

// One mask bit per byte; only the one belonging to the lowest byte is valid.

// ------------------------------ FirstN
#define OMNI_SVE_FIRSTN(BASE, CHAR, BITS, HALF, NAME, OP)                                                   \
    template <size_t N, int kPow2> OMNI_API svbool_t NAME(OMNI_SVE_D(BASE, BITS, N, kPow2) d, size_t count) \
    {                                                                                                       \
        const size_t limit = detail::IsFull(d) ? count : OMNI_MIN(Lanes(d), count);                         \
        return sv##OP##_b##BITS##_u32(uint32_t{ 0 }, static_cast<uint32_t>(limit));                         \
    }

OMNI_SVE_FOREACH(OMNI_SVE_FIRSTN, FirstN, whilelt)

OMNI_SVE_FOREACH_BF16_UNCONDITIONAL(OMNI_SVE_FIRSTN, FirstN, whilelt)

template <class D, OMNI_SVE_IF_EMULATED_D(D)> svbool_t FirstN(D /* tag */, size_t count)
{
    return FirstN(RebindToUnsigned<D>(), count);
}

#undef OMNI_SVE_FIRSTN

template <class D> using MFromD = svbool_t;

namespace detail {
#define OMNI_SVE_WRAP_PTRUE(BASE, CHAR, BITS, HALF, NAME, OP)                                            \
    template <size_t N, int kPow2> OMNI_API svbool_t NAME(OMNI_SVE_D(BASE, BITS, N, kPow2) /* d */)      \
    {                                                                                                    \
        return OMNI_SVE_PTRUE(BITS);                                                                     \
    }                                                                                                    \
    template <size_t N, int kPow2> OMNI_API svbool_t All##NAME(OMNI_SVE_D(BASE, BITS, N, kPow2) /* d */) \
    {                                                                                                    \
        return OMNI_SVE_ALL_PTRUE(BITS);                                                                 \
    }

OMNI_SVE_FOREACH(OMNI_SVE_WRAP_PTRUE, PTrue, ptrue) // return all-true
OMNI_SVE_FOREACH_BF16_UNCONDITIONAL(OMNI_SVE_WRAP_PTRUE, PTrue, ptrue)

#undef OMNI_SVE_WRAP_PTRUE

OMNI_API svbool_t PFalse()
{
    return svpfalse_b();
}

// Returns all-true if d is OMNI_FULL or FirstN(N) after capping N.
// This is used in functions that load/store memory; other functions (e.g.
// arithmetic) can ignore d and use PTrue instead.
template <class D> svbool_t MakeMask(D d)
{
    return IsFull(d) ? PTrue(d) : FirstN(d, Lanes(d));
}
} // namespace detail

#ifdef OMNI_NATIVE_MASK_FALSE
#undef OMNI_NATIVE_MASK_FALSE
#else
#define OMNI_NATIVE_MASK_FALSE
#endif

template <class D> OMNI_API svbool_t MaskFalse(const D /* d */)
{
    return detail::PFalse();
}

// ================================================== INIT

// ------------------------------ Set
// vector = f(d, scalar), e.g. Set
#define OMNI_SVE_SET(BASE, CHAR, BITS, HALF, NAME, OP)                                                         \
    template <size_t N, int kPow2>                                                                             \
    OMNI_API OMNI_SVE_V(BASE, BITS) NAME(OMNI_SVE_D(BASE, BITS, N, kPow2) /* d */, OMNI_SVE_T(BASE, BITS) arg) \
    {                                                                                                          \
        return sv##OP##_##CHAR##BITS(arg);                                                                     \
    }

OMNI_SVE_FOREACH(OMNI_SVE_SET, Set, dup_n)

// Required for Zero and VFromD
template <class D, OMNI_IF_BF16_D(D)> OMNI_API svbfloat16_t Set(D d, bfloat16_t arg)
{
    return svreinterpret_bf16_u16(Set(RebindToUnsigned<decltype(d)>(), BitCastScalar<uint16_t>(arg)));
}

#undef OMNI_SVE_SET

template <class D> using VFromD = decltype(Set(D(), TFromD<D>()));

using VBF16 = VFromD<ScalableTag<bfloat16_t>>;

// ------------------------------ Zero

template <class D> VFromD<D> Zero(D d)
{
    // Cast to support bfloat16_t.
    const RebindToUnsigned<decltype(d)> du;
    return BitCast(d, Set(du, 0));
}

// ------------------------------ BitCast

namespace detail {
// u8: no change
#define OMNI_SVE_CAST_NOP(BASE, CHAR, BITS, HALF, NAME, OP)                                 \
    OMNI_API OMNI_SVE_V(BASE, BITS) BitCastToByte(OMNI_SVE_V(BASE, BITS) v)                 \
    {                                                                                       \
        return v;                                                                           \
    }                                                                                       \
    template <size_t N, int kPow2>                                                          \
    OMNI_API OMNI_SVE_V(BASE, BITS)                                                         \
        BitCastFromByte(OMNI_SVE_D(BASE, BITS, N, kPow2) /* d */, OMNI_SVE_V(BASE, BITS) v) \
    {                                                                                       \
        return v;                                                                           \
    }

// All other types
#define OMNI_SVE_CAST(BASE, CHAR, BITS, HALF, NAME, OP)                                                       \
    OMNI_INLINE svuint8_t BitCastToByte(OMNI_SVE_V(BASE, BITS) v)                                             \
    {                                                                                                         \
        return sv##OP##_u8_##CHAR##BITS(v);                                                                   \
    }                                                                                                         \
    template <size_t N, int kPow2>                                                                            \
    OMNI_INLINE OMNI_SVE_V(BASE, BITS) BitCastFromByte(OMNI_SVE_D(BASE, BITS, N, kPow2) /* d */, svuint8_t v) \
    {                                                                                                         \
        return sv##OP##_##CHAR##BITS##_u8(v);                                                                 \
    }

// U08 is special-cased, hence do not use FOREACH.
OMNI_SVE_FOREACH_U08(OMNI_SVE_CAST_NOP, _, _)

OMNI_SVE_FOREACH_I08(OMNI_SVE_CAST, _, reinterpret)

OMNI_SVE_FOREACH_UI16(OMNI_SVE_CAST, _, reinterpret)

OMNI_SVE_FOREACH_UI32(OMNI_SVE_CAST, _, reinterpret)

OMNI_SVE_FOREACH_UI64(OMNI_SVE_CAST, _, reinterpret)

OMNI_SVE_FOREACH_F(OMNI_SVE_CAST, _, reinterpret)

OMNI_SVE_FOREACH_BF16_UNCONDITIONAL(OMNI_SVE_CAST, _, reinterpret)

#undef OMNI_SVE_CAST_NOP
#undef OMNI_SVE_CAST
} // namespace detail

template <class D, class FromV> OMNI_API VFromD<D> BitCast(D d, FromV v)
{
    return detail::BitCastFromByte(d, detail::BitCastToByte(v));
}

// ------------------------------ Undefined

#define OMNI_SVE_UNDEFINED(BASE, CHAR, BITS, HALF, NAME, OP)                                                      \
    template <size_t N, int kPow2> OMNI_API OMNI_SVE_V(BASE, BITS) NAME(OMNI_SVE_D(BASE, BITS, N, kPow2) /* d */) \
    {                                                                                                             \
        return sv##OP##_##CHAR##BITS();                                                                           \
    }

OMNI_SVE_FOREACH(OMNI_SVE_UNDEFINED, Undefined, undef)

OMNI_SVE_FOREACH_BF16_UNCONDITIONAL(OMNI_SVE_UNDEFINED, Undefined, undef)

template <class D, OMNI_SVE_IF_EMULATED_D(D)> VFromD<D> Undefined(D d)
{
    const RebindToUnsigned<D> du;
    return BitCast(d, Undefined(du));
}

// ------------------------------ Tuple

// tuples = f(d, v..), e.g. Create2
#define OMNI_SVE_CREATE(BASE, CHAR, BITS, HALF, NAME, OP)                                                           \
    template <size_t N, int kPow2>                                                                                  \
    OMNI_API OMNI_SVE_TUPLE(BASE, BITS, 2)                                                                          \
        NAME##2(OMNI_SVE_D(BASE, BITS, N, kPow2) /* d */, OMNI_SVE_V(BASE, BITS) v0, OMNI_SVE_V(BASE, BITS) v1)     \
    {                                                                                                               \
        return sv##OP##2_##CHAR##BITS(v0, v1);                                                                      \
    }                                                                                                               \
    template <size_t N, int kPow2>                                                                                  \
    OMNI_API OMNI_SVE_TUPLE(BASE, BITS, 3) NAME##3(OMNI_SVE_D(BASE, BITS, N, kPow2) /* d */,                        \
        OMNI_SVE_V(BASE, BITS) v0, OMNI_SVE_V(BASE, BITS) v1, OMNI_SVE_V(BASE, BITS) v2)                            \
    {                                                                                                               \
        return sv##OP##3_##CHAR##BITS(v0, v1, v2);                                                                  \
    }                                                                                                               \
    template <size_t N, int kPow2>                                                                                  \
    OMNI_API OMNI_SVE_TUPLE(BASE, BITS, 4) NAME##4(OMNI_SVE_D(BASE, BITS, N, kPow2) /* d */,                        \
        OMNI_SVE_V(BASE, BITS) v0, OMNI_SVE_V(BASE, BITS) v1, OMNI_SVE_V(BASE, BITS) v2, OMNI_SVE_V(BASE, BITS) v3) \
    {                                                                                                               \
        return sv##OP##4_##CHAR##BITS(v0, v1, v2, v3);                                                              \
    }

OMNI_SVE_FOREACH(OMNI_SVE_CREATE, Create, create)

OMNI_SVE_FOREACH_BF16_UNCONDITIONAL(OMNI_SVE_CREATE, Create, create)

#undef OMNI_SVE_CREATE

template <class D> using Vec2 = decltype(Create2(D(), Zero(D()), Zero(D())));
template <class D> using Vec3 = decltype(Create3(D(), Zero(D()), Zero(D()), Zero(D())));
template <class D> using Vec4 = decltype(Create4(D(), Zero(D()), Zero(D()), Zero(D()), Zero(D())));

#define OMNI_SVE_GET(BASE, CHAR, BITS, HALF, NAME, OP)                                                    \
    template <size_t kIndex> OMNI_API OMNI_SVE_V(BASE, BITS) NAME##2(OMNI_SVE_TUPLE(BASE, BITS, 2) tuple) \
    {                                                                                                     \
        return sv##OP##2_##CHAR##BITS(tuple, kIndex);                                                     \
    }                                                                                                     \
    template <size_t kIndex> OMNI_API OMNI_SVE_V(BASE, BITS) NAME##3(OMNI_SVE_TUPLE(BASE, BITS, 3) tuple) \
    {                                                                                                     \
        return sv##OP##3_##CHAR##BITS(tuple, kIndex);                                                     \
    }                                                                                                     \
    template <size_t kIndex> OMNI_API OMNI_SVE_V(BASE, BITS) NAME##4(OMNI_SVE_TUPLE(BASE, BITS, 4) tuple) \
    {                                                                                                     \
        return sv##OP##4_##CHAR##BITS(tuple, kIndex);                                                     \
    }

OMNI_SVE_FOREACH(OMNI_SVE_GET, Get, get)

OMNI_SVE_FOREACH_BF16_UNCONDITIONAL(OMNI_SVE_GET, Get, get)

#undef OMNI_SVE_GET

#define OMNI_SVE_SET(BASE, CHAR, BITS, HALF, NAME, OP)                                                              \
    template <size_t kIndex>                                                                                        \
    OMNI_API OMNI_SVE_TUPLE(BASE, BITS, 2) NAME##2(OMNI_SVE_TUPLE(BASE, BITS, 2) tuple, OMNI_SVE_V(BASE, BITS) vec) \
    {                                                                                                               \
        return sv##OP##2_##CHAR##BITS(tuple, kIndex, vec);                                                          \
    }                                                                                                               \
    template <size_t kIndex>                                                                                        \
    OMNI_API OMNI_SVE_TUPLE(BASE, BITS, 3) NAME##3(OMNI_SVE_TUPLE(BASE, BITS, 3) tuple, OMNI_SVE_V(BASE, BITS) vec) \
    {                                                                                                               \
        return sv##OP##3_##CHAR##BITS(tuple, kIndex, vec);                                                          \
    }                                                                                                               \
    template <size_t kIndex>                                                                                        \
    OMNI_API OMNI_SVE_TUPLE(BASE, BITS, 4) NAME##4(OMNI_SVE_TUPLE(BASE, BITS, 4) tuple, OMNI_SVE_V(BASE, BITS) vec) \
    {                                                                                                               \
        return sv##OP##4_##CHAR##BITS(tuple, kIndex, vec);                                                          \
    }

OMNI_SVE_FOREACH(OMNI_SVE_SET, Set, set)

OMNI_SVE_FOREACH_BF16_UNCONDITIONAL(OMNI_SVE_SET, Set, set)

#undef OMNI_SVE_SET

// ------------------------------ ResizeBitCast

// Same as BitCast on SVE
template <class D, class FromV> OMNI_API VFromD<D> ResizeBitCast(D d, FromV v)
{
    return BitCast(d, v);
}

// ------------------------------ Dup128VecFromValues

template <class D, OMNI_IF_I8_D(D)>
OMNI_API svint8_t Dup128VecFromValues(D /* d */, TFromD<D> t0, TFromD<D> t1, TFromD<D> t2, TFromD<D> t3, TFromD<D> t4,
    TFromD<D> t5, TFromD<D> t6, TFromD<D> t7, TFromD<D> t8, TFromD<D> t9, TFromD<D> t10, TFromD<D> t11, TFromD<D> t12,
    TFromD<D> t13, TFromD<D> t14, TFromD<D> t15)
{
    return svdupq_n_s8(t0, t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, t14, t15);
}

template <class D, OMNI_IF_U8_D(D)>
OMNI_API svuint8_t Dup128VecFromValues(D /* d */, TFromD<D> t0, TFromD<D> t1, TFromD<D> t2, TFromD<D> t3, TFromD<D> t4,
    TFromD<D> t5, TFromD<D> t6, TFromD<D> t7, TFromD<D> t8, TFromD<D> t9, TFromD<D> t10, TFromD<D> t11, TFromD<D> t12,
    TFromD<D> t13, TFromD<D> t14, TFromD<D> t15)
{
    return svdupq_n_u8(t0, t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, t14, t15);
}

template <class D, OMNI_IF_I16_D(D)>
OMNI_API svint16_t Dup128VecFromValues(D /* d */, TFromD<D> t0, TFromD<D> t1, TFromD<D> t2, TFromD<D> t3, TFromD<D> t4,
    TFromD<D> t5, TFromD<D> t6, TFromD<D> t7)
{
    return svdupq_n_s16(t0, t1, t2, t3, t4, t5, t6, t7);
}

template <class D, OMNI_IF_U16_D(D)>
OMNI_API svuint16_t Dup128VecFromValues(D /* d */, TFromD<D> t0, TFromD<D> t1, TFromD<D> t2, TFromD<D> t3, TFromD<D> t4,
    TFromD<D> t5, TFromD<D> t6, TFromD<D> t7)
{
    return svdupq_n_u16(t0, t1, t2, t3, t4, t5, t6, t7);
}

template <class D, OMNI_IF_F16_D(D)>
OMNI_API svfloat16_t Dup128VecFromValues(D /* d */, TFromD<D> t0, TFromD<D> t1, TFromD<D> t2, TFromD<D> t3,
    TFromD<D> t4, TFromD<D> t5, TFromD<D> t6, TFromD<D> t7)
{
    return svdupq_n_f16(t0, t1, t2, t3, t4, t5, t6, t7);
}

template <class D, OMNI_IF_BF16_D(D)>
OMNI_API VBF16 Dup128VecFromValues(D d, TFromD<D> t0, TFromD<D> t1, TFromD<D> t2, TFromD<D> t3, TFromD<D> t4,
    TFromD<D> t5, TFromD<D> t6, TFromD<D> t7)
{
    const RebindToUnsigned<decltype(d)> du;
    return BitCast(d, Dup128VecFromValues(du, BitCastScalar<uint16_t>(t0), BitCastScalar<uint16_t>(t1),
        BitCastScalar<uint16_t>(t2), BitCastScalar<uint16_t>(t3), BitCastScalar<uint16_t>(t4),
        BitCastScalar<uint16_t>(t5), BitCastScalar<uint16_t>(t6), BitCastScalar<uint16_t>(t7)));
}

template <class D, OMNI_IF_I32_D(D)>
OMNI_API svint32_t Dup128VecFromValues(D /* d */, TFromD<D> t0, TFromD<D> t1, TFromD<D> t2, TFromD<D> t3)
{
    return svdupq_n_s32(t0, t1, t2, t3);
}

template <class D, OMNI_IF_U32_D(D)>
OMNI_API svuint32_t Dup128VecFromValues(D /* d */, TFromD<D> t0, TFromD<D> t1, TFromD<D> t2, TFromD<D> t3)
{
    return svdupq_n_u32(t0, t1, t2, t3);
}

template <class D, OMNI_IF_F32_D(D)>
OMNI_API svfloat32_t Dup128VecFromValues(D /* d */, TFromD<D> t0, TFromD<D> t1, TFromD<D> t2, TFromD<D> t3)
{
    return svdupq_n_f32(t0, t1, t2, t3);
}

template <class D, OMNI_IF_I64_D(D)> OMNI_API svint64_t Dup128VecFromValues(D /* d */, TFromD<D> t0, TFromD<D> t1)
{
    return svdupq_n_s64(t0, t1);
}

template <class D, OMNI_IF_U64_D(D)> OMNI_API svuint64_t Dup128VecFromValues(D /* d */, TFromD<D> t0, TFromD<D> t1)
{
    return svdupq_n_u64(t0, t1);
}

template <class D, OMNI_IF_F64_D(D)> OMNI_API svfloat64_t Dup128VecFromValues(D /* d */, TFromD<D> t0, TFromD<D> t1)
{
    return svdupq_n_f64(t0, t1);
}

// ================================================== LOGICAL

// detail::*N() functions accept a scalar argument to avoid extra Set().

// ------------------------------ Not
OMNI_SVE_FOREACH_UI(OMNI_SVE_RETV_ARGPV, Not, not) // NOLINT

// ------------------------------ And

namespace detail {
OMNI_SVE_FOREACH_UI(OMNI_SVE_RETV_ARGPVN, AndN, and_n)
} // namespace detail

OMNI_SVE_FOREACH_UI(OMNI_SVE_RETV_ARGPVV, And, and)

template <class V, OMNI_IF_FLOAT_V(V)> OMNI_API V And(const V a, const V b)
{
    const DFromV<V> df;
    const RebindToUnsigned<decltype(df)> du;
    return BitCast(df, And(BitCast(du, a), BitCast(du, b)));
}

// ------------------------------ Or

namespace detail {
OMNI_SVE_FOREACH_UI(OMNI_SVE_RETV_ARGPVN, OrN, orr_n)
} // namespace detail

OMNI_SVE_FOREACH_UI(OMNI_SVE_RETV_ARGPVV, Or, orr)

template <class V, OMNI_IF_FLOAT_V(V)> OMNI_API V Or(const V a, const V b)
{
    const DFromV<V> df;
    const RebindToUnsigned<decltype(df)> du;
    return BitCast(df, Or(BitCast(du, a), BitCast(du, b)));
}

// ------------------------------ Xor

namespace detail {
OMNI_SVE_FOREACH_UI(OMNI_SVE_RETV_ARGPVN, XorN, eor_n)
} // namespace detail

OMNI_SVE_FOREACH_UI(OMNI_SVE_RETV_ARGPVV, Xor, eor)

template <class V, OMNI_IF_FLOAT_V(V)> OMNI_API V Xor(const V a, const V b)
{
    const DFromV<V> df;
    const RebindToUnsigned<decltype(df)> du;
    return BitCast(df, Xor(BitCast(du, a), BitCast(du, b)));
}

// ------------------------------ AndNot

namespace detail {
#define OMNI_SVE_RETV_ARGPVN_SWAP(BASE, CHAR, BITS, HALF, NAME, OP)                          \
    OMNI_API OMNI_SVE_V(BASE, BITS) NAME(OMNI_SVE_T(BASE, BITS) a, OMNI_SVE_V(BASE, BITS) b) \
    {                                                                                        \
        return sv##OP##_##CHAR##BITS##_x(OMNI_SVE_PTRUE(BITS), b, a);                        \
    }

OMNI_SVE_FOREACH_UI(OMNI_SVE_RETV_ARGPVN_SWAP, AndNotN, bic_n)

#undef OMNI_SVE_RETV_ARGPVN_SWAP
} // namespace detail

#define OMNI_SVE_RETV_ARGPVV_SWAP(BASE, CHAR, BITS, HALF, NAME, OP)                          \
    OMNI_API OMNI_SVE_V(BASE, BITS) NAME(OMNI_SVE_V(BASE, BITS) a, OMNI_SVE_V(BASE, BITS) b) \
    {                                                                                        \
        return sv##OP##_##CHAR##BITS##_x(OMNI_SVE_PTRUE(BITS), b, a);                        \
    }

OMNI_SVE_FOREACH_UI(OMNI_SVE_RETV_ARGPVV_SWAP, AndNot, bic)

#undef OMNI_SVE_RETV_ARGPVV_SWAP

template <class V, OMNI_IF_FLOAT_V(V)> OMNI_API V AndNot(const V a, const V b)
{
    const DFromV<V> df;
    const RebindToUnsigned<decltype(df)> du;
    return BitCast(df, AndNot(BitCast(du, a), BitCast(du, b)));
}

// ------------------------------ Xor3
template <class V> OMNI_API V Xor3(V x1, V x2, V x3)
{
    return Xor(x1, Xor(x2, x3));
}

// ------------------------------ Or3
template <class V> OMNI_API V Or3(V o1, V o2, V o3)
{
    return Or(o1, Or(o2, o3));
}

// ------------------------------ OrAnd
template <class V> OMNI_API V OrAnd(const V o, const V a1, const V a2)
{
    return Or(o, And(a1, a2));
}

// ------------------------------ PopulationCount

#ifdef OMNI_NATIVE_POPCNT
#undef OMNI_NATIVE_POPCNT
#else
#define OMNI_NATIVE_POPCNT
#endif

// Need to return original type instead of unsigned.
#define OMNI_SVE_POPCNT(BASE, CHAR, BITS, HALF, NAME, OP)                                          \
    OMNI_API OMNI_SVE_V(BASE, BITS) NAME(OMNI_SVE_V(BASE, BITS) v)                                 \
    {                                                                                              \
        return BitCast(DFromV<decltype(v)>(), sv##OP##_##CHAR##BITS##_x(OMNI_SVE_PTRUE(BITS), v)); \
    }

OMNI_SVE_FOREACH_UI(OMNI_SVE_POPCNT, PopulationCount, cnt)

#undef OMNI_SVE_POPCNT

// ================================================== SIGN

// ------------------------------ Neg
OMNI_SVE_FOREACH_IF(OMNI_SVE_RETV_ARGPV, Neg, neg)

OMNI_API VBF16 Neg(VBF16 v)
{
    const DFromV<decltype(v)> d;
    const RebindToUnsigned<decltype(d)> du;
    using TU = TFromD<decltype(du)>;
    return BitCast(d, Xor(BitCast(du, v), Set(du, SignMask<TU>())));
}

// ------------------------------ Abs
OMNI_SVE_FOREACH_IF(OMNI_SVE_RETV_ARGPV, Abs, abs)

// ================================================== ARITHMETIC

// Per-target flags to prevent generic_ops-inl.h defining Add etc.
#ifdef OMNI_NATIVE_OPERATOR_REPLACEMENTS
#undef OMNI_NATIVE_OPERATOR_REPLACEMENTS
#else
#define OMNI_NATIVE_OPERATOR_REPLACEMENTS
#endif

// ------------------------------ Add

namespace detail {
OMNI_SVE_FOREACH(OMNI_SVE_RETV_ARGPVN, AddN, add_n)
} // namespace detail

OMNI_SVE_FOREACH(OMNI_SVE_RETV_ARGPVV, Add, add)

// ------------------------------ Sub

namespace detail {
// Can't use OMNI_SVE_RETV_ARGPVN because caller wants to specify pg.
#define OMNI_SVE_RETV_ARGPVN_MASK(BASE, CHAR, BITS, HALF, NAME, OP)                                       \
    OMNI_API OMNI_SVE_V(BASE, BITS) NAME(svbool_t pg, OMNI_SVE_V(BASE, BITS) a, OMNI_SVE_T(BASE, BITS) b) \
    {                                                                                                     \
        return sv##OP##_##CHAR##BITS##_z(pg, a, b);                                                       \
    }

OMNI_SVE_FOREACH(OMNI_SVE_RETV_ARGPVN_MASK, SubN, sub_n)

#undef OMNI_SVE_RETV_ARGPVN_MASK
} // namespace detail

OMNI_SVE_FOREACH(OMNI_SVE_RETV_ARGPVV, Sub, sub)

// ------------------------------ SumsOf8
OMNI_API svuint64_t SumsOf8(const svuint8_t v)
{
    const ScalableTag<uint32_t> du32;
    const ScalableTag<uint64_t> du64;
    const svbool_t pg = detail::PTrue(du64);

    const svuint32_t sums_of_4 = svdot_n_u32(Zero(du32), v, 1);
    // Compute pairwise sum of u32 and extend to u64.

    const svuint64_t hi = svlsr_n_u64_x(pg, BitCast(du64, sums_of_4), 32);
    // Isolate the lower 32 bits (to be added to the upper 32 and zero-extended)
    const svuint64_t lo = svextw_u64_x(pg, BitCast(du64, sums_of_4));
    return Add(hi, lo);
}

OMNI_API svint64_t SumsOf8(const svint8_t v)
{
    const ScalableTag<int32_t> di32;
    const ScalableTag<int64_t> di64;
    const svbool_t pg = detail::PTrue(di64);

    const svint32_t sums_of_4 = svdot_n_s32(Zero(di32), v, 1);
    const svint64_t hi = svasr_n_s64_x(pg, BitCast(di64, sums_of_4), 32);
    // Isolate the lower 32 bits (to be added to the upper 32 and sign-extended)
    const svint64_t lo = svextw_s64_x(pg, BitCast(di64, sums_of_4));
    return Add(hi, lo);
}

// ------------------------------ SumsOf4
namespace detail {
OMNI_INLINE svint32_t SumsOf4(simd::SignedTag /* type_tag */, simd::SizeTag<1> /* lane_size_tag */, svint8_t v)
{
    return svdot_n_s32(Zero(ScalableTag<int32_t>()), v, 1);
}

OMNI_INLINE svuint32_t SumsOf4(simd::UnsignedTag /* type_tag */, simd::SizeTag<1> /* lane_size_tag */, svuint8_t v)
{
    return svdot_n_u32(Zero(ScalableTag<uint32_t>()), v, 1);
}

OMNI_INLINE svint64_t SumsOf4(simd::SignedTag /* type_tag */, simd::SizeTag<2> /* lane_size_tag */, svint16_t v)
{
    return svdot_n_s64(Zero(ScalableTag<int64_t>()), v, 1);
}

OMNI_INLINE svuint64_t SumsOf4(simd::UnsignedTag /* type_tag */, simd::SizeTag<2> /* lane_size_tag */, svuint16_t v)
{
    return svdot_n_u64(Zero(ScalableTag<uint64_t>()), v, 1);
}
} // namespace detail

// ------------------------------ SaturatedAdd

#ifdef OMNI_NATIVE_I32_SATURATED_ADDSUB
#undef OMNI_NATIVE_I32_SATURATED_ADDSUB
#else
#define OMNI_NATIVE_I32_SATURATED_ADDSUB
#endif

#ifdef OMNI_NATIVE_U32_SATURATED_ADDSUB
#undef OMNI_NATIVE_U32_SATURATED_ADDSUB
#else
#define OMNI_NATIVE_U32_SATURATED_ADDSUB
#endif

#ifdef OMNI_NATIVE_I64_SATURATED_ADDSUB
#undef OMNI_NATIVE_I64_SATURATED_ADDSUB
#else
#define OMNI_NATIVE_I64_SATURATED_ADDSUB
#endif

#ifdef OMNI_NATIVE_U64_SATURATED_ADDSUB
#undef OMNI_NATIVE_U64_SATURATED_ADDSUB
#else
#define OMNI_NATIVE_U64_SATURATED_ADDSUB
#endif

OMNI_SVE_FOREACH_UI(OMNI_SVE_RETV_ARGVV, SaturatedAdd, qadd)

// ------------------------------ SaturatedSub
OMNI_SVE_FOREACH_UI(OMNI_SVE_RETV_ARGVV, SaturatedSub, qsub)

// ------------------------------ AbsDiff
#ifdef OMNI_NATIVE_INTEGER_ABS_DIFF
#undef OMNI_NATIVE_INTEGER_ABS_DIFF
#else
#define OMNI_NATIVE_INTEGER_ABS_DIFF
#endif

OMNI_SVE_FOREACH(OMNI_SVE_RETV_ARGPVV, AbsDiff, abd)

// ------------------------------ ShiftLeft[Same]

#define OMNI_SVE_SHIFT_N(BASE, CHAR, BITS, HALF, NAME, OP)                                                    \
    template <int kBits> OMNI_API OMNI_SVE_V(BASE, BITS) NAME(OMNI_SVE_V(BASE, BITS) v)                       \
    {                                                                                                         \
        return sv##OP##_##CHAR##BITS##_x(OMNI_SVE_PTRUE(BITS), v, kBits);                                     \
    }                                                                                                         \
    OMNI_API OMNI_SVE_V(BASE, BITS) NAME##Same(OMNI_SVE_V(BASE, BITS) v, int bits)                            \
    {                                                                                                         \
        return sv##OP##_##CHAR##BITS##_x(OMNI_SVE_PTRUE(BITS), v, static_cast<OMNI_SVE_T(uint, BITS)>(bits)); \
    }

OMNI_SVE_FOREACH_UI(OMNI_SVE_SHIFT_N, ShiftLeft, lsl_n)

// ------------------------------ ShiftRight[Same]
OMNI_SVE_FOREACH_U(OMNI_SVE_SHIFT_N, ShiftRight, lsr_n)

OMNI_SVE_FOREACH_I(OMNI_SVE_SHIFT_N, ShiftRight, asr_n)

#undef OMNI_SVE_SHIFT_N

// ------------------------------ RotateRight
template <int kBits, class V, OMNI_IF_NOT_FLOAT_NOR_SPECIAL_V(V)> OMNI_API V RotateRight(const V v)
{
    const DFromV<decltype(v)> d;
    const RebindToUnsigned<decltype(d)> du;

    constexpr size_t kSizeInBits = sizeof(TFromV<V>) * 8;
    static_assert(0 <= kBits && kBits < kSizeInBits, "Invalid shift count");
    if (kBits == 0)
        return v;

    return Or(BitCast(d, ShiftRight<kBits>(BitCast(du, v))),
        ShiftLeft<OMNI_MIN(kSizeInBits - 1, kSizeInBits - kBits)>(v));
}

// ------------------------------ Shl/r

#define OMNI_SVE_SHIFT(BASE, CHAR, BITS, HALF, NAME, OP)                                        \
    OMNI_API OMNI_SVE_V(BASE, BITS) NAME(OMNI_SVE_V(BASE, BITS) v, OMNI_SVE_V(BASE, BITS) bits) \
    {                                                                                           \
        const RebindToUnsigned<DFromV<decltype(v)>> du;                                         \
        return sv##OP##_##CHAR##BITS##_x(OMNI_SVE_PTRUE(BITS), v, BitCast(du, bits));           \
    }

OMNI_SVE_FOREACH_UI(OMNI_SVE_SHIFT, Shl, lsl)

OMNI_SVE_FOREACH_U(OMNI_SVE_SHIFT, Shr, lsr)

OMNI_SVE_FOREACH_I(OMNI_SVE_SHIFT, Shr, asr)

#undef OMNI_SVE_SHIFT

// ------------------------------ Min/Max

OMNI_SVE_FOREACH_UI(OMNI_SVE_RETV_ARGPVV, Min, min)

OMNI_SVE_FOREACH_UI(OMNI_SVE_RETV_ARGPVV, Max, max)

OMNI_SVE_FOREACH_F(OMNI_SVE_RETV_ARGPVV, Min, minnm)

OMNI_SVE_FOREACH_F(OMNI_SVE_RETV_ARGPVV, Max, maxnm)

namespace detail {
OMNI_SVE_FOREACH_UI(OMNI_SVE_RETV_ARGPVN, MinN, min_n)

OMNI_SVE_FOREACH_UI(OMNI_SVE_RETV_ARGPVN, MaxN, max_n)
} // namespace detail

// ------------------------------ Mul

// Per-target flags to prevent generic_ops-inl.h defining 8/64-bit operator*.
#ifdef OMNI_NATIVE_MUL_8
#undef OMNI_NATIVE_MUL_8
#else
#define OMNI_NATIVE_MUL_8
#endif
#ifdef OMNI_NATIVE_MUL_64
#undef OMNI_NATIVE_MUL_64
#else
#define OMNI_NATIVE_MUL_64
#endif

OMNI_SVE_FOREACH(OMNI_SVE_RETV_ARGPVV, Mul, mul)

// ------------------------------ MulHigh
OMNI_SVE_FOREACH_UI(OMNI_SVE_RETV_ARGPVV, MulHigh, mulh)

// ------------------------------ MulFixedPoint15
OMNI_API svint16_t MulFixedPoint15(svint16_t a, svint16_t b)
{
    const DFromV<decltype(a)> d;
    const RebindToUnsigned<decltype(d)> du;

    const svuint16_t lo = BitCast(du, Mul(a, b));
    const svint16_t hi = MulHigh(a, b);
    // We want (lo + 0x4000) >> 15, but that can overflow, and if it does we must
    // carry that into the result. Instead isolate the top two bits because only
    // they can influence the result.
    const svuint16_t lo_top2 = ShiftRight<14>(lo);
    // Bits 11: add 2, 10: add 1, 01: add 1, 00: add 0.
    const svuint16_t rounding = ShiftRight<1>(detail::AddN(lo_top2, 1));
    return Add(Add(hi, hi), BitCast(d, rounding));
}

// ------------------------------ Div
#ifdef OMNI_NATIVE_INT_DIV
#undef OMNI_NATIVE_INT_DIV
#else
#define OMNI_NATIVE_INT_DIV
#endif

OMNI_SVE_FOREACH_UI32(OMNI_SVE_RETV_ARGPVV, Div, div)

OMNI_SVE_FOREACH_UI64(OMNI_SVE_RETV_ARGPVV, Div, div)

OMNI_SVE_FOREACH_F(OMNI_SVE_RETV_ARGPVV, Div, div)

// ------------------------------ ApproximateReciprocal
#ifdef OMNI_NATIVE_F64_APPROX_RECIP
#undef OMNI_NATIVE_F64_APPROX_RECIP
#else
#define OMNI_NATIVE_F64_APPROX_RECIP
#endif

OMNI_SVE_FOREACH_F(OMNI_SVE_RETV_ARGV, ApproximateReciprocal, recpe)

// ------------------------------ Sqrt
OMNI_SVE_FOREACH_F(OMNI_SVE_RETV_ARGPV, Sqrt, sqrt)

// ------------------------------ ApproximateReciprocalSqrt
#ifdef OMNI_NATIVE_F64_APPROX_RSQRT
#undef OMNI_NATIVE_F64_APPROX_RSQRT
#else
#define OMNI_NATIVE_F64_APPROX_RSQRT
#endif

OMNI_SVE_FOREACH_F(OMNI_SVE_RETV_ARGV, ApproximateReciprocalSqrt, rsqrte)

// ------------------------------ MulAdd

// Per-target flag to prevent generic_ops-inl.h from defining int MulAdd.
#ifdef OMNI_NATIVE_INT_FMA
#undef OMNI_NATIVE_INT_FMA
#else
#define OMNI_NATIVE_INT_FMA
#endif

#define OMNI_SVE_FMA(BASE, CHAR, BITS, HALF, NAME, OP)                                         \
    OMNI_API OMNI_SVE_V(BASE, BITS)                                                            \
        NAME(OMNI_SVE_V(BASE, BITS) mul, OMNI_SVE_V(BASE, BITS) x, OMNI_SVE_V(BASE, BITS) add) \
    {                                                                                          \
        return sv##OP##_##CHAR##BITS##_x(OMNI_SVE_PTRUE(BITS), x, mul, add);                   \
    }

OMNI_SVE_FOREACH(OMNI_SVE_FMA, MulAdd, mad)

// ------------------------------ NegMulAdd
OMNI_SVE_FOREACH(OMNI_SVE_FMA, NegMulAdd, msb)

// ------------------------------ MulSub
OMNI_SVE_FOREACH_F(OMNI_SVE_FMA, MulSub, nmsb)

// ------------------------------ NegMulSub
OMNI_SVE_FOREACH_F(OMNI_SVE_FMA, NegMulSub, nmad)

#undef OMNI_SVE_FMA

// ------------------------------ Round etc.

OMNI_SVE_FOREACH_F(OMNI_SVE_RETV_ARGPV, Round, rintn)

OMNI_SVE_FOREACH_F(OMNI_SVE_RETV_ARGPV, Floor, rintm)

OMNI_SVE_FOREACH_F(OMNI_SVE_RETV_ARGPV, Ceil, rintp)

OMNI_SVE_FOREACH_F(OMNI_SVE_RETV_ARGPV, Trunc, rintz)

// ================================================== MASK

// ------------------------------ RebindMask
template <class D, typename MFrom> OMNI_API svbool_t RebindMask(const D /* d */, const MFrom mask)
{
    return mask;
}

// ------------------------------ Mask logical

OMNI_API svbool_t Not(svbool_t m)
{
    // We don't know the lane type, so assume 8-bit. For larger types, this will
    // de-canonicalize the predicate, i.e. set bits to 1 even though they do not
    // correspond to the lowest byte in the lane. Arm says such bits are ignored.
    return svnot_b_z(OMNI_SVE_PTRUE(8), m);
}

OMNI_API svbool_t And(svbool_t a, svbool_t b)
{
    return svand_b_z(b, b, a); // same order as AndNot for consistency
}

OMNI_API svbool_t AndNot(svbool_t a, svbool_t b)
{
    return svbic_b_z(b, b, a); // reversed order like NEON
}

OMNI_API svbool_t Or(svbool_t a, svbool_t b)
{
    return svsel_b(a, a, b); // a ? true : b
}

OMNI_API svbool_t Xor(svbool_t a, svbool_t b)
{
    return svsel_b(a, svnand_b_z(a, a, b), b); // a ? !(a & b) : b.
}

OMNI_API svbool_t ExclusiveNeither(svbool_t a, svbool_t b)
{
    return svnor_b_z(OMNI_SVE_PTRUE(8), a, b); // !a && !b, undefined if a && b.
}

// ------------------------------ CountTrue

#define OMNI_SVE_COUNT_TRUE(BASE, CHAR, BITS, HALF, NAME, OP)                                           \
    template <size_t N, int kPow2> OMNI_API size_t NAME(OMNI_SVE_D(BASE, BITS, N, kPow2) d, svbool_t m) \
    {                                                                                                   \
        return sv##OP##_b##BITS(detail::MakeMask(d), m);                                                \
    }

OMNI_SVE_FOREACH(OMNI_SVE_COUNT_TRUE, CountTrue, cntp)

#undef OMNI_SVE_COUNT_TRUE

// For 16-bit Compress: full vector, not limited to SV_POW2.
namespace detail {
#define OMNI_SVE_COUNT_TRUE_FULL(BASE, CHAR, BITS, HALF, NAME, OP)                                            \
    template <size_t N, int kPow2> OMNI_API size_t NAME(OMNI_SVE_D(BASE, BITS, N, kPow2) /* d */, svbool_t m) \
    {                                                                                                         \
        return sv##OP##_b##BITS(svptrue_b##BITS(), m);                                                        \
    }

OMNI_SVE_FOREACH(OMNI_SVE_COUNT_TRUE_FULL, CountTrueFull, cntp)

#undef OMNI_SVE_COUNT_TRUE_FULL
} // namespace detail

// ------------------------------ AllFalse
template <class D> OMNI_API bool AllFalse(D d, svbool_t m)
{
    return !svptest_any(detail::MakeMask(d), m);
}

// ------------------------------ AllTrue
template <class D> OMNI_API bool AllTrue(D d, svbool_t m)
{
    return CountTrue(d, m) == Lanes(d);
}

// ------------------------------ FindFirstTrue
template <class D> OMNI_API intptr_t FindFirstTrue(D d, svbool_t m)
{
    return AllFalse(d, m) ? intptr_t{ -1 } : static_cast<intptr_t>(CountTrue(d, svbrkb_b_z(detail::MakeMask(d), m)));
}

// ------------------------------ FindKnownFirstTrue
template <class D> OMNI_API size_t FindKnownFirstTrue(D d, svbool_t m)
{
    return CountTrue(d, svbrkb_b_z(detail::MakeMask(d), m));
}

// ------------------------------ IfThenElse
#define OMNI_SVE_IF_THEN_ELSE(BASE, CHAR, BITS, HALF, NAME, OP)                                             \
    OMNI_API OMNI_SVE_V(BASE, BITS) NAME(svbool_t m, OMNI_SVE_V(BASE, BITS) yes, OMNI_SVE_V(BASE, BITS) no) \
    {                                                                                                       \
        return sv##OP##_##CHAR##BITS(m, yes, no);                                                           \
    }

OMNI_SVE_FOREACH(OMNI_SVE_IF_THEN_ELSE, IfThenElse, sel)

OMNI_SVE_FOREACH_BF16(OMNI_SVE_IF_THEN_ELSE, IfThenElse, sel)
#undef OMNI_SVE_IF_THEN_ELSE

template <class V, class D = DFromV<V>, OMNI_SVE_IF_EMULATED_D(D)>
OMNI_API V IfThenElse(const svbool_t mask, V yes, V no)
{
    const RebindToUnsigned<D> du;
    return BitCast(D(), IfThenElse(RebindMask(du, mask), BitCast(du, yes), BitCast(du, no)));
}

// ------------------------------ IfThenElseZero
template <class V, class D = DFromV<V>, OMNI_SVE_IF_NOT_EMULATED_D(D)>
OMNI_API V IfThenElseZero(const svbool_t mask, const V yes)
{
    return IfThenElse(mask, yes, Zero(D()));
}

template <class V, class D = DFromV<V>, OMNI_SVE_IF_EMULATED_D(D)> OMNI_API V IfThenElseZero(const svbool_t mask, V yes)
{
    const RebindToUnsigned<D> du;
    return BitCast(D(), IfThenElseZero(RebindMask(du, mask), BitCast(du, yes)));
}

// ------------------------------ IfThenZeroElse
template <class V, class D = DFromV<V>, OMNI_SVE_IF_NOT_EMULATED_D(D)>
OMNI_API V IfThenZeroElse(const svbool_t mask, const V no)
{
    return IfThenElse(mask, Zero(D()), no);
}

template <class V, class D = DFromV<V>, OMNI_SVE_IF_EMULATED_D(D)> OMNI_API V IfThenZeroElse(const svbool_t mask, V no)
{
    const RebindToUnsigned<D> du;
    return BitCast(D(), IfThenZeroElse(RebindMask(du, mask), BitCast(du, no)));
}

// ------------------------------ Additional mask logical operations
OMNI_API svbool_t SetBeforeFirst(svbool_t m)
{
    // We don't know the lane type, so assume 8-bit. For larger types, this will
    // de-canonicalize the predicate, i.e. set bits to 1 even though they do not
    // correspond to the lowest byte in the lane. Arm says such bits are ignored.
    return svbrkb_b_z(OMNI_SVE_PTRUE(8), m);
}

OMNI_API svbool_t SetAtOrBeforeFirst(svbool_t m)
{
    // We don't know the lane type, so assume 8-bit. For larger types, this will
    // de-canonicalize the predicate, i.e. set bits to 1 even though they do not
    // correspond to the lowest byte in the lane. Arm says such bits are ignored.
    return svbrka_b_z(OMNI_SVE_PTRUE(8), m);
}

OMNI_API svbool_t SetOnlyFirst(svbool_t m)
{
    return svbrka_b_z(m, m);
}

OMNI_API svbool_t SetAtOrAfterFirst(svbool_t m)
{
    return Not(SetBeforeFirst(m));
}

// ------------------------------ PromoteMaskTo

#ifdef OMNI_NATIVE_PROMOTE_MASK_TO
#undef OMNI_NATIVE_PROMOTE_MASK_TO
#else
#define OMNI_NATIVE_PROMOTE_MASK_TO
#endif

template <class DTo, class DFrom, OMNI_IF_T_SIZE_D(DTo, sizeof(TFromD<DFrom>) * 2)>
OMNI_API svbool_t PromoteMaskTo(DTo /* d_to */, DFrom /* d_from */, svbool_t m)
{
    return svunpklo_b(m);
}

template <class DTo, class DFrom, OMNI_IF_T_SIZE_GT_D(DTo, sizeof(TFromD<DFrom>) * 2)>
OMNI_API svbool_t PromoteMaskTo(DTo d_to, DFrom d_from, svbool_t m)
{
    using TFrom = TFromD<DFrom>;
    using TWFrom = MakeWide<MakeUnsigned<TFrom>>;
    static_assert(sizeof(TWFrom) > sizeof(TFrom), "sizeof(TWFrom) > sizeof(TFrom) must be true");

    const Rebind<TWFrom, decltype(d_from)> dw_from;
    return PromoteMaskTo(d_to, dw_from, PromoteMaskTo(dw_from, d_from, m));
}

// ------------------------------ DemoteMaskTo

#ifdef OMNI_NATIVE_DEMOTE_MASK_TO
#undef OMNI_NATIVE_DEMOTE_MASK_TO
#else
#define OMNI_NATIVE_DEMOTE_MASK_TO
#endif

template <class DTo, class DFrom, OMNI_IF_T_SIZE_D(DTo, 1), OMNI_IF_T_SIZE_D(DFrom, 2)>
OMNI_API svbool_t DemoteMaskTo(DTo /* d_to */, DFrom /* d_from */, svbool_t m)
{
    return svuzp1_b8(m, m);
}

template <class DTo, class DFrom, OMNI_IF_T_SIZE_D(DTo, 2), OMNI_IF_T_SIZE_D(DFrom, 4)>
OMNI_API svbool_t DemoteMaskTo(DTo /* d_to */, DFrom /* d_from */, svbool_t m)
{
    return svuzp1_b16(m, m);
}

template <class DTo, class DFrom, OMNI_IF_T_SIZE_D(DTo, 4), OMNI_IF_T_SIZE_D(DFrom, 8)>
OMNI_API svbool_t DemoteMaskTo(DTo /* d_to */, DFrom /* d_from */, svbool_t m)
{
    return svuzp1_b32(m, m);
}

template <class DTo, class DFrom, OMNI_IF_T_SIZE_LE_D(DTo, sizeof(TFromD<DFrom>) / 4)>
OMNI_API svbool_t DemoteMaskTo(DTo d_to, DFrom d_from, svbool_t m)
{
    using TFrom = TFromD<DFrom>;
    using TNFrom = MakeNarrow<MakeUnsigned<TFrom>>;
    static_assert(sizeof(TNFrom) < sizeof(TFrom), "sizeof(TNFrom) < sizeof(TFrom) must be true");

    const Rebind<TNFrom, decltype(d_from)> dn_from;
    return DemoteMaskTo(d_to, dn_from, DemoteMaskTo(dn_from, d_from, m));
}

// ------------------------------ LowerHalfOfMask
#ifdef OMNI_NATIVE_LOWER_HALF_OF_MASK
#undef OMNI_NATIVE_LOWER_HALF_OF_MASK
#else
#define OMNI_NATIVE_LOWER_HALF_OF_MASK
#endif

template <class D> OMNI_API svbool_t LowerHalfOfMask(D /* d */, svbool_t m)
{
    return m;
}

// ------------------------------ MaskedAddOr etc. (IfThenElse)

#ifdef OMNI_NATIVE_MASKED_ARITH
#undef OMNI_NATIVE_MASKED_ARITH
#else
#define OMNI_NATIVE_MASKED_ARITH
#endif

namespace detail {
OMNI_SVE_FOREACH(OMNI_SVE_RETV_ARGMVV, MaskedMin, min)

OMNI_SVE_FOREACH(OMNI_SVE_RETV_ARGMVV, MaskedMax, max)

OMNI_SVE_FOREACH(OMNI_SVE_RETV_ARGMVV, MaskedAdd, add)

OMNI_SVE_FOREACH(OMNI_SVE_RETV_ARGMVV, MaskedSub, sub)

OMNI_SVE_FOREACH(OMNI_SVE_RETV_ARGMVV, MaskedMul, mul)

OMNI_SVE_FOREACH_F(OMNI_SVE_RETV_ARGMVV, MaskedDiv, div)

OMNI_SVE_FOREACH_UI32(OMNI_SVE_RETV_ARGMVV, MaskedDiv, div)

OMNI_SVE_FOREACH_UI64(OMNI_SVE_RETV_ARGMVV, MaskedDiv, div)
} // namespace detail

template <class V, class M> OMNI_API V MaskedMinOr(V no, M m, V a, V b)
{
    return IfThenElse(m, detail::MaskedMin(m, a, b), no);
}

template <class V, class M> OMNI_API V MaskedMaxOr(V no, M m, V a, V b)
{
    return IfThenElse(m, detail::MaskedMax(m, a, b), no);
}

template <class V, class M> OMNI_API V MaskedAddOr(V no, M m, V a, V b)
{
    return IfThenElse(m, detail::MaskedAdd(m, a, b), no);
}

template <class V, class M> OMNI_API V MaskedSubOr(V no, M m, V a, V b)
{
    return IfThenElse(m, detail::MaskedSub(m, a, b), no);
}

template <class V, class M> OMNI_API V MaskedMulOr(V no, M m, V a, V b)
{
    return IfThenElse(m, detail::MaskedMul(m, a, b), no);
}

template <class V, class M,
    OMNI_IF_T_SIZE_ONE_OF_V(V, (simd::IsSame<TFromV<V>, simd::float16_t>() ? (1 << 2) : 0) | (1 << 4) | (1 << 8))>
OMNI_API V MaskedDivOr(V no, M m, V a, V b)
{
    return IfThenElse(m, detail::MaskedDiv(m, a, b), no);
}

// I8/U8/I16/U16 MaskedDivOr is implemented after I8/U8/I16/U16 Div
template <class V, class M> OMNI_API V MaskedSatAddOr(V no, M m, V a, V b)
{
    return IfThenElse(m, SaturatedAdd(a, b), no);
}

template <class V, class M> OMNI_API V MaskedSatSubOr(V no, M m, V a, V b)
{
    return IfThenElse(m, SaturatedSub(a, b), no);
}

// ================================================== COMPARE

// mask = f(vector, vector)
#define OMNI_SVE_COMPARE(BASE, CHAR, BITS, HALF, NAME, OP)                     \
    OMNI_API svbool_t NAME(OMNI_SVE_V(BASE, BITS) a, OMNI_SVE_V(BASE, BITS) b) \
    {                                                                          \
        return sv##OP##_##CHAR##BITS(OMNI_SVE_PTRUE(BITS), a, b);              \
    }
#define OMNI_SVE_COMPARE_N(BASE, CHAR, BITS, HALF, NAME, OP)                   \
    OMNI_API svbool_t NAME(OMNI_SVE_V(BASE, BITS) a, OMNI_SVE_T(BASE, BITS) b) \
    {                                                                          \
        return sv##OP##_##CHAR##BITS(OMNI_SVE_PTRUE(BITS), a, b);              \
    }

// ------------------------------ Eq
OMNI_SVE_FOREACH(OMNI_SVE_COMPARE, Eq, cmpeq)

namespace detail {
OMNI_SVE_FOREACH(OMNI_SVE_COMPARE_N, EqN, cmpeq_n)
} // namespace detail

// ------------------------------ Ne
OMNI_SVE_FOREACH(OMNI_SVE_COMPARE, Ne, cmpne)

namespace detail {
OMNI_SVE_FOREACH(OMNI_SVE_COMPARE_N, NeN, cmpne_n)
} // namespace detail

// ------------------------------ Lt
OMNI_SVE_FOREACH(OMNI_SVE_COMPARE, Lt, cmplt)

namespace detail {
OMNI_SVE_FOREACH(OMNI_SVE_COMPARE_N, LtN, cmplt_n)
} // namespace detail

// ------------------------------ Le
OMNI_SVE_FOREACH(OMNI_SVE_COMPARE, Le, cmple)

namespace detail {
OMNI_SVE_FOREACH(OMNI_SVE_COMPARE_N, LeN, cmple_n)
} // namespace detail

// ------------------------------ Gt/Ge (swapped order)
template <class V> OMNI_API svbool_t Gt(const V a, const V b)
{
    return Lt(b, a);
}

template <class V> OMNI_API svbool_t Ge(const V a, const V b)
{
    return Le(b, a);
}

namespace detail {
OMNI_SVE_FOREACH(OMNI_SVE_COMPARE_N, GeN, cmpge_n)

OMNI_SVE_FOREACH(OMNI_SVE_COMPARE_N, GtN, cmpgt_n)
} // namespace detail

#undef OMNI_SVE_COMPARE
#undef OMNI_SVE_COMPARE_N

// ------------------------------ TestBit
template <class V> OMNI_API svbool_t TestBit(const V a, const V bit)
{
    return detail::NeN(And(a, bit), 0);
}

// ------------------------------ MaskFromVec (Ne)
template <class V> OMNI_API svbool_t MaskFromVec(const V v)
{
    using T = TFromV<V>;
    return detail::NeN(v, ConvertScalarTo<T>(0));
}

// ------------------------------ VecFromMask
template <class D> OMNI_API VFromD<D> VecFromMask(const D d, svbool_t mask)
{
    const RebindToSigned<D> di;
    // This generates MOV imm, whereas svdup_n_s8_z generates MOV scalar, which
    // requires an extra instruction plus M0 pipeline.
    return BitCast(d, IfThenElseZero(mask, Set(di, -1)));
}

// ------------------------------ IsNegative (Lt)
#ifdef OMNI_NATIVE_IS_NEGATIVE
#undef OMNI_NATIVE_IS_NEGATIVE
#else
#define OMNI_NATIVE_IS_NEGATIVE
#endif

template <class V, OMNI_IF_NOT_UNSIGNED_V(V)> OMNI_API svbool_t IsNegative(V v)
{
    const DFromV<decltype(v)> d;
    const RebindToSigned<decltype(d)> di;
    using TI = TFromD<decltype(di)>;

    return detail::LtN(BitCast(di, v), static_cast<TI>(0));
}

// ------------------------------ IfVecThenElse (MaskFromVec, IfThenElse)
template <class V> OMNI_API V IfVecThenElse(const V mask, const V yes, const V no)
{
    return Or(And(mask, yes), AndNot(mask, no));
}

// ------------------------------ BitwiseIfThenElse

#ifdef OMNI_NATIVE_BITWISE_IF_THEN_ELSE
#undef OMNI_NATIVE_BITWISE_IF_THEN_ELSE
#else
#define OMNI_NATIVE_BITWISE_IF_THEN_ELSE
#endif

template <class V> OMNI_API V BitwiseIfThenElse(V mask, V yes, V no)
{
    return IfVecThenElse(mask, yes, no);
}

// ------------------------------ CopySign (BitwiseIfThenElse)
template <class V> OMNI_API V CopySign(const V magn, const V sign)
{
    const DFromV<decltype(magn)> d;
    return BitwiseIfThenElse(SignBit(d), sign, magn);
}

// ------------------------------ CopySignToAbs
template <class V> OMNI_API V CopySignToAbs(const V abs, const V sign)
{
    const DFromV<V> d;
    return OrAnd(abs, SignBit(d), sign);
}

// ------------------------------ Floating-point classification (Ne)

template <class V> OMNI_API svbool_t IsNaN(const V v)
{
    return Ne(v, v); // could also use cmpuo
}

// Per-target flag to prevent generic_ops-inl.h from defining IsInf / IsFinite.
// We use a fused Set/comparison for IsFinite.
#ifdef OMNI_NATIVE_ISINF
#undef OMNI_NATIVE_ISINF
#else
#define OMNI_NATIVE_ISINF
#endif

template <class V> OMNI_API svbool_t IsInf(const V v)
{
    using T = TFromV<V>;
    const DFromV<decltype(v)> d;
    const RebindToUnsigned<decltype(d)> du;
    const RebindToSigned<decltype(d)> di;

    // 'Shift left' to clear the sign bit
    const VFromD<decltype(du)> vu = BitCast(du, v);
    const VFromD<decltype(du)> v2 = Add(vu, vu);
    // Check for exponent=max and mantissa=0.
    const VFromD<decltype(di)> max2 = Set(di, simd::MaxExponentTimes2<T>());
    return RebindMask(d, Eq(v2, BitCast(du, max2)));
}

// Returns whether normal/subnormal/zero.
template <class V> OMNI_API svbool_t IsFinite(const V v)
{
    using T = TFromV<V>;
    const DFromV<decltype(v)> d;
    const RebindToUnsigned<decltype(d)> du;
    const RebindToSigned<decltype(d)> di; // cheaper than unsigned comparison
    const VFromD<decltype(du)> vu = BitCast(du, v);
    // 'Shift left' to clear the sign bit, then right so we can compare with the
    // max exponent (cannot compare with MaxExponentTimes2 directly because it is
    // negative and non-negative floats would be greater).
    const VFromD<decltype(di)> exp = BitCast(di, ShiftRight<simd::MantissaBits<T>() + 1>(Add(vu, vu)));
    return RebindMask(d, detail::LtN(exp, simd::MaxExponentField<T>()));
}

// ================================================== MEMORY

// ------------------------------ LoadU/MaskedLoad/LoadDup128/StoreU/Stream

#define OMNI_SVE_MEM(BASE, CHAR, BITS, HALF, NAME, OP)                                                         \
    template <size_t N, int kPow2>                                                                             \
    OMNI_API OMNI_SVE_V(BASE, BITS)                                                                            \
        LoadU(OMNI_SVE_D(BASE, BITS, N, kPow2) d, const OMNI_SVE_T(BASE, BITS) * OMNI_RESTRICT p)              \
    {                                                                                                          \
        return svld1_##CHAR##BITS(detail::MakeMask(d), detail::NativeLanePointer(p));                          \
    }                                                                                                          \
    template <size_t N, int kPow2>                                                                             \
    OMNI_API OMNI_SVE_V(BASE, BITS) MaskedLoad(svbool_t m, OMNI_SVE_D(BASE, BITS, N, kPow2) /* d */,           \
        const OMNI_SVE_T(BASE, BITS) * OMNI_RESTRICT p)                                                        \
    {                                                                                                          \
        return svld1_##CHAR##BITS(m, detail::NativeLanePointer(p));                                            \
    }                                                                                                          \
    template <size_t N, int kPow2>                                                                             \
    OMNI_API void StoreU(OMNI_SVE_V(BASE, BITS) v, OMNI_SVE_D(BASE, BITS, N, kPow2) d,                         \
        OMNI_SVE_T(BASE, BITS) * OMNI_RESTRICT p)                                                              \
    {                                                                                                          \
        svst1_##CHAR##BITS(detail::MakeMask(d), detail::NativeLanePointer(p), v);                              \
    }                                                                                                          \
    template <size_t N, int kPow2>                                                                             \
    OMNI_API void Stream(OMNI_SVE_V(BASE, BITS) v, OMNI_SVE_D(BASE, BITS, N, kPow2) d,                         \
        OMNI_SVE_T(BASE, BITS) * OMNI_RESTRICT p)                                                              \
    {                                                                                                          \
        svstnt1_##CHAR##BITS(detail::MakeMask(d), detail::NativeLanePointer(p), v);                            \
    }                                                                                                          \
    template <size_t N, int kPow2>                                                                             \
    OMNI_API void BlendedStore(OMNI_SVE_V(BASE, BITS) v, svbool_t m, OMNI_SVE_D(BASE, BITS, N, kPow2) /* d */, \
        OMNI_SVE_T(BASE, BITS) * OMNI_RESTRICT p)                                                              \
    {                                                                                                          \
        svst1_##CHAR##BITS(m, detail::NativeLanePointer(p), v);                                                \
    }

OMNI_SVE_FOREACH(OMNI_SVE_MEM, _, _)

OMNI_SVE_FOREACH_BF16(OMNI_SVE_MEM, _, _)

template <class D, OMNI_SVE_IF_EMULATED_D(D)> OMNI_API VFromD<D> LoadU(D d, const TFromD<D> *OMNI_RESTRICT p)
{
    const RebindToUnsigned<decltype(d)> du;
    return BitCast(d, LoadU(du, detail::U16LanePointer(p)));
}

template <class D, OMNI_SVE_IF_EMULATED_D(D)> OMNI_API void StoreU(VFromD<D> v, D d, TFromD<D> *OMNI_RESTRICT p)
{
    const RebindToUnsigned<decltype(d)> du;
    StoreU(BitCast(du, v), du, detail::U16LanePointer(p));
}

template <class D, OMNI_SVE_IF_EMULATED_D(D)>
OMNI_API VFromD<D> MaskedLoad(MFromD<D> m, D d, const TFromD<D> *OMNI_RESTRICT p)
{
    const RebindToUnsigned<decltype(d)> du;
    return BitCast(d, MaskedLoad(RebindMask(du, m), du, detail::U16LanePointer(p)));
}

// MaskedLoadOr is generic and does not require emulation.

template <class D, OMNI_SVE_IF_EMULATED_D(D)>
OMNI_API void BlendedStore(VFromD<D> v, MFromD<D> m, D d, TFromD<D> *OMNI_RESTRICT p)
{
    const RebindToUnsigned<decltype(d)> du;
    BlendedStore(BitCast(du, v), RebindMask(du, m), du, detail::U16LanePointer(p));
}

#undef OMNI_SVE_MEM

namespace detail {
#define OMNI_SVE_LOAD_DUP128(BASE, CHAR, BITS, HALF, NAME, OP)                                         \
    template <size_t N, int kPow2>                                                                     \
    OMNI_API OMNI_SVE_V(BASE, BITS)                                                                    \
        NAME(OMNI_SVE_D(BASE, BITS, N, kPow2) /* d */, const OMNI_SVE_T(BASE, BITS) * OMNI_RESTRICT p) \
    {                                                                                                  \
        /* All-true predicate to load all 128 bits. */                                                 \
        return sv##OP##_##CHAR##BITS(OMNI_SVE_PTRUE(8), detail::NativeLanePointer(p));                 \
    }

OMNI_SVE_FOREACH(OMNI_SVE_LOAD_DUP128, LoadDupFull128, ld1rq)

OMNI_SVE_FOREACH_BF16(OMNI_SVE_LOAD_DUP128, LoadDupFull128, ld1rq)

template <class D, OMNI_SVE_IF_EMULATED_D(D)> OMNI_API VFromD<D> LoadDupFull128(D d, const TFromD<D> *OMNI_RESTRICT p)
{
    const RebindToUnsigned<decltype(d)> du;
    return BitCast(d, LoadDupFull128(du, detail::U16LanePointer(p)));
}
} // namespace detail

// If D().MaxBytes() <= 16 is true, simply do a LoadU operation.
template <class D, OMNI_IF_V_SIZE_LE_D(D, 16)> OMNI_API VFromD<D> LoadDup128(D d, const TFromD<D> *OMNI_RESTRICT p)
{
    return LoadU(d, p);
}

// If D().MaxBytes() > 16 is true, need to load the vector using ld1rq
template <class D, OMNI_IF_V_SIZE_GT_D(D, 16)> OMNI_API VFromD<D> LoadDup128(D d, const TFromD<D> *OMNI_RESTRICT p)
{
    return detail::LoadDupFull128(d, p);
}

// ------------------------------ Load/Store

// SVE only requires lane alignment, not natural alignment of the entire
// vector, so Load/Store are the same as LoadU/StoreU.
template <class D> OMNI_API VFromD<D> Load(D d, const TFromD<D> *OMNI_RESTRICT p)
{
    return LoadU(d, p);
}

template <class V, class D> OMNI_API void Store(const V v, D d, TFromD<D> *OMNI_RESTRICT p)
{
    StoreU(v, d, p);
}

// ------------------------------ MaskedLoadOr

// SVE MaskedLoad hard-codes zero, so this requires an extra blend.
template <class D> OMNI_API VFromD<D> MaskedLoadOr(VFromD<D> v, MFromD<D> m, D d, const TFromD<D> *OMNI_RESTRICT p)
{
    return IfThenElse(m, MaskedLoad(m, d, p), v);
}

// ------------------------------ ScatterOffset/Index

#ifdef OMNI_NATIVE_SCATTER
#undef OMNI_NATIVE_SCATTER
#else
#define OMNI_NATIVE_SCATTER
#endif

#define OMNI_SVE_SCATTER_OFFSET(BASE, CHAR, BITS, HALF, NAME, OP)                    \
    template <size_t N, int kPow2>                                                   \
    OMNI_API void NAME(OMNI_SVE_V(BASE, BITS) v, OMNI_SVE_D(BASE, BITS, N, kPow2) d, \
        OMNI_SVE_T(BASE, BITS) * OMNI_RESTRICT base, OMNI_SVE_V(int, BITS) offset)   \
    {                                                                                \
        sv##OP##_s##BITS##offset_##CHAR##BITS(detail::MakeMask(d), base, offset, v); \
    }

#define OMNI_SVE_MASKED_SCATTER_INDEX(BASE, CHAR, BITS, HALF, NAME, OP)                                \
    template <size_t N, int kPow2>                                                                     \
    OMNI_API void NAME(OMNI_SVE_V(BASE, BITS) v, svbool_t m, OMNI_SVE_D(BASE, BITS, N, kPow2) /* d */, \
        OMNI_SVE_T(BASE, BITS) * OMNI_RESTRICT base, OMNI_SVE_V(int, BITS) indices)                    \
    {                                                                                                  \
        sv##OP##_s##BITS##index_##CHAR##BITS(m, base, indices, v);                                     \
    }

OMNI_SVE_FOREACH_UIF3264(OMNI_SVE_SCATTER_OFFSET, ScatterOffset, st1_scatter)

OMNI_SVE_FOREACH_UIF3264(OMNI_SVE_MASKED_SCATTER_INDEX, MaskedScatterIndex, st1_scatter)

#undef OMNI_SVE_SCATTER_OFFSET
#undef OMNI_SVE_MASKED_SCATTER_INDEX

template <class D>
OMNI_API void ScatterIndex(VFromD<D> v, D d, TFromD<D> *OMNI_RESTRICT p, VFromD<RebindToSigned<D>> indices)
{
    MaskedScatterIndex(v, detail::MakeMask(d), d, p, indices);
}

// ------------------------------ GatherOffset/Index

#ifdef OMNI_NATIVE_GATHER
#undef OMNI_NATIVE_GATHER
#else
#define OMNI_NATIVE_GATHER
#endif

#define OMNI_SVE_GATHER_OFFSET(BASE, CHAR, BITS, HALF, NAME, OP)                         \
    template <size_t N, int kPow2>                                                       \
    OMNI_API OMNI_SVE_V(BASE, BITS) NAME(OMNI_SVE_D(BASE, BITS, N, kPow2) d,             \
        const OMNI_SVE_T(BASE, BITS) * OMNI_RESTRICT base, OMNI_SVE_V(int, BITS) offset) \
    {                                                                                    \
        return sv##OP##_s##BITS##offset_##CHAR##BITS(detail::MakeMask(d), base, offset); \
    }
#define OMNI_SVE_MASKED_GATHER_INDEX(BASE, CHAR, BITS, HALF, NAME, OP)                    \
    template <size_t N, int kPow2>                                                        \
    OMNI_API OMNI_SVE_V(BASE, BITS) NAME(svbool_t m, OMNI_SVE_D(BASE, BITS, N, kPow2) d,  \
        const OMNI_SVE_T(BASE, BITS) * OMNI_RESTRICT base, OMNI_SVE_V(int, BITS) indices) \
    {                                                                                     \
        const RebindToSigned<decltype(d)> di;                                             \
        (void)di; /* for OMNI_DASSERT */                                                  \
        return sv##OP##_s##BITS##index_##CHAR##BITS(m, base, indices);                    \
    }

OMNI_SVE_FOREACH_UIF3264(OMNI_SVE_GATHER_OFFSET, GatherOffset, ld1_gather)

OMNI_SVE_FOREACH_UIF3264(OMNI_SVE_MASKED_GATHER_INDEX, MaskedGatherIndex, ld1_gather)

#undef OMNI_SVE_GATHER_OFFSET
#undef OMNI_SVE_MASKED_GATHER_INDEX

template <class D>
OMNI_API VFromD<D> MaskedGatherIndexOr(VFromD<D> no, svbool_t m, D d, const TFromD<D> *OMNI_RESTRICT p,
    VFromD<RebindToSigned<D>> indices)
{
    return IfThenElse(m, MaskedGatherIndex(m, d, p, indices), no);
}

template <class D>
OMNI_API VFromD<D> GatherIndex(D d, const TFromD<D> *OMNI_RESTRICT p, VFromD<RebindToSigned<D>> indices)
{
    return MaskedGatherIndex(detail::MakeMask(d), d, p, indices);
}

// ------------------------------ LoadInterleaved2

// Per-target flag to prevent generic_ops-inl.h from defining LoadInterleaved2.
#ifdef OMNI_NATIVE_LOAD_STORE_INTERLEAVED
#undef OMNI_NATIVE_LOAD_STORE_INTERLEAVED
#else
#define OMNI_NATIVE_LOAD_STORE_INTERLEAVED
#endif

#define OMNI_SVE_LOAD2(BASE, CHAR, BITS, HALF, NAME, OP)                                                           \
    template <size_t N, int kPow2>                                                                                 \
    OMNI_API void NAME(OMNI_SVE_D(BASE, BITS, N, kPow2) d, const OMNI_SVE_T(BASE, BITS) * OMNI_RESTRICT unaligned, \
        OMNI_SVE_V(BASE, BITS) & v0, OMNI_SVE_V(BASE, BITS) & v1)                                                  \
    {                                                                                                              \
        const OMNI_SVE_TUPLE(BASE, BITS, 2) tuple =                                                                \
            sv##OP##_##CHAR##BITS(detail::MakeMask(d), detail::NativeLanePointer(unaligned));                      \
        v0 = svget2(tuple, 0);                                                                                     \
        v1 = svget2(tuple, 1);                                                                                     \
    }

OMNI_SVE_FOREACH(OMNI_SVE_LOAD2, LoadInterleaved2, ld2)

OMNI_SVE_FOREACH_BF16(OMNI_SVE_LOAD2, LoadInterleaved2, ld2)

#undef OMNI_SVE_LOAD2

// ------------------------------ LoadInterleaved3

#define OMNI_SVE_LOAD3(BASE, CHAR, BITS, HALF, NAME, OP)                                                           \
    template <size_t N, int kPow2>                                                                                 \
    OMNI_API void NAME(OMNI_SVE_D(BASE, BITS, N, kPow2) d, const OMNI_SVE_T(BASE, BITS) * OMNI_RESTRICT unaligned, \
        OMNI_SVE_V(BASE, BITS) & v0, OMNI_SVE_V(BASE, BITS) & v1, OMNI_SVE_V(BASE, BITS) & v2)                     \
    {                                                                                                              \
        const OMNI_SVE_TUPLE(BASE, BITS, 3) tuple =                                                                \
            sv##OP##_##CHAR##BITS(detail::MakeMask(d), detail::NativeLanePointer(unaligned));                      \
        v0 = svget3(tuple, 0);                                                                                     \
        v1 = svget3(tuple, 1);                                                                                     \
        v2 = svget3(tuple, 2);                                                                                     \
    }

OMNI_SVE_FOREACH(OMNI_SVE_LOAD3, LoadInterleaved3, ld3)

OMNI_SVE_FOREACH_BF16(OMNI_SVE_LOAD3, LoadInterleaved3, ld3)

#undef OMNI_SVE_LOAD3

// ------------------------------ LoadInterleaved4

#define OMNI_SVE_LOAD4(BASE, CHAR, BITS, HALF, NAME, OP)                                                           \
    template <size_t N, int kPow2>                                                                                 \
    OMNI_API void NAME(OMNI_SVE_D(BASE, BITS, N, kPow2) d, const OMNI_SVE_T(BASE, BITS) * OMNI_RESTRICT unaligned, \
        OMNI_SVE_V(BASE, BITS) & v0, OMNI_SVE_V(BASE, BITS) & v1, OMNI_SVE_V(BASE, BITS) & v2,                     \
        OMNI_SVE_V(BASE, BITS) & v3)                                                                               \
    {                                                                                                              \
        const OMNI_SVE_TUPLE(BASE, BITS, 4) tuple =                                                                \
            sv##OP##_##CHAR##BITS(detail::MakeMask(d), detail::NativeLanePointer(unaligned));                      \
        v0 = svget4(tuple, 0);                                                                                     \
        v1 = svget4(tuple, 1);                                                                                     \
        v2 = svget4(tuple, 2);                                                                                     \
        v3 = svget4(tuple, 3);                                                                                     \
    }

OMNI_SVE_FOREACH(OMNI_SVE_LOAD4, LoadInterleaved4, ld4)

OMNI_SVE_FOREACH_BF16(OMNI_SVE_LOAD4, LoadInterleaved4, ld4)

#undef OMNI_SVE_LOAD4

// ------------------------------ StoreInterleaved2

#define OMNI_SVE_STORE2(BASE, CHAR, BITS, HALF, NAME, OP)                                                        \
    template <size_t N, int kPow2>                                                                               \
    OMNI_API void NAME(OMNI_SVE_V(BASE, BITS) v0, OMNI_SVE_V(BASE, BITS) v1, OMNI_SVE_D(BASE, BITS, N, kPow2) d, \
        OMNI_SVE_T(BASE, BITS) * OMNI_RESTRICT unaligned)                                                        \
    {                                                                                                            \
        sv##OP##_##CHAR##BITS(detail::MakeMask(d), detail::NativeLanePointer(unaligned), Create2(d, v0, v1));    \
    }

OMNI_SVE_FOREACH(OMNI_SVE_STORE2, StoreInterleaved2, st2)

OMNI_SVE_FOREACH_BF16(OMNI_SVE_STORE2, StoreInterleaved2, st2)

#undef OMNI_SVE_STORE2

// ------------------------------ StoreInterleaved3

#define OMNI_SVE_STORE3(BASE, CHAR, BITS, HALF, NAME, OP)                                                         \
    template <size_t N, int kPow2>                                                                                \
    OMNI_API void NAME(OMNI_SVE_V(BASE, BITS) v0, OMNI_SVE_V(BASE, BITS) v1, OMNI_SVE_V(BASE, BITS) v2,           \
        OMNI_SVE_D(BASE, BITS, N, kPow2) d, OMNI_SVE_T(BASE, BITS) * OMNI_RESTRICT unaligned)                     \
    {                                                                                                             \
        sv##OP##_##CHAR##BITS(detail::MakeMask(d), detail::NativeLanePointer(unaligned), Create3(d, v0, v1, v2)); \
    }

OMNI_SVE_FOREACH(OMNI_SVE_STORE3, StoreInterleaved3, st3)

OMNI_SVE_FOREACH_BF16(OMNI_SVE_STORE3, StoreInterleaved3, st3)

#undef OMNI_SVE_STORE3

// ------------------------------ StoreInterleaved4

#define OMNI_SVE_STORE4(BASE, CHAR, BITS, HALF, NAME, OP)                                                             \
    template <size_t N, int kPow2>                                                                                    \
    OMNI_API void NAME(OMNI_SVE_V(BASE, BITS) v0, OMNI_SVE_V(BASE, BITS) v1, OMNI_SVE_V(BASE, BITS) v2,               \
        OMNI_SVE_V(BASE, BITS) v3, OMNI_SVE_D(BASE, BITS, N, kPow2) d,                                                \
        OMNI_SVE_T(BASE, BITS) * OMNI_RESTRICT unaligned)                                                             \
    {                                                                                                                 \
        sv##OP##_##CHAR##BITS(detail::MakeMask(d), detail::NativeLanePointer(unaligned), Create4(d, v0, v1, v2, v3)); \
    }

OMNI_SVE_FOREACH(OMNI_SVE_STORE4, StoreInterleaved4, st4)

OMNI_SVE_FOREACH_BF16(OMNI_SVE_STORE4, StoreInterleaved4, st4)

#undef OMNI_SVE_STORE4

// Fall back on generic Load/StoreInterleaved[234] for any emulated types.
// Requires OMNI_GENERIC_IF_EMULATED_D mirrors OMNI_SVE_IF_EMULATED_D.

// ================================================== CONVERT

// ------------------------------ PromoteTo

// Same sign
#define OMNI_SVE_PROMOTE_TO(BASE, CHAR, BITS, HALF, NAME, OP)                                                  \
    template <size_t N, int kPow2>                                                                             \
    OMNI_API OMNI_SVE_V(BASE, BITS) NAME(OMNI_SVE_D(BASE, BITS, N, kPow2) /* tag */, OMNI_SVE_V(BASE, HALF) v) \
    {                                                                                                          \
        return sv##OP##_##CHAR##BITS(v);                                                                       \
    }

OMNI_SVE_FOREACH_UI16(OMNI_SVE_PROMOTE_TO, PromoteTo, unpklo)

OMNI_SVE_FOREACH_UI32(OMNI_SVE_PROMOTE_TO, PromoteTo, unpklo)

OMNI_SVE_FOREACH_UI64(OMNI_SVE_PROMOTE_TO, PromoteTo, unpklo)

// 2x
template <size_t N, int kPow2> OMNI_API svuint32_t PromoteTo(Simd<uint32_t, N, kPow2> dto, svuint8_t vfrom)
{
    const RepartitionToWide<DFromV<decltype(vfrom)>> d2;
    return PromoteTo(dto, PromoteTo(d2, vfrom));
}

template <size_t N, int kPow2> OMNI_API svint32_t PromoteTo(Simd<int32_t, N, kPow2> dto, svint8_t vfrom)
{
    const RepartitionToWide<DFromV<decltype(vfrom)>> d2;
    return PromoteTo(dto, PromoteTo(d2, vfrom));
}

template <size_t N, int kPow2> OMNI_API svuint64_t PromoteTo(Simd<uint64_t, N, kPow2> dto, svuint16_t vfrom)
{
    const RepartitionToWide<DFromV<decltype(vfrom)>> d2;
    return PromoteTo(dto, PromoteTo(d2, vfrom));
}

template <size_t N, int kPow2> OMNI_API svint64_t PromoteTo(Simd<int64_t, N, kPow2> dto, svint16_t vfrom)
{
    const RepartitionToWide<DFromV<decltype(vfrom)>> d2;
    return PromoteTo(dto, PromoteTo(d2, vfrom));
}

// 3x
template <size_t N, int kPow2> OMNI_API svuint64_t PromoteTo(Simd<uint64_t, N, kPow2> dto, svuint8_t vfrom)
{
    const RepartitionToNarrow<decltype(dto)> d4;
    const RepartitionToNarrow<decltype(d4)> d2;
    return PromoteTo(dto, PromoteTo(d4, PromoteTo(d2, vfrom)));
}

template <size_t N, int kPow2> OMNI_API svint64_t PromoteTo(Simd<int64_t, N, kPow2> dto, svint8_t vfrom)
{
    const RepartitionToNarrow<decltype(dto)> d4;
    const RepartitionToNarrow<decltype(d4)> d2;
    return PromoteTo(dto, PromoteTo(d4, PromoteTo(d2, vfrom)));
}

// Sign change
template <class D, class V, OMNI_IF_SIGNED_D(D), OMNI_IF_UNSIGNED_V(V),
    OMNI_IF_LANES_GT(sizeof(TFromD<D>), sizeof(TFromV<V>))>
OMNI_API VFromD<D> PromoteTo(D di, V v)
{
    const RebindToUnsigned<decltype(di)> du;
    return BitCast(di, PromoteTo(du, v));
}

// ------------------------------ PromoteTo F

// Per-target flag to prevent generic_ops-inl.h from defining f16 conversions.
#ifdef OMNI_NATIVE_F16C
#undef OMNI_NATIVE_F16C
#else
#define OMNI_NATIVE_F16C
#endif

// Unlike Simd's ZipLower, this returns the same type.
namespace detail {
OMNI_SVE_FOREACH(OMNI_SVE_RETV_ARGVV, ZipLowerSame, zip1)
} // namespace detail

#ifdef OMNI_NATIVE_PROMOTE_F16_TO_F64
#undef OMNI_NATIVE_PROMOTE_F16_TO_F64
#else
#define OMNI_NATIVE_PROMOTE_F16_TO_F64
#endif

template <size_t N, int kPow2> OMNI_API svfloat64_t PromoteTo(Simd<float64_t, N, kPow2> /* d */, const svint32_t v)
{
    const svint32_t vv = detail::ZipLowerSame(v, v);
    return svcvt_f64_s32_x(detail::PTrue(Simd<int32_t, N, kPow2>()), vv);
}

template <size_t N, int kPow2> OMNI_API svfloat64_t PromoteTo(Simd<float64_t, N, kPow2> /* d */, const svuint32_t v)
{
    const svuint32_t vv = detail::ZipLowerSame(v, v);
    return svcvt_f64_u32_x(detail::PTrue(Simd<uint32_t, N, kPow2>()), vv);
}

template <size_t N, int kPow2> OMNI_API svint64_t PromoteTo(Simd<int64_t, N, kPow2> /* d */, const svfloat32_t v)
{
    const svfloat32_t vv = detail::ZipLowerSame(v, v);
    return svcvt_s64_f32_x(detail::PTrue(Simd<float, N, kPow2>()), vv);
}

template <size_t N, int kPow2> OMNI_API svuint64_t PromoteTo(Simd<uint64_t, N, kPow2> /* d */, const svfloat32_t v)
{
    const svfloat32_t vv = detail::ZipLowerSame(v, v);
    return svcvt_u64_f32_x(detail::PTrue(Simd<float, N, kPow2>()), vv);
}

// ------------------------------
template <class D, OMNI_IF_U8_D(D)> OMNI_API svuint8_t PromoteTo(D /* d */, const svuint8_t v)
{
    return v;
}

template <class D, OMNI_IF_U8_D(D)> OMNI_API svuint8_t PromoteTo(D /* d */, const svint8_t v)
{
    return svreinterpret_u8_s8(v);
}

template <class D, OMNI_IF_I8_D(D)> OMNI_API svint8_t PromoteTo(D /* d */, const svuint8_t v)
{
    return svreinterpret_s8_u8(v);
}

// ------------------------------ PromoteUpperTo

namespace detail {
OMNI_SVE_FOREACH_UI16(OMNI_SVE_PROMOTE_TO, PromoteUpperTo, unpkhi)

OMNI_SVE_FOREACH_UI32(OMNI_SVE_PROMOTE_TO, PromoteUpperTo, unpkhi)

OMNI_SVE_FOREACH_UI64(OMNI_SVE_PROMOTE_TO, PromoteUpperTo, unpkhi)

#undef OMNI_SVE_PROMOTE_TO
} // namespace detail

#ifdef OMNI_NATIVE_PROMOTE_UPPER_TO
#undef OMNI_NATIVE_PROMOTE_UPPER_TO
#else
#define OMNI_NATIVE_PROMOTE_UPPER_TO
#endif

// Unsigned->Unsigned or Signed->Signed
template <class D, class V, typename TD = TFromD<D>, typename TV = TFromV<V>,
    simd::EnableIf<IsInteger<TD>() && IsInteger<TV>() && (IsSigned<TD>() == IsSigned<TV>())> * = nullptr>
OMNI_API VFromD<D> PromoteUpperTo(D d, V v)
{
    if (detail::IsFull(d)) {
        return detail::PromoteUpperTo(d, v);
    }
    const Rebind<TFromV<V>, decltype(d)> dh;
    return PromoteTo(d, UpperHalf(dh, v));
}

// Differing signs or either is float
template <class D, class V, typename TD = TFromD<D>, typename TV = TFromV<V>,
    simd::EnableIf<!IsInteger<TD>() || !IsInteger<TV>() || (IsSigned<TD>() != IsSigned<TV>())> * = nullptr>
OMNI_API VFromD<D> PromoteUpperTo(D d, V v)
{
    // Lanes(d) may differ from Lanes(DFromV<V>()). Use the lane type from V
    // because it cannot be deduced from D (could be either bf16 or f16).
    const Rebind<TFromV<V>, decltype(d)> dh;
    return PromoteTo(d, UpperHalf(dh, v));
}

// ------------------------------ DemoteTo U

namespace detail {
// Saturates unsigned vectors to half/quarter-width TN.
template <typename TN, class VU> VU SaturateU(VU v)
{
    return detail::MinN(v, static_cast<TFromV<VU>>(LimitsMax<TN>()));
}

// Saturates unsigned vectors to half/quarter-width TN.
template <typename TN, class VI> VI SaturateI(VI v)
{
    return detail::MinN(detail::MaxN(v, LimitsMin<TN>()), LimitsMax<TN>());
}
} // namespace detail

template <size_t N, int kPow2> OMNI_API svuint8_t DemoteTo(Simd<uint8_t, N, kPow2> dn, const svint16_t v)
{
    const DFromV<decltype(v)> di;
    const RebindToUnsigned<decltype(di)> du;
    using TN = TFromD<decltype(dn)>;
    // First clamp negative numbers to zero and cast to unsigned.
    const svuint16_t clamped = BitCast(du, detail::MaxN(v, 0));
    // Saturate to unsigned-max and halve the width.
    const svuint8_t vn = BitCast(dn, detail::SaturateU<TN>(clamped));
    return svuzp1_u8(vn, vn);
}

template <size_t N, int kPow2> OMNI_API svuint16_t DemoteTo(Simd<uint16_t, N, kPow2> dn, const svint32_t v)
{
    const DFromV<decltype(v)> di;
    const RebindToUnsigned<decltype(di)> du;
    using TN = TFromD<decltype(dn)>;
    // First clamp negative numbers to zero and cast to unsigned.
    const svuint32_t clamped = BitCast(du, detail::MaxN(v, 0));
    // Saturate to unsigned-max and halve the width.
    const svuint16_t vn = BitCast(dn, detail::SaturateU<TN>(clamped));
    return svuzp1_u16(vn, vn);
}

template <size_t N, int kPow2> OMNI_API svuint8_t DemoteTo(Simd<uint8_t, N, kPow2> dn, const svint32_t v)
{
    const DFromV<decltype(v)> di;
    const RebindToUnsigned<decltype(di)> du;
    const RepartitionToNarrow<decltype(du)> d2;
    using TN = TFromD<decltype(dn)>;
    // First clamp negative numbers to zero and cast to unsigned.
    const svuint32_t clamped = BitCast(du, detail::MaxN(v, 0));
    // Saturate to unsigned-max and quarter the width.
    const svuint16_t cast16 = BitCast(d2, detail::SaturateU<TN>(clamped));
    const svuint8_t x2 = BitCast(dn, svuzp1_u16(cast16, cast16));
    return svuzp1_u8(x2, x2);
}

OMNI_API svuint8_t U8FromU32(const svuint32_t v)
{
    const DFromV<svuint32_t> du32;
    const RepartitionToNarrow<decltype(du32)> du16;
    const RepartitionToNarrow<decltype(du16)> du8;

    const svuint16_t cast16 = BitCast(du16, v);
    const svuint16_t x2 = svuzp1_u16(cast16, cast16);
    const svuint8_t cast8 = BitCast(du8, x2);
    return svuzp1_u8(cast8, cast8);
}

template <size_t N, int kPow2> OMNI_API svuint8_t DemoteTo(Simd<uint8_t, N, kPow2> dn, const svuint16_t v)
{
    using TN = TFromD<decltype(dn)>;
    const svuint8_t vn = BitCast(dn, detail::SaturateU<TN>(v));
    return svuzp1_u8(vn, vn);
}

template <size_t N, int kPow2> OMNI_API svuint16_t DemoteTo(Simd<uint16_t, N, kPow2> dn, const svuint32_t v)
{
    using TN = TFromD<decltype(dn)>;
    const svuint16_t vn = BitCast(dn, detail::SaturateU<TN>(v));
    return svuzp1_u16(vn, vn);
}

template <size_t N, int kPow2> OMNI_API svuint8_t DemoteTo(Simd<uint8_t, N, kPow2> dn, const svuint32_t v)
{
    using TN = TFromD<decltype(dn)>;
    return U8FromU32(detail::SaturateU<TN>(v));
}

// ------------------------------ Truncations

template <size_t N, int kPow2> OMNI_API svuint8_t TruncateTo(Simd<uint8_t, N, kPow2> /* tag */, const svuint64_t v)
{
    const DFromV<svuint8_t> d;
    const svuint8_t v1 = BitCast(d, v);
    const svuint8_t v2 = svuzp1_u8(v1, v1);
    const svuint8_t v3 = svuzp1_u8(v2, v2);
    return svuzp1_u8(v3, v3);
}

template <size_t N, int kPow2> OMNI_API svuint16_t TruncateTo(Simd<uint16_t, N, kPow2> /* tag */, const svuint64_t v)
{
    const DFromV<svuint16_t> d;
    const svuint16_t v1 = BitCast(d, v);
    const svuint16_t v2 = svuzp1_u16(v1, v1);
    return svuzp1_u16(v2, v2);
}

template <size_t N, int kPow2> OMNI_API svuint32_t TruncateTo(Simd<uint32_t, N, kPow2> /* tag */, const svuint64_t v)
{
    const DFromV<svuint32_t> d;
    const svuint32_t v1 = BitCast(d, v);
    return svuzp1_u32(v1, v1);
}

template <size_t N, int kPow2> OMNI_API svuint8_t TruncateTo(Simd<uint8_t, N, kPow2> /* tag */, const svuint32_t v)
{
    const DFromV<svuint8_t> d;
    const svuint8_t v1 = BitCast(d, v);
    const svuint8_t v2 = svuzp1_u8(v1, v1);
    return svuzp1_u8(v2, v2);
}

template <size_t N, int kPow2> OMNI_API svuint16_t TruncateTo(Simd<uint16_t, N, kPow2> /* tag */, const svuint32_t v)
{
    const DFromV<svuint16_t> d;
    const svuint16_t v1 = BitCast(d, v);
    return svuzp1_u16(v1, v1);
}

template <size_t N, int kPow2> OMNI_API svuint8_t TruncateTo(Simd<uint8_t, N, kPow2> /* tag */, const svuint16_t v)
{
    const DFromV<svuint8_t> d;
    const svuint8_t v1 = BitCast(d, v);
    return svuzp1_u8(v1, v1);
}

// ------------------------------ DemoteTo I

template <size_t N, int kPow2> OMNI_API svint8_t DemoteTo(Simd<int8_t, N, kPow2> dn, const svint16_t v)
{
    using TN = TFromD<decltype(dn)>;
    const svint8_t vn = BitCast(dn, detail::SaturateI<TN>(v));
    return svuzp1_s8(vn, vn);
}

template <size_t N, int kPow2> OMNI_API svint16_t DemoteTo(Simd<int16_t, N, kPow2> dn, const svint32_t v)
{
    using TN = TFromD<decltype(dn)>;
    const svint16_t vn = BitCast(dn, detail::SaturateI<TN>(v));
    return svuzp1_s16(vn, vn);
}

template <size_t N, int kPow2> OMNI_API svint8_t DemoteTo(Simd<int8_t, N, kPow2> dn, const svint32_t v)
{
    const RepartitionToWide<decltype(dn)> d2;
    using TN = TFromD<decltype(dn)>;
    const svint16_t cast16 = BitCast(d2, detail::SaturateI<TN>(v));
    const svint8_t v2 = BitCast(dn, svuzp1_s16(cast16, cast16));
    return BitCast(dn, svuzp1_s8(v2, v2));
}

// ------------------------------ I64/U64 DemoteTo

template <size_t N, int kPow2> OMNI_API svint32_t DemoteTo(Simd<int32_t, N, kPow2> dn, const svint64_t v)
{
    const Rebind<uint64_t, decltype(dn)> du64;
    const RebindToUnsigned<decltype(dn)> dn_u;
    using TN = TFromD<decltype(dn)>;
    const svuint64_t vn = BitCast(du64, detail::SaturateI<TN>(v));
    return BitCast(dn, TruncateTo(dn_u, vn));
}

template <size_t N, int kPow2> OMNI_API svint16_t DemoteTo(Simd<int16_t, N, kPow2> dn, const svint64_t v)
{
    const Rebind<uint64_t, decltype(dn)> du64;
    const RebindToUnsigned<decltype(dn)> dn_u;
    using TN = TFromD<decltype(dn)>;
    const svuint64_t vn = BitCast(du64, detail::SaturateI<TN>(v));
    return BitCast(dn, TruncateTo(dn_u, vn));
}

template <size_t N, int kPow2> OMNI_API svint8_t DemoteTo(Simd<int8_t, N, kPow2> dn, const svint64_t v)
{
    const Rebind<uint64_t, decltype(dn)> du64;
    const RebindToUnsigned<decltype(dn)> dn_u;
    using TN = TFromD<decltype(dn)>;
    const svuint64_t vn = BitCast(du64, detail::SaturateI<TN>(v));
    return BitCast(dn, TruncateTo(dn_u, vn));
}

template <size_t N, int kPow2> OMNI_API svuint32_t DemoteTo(Simd<uint32_t, N, kPow2> dn, const svint64_t v)
{
    const Rebind<uint64_t, decltype(dn)> du64;
    using TN = TFromD<decltype(dn)>;
    // First clamp negative numbers to zero and cast to unsigned.
    const svuint64_t clamped = BitCast(du64, detail::MaxN(v, 0));
    // Saturate to unsigned-max
    const svuint64_t vn = detail::SaturateU<TN>(clamped);
    return TruncateTo(dn, vn);
}

template <size_t N, int kPow2> OMNI_API svuint16_t DemoteTo(Simd<uint16_t, N, kPow2> dn, const svint64_t v)
{
    const Rebind<uint64_t, decltype(dn)> du64;
    using TN = TFromD<decltype(dn)>;
    // First clamp negative numbers to zero and cast to unsigned.
    const svuint64_t clamped = BitCast(du64, detail::MaxN(v, 0));
    // Saturate to unsigned-max
    const svuint64_t vn = detail::SaturateU<TN>(clamped);
    return TruncateTo(dn, vn);
}

template <size_t N, int kPow2> OMNI_API svuint8_t DemoteTo(Simd<uint8_t, N, kPow2> dn, const svint64_t v)
{
    const Rebind<uint64_t, decltype(dn)> du64;
    using TN = TFromD<decltype(dn)>;
    // First clamp negative numbers to zero and cast to unsigned.
    const svuint64_t clamped = BitCast(du64, detail::MaxN(v, 0));
    // Saturate to unsigned-max
    const svuint64_t vn = detail::SaturateU<TN>(clamped);
    return TruncateTo(dn, vn);
}

template <size_t N, int kPow2> OMNI_API svuint32_t DemoteTo(Simd<uint32_t, N, kPow2> dn, const svuint64_t v)
{
    const Rebind<uint64_t, decltype(dn)> du64;
    using TN = TFromD<decltype(dn)>;
    const svuint64_t vn = BitCast(du64, detail::SaturateU<TN>(v));
    return TruncateTo(dn, vn);
}

template <size_t N, int kPow2> OMNI_API svuint16_t DemoteTo(Simd<uint16_t, N, kPow2> dn, const svuint64_t v)
{
    const Rebind<uint64_t, decltype(dn)> du64;
    using TN = TFromD<decltype(dn)>;
    const svuint64_t vn = BitCast(du64, detail::SaturateU<TN>(v));
    return TruncateTo(dn, vn);
}

template <size_t N, int kPow2> OMNI_API svuint8_t DemoteTo(Simd<uint8_t, N, kPow2> dn, const svuint64_t v)
{
    const Rebind<uint64_t, decltype(dn)> du64;
    using TN = TFromD<decltype(dn)>;
    const svuint64_t vn = BitCast(du64, detail::SaturateU<TN>(v));
    return TruncateTo(dn, vn);
}

// ------------------------------ Unsigned to signed demotions

// Disable the default unsigned to signed DemoteTo/ReorderDemote2To
// implementations in generic_ops-inl.h on SVE/SVE2 as the SVE/SVE2 targets have
// target-specific implementations of the unsigned to signed DemoteTo and
// ReorderDemote2To ops

// NOTE: simd::EnableIf<!simd::IsSame<V, V>()>* = nullptr is used instead of
// simd::EnableIf<false>* = nullptr to avoid compiler errors since
// !simd::IsSame<V, V>() is always false and as !simd::IsSame<V, V>() will cause
// SFINAE to occur instead of a hard error due to a dependency on the V template
// argument
#undef OMNI_IF_U2I_DEMOTE_FROM_LANE_SIZE_V
#define OMNI_IF_U2I_DEMOTE_FROM_LANE_SIZE_V(V) simd::EnableIf<!simd::IsSame<V, V>()> * = nullptr

template <class D, class V, OMNI_IF_SIGNED_D(D), OMNI_IF_UNSIGNED_V(V), OMNI_IF_T_SIZE_LE_D(D, sizeof(TFromV<V>) - 1)>
OMNI_API VFromD<D> DemoteTo(D dn, V v)
{
    const RebindToUnsigned<D> dn_u;
    return BitCast(dn, TruncateTo(dn_u, detail::SaturateU<TFromD<D>>(v)));
}

// ------------------------------ ConcatEven/ConcatOdd

// WARNING: the upper half of these needs fixing up (uzp1/uzp2 use the
// full vector length, not rounded down to a power of two as we require).
namespace detail {
#define OMNI_SVE_CONCAT_EVERY_SECOND(BASE, CHAR, BITS, HALF, NAME, OP)                            \
    OMNI_INLINE OMNI_SVE_V(BASE, BITS) NAME(OMNI_SVE_V(BASE, BITS) hi, OMNI_SVE_V(BASE, BITS) lo) \
    {                                                                                             \
        return sv##OP##_##CHAR##BITS(lo, hi);                                                     \
    }

OMNI_SVE_FOREACH(OMNI_SVE_CONCAT_EVERY_SECOND, ConcatEvenFull, uzp1)

OMNI_SVE_FOREACH(OMNI_SVE_CONCAT_EVERY_SECOND, ConcatOddFull, uzp2)

OMNI_SVE_FOREACH_BF16_UNCONDITIONAL(OMNI_SVE_CONCAT_EVERY_SECOND, ConcatEvenFull, uzp1)

OMNI_SVE_FOREACH_BF16_UNCONDITIONAL(OMNI_SVE_CONCAT_EVERY_SECOND, ConcatOddFull, uzp2)

#undef OMNI_SVE_CONCAT_EVERY_SECOND

// Used to slide up / shift whole register left; mask indicates which range
// to take from lo, and the rest is filled from hi starting at its lowest.
#define OMNI_SVE_SPLICE(BASE, CHAR, BITS, HALF, NAME, OP)                                                     \
    OMNI_API OMNI_SVE_V(BASE, BITS) NAME(OMNI_SVE_V(BASE, BITS) hi, OMNI_SVE_V(BASE, BITS) lo, svbool_t mask) \
    {                                                                                                         \
        return sv##OP##_##CHAR##BITS(mask, lo, hi);                                                           \
    }

OMNI_SVE_FOREACH(OMNI_SVE_SPLICE, Splice, splice)

template <class V, OMNI_IF_BF16_D(DFromV<V>)> OMNI_INLINE V Splice(V hi, V lo, svbool_t mask)
{
    const DFromV<V> d;
    const RebindToUnsigned<decltype(d)> du;
    return BitCast(d, Splice(BitCast(du, hi), BitCast(du, lo), mask));
}

#undef OMNI_SVE_SPLICE
} // namespace detail

template <class D> OMNI_API VFromD<D> ConcatOdd(D d, VFromD<D> hi, VFromD<D> lo)
{
    if (detail::IsFull(d))
        return detail::ConcatOddFull(hi, lo);
    const VFromD<D> hi_odd = detail::ConcatOddFull(hi, hi);
    const VFromD<D> lo_odd = detail::ConcatOddFull(lo, lo);
    return detail::Splice(hi_odd, lo_odd, FirstN(d, Lanes(d) / 2));
}

template <class D> OMNI_API VFromD<D> ConcatEven(D d, VFromD<D> hi, VFromD<D> lo)
{
    if (detail::IsFull(d))
        return detail::ConcatEvenFull(hi, lo);
    const VFromD<D> hi_odd = detail::ConcatEvenFull(hi, hi);
    const VFromD<D> lo_odd = detail::ConcatEvenFull(lo, lo);
    return detail::Splice(hi_odd, lo_odd, FirstN(d, Lanes(d) / 2));
}

// ------------------------------ PromoteEvenTo/PromoteOddTo

// Signed to signed PromoteEvenTo: 1 instruction instead of 2 in generic-inl.h.
// Might as well also enable unsigned to unsigned, though it is just an And.
namespace detail {
OMNI_SVE_FOREACH_UI16(OMNI_SVE_RETV_ARGPV, NativePromoteEvenTo, extb)

OMNI_SVE_FOREACH_UI32(OMNI_SVE_RETV_ARGPV, NativePromoteEvenTo, exth)

OMNI_SVE_FOREACH_UI64(OMNI_SVE_RETV_ARGPV, NativePromoteEvenTo, extw)
} // namespace detail

#include "simd/instruction/inside-inl.h"

#ifdef OMNI_NATIVE_DEMOTE_F64_TO_F16
#undef OMNI_NATIVE_DEMOTE_F64_TO_F16
#else
#define OMNI_NATIVE_DEMOTE_F64_TO_F16
#endif

#ifdef OMNI_NATIVE_DEMOTE_F32_TO_BF16
#undef OMNI_NATIVE_DEMOTE_F32_TO_BF16
#else
#define OMNI_NATIVE_DEMOTE_F32_TO_BF16
#endif

namespace detail {
// Round a F32 value to the nearest BF16 value, with the result returned as the
// rounded F32 value bitcasted to an U32

// RoundF32ForDemoteToBF16 also converts NaN values to QNaN values to prevent
// NaN F32 values from being converted to an infinity
OMNI_INLINE svuint32_t RoundF32ForDemoteToBF16(svfloat32_t v)
{
    const DFromV<decltype(v)> df32;
    const RebindToUnsigned<decltype(df32)> du32;

    const auto is_non_nan = Eq(v, v);
    const auto bits32 = BitCast(du32, v);

    const auto round_incr = detail::AddN(detail::AndN(ShiftRight<16>(bits32), 1u), 0x7FFFu);
    return MaskedAddOr(detail::OrN(bits32, 0x00400000u), is_non_nan, bits32, round_incr);
}
} // namespace detail

template <size_t N, int kPow2> OMNI_API VBF16 DemoteTo(Simd<bfloat16_t, N, kPow2> dbf16, svfloat32_t v)
{
    const svuint16_t in_odd = BitCast(ScalableTag<uint16_t>(), detail::RoundF32ForDemoteToBF16(v));
    return BitCast(dbf16, detail::ConcatOddFull(in_odd, in_odd)); // lower half
}

template <size_t N, int kPow2> OMNI_API svfloat32_t DemoteTo(Simd<float32_t, N, kPow2> d, const svfloat64_t v)
{
    const svfloat32_t in_even = svcvt_f32_f64_x(detail::PTrue(d), v);
    return detail::ConcatEvenFull(in_even, in_even); // lower half
}

template <size_t N, int kPow2> OMNI_API svint32_t DemoteTo(Simd<int32_t, N, kPow2> d, const svfloat64_t v)
{
    const svint32_t in_even = svcvt_s32_f64_x(detail::PTrue(d), v);
    return detail::ConcatEvenFull(in_even, in_even); // lower half
}

template <size_t N, int kPow2> OMNI_API svuint32_t DemoteTo(Simd<uint32_t, N, kPow2> d, const svfloat64_t v)
{
    const svuint32_t in_even = svcvt_u32_f64_x(detail::PTrue(d), v);
    return detail::ConcatEvenFull(in_even, in_even); // lower half
}

template <size_t N, int kPow2> OMNI_API svfloat32_t DemoteTo(Simd<float, N, kPow2> d, const svint64_t v)
{
    const svfloat32_t in_even = svcvt_f32_s64_x(detail::PTrue(d), v);
    return detail::ConcatEvenFull(in_even, in_even); // lower half
}

template <size_t N, int kPow2> OMNI_API svfloat32_t DemoteTo(Simd<float, N, kPow2> d, const svuint64_t v)
{
    const svfloat32_t in_even = svcvt_f32_u64_x(detail::PTrue(d), v);
    return detail::ConcatEvenFull(in_even, in_even); // lower half
}

// ------------------------------ ConvertTo F

#define OMNI_SVE_CONVERT(BASE, CHAR, BITS, HALF, NAME, OP)                                                   \
    /* Float from signed */                                                                                  \
    template <size_t N, int kPow2>                                                                           \
    OMNI_API OMNI_SVE_V(BASE, BITS) NAME(OMNI_SVE_D(BASE, BITS, N, kPow2) /* d */, OMNI_SVE_V(int, BITS) v)  \
    {                                                                                                        \
        return sv##OP##_##CHAR##BITS##_s##BITS##_x(OMNI_SVE_PTRUE(BITS), v);                                 \
    }                                                                                                        \
    /* Float from unsigned */                                                                                \
    template <size_t N, int kPow2>                                                                           \
    OMNI_API OMNI_SVE_V(BASE, BITS) NAME(OMNI_SVE_D(BASE, BITS, N, kPow2) /* d */, OMNI_SVE_V(uint, BITS) v) \
    {                                                                                                        \
        return sv##OP##_##CHAR##BITS##_u##BITS##_x(OMNI_SVE_PTRUE(BITS), v);                                 \
    }                                                                                                        \
    /* Signed from float, rounding toward zero */                                                            \
    template <size_t N, int kPow2>                                                                           \
    OMNI_API OMNI_SVE_V(int, BITS) NAME(OMNI_SVE_D(int, BITS, N, kPow2) /* d */, OMNI_SVE_V(BASE, BITS) v)   \
    {                                                                                                        \
        return sv##OP##_s##BITS##_##CHAR##BITS##_x(OMNI_SVE_PTRUE(BITS), v);                                 \
    }                                                                                                        \
    /* Unsigned from float, rounding toward zero */                                                          \
    template <size_t N, int kPow2>                                                                           \
    OMNI_API OMNI_SVE_V(uint, BITS) NAME(OMNI_SVE_D(uint, BITS, N, kPow2) /* d */, OMNI_SVE_V(BASE, BITS) v) \
    {                                                                                                        \
        return sv##OP##_u##BITS##_##CHAR##BITS##_x(OMNI_SVE_PTRUE(BITS), v);                                 \
    }

OMNI_SVE_FOREACH_F(OMNI_SVE_CONVERT, ConvertTo, cvt)

#undef OMNI_SVE_CONVERT

// ------------------------------ NearestInt (Round, ConvertTo)
template <class VF, class DI = RebindToSigned<DFromV<VF>>> OMNI_API VFromD<DI> NearestInt(VF v)
{
    // No single instruction, round then truncate.
    return ConvertTo(DI(), Round(v));
}

template <class DI32, OMNI_IF_I32_D(DI32)>
OMNI_API VFromD<DI32> DemoteToNearestInt(DI32 di32, VFromD<Rebind<double, DI32>> v)
{
    // No single instruction, round then demote.
    return DemoteTo(di32, Round(v));
}

// ------------------------------ Iota (Add, ConvertTo)

#define OMNI_SVE_IOTA(BASE, CHAR, BITS, HALF, NAME, OP)                                      \
    template <size_t N, int kPow2, typename T2>                                              \
    OMNI_API OMNI_SVE_V(BASE, BITS) NAME(OMNI_SVE_D(BASE, BITS, N, kPow2) /* d */, T2 first) \
    {                                                                                        \
        return sv##OP##_##CHAR##BITS(ConvertScalarTo<OMNI_SVE_T(BASE, BITS)>(first), 1);     \
    }

OMNI_SVE_FOREACH_UI(OMNI_SVE_IOTA, Iota, index)

#undef OMNI_SVE_IOTA

template <class D, typename T2, OMNI_IF_FLOAT_D(D)> OMNI_API VFromD<D> Iota(const D d, T2 first)
{
    const RebindToSigned<D> di;
    return detail::AddN(ConvertTo(d, Iota(di, 0)), ConvertScalarTo<TFromD<D>>(first));
}

// ------------------------------ InterleaveLower

template <class D, class V> OMNI_API V InterleaveLower(D d, const V a, const V b)
{
    static_assert(IsSame<TFromD<D>, TFromV<V>>(), "D/V mismatch");
    // Move lower halves of blocks to lower half of vector.
    const Repartition<uint64_t, decltype(d)> d64;
    const auto a64 = BitCast(d64, a);
    const auto b64 = BitCast(d64, b);
    const auto a_blocks = detail::ConcatEvenFull(a64, a64); // lower half
    const auto b_blocks = detail::ConcatEvenFull(b64, b64);
    return detail::ZipLowerSame(BitCast(d, a_blocks), BitCast(d, b_blocks));
}

template <class V> OMNI_API V InterleaveLower(const V a, const V b)
{
    return InterleaveLower(DFromV<V>(), a, b);
}

// ------------------------------ InterleaveUpper

// Only use zip2 if vector are a powers of two, otherwise getting the actual
// "upper half" requires MaskUpperHalf.
namespace detail {
// Unlike Simd's ZipUpper, this returns the same type.
OMNI_SVE_FOREACH(OMNI_SVE_RETV_ARGVV, ZipUpperSame, zip2)
} // namespace detail

// Full vector: guaranteed to have at least one block
template <class D, class V = VFromD<D>, simd::EnableIf<detail::IsFull(D())> * = nullptr>
OMNI_API V InterleaveUpper(D d, const V a, const V b)
{
    // Move upper halves of blocks to lower half of vector.
    const Repartition<uint64_t, decltype(d)> d64;
    const auto a64 = BitCast(d64, a);
    const auto b64 = BitCast(d64, b);
    const auto a_blocks = detail::ConcatOddFull(a64, a64); // lower half
    const auto b_blocks = detail::ConcatOddFull(b64, b64);
    return detail::ZipLowerSame(BitCast(d, a_blocks), BitCast(d, b_blocks));
}

// Capped/fraction: need runtime check
template <class D, class V = VFromD<D>, simd::EnableIf<!detail::IsFull(D())> * = nullptr>
OMNI_API V InterleaveUpper(D d, const V a, const V b)
{
    // Less than one block: treat as capped
    if (Lanes(d) * sizeof(TFromD<D>) < 16) {
        const Half<decltype(d)> d2;
        return InterleaveLower(d, UpperHalf(d2, a), UpperHalf(d2, b));
    }
    return InterleaveUpper(DFromV<V>(), a, b);
}

// ------------------------------ InterleaveWholeLower
#ifdef OMNI_NATIVE_INTERLEAVE_WHOLE
#undef OMNI_NATIVE_INTERLEAVE_WHOLE
#else
#define OMNI_NATIVE_INTERLEAVE_WHOLE
#endif

template <class D> OMNI_API VFromD<D> InterleaveWholeLower(D /* d */, VFromD<D> a, VFromD<D> b)
{
    return detail::ZipLowerSame(a, b);
}

// ------------------------------ InterleaveWholeUpper

template <class D> OMNI_API VFromD<D> InterleaveWholeUpper(D d, VFromD<D> a, VFromD<D> b)
{
    if (detail::IsFull(d)) {
        return detail::ZipUpperSame(a, b);
    }

    const Half<decltype(d)> d2;
    return InterleaveWholeLower(d, UpperHalf(d2, a), UpperHalf(d2, b));
}

// ------------------------------ Per4LaneBlockShuffle

namespace detail {
template <size_t kLaneSize, size_t kVectSize, class V, OMNI_IF_NOT_T_SIZE_V(V, 8)>
OMNI_INLINE V Per4LaneBlockShuffle(simd::SizeTag<0x88> /* idx_3210_tag */, simd::SizeTag<kLaneSize> /* lane_size_tag */,
    simd::SizeTag<kVectSize> /* vect_size_tag */, V v)
{
    const DFromV<decltype(v)> d;
    const RebindToUnsigned<decltype(d)> du;
    const RepartitionToWide<decltype(du)> dw;

    const auto evens = BitCast(dw, ConcatEvenFull(v, v));
    return BitCast(d, ZipLowerSame(evens, evens));
}

template <size_t kLaneSize, size_t kVectSize, class V, OMNI_IF_NOT_T_SIZE_V(V, 8)>
OMNI_INLINE V Per4LaneBlockShuffle(simd::SizeTag<0xDD> /* idx_3210_tag */, simd::SizeTag<kLaneSize> /* lane_size_tag */,
    simd::SizeTag<kVectSize> /* vect_size_tag */, V v)
{
    const DFromV<decltype(v)> d;
    const RebindToUnsigned<decltype(d)> du;
    const RepartitionToWide<decltype(du)> dw;

    const auto odds = BitCast(dw, ConcatOddFull(v, v));
    return BitCast(d, ZipLowerSame(odds, odds));
}
} // namespace detail

// ================================================== COMBINE
namespace detail {
template <class D, OMNI_IF_T_SIZE_D(D, 1)> svbool_t MaskLowerHalf(D d)
{
    switch (Lanes(d)) {
        case 32:
            return svptrue_pat_b8(SV_VL16);
        case 16:
            return svptrue_pat_b8(SV_VL8);
        case 8:
            return svptrue_pat_b8(SV_VL4);
        case 4:
            return svptrue_pat_b8(SV_VL2);
        default:
            return svptrue_pat_b8(SV_VL1);
    }
}

template <class D, OMNI_IF_T_SIZE_D(D, 2)> svbool_t MaskLowerHalf(D d)
{
    switch (Lanes(d)) {
        case 16:
            return svptrue_pat_b16(SV_VL8);
        case 8:
            return svptrue_pat_b16(SV_VL4);
        case 4:
            return svptrue_pat_b16(SV_VL2);
        default:
            return svptrue_pat_b16(SV_VL1);
    }
}

template <class D, OMNI_IF_T_SIZE_D(D, 4)> svbool_t MaskLowerHalf(D d)
{
    switch (Lanes(d)) {
        case 8:
            return svptrue_pat_b32(SV_VL4);
        case 4:
            return svptrue_pat_b32(SV_VL2);
        default:
            return svptrue_pat_b32(SV_VL1);
    }
}

template <class D, OMNI_IF_T_SIZE_D(D, 8)> svbool_t MaskLowerHalf(D d)
{
    switch (Lanes(d)) {
        case 4:
            return svptrue_pat_b64(SV_VL2);
        default:
            return svptrue_pat_b64(SV_VL1);
    }
}

template <class D> svbool_t MaskUpperHalf(D d)
{
    if (IsFull(d)) {
        return Not(MaskLowerHalf(d));
    }

    // For Splice to work as intended, make sure bits above Lanes(d) are zero.
    return AndNot(MaskLowerHalf(d), detail::MakeMask(d));
}

// Right-shift vector pair by constexpr; can be used to slide down (=N) or up
// (=Lanes()-N).
#define OMNI_SVE_EXT(BASE, CHAR, BITS, HALF, NAME, OP)                                         \
    template <size_t kIndex>                                                                   \
    OMNI_API OMNI_SVE_V(BASE, BITS) NAME(OMNI_SVE_V(BASE, BITS) hi, OMNI_SVE_V(BASE, BITS) lo) \
    {                                                                                          \
        return sv##OP##_##CHAR##BITS(lo, hi, kIndex);                                          \
    }

OMNI_SVE_FOREACH(OMNI_SVE_EXT, Ext, ext)

#undef OMNI_SVE_EXT
} // namespace detail

// ------------------------------ ConcatUpperLower
template <class D, class V> OMNI_API V ConcatUpperLower(const D d, const V hi, const V lo)
{
    return IfThenElse(detail::MaskLowerHalf(d), lo, hi);
}

// ------------------------------ ConcatLowerLower
template <class D, class V> OMNI_API V ConcatLowerLower(const D d, const V hi, const V lo)
{
    return detail::Splice(hi, lo, detail::MaskLowerHalf(d));
}

// ------------------------------ ConcatLowerUpper
template <class D, class V> OMNI_API V ConcatLowerUpper(const D d, const V hi, const V lo)
{
    if (detail::IsFull(d)) {
        return detail::Ext<Lanes(d) / 2>(hi, lo);
    }
}

// ------------------------------ ConcatUpperUpper
template <class D, class V> OMNI_API V ConcatUpperUpper(const D d, const V hi, const V lo)
{
    const svbool_t mask_upper = detail::MaskUpperHalf(d);
    const V lo_upper = detail::Splice(lo, lo, mask_upper);
    return IfThenElse(mask_upper, hi, lo_upper);
}

// ------------------------------ Combine
template <class D, class V2> OMNI_API VFromD<D> Combine(const D d, const V2 hi, const V2 lo)
{
    return ConcatLowerLower(d, hi, lo);
}

// ------------------------------ ZeroExtendVector
template <class D, class V> OMNI_API V ZeroExtendVector(const D d, const V lo)
{
    return Combine(d, Zero(Half<D>()), lo);
}

// ------------------------------ Lower/UpperHalf

template <class D2, class V> OMNI_API V LowerHalf(D2 /* tag */, const V v)
{
    return v;
}

template <class V> OMNI_API V LowerHalf(const V v)
{
    return v;
}

template <class DH, class V> OMNI_API V UpperHalf(const DH dh, const V v)
{
    const Twice<decltype(dh)> d;
    // Cast so that we support bfloat16_t.
    const RebindToUnsigned<decltype(d)> du;
    const VFromD<decltype(du)> vu = BitCast(du, v);
    return BitCast(d, detail::Ext<Lanes(dh)>(vu, vu));
}

// ================================================== REDUCE

#ifdef OMNI_NATIVE_REDUCE_SCALAR
#undef OMNI_NATIVE_REDUCE_SCALAR
#else
#define OMNI_NATIVE_REDUCE_SCALAR
#endif

// These return T, suitable for ReduceSum.
namespace detail {
#define OMNI_SVE_REDUCE_ADD(BASE, CHAR, BITS, HALF, NAME, OP)                                                \
    OMNI_API OMNI_SVE_T(BASE, BITS) NAME(svbool_t pg, OMNI_SVE_V(BASE, BITS) v)                              \
    {                                                                                                        \
        /* The intrinsic returns [u]int64_t; truncate to T so we can broadcast. */                           \
        using T = OMNI_SVE_T(BASE, BITS);                                                                    \
        using TU = MakeUnsigned<T>;                                                                          \
        constexpr uint64_t kMask = LimitsMax<TU>();                                                          \
        return static_cast<T>(static_cast<TU>(static_cast<uint64_t>(sv##OP##_##CHAR##BITS(pg, v)) & kMask)); \
    }

#define OMNI_SVE_REDUCE(BASE, CHAR, BITS, HALF, NAME, OP)                       \
    OMNI_API OMNI_SVE_T(BASE, BITS) NAME(svbool_t pg, OMNI_SVE_V(BASE, BITS) v) \
    {                                                                           \
        return sv##OP##_##CHAR##BITS(pg, v);                                    \
    }

OMNI_SVE_FOREACH_UI(OMNI_SVE_REDUCE_ADD, SumOfLanesM, addv)

OMNI_SVE_FOREACH_F(OMNI_SVE_REDUCE, SumOfLanesM, addv)

OMNI_SVE_FOREACH_UI(OMNI_SVE_REDUCE, MinOfLanesM, minv)

OMNI_SVE_FOREACH_UI(OMNI_SVE_REDUCE, MaxOfLanesM, maxv)
// NaN if all are
OMNI_SVE_FOREACH_F(OMNI_SVE_REDUCE, MinOfLanesM, minnmv)

OMNI_SVE_FOREACH_F(OMNI_SVE_REDUCE, MaxOfLanesM, maxnmv)

#undef OMNI_SVE_REDUCE
#undef OMNI_SVE_REDUCE_ADD
} // namespace detail

// detail::SumOfLanesM, detail::MinOfLanesM, and detail::MaxOfLanesM is more
// efficient for N=4 I8/U8 reductions on SVE than the default implementations
// of the N=4 I8/U8 ReduceSum/ReduceMin/ReduceMax operations in
// generic_ops-inl.h
#undef OMNI_IF_REDUCE_D
#define OMNI_IF_REDUCE_D(D) simd::EnableIf<OMNI_MAX_LANES_D(D) != 1> * = nullptr

#ifdef OMNI_NATIVE_REDUCE_SUM_4_UI8
#undef OMNI_NATIVE_REDUCE_SUM_4_UI8
#else
#define OMNI_NATIVE_REDUCE_SUM_4_UI8
#endif

#ifdef OMNI_NATIVE_REDUCE_MINMAX_4_UI8
#undef OMNI_NATIVE_REDUCE_MINMAX_4_UI8
#else
#define OMNI_NATIVE_REDUCE_MINMAX_4_UI8
#endif

template <class D, OMNI_IF_REDUCE_D(D)> OMNI_API TFromD<D> ReduceSum(D d, VFromD<D> v)
{
    return detail::SumOfLanesM(detail::MakeMask(d), v);
}

template <class D, OMNI_IF_REDUCE_D(D)> OMNI_API TFromD<D> ReduceMin(D d, VFromD<D> v)
{
    return detail::MinOfLanesM(detail::MakeMask(d), v);
}

template <class D, OMNI_IF_REDUCE_D(D)> OMNI_API TFromD<D> ReduceMax(D d, VFromD<D> v)
{
    return detail::MaxOfLanesM(detail::MakeMask(d), v);
}

// ------------------------------ SumOfLanes

template <class D, OMNI_IF_LANES_GT_D(D, 1)> OMNI_API VFromD<D> SumOfLanes(D d, VFromD<D> v)
{
    return Set(d, ReduceSum(d, v));
}

template <class D, OMNI_IF_LANES_GT_D(D, 1)> OMNI_API VFromD<D> MinOfLanes(D d, VFromD<D> v)
{
    return Set(d, ReduceMin(d, v));
}

template <class D, OMNI_IF_LANES_GT_D(D, 1)> OMNI_API VFromD<D> MaxOfLanes(D d, VFromD<D> v)
{
    return Set(d, ReduceMax(d, v));
}

// ================================================== SWIZZLE

// ------------------------------ GetLane

namespace detail {
#define OMNI_SVE_GET_LANE(BASE, CHAR, BITS, HALF, NAME, OP)                          \
    OMNI_INLINE OMNI_SVE_T(BASE, BITS) NAME(OMNI_SVE_V(BASE, BITS) v, svbool_t mask) \
    {                                                                                \
        return sv##OP##_##CHAR##BITS(mask, v);                                       \
    }

OMNI_SVE_FOREACH(OMNI_SVE_GET_LANE, GetLaneM, lasta)

OMNI_SVE_FOREACH(OMNI_SVE_GET_LANE, ExtractLastMatchingLaneM, lastb)

#undef OMNI_SVE_GET_LANE
} // namespace detail

template <class V> OMNI_API TFromV<V> GetLane(V v)
{
    return detail::GetLaneM(v, detail::PFalse());
}

// ------------------------------ ExtractLane
template <class V> OMNI_API TFromV<V> ExtractLane(V v, size_t i)
{
    return detail::GetLaneM(v, FirstN(DFromV<V>(), i));
}

// ------------------------------ InsertLane (IfThenElse)
template <class V, typename T> OMNI_API V InsertLane(const V v, size_t i, T t)
{
    static_assert(sizeof(TFromV<V>) == sizeof(T), "Lane size mismatch");
    const DFromV<V> d;
    const RebindToSigned<decltype(d)> di;
    using TI = TFromD<decltype(di)>;
    const svbool_t is_i = detail::EqN(Iota(di, 0), static_cast<TI>(i));
    return IfThenElse(RebindMask(d, is_i), Set(d, simd::ConvertScalarTo<TFromV<V>>(t)), v);
}

// ------------------------------ DupEven

namespace detail {
OMNI_SVE_FOREACH(OMNI_SVE_RETV_ARGVV, InterleaveEven, trn1)
} // namespace detail

template <class V> OMNI_API V DupEven(const V v)
{
    return detail::InterleaveEven(v, v);
}

// ------------------------------ DupOdd

namespace detail {
OMNI_SVE_FOREACH(OMNI_SVE_RETV_ARGVV, InterleaveOdd, trn2)
} // namespace detail

template <class V> OMNI_API V DupOdd(const V v)
{
    return detail::InterleaveOdd(v, v);
}

// ------------------------------ OddEven
template <class V> OMNI_API V OddEven(const V odd, const V even)
{
    const auto odd_in_even = detail::Ext<1>(odd, odd);
    return detail::InterleaveEven(even, odd_in_even);
}

// ------------------------------ InterleaveEven
template <class D> OMNI_API VFromD<D> InterleaveEven(D /* d */, VFromD<D> a, VFromD<D> b)
{
    return detail::InterleaveEven(a, b);
}

// ------------------------------ InterleaveOdd
template <class D> OMNI_API VFromD<D> InterleaveOdd(D /* d */, VFromD<D> a, VFromD<D> b)
{
    return detail::InterleaveOdd(a, b);
}

// ------------------------------ OddEvenBlocks
template <class V> OMNI_API V OddEvenBlocks(const V odd, const V even)
{
    const DFromV<V> d;
    return ConcatUpperLower(d, odd, even);
}

// ------------------------------ TableLookupLanes

template <class D, class VI> OMNI_API VFromD<RebindToUnsigned<D>> IndicesFromVec(D d, VI vec)
{
    using TI = TFromV<VI>;
    static_assert(sizeof(TFromD<D>) == sizeof(TI), "Index/lane size mismatch");
    const RebindToUnsigned<D> du;
    const auto indices = BitCast(du, vec);
    (void)d;
    return indices;
}

template <class D, typename TI> OMNI_API VFromD<RebindToUnsigned<D>> SetTableIndices(D d, const TI *idx)
{
    static_assert(sizeof(TFromD<D>) == sizeof(TI), "Index size must match lane");
    return IndicesFromVec(d, LoadU(Rebind<TI, D>(), idx));
}

#define OMNI_SVE_TABLE(BASE, CHAR, BITS, HALF, NAME, OP)                                       \
    OMNI_API OMNI_SVE_V(BASE, BITS) NAME(OMNI_SVE_V(BASE, BITS) v, OMNI_SVE_V(uint, BITS) idx) \
    {                                                                                          \
        return sv##OP##_##CHAR##BITS(v, idx);                                                  \
    }

OMNI_SVE_FOREACH(OMNI_SVE_TABLE, TableLookupLanes, tbl)

OMNI_SVE_FOREACH_BF16_UNCONDITIONAL(OMNI_SVE_TABLE, TableLookupLanes, tbl)

#undef OMNI_SVE_TABLE

template <class D>
OMNI_API VFromD<D> TwoTablesLookupLanes(D d, VFromD<D> a, VFromD<D> b, VFromD<RebindToUnsigned<D>> idx)
{
    // SVE2 has an instruction for this, but it only works for full 2^n vectors.
    const RebindToUnsigned<decltype(d)> du;
    using TU = TFromD<decltype(du)>;

    const size_t num_of_lanes = Lanes(d);
    const auto idx_mod = detail::AndN(idx, static_cast<TU>(num_of_lanes - 1));
    const auto sel_a_mask = Eq(idx, idx_mod);

    const auto a_lookup_result = TableLookupLanes(a, idx_mod);
    const auto b_lookup_result = TableLookupLanes(b, idx_mod);
    return IfThenElse(sel_a_mask, a_lookup_result, b_lookup_result);
}

template <class V> OMNI_API V TwoTablesLookupLanes(V a, V b, VFromD<RebindToUnsigned<DFromV<V>>> idx)
{
    const DFromV<decltype(a)> d;
    return TwoTablesLookupLanes(d, a, b, idx);
}

// ------------------------------ SwapAdjacentBlocks (TableLookupLanes)

namespace detail {
template <typename T, size_t N, int kPow2> constexpr size_t LanesPerBlock(Simd<T, N, kPow2> d)
{
    // We might have a capped vector smaller than a block, so honor that.
    return OMNI_MIN(16 / sizeof(T), MaxLanes(d));
}
} // namespace detail

template <class V> OMNI_API V SwapAdjacentBlocks(const V v)
{
    const DFromV<V> d;
    return ConcatLowerUpper(d, v, v);
}

// ------------------------------ Reverse

namespace detail {
#define OMNI_SVE_REVERSE(BASE, CHAR, BITS, HALF, NAME, OP)         \
    OMNI_API OMNI_SVE_V(BASE, BITS) NAME(OMNI_SVE_V(BASE, BITS) v) \
    {                                                              \
        return sv##OP##_##CHAR##BITS(v);                           \
    }

OMNI_SVE_FOREACH(OMNI_SVE_REVERSE, ReverseFull, rev)

OMNI_SVE_FOREACH_BF16_UNCONDITIONAL(OMNI_SVE_REVERSE, ReverseFull, rev)

#undef OMNI_SVE_REVERSE
} // namespace detail

template <class D, class V> OMNI_API V Reverse(D d, V v)
{
    using T = TFromD<D>;
    const auto reversed = detail::ReverseFull(v);
    if (detail::IsFull(d))
        return reversed;
    // Shift right to remove extra (non-pow2 and remainder) lanes.
    // Avoids FirstN truncating to the return vector size. Must also avoid Not
    // because that is limited to SV_POW2.
    const ScalableTag<T> dfull;
    const svbool_t all_true = detail::AllPTrue(dfull);
    const size_t all_lanes = detail::AllHardwareLanes<T>();
    const size_t want_lanes = Lanes(d);
    const svbool_t mask = svnot_b_z(all_true, FirstN(dfull, all_lanes - want_lanes));
    return detail::Splice(reversed, reversed, mask);
}

// ------------------------------ Reverse2

// Per-target flag to prevent generic_ops-inl.h defining 8-bit Reverse2/4/8.
#ifdef OMNI_NATIVE_REVERSE2_8
#undef OMNI_NATIVE_REVERSE2_8
#else
#define OMNI_NATIVE_REVERSE2_8
#endif

template <class D, OMNI_IF_T_SIZE_D(D, 1)> OMNI_API VFromD<D> Reverse2(D d, const VFromD<D> v)
{
    const RebindToUnsigned<decltype(d)> du;
    const RepartitionToWide<decltype(du)> dw;
    return BitCast(d, svrevb_u16_x(detail::PTrue(d), BitCast(dw, v)));
}

template <class D, OMNI_IF_T_SIZE_D(D, 2)> OMNI_API VFromD<D> Reverse2(D d, const VFromD<D> v)
{
    const RebindToUnsigned<decltype(d)> du;
    const RepartitionToWide<decltype(du)> dw;
    return BitCast(d, svrevh_u32_x(detail::PTrue(d), BitCast(dw, v)));
}

template <class D, OMNI_IF_T_SIZE_D(D, 4)> OMNI_API VFromD<D> Reverse2(D d, const VFromD<D> v)
{
    const RebindToUnsigned<decltype(d)> du;
    const RepartitionToWide<decltype(du)> dw;
    return BitCast(d, svrevw_u64_x(detail::PTrue(d), BitCast(dw, v)));
}

template <class D, OMNI_IF_T_SIZE_D(D, 8)> OMNI_API VFromD<D> Reverse2(D d, const VFromD<D> v)
{ // 3210
    (void)d;
    const auto odd_in_even = detail::Ext<1>(v, v); // x321
    return detail::InterleaveEven(odd_in_even, v); // 2301
}

// ------------------------------ Reverse4 (TableLookupLanes)

template <class D, OMNI_IF_T_SIZE_D(D, 1)> OMNI_API VFromD<D> Reverse4(D d, const VFromD<D> v)
{
    const RebindToUnsigned<decltype(d)> du;
    const RepartitionToWideX2<decltype(du)> du32;
    return BitCast(d, svrevb_u32_x(detail::PTrue(d), BitCast(du32, v)));
}

template <class D, OMNI_IF_T_SIZE_D(D, 2)> OMNI_API VFromD<D> Reverse4(D d, const VFromD<D> v)
{
    const RebindToUnsigned<decltype(d)> du;
    const RepartitionToWideX2<decltype(du)> du64;
    return BitCast(d, svrevh_u64_x(detail::PTrue(d), BitCast(du64, v)));
}

template <class D, OMNI_IF_T_SIZE_D(D, 4)> OMNI_API VFromD<D> Reverse4(D d, const VFromD<D> v)
{
    const RebindToUnsigned<decltype(d)> du;
    const auto idx = detail::XorN(Iota(du, 0), 3);
    return TableLookupLanes(v, idx);
}

template <class D, OMNI_IF_T_SIZE_D(D, 8)> OMNI_API VFromD<D> Reverse4(D d, const VFromD<D> v)
{
    if (detail::IsFull(d)) {
        return detail::ReverseFull(v);
    }
    const RebindToUnsigned<decltype(d)> du;
    const auto idx = detail::XorN(Iota(du, 0), 3);
    return TableLookupLanes(v, idx);
}

// ------------------------------ Reverse8 (TableLookupLanes)

template <class D, OMNI_IF_T_SIZE_D(D, 1)> OMNI_API VFromD<D> Reverse8(D d, const VFromD<D> v)
{
    const Repartition<uint64_t, decltype(d)> du64;
    return BitCast(d, svrevb_u64_x(detail::PTrue(d), BitCast(du64, v)));
}

template <class D, OMNI_IF_NOT_T_SIZE_D(D, 1)> OMNI_API VFromD<D> Reverse8(D d, const VFromD<D> v)
{
    const RebindToUnsigned<decltype(d)> du;
    const auto idx = detail::XorN(Iota(du, 0), 7);
    return TableLookupLanes(v, idx);
}

// ------------------------------- ReverseBits

#ifdef OMNI_NATIVE_REVERSE_BITS_UI8
#undef OMNI_NATIVE_REVERSE_BITS_UI8
#else
#define OMNI_NATIVE_REVERSE_BITS_UI8
#endif

#ifdef OMNI_NATIVE_REVERSE_BITS_UI16_32_64
#undef OMNI_NATIVE_REVERSE_BITS_UI16_32_64
#else
#define OMNI_NATIVE_REVERSE_BITS_UI16_32_64
#endif

#define OMNI_SVE_REVERSE_BITS(BASE, CHAR, BITS, HALF, NAME, OP)    \
    OMNI_API OMNI_SVE_V(BASE, BITS) NAME(OMNI_SVE_V(BASE, BITS) v) \
    {                                                              \
        const DFromV<decltype(v)> d;                               \
        return sv##OP##_##CHAR##BITS##_x(detail::PTrue(d), v);     \
    }

OMNI_SVE_FOREACH_UI(OMNI_SVE_REVERSE_BITS, ReverseBits, rbit)

#undef OMNI_SVE_REVERSE_BITS

// ------------------------------ SlideUpLanes

template <class D> OMNI_API VFromD<D> SlideUpLanes(D d, VFromD<D> v, size_t amt)
{
    return detail::Splice(v, Zero(d), FirstN(d, amt));
}

// ------------------------------ Slide1Up

#ifdef OMNI_NATIVE_SLIDE1_UP_DOWN
#undef OMNI_NATIVE_SLIDE1_UP_DOWN
#else
#define OMNI_NATIVE_SLIDE1_UP_DOWN
#endif

template <class D> OMNI_API VFromD<D> Slide1Up(D d, VFromD<D> v)
{
    return SlideUpLanes(d, v, 1);
}

// ------------------------------ SlideDownLanes (TableLookupLanes)

template <class D> OMNI_API VFromD<D> SlideDownLanes(D d, VFromD<D> v, size_t amt)
{
    const RebindToUnsigned<decltype(d)> du;
    using TU = TFromD<decltype(du)>;
    const auto idx = Iota(du, static_cast<TU>(amt));
    return IfThenElseZero(FirstN(d, Lanes(d) - amt), TableLookupLanes(v, idx));
}

// ------------------------------ Slide1Down

template <class D> OMNI_API VFromD<D> Slide1Down(D d, VFromD<D> v)
{
    return SlideDownLanes(d, v, 1);
}

// ------------------------------ Block insert/extract/broadcast ops
#ifdef OMNI_NATIVE_BLK_INSERT_EXTRACT
#undef OMNI_NATIVE_BLK_INSERT_EXTRACT
#else
#define OMNI_NATIVE_BLK_INSERT_EXTRACT
#endif

template <int kBlockIdx, class V> OMNI_API V InsertBlock(V v, V blk_to_insert)
{
    const DFromV<decltype(v)> d;
    static_assert(0 <= kBlockIdx && kBlockIdx < d.MaxBlocks(), "Invalid block index");
    return (kBlockIdx == 0) ? ConcatUpperLower(d, v, blk_to_insert) : ConcatLowerLower(d, blk_to_insert, v);
}

template <int kBlockIdx, class V> OMNI_API V ExtractBlock(V v)
{
    const DFromV<decltype(v)> d;
    static_assert(0 <= kBlockIdx && kBlockIdx < d.MaxBlocks(), "Invalid block index");

    if (kBlockIdx == 0)
        return v;
    return UpperHalf(Half<decltype(d)>(), v);
}

template <int kBlockIdx, class V> OMNI_API V BroadcastBlock(V v)
{
    const DFromV<decltype(v)> d;
    static_assert(0 <= kBlockIdx && kBlockIdx < d.MaxBlocks(), "Invalid block index");

    const RebindToUnsigned<decltype(d)> du; // for bfloat16_t
    using VU = VFromD<decltype(du)>;
    const VU vu = BitCast(du, v);

    return BitCast(d, (kBlockIdx == 0) ? ConcatLowerLower(du, vu, vu) : ConcatUpperUpper(du, vu, vu));
}

// ------------------------------ Compress (PromoteTo)

template <typename T> struct CompressIsPartition {
    // Optimization for 64-bit lanes (could also be applied to 32-bit, but that
    // requires a larger table).
    enum {
        value = (sizeof(T) == 8)
    };
};

#define OMNI_SVE_COMPRESS(BASE, CHAR, BITS, HALF, NAME, OP)                       \
    OMNI_API OMNI_SVE_V(BASE, BITS) NAME(OMNI_SVE_V(BASE, BITS) v, svbool_t mask) \
    {                                                                             \
        return sv##OP##_##CHAR##BITS(mask, v);                                    \
    }

OMNI_SVE_FOREACH_UI32(OMNI_SVE_COMPRESS, Compress, compact)

OMNI_SVE_FOREACH_F32(OMNI_SVE_COMPRESS, Compress, compact)

#undef OMNI_SVE_COMPRESS

template <class V, OMNI_IF_T_SIZE_V(V, 8)> OMNI_API V Compress(V v, svbool_t mask)
{
    const DFromV<V> d;
    const RebindToUnsigned<decltype(d)> du64;

    // Convert mask into bitfield via horizontal sum (faster than ORV) of masked
    // bits 1, 2, 4, 8. Pre-multiply by N so we can use it as an offset for
    // SetTableIndices.
    const svuint64_t bits = Shl(Set(du64, 1), Iota(du64, 2));
    const size_t offset = detail::SumOfLanesM(mask, bits);

    // See CompressIsPartition.
    alignas(16) static constexpr uint64_t table[4 * 16] = {
        // PrintCompress64x4Tables
            0, 1, 2, 3, 0, 1, 2, 3, 1, 0, 2, 3, 0, 1, 2, 3, 2, 0, 1, 3, 0, 2, 1, 3, 1, 2, 0, 3, 0, 1, 2, 3, 3, 0, 1, 2,
            0, 3, 1, 2, 1, 3, 0, 2, 0, 1, 3, 2, 2, 3, 0, 1, 0, 2, 3, 1, 1, 2, 3, 0, 0, 1, 2, 3 };
    return TableLookupLanes(v, SetTableIndices(d, table + offset));
}

template <class V, OMNI_IF_T_SIZE_V(V, 2)> OMNI_API V Compress(V v, svbool_t mask16)
{
    static_assert(!IsSame<V, svfloat16_t>(), "Must use overload");
    const DFromV<V> d16;

    // Promote vector and mask to 32-bit
    const RepartitionToWide<decltype(d16)> dw;
    const auto v32L = PromoteTo(dw, v);
    const auto v32H = detail::PromoteUpperTo(dw, v);
    const svbool_t mask32L = svunpklo_b(mask16);
    const svbool_t mask32H = svunpkhi_b(mask16);

    const auto compressedL = Compress(v32L, mask32L);
    const auto compressedH = Compress(v32H, mask32H);

    // Demote to 16-bit (already in range) - separately so we can splice
    const V evenL = BitCast(d16, compressedL);
    const V evenH = BitCast(d16, compressedH);
    const V v16L = detail::ConcatEvenFull(evenL, evenL); // lower half
    const V v16H = detail::ConcatEvenFull(evenH, evenH);

    // We need to combine two vectors of non-constexpr length, so the only option
    // is Splice, which requires us to synthesize a mask. NOTE: this function uses
    // full vectors (SV_ALL instead of SV_POW2), hence we need unmasked svcnt.
    const size_t countL = detail::CountTrueFull(dw, mask32L);
    const auto compressed_maskL = FirstN(d16, countL);
    return detail::Splice(v16H, v16L, compressed_maskL);
}

// ------------------------------ CompressNot

// 2 or 4 bytes
template <class V, OMNI_IF_T_SIZE_ONE_OF_V(V, (1 << 2) | (1 << 4))> OMNI_API V CompressNot(V v, const svbool_t mask)
{
    return Compress(v, Not(mask));
}

template <class V, OMNI_IF_T_SIZE_V(V, 8)> OMNI_API V CompressNot(V v, svbool_t mask)
{
    const DFromV<V> d;
    const RebindToUnsigned<decltype(d)> du64;

    // Convert mask into bitfield via horizontal sum (faster than ORV) of masked
    // bits 1, 2, 4, 8. Pre-multiply by N so we can use it as an offset for
    // SetTableIndices.
    const svuint64_t bits = Shl(Set(du64, 1), Iota(du64, 2));
    const size_t offset = detail::SumOfLanesM(mask, bits);

    // See CompressIsPartition.
    alignas(16) static constexpr uint64_t table[4 * 16] = {
        // PrintCompressNot64x4Tables
            0, 1, 2, 3, 1, 2, 3, 0, 0, 2, 3, 1, 2, 3, 0, 1, 0, 1, 3, 2, 1, 3, 0, 2, 0, 3, 1, 2, 3, 0, 1, 2, 0, 1, 2, 3,
            1, 2, 0, 3, 0, 2, 1, 3, 2, 0, 1, 3, 0, 1, 2, 3, 1, 0, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3 };
    return TableLookupLanes(v, SetTableIndices(d, table + offset));
}

// ------------------------------ CompressBlocksNot
OMNI_API svuint64_t CompressBlocksNot(svuint64_t v, svbool_t mask)
{
    uint64_t bits = 0;          // predicate reg is 32-bit
    CopyBytes<4>(&mask, &bits); // not same size - 64-bit more efficient
    // Concatenate LSB for upper and lower blocks, pre-scale by 4 for table idx.
    const size_t offset = ((bits & 1) ? 4u : 0u) + ((bits & 0x10000) ? 8u : 0u);
    // See CompressIsPartition. Manually generated; flip halves if mask = [0, 1].
    alignas(16) static constexpr uint64_t table[4 * 4] = { 0, 1, 2, 3, 2, 3, 0, 1, 0, 1, 2, 3, 0, 1, 2, 3 };
    const ScalableTag<uint64_t> d;
    return TableLookupLanes(v, SetTableIndices(d, table + offset));
}

// ------------------------------ CompressStore
template <class V, class D, OMNI_IF_NOT_T_SIZE_D(D, 1)>
OMNI_API size_t CompressStore(const V v, const svbool_t mask, const D d, TFromD<D> *OMNI_RESTRICT unaligned)
{
    StoreU(Compress(v, mask), d, unaligned);
    return CountTrue(d, mask);
}

// ------------------------------ CompressBlendedStore
template <class V, class D, OMNI_IF_NOT_T_SIZE_D(D, 1)>
OMNI_API size_t CompressBlendedStore(const V v, const svbool_t mask, const D d, TFromD<D> *OMNI_RESTRICT unaligned)
{
    const size_t count = CountTrue(d, mask);
    const svbool_t store_mask = FirstN(d, count);
    BlendedStore(Compress(v, mask), store_mask, d, unaligned);
    return count;
}

// ================================================== MASK (2)

// ------------------------------ FindKnownLastTrue
template <class D> OMNI_API size_t FindKnownLastTrue(D d, svbool_t m)
{
    const RebindToUnsigned<decltype(d)> du;
    return static_cast<size_t>(detail::ExtractLastMatchingLaneM(Iota(du, 0), And(m, detail::MakeMask(d))));
}

// ------------------------------ FindLastTrue
template <class D> OMNI_API intptr_t FindLastTrue(D d, svbool_t m)
{
    return AllFalse(d, m) ? intptr_t{ -1 } : static_cast<intptr_t>(FindKnownLastTrue(d, m));
}

// ================================================== BLOCKWISE

// ------------------------------ CombineShiftRightBytes

// Prevent accidentally using these for 128-bit vectors - should not be
// necessary.
namespace detail {
template <class D, class V> OMNI_INLINE V OffsetsOf128BitBlocks(const D d, const V iota0)
{
    using T = MakeUnsigned<TFromD<D>>;
    return detail::AndNotN(static_cast<T>(LanesPerBlock(d) - 1), iota0);
}

template <size_t kLanes, class D, OMNI_IF_T_SIZE_D(D, 1)> svbool_t FirstNPerBlock(D d)
{
    const RebindToUnsigned<decltype(d)> du;
    constexpr size_t kLanesPerBlock = detail::LanesPerBlock(du);
    const svuint8_t idx_mod = svdupq_n_u8(0 % kLanesPerBlock, 1 % kLanesPerBlock, 2 % kLanesPerBlock,
        3 % kLanesPerBlock, 4 % kLanesPerBlock, 5 % kLanesPerBlock, 6 % kLanesPerBlock, 7 % kLanesPerBlock,
        8 % kLanesPerBlock, 9 % kLanesPerBlock, 10 % kLanesPerBlock, 11 % kLanesPerBlock, 12 % kLanesPerBlock,
        13 % kLanesPerBlock, 14 % kLanesPerBlock, 15 % kLanesPerBlock);
    return detail::LtN(BitCast(du, idx_mod), kLanes);
}

template <size_t kLanes, class D, OMNI_IF_T_SIZE_D(D, 2)> svbool_t FirstNPerBlock(D d)
{
    const RebindToUnsigned<decltype(d)> du;
    constexpr size_t kLanesPerBlock = detail::LanesPerBlock(du);
    const svuint16_t idx_mod = svdupq_n_u16(0 % kLanesPerBlock, 1 % kLanesPerBlock, 2 % kLanesPerBlock,
        3 % kLanesPerBlock, 4 % kLanesPerBlock, 5 % kLanesPerBlock, 6 % kLanesPerBlock, 7 % kLanesPerBlock);
    return detail::LtN(BitCast(du, idx_mod), kLanes);
}

template <size_t kLanes, class D, OMNI_IF_T_SIZE_D(D, 4)> svbool_t FirstNPerBlock(D d)
{
    const RebindToUnsigned<decltype(d)> du;
    constexpr size_t kLanesPerBlock = detail::LanesPerBlock(du);
    const svuint32_t idx_mod =
        svdupq_n_u32(0 % kLanesPerBlock, 1 % kLanesPerBlock, 2 % kLanesPerBlock, 3 % kLanesPerBlock);
    return detail::LtN(BitCast(du, idx_mod), kLanes);
}

template <size_t kLanes, class D, OMNI_IF_T_SIZE_D(D, 8)> svbool_t FirstNPerBlock(D d)
{
    const RebindToUnsigned<decltype(d)> du;
    constexpr size_t kLanesPerBlock = detail::LanesPerBlock(du);
    const svuint64_t idx_mod = svdupq_n_u64(0 % kLanesPerBlock, 1 % kLanesPerBlock);
    return detail::LtN(BitCast(du, idx_mod), kLanes);
}
} // namespace detail

template <size_t kBytes, class D, class V = VFromD<D>>
OMNI_API V CombineShiftRightBytes(const D d, const V hi, const V lo)
{
    const Repartition<uint8_t, decltype(d)> d8;
    const auto hi8 = BitCast(d8, hi);
    const auto lo8 = BitCast(d8, lo);
    const auto hi_up = detail::Splice(hi8, hi8, FirstN(d8, 16 - kBytes));
    const auto lo_down = detail::Ext<kBytes>(lo8, lo8);
    const svbool_t is_lo = detail::FirstNPerBlock<16 - kBytes>(d8);
    return BitCast(d, IfThenElse(is_lo, lo_down, hi_up));
}

// ------------------------------ Shuffle2301
template <class V> OMNI_API V Shuffle2301(const V v)
{
    const DFromV<V> d;
    static_assert(sizeof(TFromD<decltype(d)>) == 4, "Defined for 32-bit types");
    return Reverse2(d, v);
}

// ------------------------------ Shuffle2103
template <class V> OMNI_API V Shuffle2103(const V v)
{
    const DFromV<V> d;
    const Repartition<uint8_t, decltype(d)> d8;
    static_assert(sizeof(TFromD<decltype(d)>) == 4, "Defined for 32-bit types");
    const svuint8_t v8 = BitCast(d8, v);
    return BitCast(d, CombineShiftRightBytes<12>(d8, v8, v8));
}

// ------------------------------ Shuffle0321
template <class V> OMNI_API V Shuffle0321(const V v)
{
    const DFromV<V> d;
    const Repartition<uint8_t, decltype(d)> d8;
    static_assert(sizeof(TFromD<decltype(d)>) == 4, "Defined for 32-bit types");
    const svuint8_t v8 = BitCast(d8, v);
    return BitCast(d, CombineShiftRightBytes<4>(d8, v8, v8));
}

// ------------------------------ Shuffle1032
template <class V> OMNI_API V Shuffle1032(const V v)
{
    const DFromV<V> d;
    const Repartition<uint8_t, decltype(d)> d8;
    static_assert(sizeof(TFromD<decltype(d)>) == 4, "Defined for 32-bit types");
    const svuint8_t v8 = BitCast(d8, v);
    return BitCast(d, CombineShiftRightBytes<8>(d8, v8, v8));
}

// ------------------------------ Shuffle01
template <class V> OMNI_API V Shuffle01(const V v)
{
    const DFromV<V> d;
    const Repartition<uint8_t, decltype(d)> d8;
    static_assert(sizeof(TFromD<decltype(d)>) == 8, "Defined for 64-bit types");
    const svuint8_t v8 = BitCast(d8, v);
    return BitCast(d, CombineShiftRightBytes<8>(d8, v8, v8));
}

// ------------------------------ Shuffle0123
template <class V> OMNI_API V Shuffle0123(const V v)
{
    return Shuffle2301(Shuffle1032(v));
}

// ------------------------------ ReverseBlocks (Reverse, Shuffle01)
template <class D, class V = VFromD<D>> OMNI_API V ReverseBlocks(D d, V v)
{
    if (detail::IsFull(d)) {
        return SwapAdjacentBlocks(v);
    } else if (detail::IsFull(Twice<D>())) {
        return v;
    }
}

// ------------------------------ TableLookupBytes

template <class V, class VI> OMNI_API VI TableLookupBytes(const V v, const VI idx)
{
    const DFromV<VI> d;
    const Repartition<uint8_t, decltype(d)> du8;
    const auto offsets128 = detail::OffsetsOf128BitBlocks(du8, Iota(du8, 0));
    const auto idx8 = Add(BitCast(du8, idx), offsets128);
    return BitCast(d, TableLookupLanes(BitCast(du8, v), idx8));
}

template <class V, class VI> OMNI_API VI TableLookupBytesOr0(const V v, const VI idx)
{
    const DFromV<VI> d;
    // Mask size must match vector type, so cast everything to this type.
    const Repartition<int8_t, decltype(d)> di8;

    auto idx8 = BitCast(di8, idx);
    const auto msb = detail::LtN(idx8, 0);

    const auto lookup = TableLookupBytes(BitCast(di8, v), idx8);
    return BitCast(d, IfThenZeroElse(msb, lookup));
}

// ------------------------------ Broadcast

#ifdef OMNI_NATIVE_BROADCASTLANE
#undef OMNI_NATIVE_BROADCASTLANE
#else
#define OMNI_NATIVE_BROADCASTLANE
#endif

namespace detail {
#define OMNI_SVE_BROADCAST(BASE, CHAR, BITS, HALF, NAME, OP)                               \
    template <int kLane> OMNI_INLINE OMNI_SVE_V(BASE, BITS) NAME(OMNI_SVE_V(BASE, BITS) v) \
    {                                                                                      \
        return sv##OP##_##CHAR##BITS(v, kLane);                                            \
    }

OMNI_SVE_FOREACH(OMNI_SVE_BROADCAST, BroadcastLane, dup_lane)

#undef OMNI_SVE_BROADCAST
} // namespace detail

template <int kLane, class V> OMNI_API V Broadcast(const V v)
{
    const DFromV<V> d;
    const RebindToUnsigned<decltype(d)> du;
    constexpr size_t kLanesPerBlock = detail::LanesPerBlock(du);
    static_assert(0 <= kLane && kLane < kLanesPerBlock, "Invalid lane");
    auto idx = detail::OffsetsOf128BitBlocks(du, Iota(du, 0));
    if (kLane != 0) {
        idx = detail::AddN(idx, kLane);
    }
    return TableLookupLanes(v, idx);
}

template <int kLane, class V> OMNI_API V BroadcastLane(const V v)
{
    static_assert(0 <= kLane && kLane < OMNI_MAX_LANES_V(V), "Invalid lane");
    return detail::BroadcastLane<kLane>(v);
}

// ------------------------------ ShiftLeftLanes

template <size_t kLanes, class D, class V = VFromD<D>> OMNI_API V ShiftLeftLanes(D d, const V v)
{
    const auto zero = Zero(d);
    const auto shifted = detail::Splice(v, zero, FirstN(d, kLanes));
    return IfThenElse(detail::FirstNPerBlock<kLanes>(d), zero, shifted);
}

template <size_t kLanes, class V> OMNI_API V ShiftLeftLanes(const V v)
{
    return ShiftLeftLanes<kLanes>(DFromV<V>(), v);
}

// ------------------------------ ShiftRightLanes
template <size_t kLanes, class D, class V = VFromD<D>> OMNI_API V ShiftRightLanes(D d, V v)
{
    // For capped/fractional vectors, clear upper lanes so we shift in zeros.
    if (!detail::IsFull(d)) {
        v = IfThenElseZero(detail::MakeMask(d), v);
    }

    const auto shifted = detail::Ext<kLanes>(v, v);
    constexpr size_t kLanesPerBlock = detail::LanesPerBlock(d);
    const svbool_t mask = detail::FirstNPerBlock<kLanesPerBlock - kLanes>(d);
    return IfThenElseZero(mask, shifted);
}

// ------------------------------ ShiftLeftBytes

template <int kBytes, class D, class V = VFromD<D>> OMNI_API V ShiftLeftBytes(const D d, const V v)
{
    const Repartition<uint8_t, decltype(d)> d8;
    return BitCast(d, ShiftLeftLanes<kBytes>(BitCast(d8, v)));
}

template <int kBytes, class V> OMNI_API V ShiftLeftBytes(const V v)
{
    return ShiftLeftBytes<kBytes>(DFromV<V>(), v);
}

// ------------------------------ ShiftRightBytes
template <int kBytes, class D, class V = VFromD<D>> OMNI_API V ShiftRightBytes(const D d, const V v)
{
    const Repartition<uint8_t, decltype(d)> d8;
    return BitCast(d, ShiftRightLanes<kBytes>(d8, BitCast(d8, v)));
}

// ------------------------------ ZipLower

template <class V, class DW = RepartitionToWide<DFromV<V>>> OMNI_API VFromD<DW> ZipLower(DW dw, V a, V b)
{
    const RepartitionToNarrow<DW> dn;
    static_assert(IsSame<TFromD<decltype(dn)>, TFromV<V>>(), "D/V mismatch");
    return BitCast(dw, InterleaveLower(dn, a, b));
}

template <class V, class D = DFromV<V>, class DW = RepartitionToWide<D>>
OMNI_API VFromD<DW> ZipLower(const V a, const V b)
{
    return BitCast(DW(), InterleaveLower(D(), a, b));
}

// ------------------------------ ZipUpper
template <class V, class DW = RepartitionToWide<DFromV<V>>> OMNI_API VFromD<DW> ZipUpper(DW dw, V a, V b)
{
    const RepartitionToNarrow<DW> dn;
    static_assert(IsSame<TFromD<decltype(dn)>, TFromV<V>>(), "D/V mismatch");
    return BitCast(dw, InterleaveUpper(dn, a, b));
}

// ================================================== Ops with dependencies

// ------------------------------ AddSub (Reverse2)

// NOTE: svcadd_f*_x(OMNI_SVE_PTRUE(BITS), a, b, 90) computes a[i] - b[i + 1] in
// the even lanes and a[i] + b[i - 1] in the odd lanes.

#define OMNI_SVE_ADDSUB_F(BASE, CHAR, BITS, HALF, NAME, OP)                                  \
    OMNI_API OMNI_SVE_V(BASE, BITS) NAME(OMNI_SVE_V(BASE, BITS) a, OMNI_SVE_V(BASE, BITS) b) \
    {                                                                                        \
        const DFromV<decltype(b)> d;                                                         \
        return sv##OP##_##CHAR##BITS##_x(OMNI_SVE_PTRUE(BITS), a, Reverse2(d, b), 90);       \
    }

OMNI_SVE_FOREACH_F(OMNI_SVE_ADDSUB_F, AddSub, cadd)

#undef OMNI_SVE_ADDSUB_F

// NOTE: svcadd_s*(a, b, 90) and svcadd_u*(a, b, 90) compute a[i] - b[i + 1] in
// the even lanes and a[i] + b[i - 1] in the odd lanes.
// Disable the default implementation of AddSub in generic_ops-inl.h for
// floating-point vectors on SVE, but enable the default implementation of
// AddSub in generic_ops-inl.h for integer vectors on SVE that do not support
// SVE2
#undef OMNI_IF_ADDSUB_V
#define OMNI_IF_ADDSUB_V(V) OMNI_IF_LANES_GT_D(DFromV<V>, 1), OMNI_IF_NOT_FLOAT_NOR_SPECIAL_V(V)

// ------------------------------ MulAddSub (AddSub)

template <class V, OMNI_IF_LANES_GT_D(DFromV<V>, 1), OMNI_IF_FLOAT_V(V)> OMNI_API V MulAddSub(V mul, V x, V sub_or_add)
{
    using T = TFromV<V>;

    const DFromV<V> d;
    const T neg_zero = ConvertScalarTo<T>(-0.0f);

    return MulAdd(mul, x, AddSub(Set(d, neg_zero), sub_or_add));
}

// Disable the default implementation of MulAddSub in generic_ops-inl.h for
// floating-point vectors on SVE, but enable the default implementation of
// AddSub in generic_ops-inl.h for integer vectors on SVE targets that do not
// support SVE2
#undef OMNI_IF_MULADDSUB_V
#define OMNI_IF_MULADDSUB_V(V) OMNI_IF_LANES_GT_D(DFromV<V>, 1), OMNI_IF_NOT_FLOAT_NOR_SPECIAL_V(V)

// ------------------------------ PromoteTo bfloat16 (ZipLower)
template <size_t N, int kPow2> OMNI_API svfloat32_t PromoteTo(Simd<float32_t, N, kPow2> df32, VBF16 v)
{
    const ScalableTag<uint16_t> du16;
    return BitCast(df32, detail::ZipLowerSame(svdup_n_u16(0), BitCast(du16, v)));
}

// ------------------------------ PromoteEvenTo/PromoteOddTo (ConcatOddFull)

namespace detail {
// Signed to signed PromoteEvenTo
template <class D>
OMNI_INLINE VFromD<D> PromoteEvenTo(simd::SignedTag /* to_type_tag */, simd::SizeTag<2> /* to_lane_size_tag */,
    simd::SignedTag /* from_type_tag */, D d_to, svint8_t v)
{
    return svextb_s16_x(detail::PTrue(d_to), BitCast(d_to, v));
}

template <class D>
OMNI_INLINE VFromD<D> PromoteEvenTo(simd::SignedTag /* to_type_tag */, simd::SizeTag<4> /* to_lane_size_tag */,
    simd::SignedTag /* from_type_tag */, D d_to, svint16_t v)
{
    return svexth_s32_x(detail::PTrue(d_to), BitCast(d_to, v));
}

template <class D>
OMNI_INLINE VFromD<D> PromoteEvenTo(simd::SignedTag /* to_type_tag */, simd::SizeTag<8> /* to_lane_size_tag */,
    simd::SignedTag /* from_type_tag */, D d_to, svint32_t v)
{
    return svextw_s64_x(detail::PTrue(d_to), BitCast(d_to, v));
}

// F16->F32 PromoteEvenTo
template <class D>
OMNI_INLINE VFromD<D> PromoteEvenTo(simd::FloatTag /* to_type_tag */, simd::SizeTag<4> /* to_lane_size_tag */,
    simd::FloatTag /* from_type_tag */, D d_to, svfloat16_t v)
{
    const Repartition<float, decltype(d_to)> d_from;
    return svcvt_f32_f16_x(detail::PTrue(d_from), v);
}

// F32->F64 PromoteEvenTo
template <class D>
OMNI_INLINE VFromD<D> PromoteEvenTo(simd::FloatTag /* to_type_tag */, simd::SizeTag<8> /* to_lane_size_tag */,
    simd::FloatTag /* from_type_tag */, D d_to, svfloat32_t v)
{
    const Repartition<float, decltype(d_to)> d_from;
    return svcvt_f64_f32_x(detail::PTrue(d_from), v);
}

// I32->F64 PromoteEvenTo
template <class D>
OMNI_INLINE VFromD<D> PromoteEvenTo(simd::FloatTag /* to_type_tag */, simd::SizeTag<8> /* to_lane_size_tag */,
    simd::SignedTag /* from_type_tag */, D d_to, svint32_t v)
{
    const Repartition<float, decltype(d_to)> d_from;
    return svcvt_f64_s32_x(detail::PTrue(d_from), v);
}

// U32->F64 PromoteEvenTo
template <class D>
OMNI_INLINE VFromD<D> PromoteEvenTo(simd::FloatTag /* to_type_tag */, simd::SizeTag<8> /* to_lane_size_tag */,
    simd::UnsignedTag /* from_type_tag */, D d_to, svuint32_t v)
{
    const Repartition<float, decltype(d_to)> d_from;
    return svcvt_f64_u32_x(detail::PTrue(d_from), v);
}

// F32->I64 PromoteEvenTo
template <class D>
OMNI_INLINE VFromD<D> PromoteEvenTo(simd::SignedTag /* to_type_tag */, simd::SizeTag<8> /* to_lane_size_tag */,
    simd::FloatTag /* from_type_tag */, D d_to, svfloat32_t v)
{
    const Repartition<float, decltype(d_to)> d_from;
    return svcvt_s64_f32_x(detail::PTrue(d_from), v);
}

// F32->U64 PromoteEvenTo
template <class D>
OMNI_INLINE VFromD<D> PromoteEvenTo(simd::UnsignedTag /* to_type_tag */, simd::SizeTag<8> /* to_lane_size_tag */,
    simd::FloatTag /* from_type_tag */, D d_to, svfloat32_t v)
{
    const Repartition<float, decltype(d_to)> d_from;
    return svcvt_u64_f32_x(detail::PTrue(d_from), v);
}

// F16->F32 PromoteOddTo
template <class D>
OMNI_INLINE VFromD<D> PromoteOddTo(simd::FloatTag to_type_tag, simd::SizeTag<4> to_lane_size_tag,
    simd::FloatTag from_type_tag, D d_to, svfloat16_t v)
{
    return PromoteEvenTo(to_type_tag, to_lane_size_tag, from_type_tag, d_to, DupOdd(v));
}

// I32/U32/F32->F64 PromoteOddTo
template <class FromTypeTag, class D, class V>
OMNI_INLINE VFromD<D> PromoteOddTo(simd::FloatTag to_type_tag, simd::SizeTag<8> to_lane_size_tag,
    FromTypeTag from_type_tag, D d_to, V v)
{
    return PromoteEvenTo(to_type_tag, to_lane_size_tag, from_type_tag, d_to, DupOdd(v));
}

// F32->I64/U64 PromoteOddTo
template <class ToTypeTag, class D, OMNI_IF_UI64_D(D)>
OMNI_INLINE VFromD<D> PromoteOddTo(ToTypeTag to_type_tag, simd::SizeTag<8> to_lane_size_tag,
    simd::FloatTag from_type_tag, D d_to, svfloat32_t v)
{
    return PromoteEvenTo(to_type_tag, to_lane_size_tag, from_type_tag, d_to, DupOdd(v));
}
} // namespace detail

// ------------------------------ ReorderDemote2To (OddEven)

template <size_t N, int kPow2>
OMNI_API VBF16 ReorderDemote2To(Simd<bfloat16_t, N, kPow2> dbf16, svfloat32_t a, svfloat32_t b)
{
    (void)dbf16;
    const auto a_in_odd = BitCast(ScalableTag<uint16_t>(), detail::RoundF32ForDemoteToBF16(a));
    const auto b_in_odd = BitCast(ScalableTag<uint16_t>(), detail::RoundF32ForDemoteToBF16(b));
    return BitCast(dbf16, detail::InterleaveOdd(b_in_odd, a_in_odd));
}

template <size_t N, int kPow2>
OMNI_API svint16_t ReorderDemote2To(Simd<int16_t, N, kPow2> d16, svint32_t a, svint32_t b)
{
    const svint16_t a16 = BitCast(d16, detail::SaturateI<int16_t>(a));
    const svint16_t b16 = BitCast(d16, detail::SaturateI<int16_t>(b));
    return detail::InterleaveEven(a16, b16);
}

template <size_t N, int kPow2>
OMNI_API svuint16_t ReorderDemote2To(Simd<uint16_t, N, kPow2> d16, svint32_t a, svint32_t b)
{
    const Repartition<uint32_t, decltype(d16)> du32;
    const svuint32_t clamped_a = BitCast(du32, detail::MaxN(a, 0));
    const svuint32_t clamped_b = BitCast(du32, detail::MaxN(b, 0));
    const svuint16_t a16 = BitCast(d16, detail::SaturateU<uint16_t>(clamped_a));
    const svuint16_t b16 = BitCast(d16, detail::SaturateU<uint16_t>(clamped_b));
    return detail::InterleaveEven(a16, b16);
}

template <size_t N, int kPow2>
OMNI_API svuint16_t ReorderDemote2To(Simd<uint16_t, N, kPow2> d16, svuint32_t a, svuint32_t b)
{
    const svuint16_t a16 = BitCast(d16, detail::SaturateU<uint16_t>(a));
    const svuint16_t b16 = BitCast(d16, detail::SaturateU<uint16_t>(b));
    return detail::InterleaveEven(a16, b16);
}

template <size_t N, int kPow2> OMNI_API svint8_t ReorderDemote2To(Simd<int8_t, N, kPow2> d8, svint16_t a, svint16_t b)
{
    const svint8_t a8 = BitCast(d8, detail::SaturateI<int8_t>(a));
    const svint8_t b8 = BitCast(d8, detail::SaturateI<int8_t>(b));
    return detail::InterleaveEven(a8, b8);
}

template <size_t N, int kPow2> OMNI_API svuint8_t ReorderDemote2To(Simd<uint8_t, N, kPow2> d8, svint16_t a, svint16_t b)
{
    const Repartition<uint16_t, decltype(d8)> du16;
    const svuint16_t clamped_a = BitCast(du16, detail::MaxN(a, 0));
    const svuint16_t clamped_b = BitCast(du16, detail::MaxN(b, 0));
    const svuint8_t a8 = BitCast(d8, detail::SaturateU<uint8_t>(clamped_a));
    const svuint8_t b8 = BitCast(d8, detail::SaturateU<uint8_t>(clamped_b));
    return detail::InterleaveEven(a8, b8);
}

template <size_t N, int kPow2>
OMNI_API svuint8_t ReorderDemote2To(Simd<uint8_t, N, kPow2> d8, svuint16_t a, svuint16_t b)
{
    const svuint8_t a8 = BitCast(d8, detail::SaturateU<uint8_t>(a));
    const svuint8_t b8 = BitCast(d8, detail::SaturateU<uint8_t>(b));
    return detail::InterleaveEven(a8, b8);
}

template <size_t N, int kPow2>
OMNI_API svint32_t ReorderDemote2To(Simd<int32_t, N, kPow2> d32, svint64_t a, svint64_t b)
{
    const svint32_t a32 = BitCast(d32, detail::SaturateI<int32_t>(a));
    const svint32_t b32 = BitCast(d32, detail::SaturateI<int32_t>(b));
    return detail::InterleaveEven(a32, b32);
}

template <size_t N, int kPow2>
OMNI_API svuint32_t ReorderDemote2To(Simd<uint32_t, N, kPow2> d32, svint64_t a, svint64_t b)
{
    const Repartition<uint64_t, decltype(d32)> du64;
    const svuint64_t clamped_a = BitCast(du64, detail::MaxN(a, 0));
    const svuint64_t clamped_b = BitCast(du64, detail::MaxN(b, 0));
    const svuint32_t a32 = BitCast(d32, detail::SaturateU<uint32_t>(clamped_a));
    const svuint32_t b32 = BitCast(d32, detail::SaturateU<uint32_t>(clamped_b));
    return detail::InterleaveEven(a32, b32);
}

template <size_t N, int kPow2>
OMNI_API svuint32_t ReorderDemote2To(Simd<uint32_t, N, kPow2> d32, svuint64_t a, svuint64_t b)
{
    const svuint32_t a32 = BitCast(d32, detail::SaturateU<uint32_t>(a));
    const svuint32_t b32 = BitCast(d32, detail::SaturateU<uint32_t>(b));
    return detail::InterleaveEven(a32, b32);
}

template <class D, class V, OMNI_IF_SIGNED_D(D), OMNI_IF_UNSIGNED_V(V), OMNI_IF_T_SIZE_D(D, sizeof(TFromV<V>) / 2)>
OMNI_API VFromD<D> ReorderDemote2To(D dn, V a, V b)
{
    const auto clamped_a = BitCast(dn, detail::SaturateU<TFromD<D>>(a));
    const auto clamped_b = BitCast(dn, detail::SaturateU<TFromD<D>>(b));
    return detail::InterleaveEven(clamped_a, clamped_b);
}

template <class D, class V, OMNI_IF_NOT_FLOAT_NOR_SPECIAL(TFromD<D>), OMNI_IF_NOT_FLOAT_NOR_SPECIAL_V(V),
    OMNI_IF_T_SIZE_V(V, sizeof(TFromD<D>) * 2)>
OMNI_API VFromD<D> OrderedDemote2To(D dn, V a, V b)
{
    const Half<decltype(dn)> dnh;
    const auto demoted_a = DemoteTo(dnh, a);
    const auto demoted_b = DemoteTo(dnh, b);
    return Combine(dn, demoted_b, demoted_a);
}

template <size_t N, int kPow2>
OMNI_API VBF16 OrderedDemote2To(Simd<bfloat16_t, N, kPow2> dbf16, svfloat32_t a, svfloat32_t b)
{
    const RebindToUnsigned<decltype(dbf16)> du16;
    const svuint16_t a_in_odd = BitCast(du16, detail::RoundF32ForDemoteToBF16(a));
    const svuint16_t b_in_odd = BitCast(du16, detail::RoundF32ForDemoteToBF16(b));
    return BitCast(dbf16, ConcatOdd(du16, b_in_odd, a_in_odd)); // lower half
}

// ------------------------------ I8/U8/I16/U16 Div

template <class V, OMNI_IF_NOT_FLOAT_NOR_SPECIAL_V(V), OMNI_IF_T_SIZE_ONE_OF_V(V, (1 << 1) | (1 << 2))>
OMNI_API V Div(V a, V b)
{
    const DFromV<decltype(a)> d;
    const Half<decltype(d)> dh;
    const RepartitionToWide<decltype(d)> dw;

    const auto q_lo = Div(PromoteTo(dw, LowerHalf(dh, a)), PromoteTo(dw, LowerHalf(dh, b)));
    const auto q_hi = Div(PromoteUpperTo(dw, a), PromoteUpperTo(dw, b));

    return OrderedDemote2To(d, q_lo, q_hi);
}

// ------------------------------ I8/U8/I16/U16 MaskedDivOr
template <class V, class M, OMNI_IF_T_SIZE_ONE_OF_V(V, (1 << 1) | (1 << 2)), OMNI_IF_NOT_FLOAT_NOR_SPECIAL_V(V)>
OMNI_API V MaskedDivOr(V no, M m, V a, V b)
{
    return IfThenElse(m, Div(a, b), no);
}

// ------------------------------ Mod (Div, NegMulAdd)
template <class V> OMNI_API V Mod(V a, V b)
{
    return NegMulAdd(Div(a, b), b, a);
}

// ------------------------------ MaskedModOr (Mod)
template <class V, class M> OMNI_API V MaskedModOr(V no, M m, V a, V b)
{
    return IfThenElse(m, Mod(a, b), no);
}

// ------------------------------ BroadcastSignBit (ShiftRight)
template <class V> OMNI_API V BroadcastSignBit(const V v)
{
    return ShiftRight<sizeof(TFromV<V>) * 8 - 1>(v);
}

// ------------------------------ IfNegativeThenElse (BroadcastSignBit)
template <class V> OMNI_API V IfNegativeThenElse(V v, V yes, V no)
{
    static_assert(IsSigned<TFromV<V>>(), "Only works for signed/float");
    return IfThenElse(IsNegative(v), yes, no);
}

// ------------------------------ AverageRound (ShiftRight)

#ifdef OMNI_NATIVE_AVERAGE_ROUND_UI32
#undef OMNI_NATIVE_AVERAGE_ROUND_UI32
#else
#define OMNI_NATIVE_AVERAGE_ROUND_UI32
#endif

#ifdef OMNI_NATIVE_AVERAGE_ROUND_UI64
#undef OMNI_NATIVE_AVERAGE_ROUND_UI64
#else
#define OMNI_NATIVE_AVERAGE_ROUND_UI64
#endif

template <class V, OMNI_IF_NOT_FLOAT_NOR_SPECIAL_V(V)> OMNI_API V AverageRound(const V a, const V b)
{
    return Add(Add(ShiftRight<1>(a), ShiftRight<1>(b)), detail::AndN(Or(a, b), 1));
}

// ------------------------------ LoadMaskBits (TestBit)

// `p` points to at least 8 readable bytes, not all of which need be valid.
template <class D, OMNI_IF_T_SIZE_D(D, 1)> OMNI_INLINE svbool_t LoadMaskBits(D d, const uint8_t *OMNI_RESTRICT bits)
{
    const RebindToUnsigned<D> du;
    const svuint8_t iota = Iota(du, 0);

    // Load correct number of bytes (bits/8) with 7 zeros after each.
    const svuint8_t bytes = BitCast(du, svld1ub_u64(detail::PTrue(d), bits));
    // Replicate bytes 8x such that each byte contains the bit that governs it.
    const svuint8_t rep8 = svtbl_u8(bytes, detail::AndNotN(7, iota));

    const svuint8_t bit = svdupq_n_u8(1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128);
    return TestBit(rep8, bit);
}

template <class D, OMNI_IF_T_SIZE_D(D, 2)>
OMNI_INLINE svbool_t LoadMaskBits(D /* tag */, const uint8_t *OMNI_RESTRICT bits)
{
    const RebindToUnsigned<D> du;
    const Repartition<uint8_t, D> du8;

    // There may be up to 128 bits; avoid reading past the end.
    const svuint8_t bytes = svld1(FirstN(du8, (Lanes(du) + 7) / 8), bits);

    // Replicate bytes 16x such that each lane contains the bit that governs it.
    const svuint8_t rep16 = svtbl_u8(bytes, ShiftRight<4>(Iota(du8, 0)));

    const svuint16_t bit = svdupq_n_u16(1, 2, 4, 8, 16, 32, 64, 128);
    return TestBit(BitCast(du, rep16), bit);
}

template <class D, OMNI_IF_T_SIZE_D(D, 4)>
OMNI_INLINE svbool_t LoadMaskBits(D /* tag */, const uint8_t *OMNI_RESTRICT bits)
{
    const RebindToUnsigned<D> du;
    const Repartition<uint8_t, D> du8;

    // Upper bound = 2048 bits / 32 bit = 64 bits; at least 8 bytes are readable,
    // so we can skip computing the actual length (Lanes(du)+7)/8.
    const svuint8_t bytes = svld1(FirstN(du8, 8), bits);

    // Replicate bytes 32x such that each lane contains the bit that governs it.
    const svuint8_t rep32 = svtbl_u8(bytes, ShiftRight<5>(Iota(du8, 0)));

    // 1, 2, 4, 8, 16, 32, 64, 128,  1, 2 ..
    const svuint32_t bit = Shl(Set(du, 1), detail::AndN(Iota(du, 0), 7));

    return TestBit(BitCast(du, rep32), bit);
}

template <class D, OMNI_IF_T_SIZE_D(D, 8)>
OMNI_INLINE svbool_t LoadMaskBits(D /* tag */, const uint8_t *OMNI_RESTRICT bits)
{
    const RebindToUnsigned<D> du;

    // Max 2048 bits = 32 lanes = 32 input bits; replicate those into each lane.
    // The "at least 8 byte" guarantee in quick_reference ensures this is safe.
    uint32_t mask_bits;
    CopyBytes<4>(bits, &mask_bits); // copy from bytes
    const auto vbits = Set(du, mask_bits);

    // 2 ^ {0,1, .., 31}, will not have more lanes than that.
    const svuint64_t bit = Shl(Set(du, 1), Iota(du, 0));

    return TestBit(vbits, bit);
}

// ------------------------------ Dup128MaskFromMaskBits

template <class D, OMNI_IF_T_SIZE_D(D, 1), OMNI_IF_V_SIZE_LE_D(D, 8)>
OMNI_API MFromD<D> Dup128MaskFromMaskBits(D d, unsigned mask_bits)
{
    const RebindToUnsigned<decltype(d)> du;

    constexpr size_t kN = MaxLanes(d);
    if (kN < 8)
        mask_bits &= (1u << kN) - 1;

    // Replicate the lower 8 bits of mask_bits to each u8 lane
    const svuint8_t bytes = BitCast(du, Set(du, static_cast<uint8_t>(mask_bits)));

    const svuint8_t bit = svdupq_n_u8(1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128);
    return TestBit(bytes, bit);
}

template <class D, OMNI_IF_T_SIZE_D(D, 1), OMNI_IF_V_SIZE_GT_D(D, 8)>
OMNI_API MFromD<D> Dup128MaskFromMaskBits(D d, unsigned mask_bits)
{
    const RebindToUnsigned<decltype(d)> du;
    const Repartition<uint16_t, decltype(du)> du16;

    // Replicate the lower 16 bits of mask_bits to each u16 lane of a u16 vector,
    // and then bitcast the replicated mask_bits to a u8 vector
    const svuint8_t bytes = BitCast(du, Set(du16, static_cast<uint16_t>(mask_bits)));
    // Replicate bytes 8x such that each byte contains the bit that governs it.
    const svuint8_t rep8 = svtbl_u8(bytes, ShiftRight<3>(Iota(du, 0)));

    const svuint8_t bit = svdupq_n_u8(1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128);
    return TestBit(rep8, bit);
}

template <class D, OMNI_IF_T_SIZE_D(D, 2)> OMNI_API MFromD<D> Dup128MaskFromMaskBits(D d, unsigned mask_bits)
{
    const RebindToUnsigned<decltype(d)> du;
    const Repartition<uint8_t, decltype(d)> du8;

    constexpr size_t kN = MaxLanes(d);
    if (kN < 8)
        mask_bits &= (1u << kN) - 1;

    // Set all of the u8 lanes of bytes to the lower 8 bits of mask_bits
    const svuint8_t bytes = Set(du8, static_cast<uint8_t>(mask_bits));

    const svuint16_t bit = svdupq_n_u16(1, 2, 4, 8, 16, 32, 64, 128);
    return TestBit(BitCast(du, bytes), bit);
}

template <class D, OMNI_IF_T_SIZE_D(D, 4)> OMNI_API MFromD<D> Dup128MaskFromMaskBits(D d, unsigned mask_bits)
{
    const RebindToUnsigned<decltype(d)> du;
    const Repartition<uint8_t, decltype(d)> du8;

    constexpr size_t kN = MaxLanes(d);
    if (kN < 4)
        mask_bits &= (1u << kN) - 1;

    // Set all of the u8 lanes of bytes to the lower 8 bits of mask_bits
    const svuint8_t bytes = Set(du8, static_cast<uint8_t>(mask_bits));

    const svuint32_t bit = svdupq_n_u32(1, 2, 4, 8);
    return TestBit(BitCast(du, bytes), bit);
}

template <class D, OMNI_IF_T_SIZE_D(D, 8)> OMNI_API MFromD<D> Dup128MaskFromMaskBits(D d, unsigned mask_bits)
{
    const RebindToUnsigned<decltype(d)> du;
    const Repartition<uint8_t, decltype(d)> du8;

    if (MaxLanes(d) < 2)
        mask_bits &= 1u;

    // Set all of the u8 lanes of bytes to the lower 8 bits of mask_bits
    const svuint8_t bytes = Set(du8, static_cast<uint8_t>(mask_bits));

    const svuint64_t bit = svdupq_n_u64(1, 2);
    return TestBit(BitCast(du, bytes), bit);
}

// ------------------------------ StoreMaskBits

namespace detail {
// For each mask lane (governing lane type T), store 1 or 0 in BYTE lanes.
template <class T, OMNI_IF_T_SIZE(T, 1)> OMNI_INLINE svuint8_t BoolFromMask(svbool_t m)
{
    return svdup_n_u8_z(m, 1);
}

template <class T, OMNI_IF_T_SIZE(T, 2)> OMNI_INLINE svuint8_t BoolFromMask(svbool_t m)
{
    const ScalableTag<uint8_t> d8;
    const svuint8_t b16 = BitCast(d8, svdup_n_u16_z(m, 1));
    return detail::ConcatEvenFull(b16, b16); // lower half
}

template <class T, OMNI_IF_T_SIZE(T, 4)> OMNI_INLINE svuint8_t BoolFromMask(svbool_t m)
{
    return U8FromU32(svdup_n_u32_z(m, 1));
}

template <class T, OMNI_IF_T_SIZE(T, 8)> OMNI_INLINE svuint8_t BoolFromMask(svbool_t m)
{
    const ScalableTag<uint32_t> d32;
    const svuint32_t b64 = BitCast(d32, svdup_n_u64_z(m, 1));
    return U8FromU32(detail::ConcatEvenFull(b64, b64)); // lower half
}

// Compacts groups of 8 u8 into 8 contiguous bits in a 64-bit lane.
OMNI_INLINE svuint64_t BitsFromBool(svuint8_t x)
{
    const ScalableTag<uint8_t> d8;
    const ScalableTag<uint16_t> d16;
    const ScalableTag<uint32_t> d32;
    const ScalableTag<uint64_t> d64;
    x = Or(x, BitCast(d8, ShiftRight<7>(BitCast(d16, x))));
    x = Or(x, BitCast(d8, ShiftRight<14>(BitCast(d32, x))));
    x = Or(x, BitCast(d8, ShiftRight<28>(BitCast(d64, x))));
    return BitCast(d64, x);
}
} // namespace detail

// `p` points to at least 8 writable bytes.
template <class D> OMNI_API size_t StoreMaskBits(D d, svbool_t m, uint8_t *bits)
{
    svuint64_t bits_in_u64 = detail::BitsFromBool(detail::BoolFromMask<TFromD<D>>(m));

    const size_t num_bits = Lanes(d);
    const size_t num_bytes = (num_bits + 8 - 1) / 8; // Round up, see below

    // Truncate each u64 to 8 bits and store to u8.
    svst1b_u64(FirstN(ScalableTag<uint64_t>(), num_bytes), bits, bits_in_u64);

    // Non-full byte, need to clear the undefined upper bits. Can happen for
    // capped/fractional vectors or large T and small hardware vectors.
    if (num_bits < 8) {
        const int mask = static_cast<int>((1ull << num_bits) - 1);
        bits[0] = static_cast<uint8_t>(bits[0] & mask);
    }
    // Else: we wrote full bytes because num_bits is a power of two >= 8.

    return num_bytes;
}

// ------------------------------ CompressBits (LoadMaskBits)
template <class V, OMNI_IF_NOT_T_SIZE_V(V, 1)> OMNI_INLINE V CompressBits(V v, const uint8_t *OMNI_RESTRICT bits)
{
    return Compress(v, LoadMaskBits(DFromV<V>(), bits));
}

// ------------------------------ CompressBitsStore (LoadMaskBits)
template <class D, OMNI_IF_NOT_T_SIZE_D(D, 1)>
OMNI_API size_t CompressBitsStore(VFromD<D> v, const uint8_t *OMNI_RESTRICT bits, D d,
    TFromD<D> *OMNI_RESTRICT unaligned)
{
    return CompressStore(v, LoadMaskBits(d, bits), d, unaligned);
}

// ------------------------------ Expand (StoreMaskBits)

#ifdef OMNI_NATIVE_EXPAND
#undef OMNI_NATIVE_EXPAND
#else
#define OMNI_NATIVE_EXPAND
#endif

namespace detail {
OMNI_INLINE svuint8_t IndicesForExpandFromBits(uint64_t mask_bits)
{
    const CappedTag<uint8_t, 8> du8;
    alignas(16) static constexpr uint8_t table[8 * 256] = { // PrintExpand8x8Tables
        128, 128, 128, 128, 128, 128, 128, 128, //
        0, 128, 128, 128, 128, 128, 128, 128, //
        128, 0, 128, 128, 128, 128, 128, 128, //
        0, 1, 128, 128, 128, 128, 128, 128, //
        128, 128, 0, 128, 128, 128, 128, 128, //
        0, 128, 1, 128, 128, 128, 128, 128, //
        128, 0, 1, 128, 128, 128, 128, 128, //
        0, 1, 2, 128, 128, 128, 128, 128, //
        128, 128, 128, 0, 128, 128, 128, 128, //
        0, 128, 128, 1, 128, 128, 128, 128, //
        128, 0, 128, 1, 128, 128, 128, 128, //
        0, 1, 128, 2, 128, 128, 128, 128, //
        128, 128, 0, 1, 128, 128, 128, 128, //
        0, 128, 1, 2, 128, 128, 128, 128, //
        128, 0, 1, 2, 128, 128, 128, 128, //
        0, 1, 2, 3, 128, 128, 128, 128, //
        128, 128, 128, 128, 0, 128, 128, 128, //
        0, 128, 128, 128, 1, 128, 128, 128, //
        128, 0, 128, 128, 1, 128, 128, 128, //
        0, 1, 128, 128, 2, 128, 128, 128, //
        128, 128, 0, 128, 1, 128, 128, 128, //
        0, 128, 1, 128, 2, 128, 128, 128, //
        128, 0, 1, 128, 2, 128, 128, 128, //
        0, 1, 2, 128, 3, 128, 128, 128, //
        128, 128, 128, 0, 1, 128, 128, 128, //
        0, 128, 128, 1, 2, 128, 128, 128, //
        128, 0, 128, 1, 2, 128, 128, 128, //
        0, 1, 128, 2, 3, 128, 128, 128, //
        128, 128, 0, 1, 2, 128, 128, 128, //
        0, 128, 1, 2, 3, 128, 128, 128, //
        128, 0, 1, 2, 3, 128, 128, 128, //
        0, 1, 2, 3, 4, 128, 128, 128, //
        128, 128, 128, 128, 128, 0, 128, 128, //
        0, 128, 128, 128, 128, 1, 128, 128, //
        128, 0, 128, 128, 128, 1, 128, 128, //
        0, 1, 128, 128, 128, 2, 128, 128, //
        128, 128, 0, 128, 128, 1, 128, 128, //
        0, 128, 1, 128, 128, 2, 128, 128, //
        128, 0, 1, 128, 128, 2, 128, 128, //
        0, 1, 2, 128, 128, 3, 128, 128, //
        128, 128, 128, 0, 128, 1, 128, 128, //
        0, 128, 128, 1, 128, 2, 128, 128, //
        128, 0, 128, 1, 128, 2, 128, 128, //
        0, 1, 128, 2, 128, 3, 128, 128, //
        128, 128, 0, 1, 128, 2, 128, 128, //
        0, 128, 1, 2, 128, 3, 128, 128, //
        128, 0, 1, 2, 128, 3, 128, 128, //
        0, 1, 2, 3, 128, 4, 128, 128, //
        128, 128, 128, 128, 0, 1, 128, 128, //
        0, 128, 128, 128, 1, 2, 128, 128, //
        128, 0, 128, 128, 1, 2, 128, 128, //
        0, 1, 128, 128, 2, 3, 128, 128, //
        128, 128, 0, 128, 1, 2, 128, 128, //
        0, 128, 1, 128, 2, 3, 128, 128, //
        128, 0, 1, 128, 2, 3, 128, 128, //
        0, 1, 2, 128, 3, 4, 128, 128, //
        128, 128, 128, 0, 1, 2, 128, 128, //
        0, 128, 128, 1, 2, 3, 128, 128, //
        128, 0, 128, 1, 2, 3, 128, 128, //
        0, 1, 128, 2, 3, 4, 128, 128, //
        128, 128, 0, 1, 2, 3, 128, 128, //
        0, 128, 1, 2, 3, 4, 128, 128, //
        128, 0, 1, 2, 3, 4, 128, 128, //
        0, 1, 2, 3, 4, 5, 128, 128, //
        128, 128, 128, 128, 128, 128, 0, 128, //
        0, 128, 128, 128, 128, 128, 1, 128, //
        128, 0, 128, 128, 128, 128, 1, 128, //
        0, 1, 128, 128, 128, 128, 2, 128, //
        128, 128, 0, 128, 128, 128, 1, 128, //
        0, 128, 1, 128, 128, 128, 2, 128, //
        128, 0, 1, 128, 128, 128, 2, 128, //
        0, 1, 2, 128, 128, 128, 3, 128, //
        128, 128, 128, 0, 128, 128, 1, 128, //
        0, 128, 128, 1, 128, 128, 2, 128, //
        128, 0, 128, 1, 128, 128, 2, 128, //
        0, 1, 128, 2, 128, 128, 3, 128, //
        128, 128, 0, 1, 128, 128, 2, 128, //
        0, 128, 1, 2, 128, 128, 3, 128, //
        128, 0, 1, 2, 128, 128, 3, 128, //
        0, 1, 2, 3, 128, 128, 4, 128, //
        128, 128, 128, 128, 0, 128, 1, 128, //
        0, 128, 128, 128, 1, 128, 2, 128, //
        128, 0, 128, 128, 1, 128, 2, 128, //
        0, 1, 128, 128, 2, 128, 3, 128, //
        128, 128, 0, 128, 1, 128, 2, 128, //
        0, 128, 1, 128, 2, 128, 3, 128, //
        128, 0, 1, 128, 2, 128, 3, 128, //
        0, 1, 2, 128, 3, 128, 4, 128, //
        128, 128, 128, 0, 1, 128, 2, 128, //
        0, 128, 128, 1, 2, 128, 3, 128, //
        128, 0, 128, 1, 2, 128, 3, 128, //
        0, 1, 128, 2, 3, 128, 4, 128, //
        128, 128, 0, 1, 2, 128, 3, 128, //
        0, 128, 1, 2, 3, 128, 4, 128, //
        128, 0, 1, 2, 3, 128, 4, 128, //
        0, 1, 2, 3, 4, 128, 5, 128, //
        128, 128, 128, 128, 128, 0, 1, 128, //
        0, 128, 128, 128, 128, 1, 2, 128, //
        128, 0, 128, 128, 128, 1, 2, 128, //
        0, 1, 128, 128, 128, 2, 3, 128, //
        128, 128, 0, 128, 128, 1, 2, 128, //
        0, 128, 1, 128, 128, 2, 3, 128, //
        128, 0, 1, 128, 128, 2, 3, 128, //
        0, 1, 2, 128, 128, 3, 4, 128, //
        128, 128, 128, 0, 128, 1, 2, 128, //
        0, 128, 128, 1, 128, 2, 3, 128, //
        128, 0, 128, 1, 128, 2, 3, 128, //
        0, 1, 128, 2, 128, 3, 4, 128, //
        128, 128, 0, 1, 128, 2, 3, 128, //
        0, 128, 1, 2, 128, 3, 4, 128, //
        128, 0, 1, 2, 128, 3, 4, 128, //
        0, 1, 2, 3, 128, 4, 5, 128, //
        128, 128, 128, 128, 0, 1, 2, 128, //
        0, 128, 128, 128, 1, 2, 3, 128, //
        128, 0, 128, 128, 1, 2, 3, 128, //
        0, 1, 128, 128, 2, 3, 4, 128, //
        128, 128, 0, 128, 1, 2, 3, 128, //
        0, 128, 1, 128, 2, 3, 4, 128, //
        128, 0, 1, 128, 2, 3, 4, 128, //
        0, 1, 2, 128, 3, 4, 5, 128, //
        128, 128, 128, 0, 1, 2, 3, 128, //
        0, 128, 128, 1, 2, 3, 4, 128, //
        128, 0, 128, 1, 2, 3, 4, 128, //
        0, 1, 128, 2, 3, 4, 5, 128, //
        128, 128, 0, 1, 2, 3, 4, 128, //
        0, 128, 1, 2, 3, 4, 5, 128, //
        128, 0, 1, 2, 3, 4, 5, 128, //
        0, 1, 2, 3, 4, 5, 6, 128, //
        128, 128, 128, 128, 128, 128, 128, 0,   //
        0, 128, 128, 128, 128, 128, 128, 1,   //
        128, 0, 128, 128, 128, 128, 128, 1,   //
        0, 1, 128, 128, 128, 128, 128, 2,   //
        128, 128, 0, 128, 128, 128, 128, 1,   //
        0, 128, 1, 128, 128, 128, 128, 2,   //
        128, 0, 1, 128, 128, 128, 128, 2,   //
        0, 1, 2, 128, 128, 128, 128, 3,   //
        128, 128, 128, 0, 128, 128, 128, 1,   //
        0, 128, 128, 1, 128, 128, 128, 2,   //
        128, 0, 128, 1, 128, 128, 128, 2,   //
        0, 1, 128, 2, 128, 128, 128, 3,   //
        128, 128, 0, 1, 128, 128, 128, 2,   //
        0, 128, 1, 2, 128, 128, 128, 3,   //
        128, 0, 1, 2, 128, 128, 128, 3,   //
        0, 1, 2, 3, 128, 128, 128, 4,   //
        128, 128, 128, 128, 0, 128, 128, 1,   //
        0, 128, 128, 128, 1, 128, 128, 2,   //
        128, 0, 128, 128, 1, 128, 128, 2,   //
        0, 1, 128, 128, 2, 128, 128, 3,   //
        128, 128, 0, 128, 1, 128, 128, 2,   //
        0, 128, 1, 128, 2, 128, 128, 3,   //
        128, 0, 1, 128, 2, 128, 128, 3,   //
        0, 1, 2, 128, 3, 128, 128, 4,   //
        128, 128, 128, 0, 1, 128, 128, 2,   //
        0, 128, 128, 1, 2, 128, 128, 3,   //
        128, 0, 128, 1, 2, 128, 128, 3,   //
        0, 1, 128, 2, 3, 128, 128, 4,   //
        128, 128, 0, 1, 2, 128, 128, 3,   //
        0, 128, 1, 2, 3, 128, 128, 4,   //
        128, 0, 1, 2, 3, 128, 128, 4,   //
        0, 1, 2, 3, 4, 128, 128, 5,   //
        128, 128, 128, 128, 128, 0, 128, 1,   //
        0, 128, 128, 128, 128, 1, 128, 2,   //
        128, 0, 128, 128, 128, 1, 128, 2,   //
        0, 1, 128, 128, 128, 2, 128, 3,   //
        128, 128, 0, 128, 128, 1, 128, 2,   //
        0, 128, 1, 128, 128, 2, 128, 3,   //
        128, 0, 1, 128, 128, 2, 128, 3,   //
        0, 1, 2, 128, 128, 3, 128, 4,   //
        128, 128, 128, 0, 128, 1, 128, 2,   //
        0, 128, 128, 1, 128, 2, 128, 3,   //
        128, 0, 128, 1, 128, 2, 128, 3,   //
        0, 1, 128, 2, 128, 3, 128, 4,   //
        128, 128, 0, 1, 128, 2, 128, 3,   //
        0, 128, 1, 2, 128, 3, 128, 4,   //
        128, 0, 1, 2, 128, 3, 128, 4,   //
        0, 1, 2, 3, 128, 4, 128, 5,   //
        128, 128, 128, 128, 0, 1, 128, 2,   //
        0, 128, 128, 128, 1, 2, 128, 3,   //
        128, 0, 128, 128, 1, 2, 128, 3,   //
        0, 1, 128, 128, 2, 3, 128, 4,   //
        128, 128, 0, 128, 1, 2, 128, 3,   //
        0, 128, 1, 128, 2, 3, 128, 4,   //
        128, 0, 1, 128, 2, 3, 128, 4,   //
        0, 1, 2, 128, 3, 4, 128, 5,   //
        128, 128, 128, 0, 1, 2, 128, 3,   //
        0, 128, 128, 1, 2, 3, 128, 4,   //
        128, 0, 128, 1, 2, 3, 128, 4,   //
        0, 1, 128, 2, 3, 4, 128, 5,   //
        128, 128, 0, 1, 2, 3, 128, 4,   //
        0, 128, 1, 2, 3, 4, 128, 5,   //
        128, 0, 1, 2, 3, 4, 128, 5,   //
        0, 1, 2, 3, 4, 5, 128, 6,   //
        128, 128, 128, 128, 128, 128, 0, 1,   //
        0, 128, 128, 128, 128, 128, 1, 2,   //
        128, 0, 128, 128, 128, 128, 1, 2,   //
        0, 1, 128, 128, 128, 128, 2, 3,   //
        128, 128, 0, 128, 128, 128, 1, 2,   //
        0, 128, 1, 128, 128, 128, 2, 3,   //
        128, 0, 1, 128, 128, 128, 2, 3,   //
        0, 1, 2, 128, 128, 128, 3, 4,   //
        128, 128, 128, 0, 128, 128, 1, 2,   //
        0, 128, 128, 1, 128, 128, 2, 3,   //
        128, 0, 128, 1, 128, 128, 2, 3,   //
        0, 1, 128, 2, 128, 128, 3, 4,   //
        128, 128, 0, 1, 128, 128, 2, 3,   //
        0, 128, 1, 2, 128, 128, 3, 4,   //
        128, 0, 1, 2, 128, 128, 3, 4,   //
        0, 1, 2, 3, 128, 128, 4, 5,   //
        128, 128, 128, 128, 0, 128, 1, 2,   //
        0, 128, 128, 128, 1, 128, 2, 3,   //
        128, 0, 128, 128, 1, 128, 2, 3,   //
        0, 1, 128, 128, 2, 128, 3, 4,   //
        128, 128, 0, 128, 1, 128, 2, 3,   //
        0, 128, 1, 128, 2, 128, 3, 4,   //
        128, 0, 1, 128, 2, 128, 3, 4,   //
        0, 1, 2, 128, 3, 128, 4, 5,   //
        128, 128, 128, 0, 1, 128, 2, 3,   //
        0, 128, 128, 1, 2, 128, 3, 4,   //
        128, 0, 128, 1, 2, 128, 3, 4,   //
        0, 1, 128, 2, 3, 128, 4, 5,   //
        128, 128, 0, 1, 2, 128, 3, 4,   //
        0, 128, 1, 2, 3, 128, 4, 5,   //
        128, 0, 1, 2, 3, 128, 4, 5,   //
        0, 1, 2, 3, 4, 128, 5, 6,   //
        128, 128, 128, 128, 128, 0, 1, 2,   //
        0, 128, 128, 128, 128, 1, 2, 3,   //
        128, 0, 128, 128, 128, 1, 2, 3,   //
        0, 1, 128, 128, 128, 2, 3, 4,   //
        128, 128, 0, 128, 128, 1, 2, 3,   //
        0, 128, 1, 128, 128, 2, 3, 4,   //
        128, 0, 1, 128, 128, 2, 3, 4,   //
        0, 1, 2, 128, 128, 3, 4, 5,   //
        128, 128, 128, 0, 128, 1, 2, 3,   //
        0, 128, 128, 1, 128, 2, 3, 4,   //
        128, 0, 128, 1, 128, 2, 3, 4,   //
        0, 1, 128, 2, 128, 3, 4, 5,   //
        128, 128, 0, 1, 128, 2, 3, 4,   //
        0, 128, 1, 2, 128, 3, 4, 5,   //
        128, 0, 1, 2, 128, 3, 4, 5,   //
        0, 1, 2, 3, 128, 4, 5, 6,   //
        128, 128, 128, 128, 0, 1, 2, 3,   //
        0, 128, 128, 128, 1, 2, 3, 4,   //
        128, 0, 128, 128, 1, 2, 3, 4,   //
        0, 1, 128, 128, 2, 3, 4, 5,   //
        128, 128, 0, 128, 1, 2, 3, 4,   //
        0, 128, 1, 128, 2, 3, 4, 5,   //
        128, 0, 1, 128, 2, 3, 4, 5,   //
        0, 1, 2, 128, 3, 4, 5, 6,   //
        128, 128, 128, 0, 1, 2, 3, 4,   //
        0, 128, 128, 1, 2, 3, 4, 5,   //
        128, 0, 128, 1, 2, 3, 4, 5,   //
        0, 1, 128, 2, 3, 4, 5, 6,   //
        128, 128, 0, 1, 2, 3, 4, 5,   //
        0, 128, 1, 2, 3, 4, 5, 6,   //
        128, 0, 1, 2, 3, 4, 5, 6,   //
        0, 1, 2, 3, 4, 5, 6, 7 };
    return Load(du8, table + mask_bits * 8);
}

template <class D, OMNI_IF_T_SIZE_D(D, 1)> OMNI_INLINE svuint8_t LaneIndicesFromByteIndices(D, svuint8_t idx)
{
    return idx;
}

template <class D, class DU = RebindToUnsigned<D>, OMNI_IF_NOT_T_SIZE_D(D, 1)>
OMNI_INLINE VFromD<DU> LaneIndicesFromByteIndices(D, svuint8_t idx)
{
    return PromoteTo(DU(), idx);
}

// General case when we don't know the vector size, 8 elements at a time.
template <class V> OMNI_INLINE V ExpandLoop(V v, svbool_t mask)
{
    const DFromV<V> d;
    using T = TFromV<V>;
    uint8_t mask_bytes[256 / 8];
    StoreMaskBits(d, mask, mask_bytes);

    // ShiftLeftLanes is expensive, so we're probably better off storing to memory
    // and loading the final result.
    alignas(16) T out[2 * MaxLanes(d)];

    svbool_t next = svpfalse_b();
    size_t input_consumed = 0;
    const V iota = Iota(d, 0);
    for (size_t i = 0; i < Lanes(d); i += 8) {
        uint64_t mask_bits = mask_bytes[i / 8];

        // We want to skip past the v lanes already consumed. There is no
        // instruction for variable-shift-reg, but we can splice.
        const V vH = detail::Splice(v, v, next);
        input_consumed += PopCount(mask_bits);
        next = detail::GeN(iota, ConvertScalarTo<T>(input_consumed));

        const auto idx = detail::LaneIndicesFromByteIndices(d, detail::IndicesForExpandFromBits(mask_bits));
        const V expand = TableLookupLanes(vH, idx);
        StoreU(expand, d, out + i);
    }
    return LoadU(d, out);
}
} // namespace detail

template <class V, OMNI_IF_T_SIZE_V(V, 1)> OMNI_API V Expand(V v, svbool_t mask)
{
    return detail::ExpandLoop(v, mask);
}

template <class V, OMNI_IF_T_SIZE_V(V, 2)> OMNI_API V Expand(V v, svbool_t mask)
{
    return detail::ExpandLoop(v, mask);
}

template <class V, OMNI_IF_T_SIZE_V(V, 4)> OMNI_API V Expand(V v, svbool_t mask)
{
    const DFromV<V> d;
    const RebindToUnsigned<decltype(d)> du32;
    // Convert mask into bitfield via horizontal sum (faster than ORV).
    const svuint32_t bits = Shl(Set(du32, 1), Iota(du32, 0));
    const size_t code = detail::SumOfLanesM(mask, bits);

    alignas(16) constexpr uint32_t packed_array[256] = {
        // PrintExpand32x8.
            0xffffffff, 0xfffffff0, 0xffffff0f, 0xffffff10, 0xfffff0ff, 0xfffff1f0, 0xfffff10f, 0xfffff210, 0xffff0fff,
            0xffff1ff0, 0xffff1f0f, 0xffff2f10, 0xffff10ff, 0xffff21f0, 0xffff210f, 0xffff3210, 0xfff0ffff, 0xfff1fff0,
            0xfff1ff0f, 0xfff2ff10, 0xfff1f0ff, 0xfff2f1f0, 0xfff2f10f, 0xfff3f210, 0xfff10fff, 0xfff21ff0, 0xfff21f0f,
            0xfff32f10, 0xfff210ff, 0xfff321f0, 0xfff3210f, 0xfff43210, 0xff0fffff, 0xff1ffff0, 0xff1fff0f, 0xff2fff10,
            0xff1ff0ff, 0xff2ff1f0, 0xff2ff10f, 0xff3ff210, 0xff1f0fff, 0xff2f1ff0, 0xff2f1f0f, 0xff3f2f10, 0xff2f10ff,
            0xff3f21f0, 0xff3f210f, 0xff4f3210, 0xff10ffff, 0xff21fff0, 0xff21ff0f, 0xff32ff10, 0xff21f0ff, 0xff32f1f0,
            0xff32f10f, 0xff43f210, 0xff210fff, 0xff321ff0, 0xff321f0f, 0xff432f10, 0xff3210ff, 0xff4321f0, 0xff43210f,
            0xff543210, 0xf0ffffff, 0xf1fffff0, 0xf1ffff0f, 0xf2ffff10, 0xf1fff0ff, 0xf2fff1f0, 0xf2fff10f, 0xf3fff210,
            0xf1ff0fff, 0xf2ff1ff0, 0xf2ff1f0f, 0xf3ff2f10, 0xf2ff10ff, 0xf3ff21f0, 0xf3ff210f, 0xf4ff3210, 0xf1f0ffff,
            0xf2f1fff0, 0xf2f1ff0f, 0xf3f2ff10, 0xf2f1f0ff, 0xf3f2f1f0, 0xf3f2f10f, 0xf4f3f210, 0xf2f10fff, 0xf3f21ff0,
            0xf3f21f0f, 0xf4f32f10, 0xf3f210ff, 0xf4f321f0, 0xf4f3210f, 0xf5f43210, 0xf10fffff, 0xf21ffff0, 0xf21fff0f,
            0xf32fff10, 0xf21ff0ff, 0xf32ff1f0, 0xf32ff10f, 0xf43ff210, 0xf21f0fff, 0xf32f1ff0, 0xf32f1f0f, 0xf43f2f10,
            0xf32f10ff, 0xf43f21f0, 0xf43f210f, 0xf54f3210, 0xf210ffff, 0xf321fff0, 0xf321ff0f, 0xf432ff10, 0xf321f0ff,
            0xf432f1f0, 0xf432f10f, 0xf543f210, 0xf3210fff, 0xf4321ff0, 0xf4321f0f, 0xf5432f10, 0xf43210ff, 0xf54321f0,
            0xf543210f, 0xf6543210, 0x0fffffff, 0x1ffffff0, 0x1fffff0f, 0x2fffff10, 0x1ffff0ff, 0x2ffff1f0, 0x2ffff10f,
            0x3ffff210, 0x1fff0fff, 0x2fff1ff0, 0x2fff1f0f, 0x3fff2f10, 0x2fff10ff, 0x3fff21f0, 0x3fff210f, 0x4fff3210,
            0x1ff0ffff, 0x2ff1fff0, 0x2ff1ff0f, 0x3ff2ff10, 0x2ff1f0ff, 0x3ff2f1f0, 0x3ff2f10f, 0x4ff3f210, 0x2ff10fff,
            0x3ff21ff0, 0x3ff21f0f, 0x4ff32f10, 0x3ff210ff, 0x4ff321f0, 0x4ff3210f, 0x5ff43210, 0x1f0fffff, 0x2f1ffff0,
            0x2f1fff0f, 0x3f2fff10, 0x2f1ff0ff, 0x3f2ff1f0, 0x3f2ff10f, 0x4f3ff210, 0x2f1f0fff, 0x3f2f1ff0, 0x3f2f1f0f,
            0x4f3f2f10, 0x3f2f10ff, 0x4f3f21f0, 0x4f3f210f, 0x5f4f3210, 0x2f10ffff, 0x3f21fff0, 0x3f21ff0f, 0x4f32ff10,
            0x3f21f0ff, 0x4f32f1f0, 0x4f32f10f, 0x5f43f210, 0x3f210fff, 0x4f321ff0, 0x4f321f0f, 0x5f432f10, 0x4f3210ff,
            0x5f4321f0, 0x5f43210f, 0x6f543210, 0x10ffffff, 0x21fffff0, 0x21ffff0f, 0x32ffff10, 0x21fff0ff, 0x32fff1f0,
            0x32fff10f, 0x43fff210, 0x21ff0fff, 0x32ff1ff0, 0x32ff1f0f, 0x43ff2f10, 0x32ff10ff, 0x43ff21f0, 0x43ff210f,
            0x54ff3210, 0x21f0ffff, 0x32f1fff0, 0x32f1ff0f, 0x43f2ff10, 0x32f1f0ff, 0x43f2f1f0, 0x43f2f10f, 0x54f3f210,
            0x32f10fff, 0x43f21ff0, 0x43f21f0f, 0x54f32f10, 0x43f210ff, 0x54f321f0, 0x54f3210f, 0x65f43210, 0x210fffff,
            0x321ffff0, 0x321fff0f, 0x432fff10, 0x321ff0ff, 0x432ff1f0, 0x432ff10f, 0x543ff210, 0x321f0fff, 0x432f1ff0,
            0x432f1f0f, 0x543f2f10, 0x432f10ff, 0x543f21f0, 0x543f210f, 0x654f3210, 0x3210ffff, 0x4321fff0, 0x4321ff0f,
            0x5432ff10, 0x4321f0ff, 0x5432f1f0, 0x5432f10f, 0x6543f210, 0x43210fff, 0x54321ff0, 0x54321f0f, 0x65432f10,
            0x543210ff, 0x654321f0, 0x6543210f, 0x76543210 };

    // For lane i, shift the i-th 4-bit index down and mask with 0xF because
    // svtbl zeros outputs if the index is out of bounds.
    const svuint32_t packed = Set(du32, packed_array[code]);
    const svuint32_t indices = detail::AndN(Shr(packed, svindex_u32(0, 4)), 0xF);
    return TableLookupLanes(v, indices); // already zeros mask=false lanes
}

template <class V, OMNI_IF_T_SIZE_V(V, 8)> OMNI_API V Expand(V v, svbool_t mask)
{
    const DFromV<V> d;
    const RebindToUnsigned<decltype(d)> du64;

    // Convert mask into bitfield via horizontal sum (faster than ORV) of masked
    // bits 1, 2, 4, 8. Pre-multiply by N so we can use it as an offset for
    // SetTableIndices.
    const svuint64_t bits = Shl(Set(du64, 1), Iota(du64, 2));
    const size_t offset = detail::SumOfLanesM(mask, bits);

    alignas(16) static constexpr uint64_t table[4 * 16] = {
        // PrintExpand64x4Tables - small enough to store uncompressed.
            255, 255, 255, 255, 0, 255, 255, 255, 255, 0, 255, 255, 0, 1, 255, 255, 255, 255, 0, 255, 0, 255, 1, 255,
            255, 0, 1, 255, 0, 1, 2, 255, 255, 255, 255, 0, 0, 255, 255, 1, 255, 0, 255, 1, 0, 1, 255, 2, 255, 255, 0,
            1, 0, 255, 1, 2, 255, 0, 1, 2, 0, 1, 2, 3 };
    // This already zeros mask=false lanes.
    return TableLookupLanes(v, SetTableIndices(d, table + offset));
}

// ------------------------------ LoadExpand

template <class D> OMNI_API VFromD<D> LoadExpand(MFromD<D> mask, D d, const TFromD<D> *OMNI_RESTRICT unaligned)
{
    return Expand(LoadU(d, unaligned), mask);
}

// ------------------------------ MulEven (InterleaveEven)
template <class V, class DW = RepartitionToWide<DFromV<V>>, OMNI_IF_T_SIZE_ONE_OF_V(V, (1 << 1) | (1 << 2) | (1 << 4))>
OMNI_API VFromD<DW> MulEven(const V a, const V b)
{
    const auto lo = Mul(a, b);
    const auto hi = MulHigh(a, b);
    return BitCast(DW(), detail::InterleaveEven(lo, hi));
}

template <class V, class DW = RepartitionToWide<DFromV<V>>, OMNI_IF_T_SIZE_ONE_OF_V(V, (1 << 1) | (1 << 2) | (1 << 4))>
OMNI_API VFromD<DW> MulOdd(const V a, const V b)
{
    const auto lo = Mul(a, b);
    const auto hi = MulHigh(a, b);
    return BitCast(DW(), detail::InterleaveOdd(lo, hi));
}

OMNI_API svint64_t MulEven(const svint64_t a, const svint64_t b)
{
    const auto lo = Mul(a, b);
    const auto hi = MulHigh(a, b);
    return detail::InterleaveEven(lo, hi);
}

OMNI_API svuint64_t MulEven(const svuint64_t a, const svuint64_t b)
{
    const auto lo = Mul(a, b);
    const auto hi = MulHigh(a, b);
    return detail::InterleaveEven(lo, hi);
}

OMNI_API svint64_t MulOdd(const svint64_t a, const svint64_t b)
{
    const auto lo = Mul(a, b);
    const auto hi = MulHigh(a, b);
    return detail::InterleaveOdd(lo, hi);
}

OMNI_API svuint64_t MulOdd(const svuint64_t a, const svuint64_t b)
{
    const auto lo = Mul(a, b);
    const auto hi = MulHigh(a, b);
    return detail::InterleaveOdd(lo, hi);
}

// ------------------------------ WidenMulPairwiseAdd

template <size_t N, int kPow2> OMNI_API svfloat32_t WidenMulPairwiseAdd(Simd<float, N, kPow2> df, VBF16 a, VBF16 b)
{
    return MulAdd(PromoteEvenTo(df, a), PromoteEvenTo(df, b), Mul(PromoteOddTo(df, a), PromoteOddTo(df, b)));
}

template <size_t N, int kPow2>
OMNI_API svint32_t WidenMulPairwiseAdd(Simd<int32_t, N, kPow2> d32, svint16_t a, svint16_t b)
{
    return MulAdd(PromoteEvenTo(d32, a), PromoteEvenTo(d32, b), Mul(PromoteOddTo(d32, a), PromoteOddTo(d32, b)));
}

template <size_t N, int kPow2>
OMNI_API svuint32_t WidenMulPairwiseAdd(Simd<uint32_t, N, kPow2> d32, svuint16_t a, svuint16_t b)
{
    return MulAdd(PromoteEvenTo(d32, a), PromoteEvenTo(d32, b), Mul(PromoteOddTo(d32, a), PromoteOddTo(d32, b)));
}

// ------------------------------ ReorderWidenMulAccumulate (MulAdd, ZipLower)
template <size_t N, int kPow2>
OMNI_API svint32_t ReorderWidenMulAccumulate(Simd<int32_t, N, kPow2> d32, svint16_t a, svint16_t b,
    const svint32_t sum0, svint32_t &sum1)
{
    // Lane order within sum0/1 is undefined, hence we can avoid the
    // longer-latency lane-crossing PromoteTo by using PromoteEvenTo.
    sum1 = MulAdd(PromoteOddTo(d32, a), PromoteOddTo(d32, b), sum1);
    return MulAdd(PromoteEvenTo(d32, a), PromoteEvenTo(d32, b), sum0);
}

template <size_t N, int kPow2>
OMNI_API svuint32_t ReorderWidenMulAccumulate(Simd<uint32_t, N, kPow2> d32, svuint16_t a, svuint16_t b,
    const svuint32_t sum0, svuint32_t &sum1)
{
    // Lane order within sum0/1 is undefined, hence we can avoid the
    // longer-latency lane-crossing PromoteTo by using PromoteEvenTo.
    sum1 = MulAdd(PromoteOddTo(d32, a), PromoteOddTo(d32, b), sum1);
    return MulAdd(PromoteEvenTo(d32, a), PromoteEvenTo(d32, b), sum0);
}

// ------------------------------ RearrangeToOddPlusEven
template <class VW> OMNI_API VW RearrangeToOddPlusEven(const VW sum0, const VW sum1)
{
    // sum0 is the sum of bottom/even lanes and sum1 of top/odd lanes.
    return Add(sum0, sum1);
}

// ------------------------------ SumOfMulQuadAccumulate
template <class DI32, OMNI_IF_I32_D(DI32)>
OMNI_API VFromD<DI32> SumOfMulQuadAccumulate(DI32 /* di32 */, svint8_t a, svint8_t b, svint32_t sum)
{
    return svdot_s32(sum, a, b);
}

template <class DU32, OMNI_IF_U32_D(DU32)>
OMNI_API VFromD<DU32> SumOfMulQuadAccumulate(DU32 /* du32 */, svuint8_t a, svuint8_t b, svuint32_t sum)
{
    return svdot_u32(sum, a, b);
}

template <class DI32, OMNI_IF_I32_D(DI32)>
OMNI_API VFromD<DI32> SumOfMulQuadAccumulate(DI32 di32, svuint8_t a_u, svint8_t b_i, svint32_t sum)
{
    const RebindToUnsigned<decltype(di32)> du32;
    const Repartition<uint8_t, decltype(di32)> du8;

    const auto b_u = BitCast(du8, b_i);
    const auto result_sum0 = svdot_u32(BitCast(du32, sum), a_u, b_u);
    const auto result_sum1 = ShiftLeft<8>(svdot_u32(Zero(du32), a_u, ShiftRight<7>(b_u)));

    return BitCast(di32, Sub(result_sum0, result_sum1));
}

#ifdef OMNI_NATIVE_I16_I16_SUMOFMULQUADACCUMULATE
#undef OMNI_NATIVE_I16_I16_SUMOFMULQUADACCUMULATE
#else
#define OMNI_NATIVE_I16_I16_SUMOFMULQUADACCUMULATE
#endif

template <class DI64, OMNI_IF_I64_D(DI64)>
OMNI_API VFromD<DI64> SumOfMulQuadAccumulate(DI64 /* di64 */, svint16_t a, svint16_t b, svint64_t sum)
{
    return svdot_s64(sum, a, b);
}

#ifdef OMNI_NATIVE_U16_U16_SUMOFMULQUADACCUMULATE
#undef OMNI_NATIVE_U16_U16_SUMOFMULQUADACCUMULATE
#else
#define OMNI_NATIVE_U16_U16_SUMOFMULQUADACCUMULATE
#endif

template <class DU64, OMNI_IF_U64_D(DU64)>
OMNI_API VFromD<DU64> SumOfMulQuadAccumulate(DU64 /* du64 */, svuint16_t a, svuint16_t b, svuint64_t sum)
{
    return svdot_u64(sum, a, b);
}
// ------------------------------ Lt128

namespace detail {
#define OMNI_SVE_DUP(BASE, CHAR, BITS, HALF, NAME, OP)                                                          \
    template <size_t N, int kPow2> OMNI_API svbool_t NAME(OMNI_SVE_D(BASE, BITS, N, kPow2) /* d */, svbool_t m) \
    {                                                                                                           \
        return sv##OP##_b##BITS(m, m);                                                                          \
    }

OMNI_SVE_FOREACH_U(OMNI_SVE_DUP, DupEvenB, trn1) // actually for bool
OMNI_SVE_FOREACH_U(OMNI_SVE_DUP, DupOddB, trn2)  // actually for bool
#undef OMNI_SVE_DUP

template <class D> OMNI_INLINE svuint64_t Lt128Vec(D d, const svuint64_t a, const svuint64_t b)
{
    static_assert(IsSame<TFromD<D>, uint64_t>(), "D must be u64");
    const svbool_t eqHx = Eq(a, b); // only odd lanes used
    // Convert to vector: more pipelines can execute vector TRN* instructions
    // than the predicate version.
    const svuint64_t ltHL = VecFromMask(d, Lt(a, b));
    // Move into upper lane: ltL if the upper half is equal, otherwise ltH.
    // Requires an extra IfThenElse because INSR, EXT, TRN2 are unpredicated.
    const svuint64_t ltHx = IfThenElse(eqHx, DupEven(ltHL), ltHL);
    // Duplicate upper lane into lower.
    return DupOdd(ltHx);
}
} // namespace detail

template <class D> OMNI_INLINE svbool_t Lt128(D d, const svuint64_t a, const svuint64_t b)
{
    return MaskFromVec(detail::Lt128Vec(d, a, b));
}

// ------------------------------ Lt128Upper

template <class D> OMNI_INLINE svbool_t Lt128Upper(D d, svuint64_t a, svuint64_t b)
{
    static_assert(IsSame<TFromD<D>, uint64_t>(), "D must be u64");
    const svbool_t ltHL = Lt(a, b);
    return detail::DupOddB(d, ltHL);
}

// ------------------------------ Eq128, Ne128
namespace detail {
template <class D> OMNI_INLINE svuint64_t Eq128Vec(D d, const svuint64_t a, const svuint64_t b)
{
    static_assert(IsSame<TFromD<D>, uint64_t>(), "D must be u64");
    // Convert to vector: more pipelines can execute vector TRN* instructions
    // than the predicate version.
    const svuint64_t eqHL = VecFromMask(d, Eq(a, b));
    // Duplicate upper and lower.
    const svuint64_t eqHH = DupOdd(eqHL);
    const svuint64_t eqLL = DupEven(eqHL);
    return And(eqLL, eqHH);
}

template <class D> OMNI_INLINE svuint64_t Ne128Vec(D d, const svuint64_t a, const svuint64_t b)
{
    static_assert(IsSame<TFromD<D>, uint64_t>(), "D must be u64");
    // Convert to vector: more pipelines can execute vector TRN* instructions
    // than the predicate version.
    const svuint64_t neHL = VecFromMask(d, Ne(a, b));
    // Duplicate upper and lower.
    const svuint64_t neHH = DupOdd(neHL);
    const svuint64_t neLL = DupEven(neHL);
    return Or(neLL, neHH);
}
} // namespace detail

template <class D> OMNI_INLINE svbool_t Eq128(D d, const svuint64_t a, const svuint64_t b)
{
    return MaskFromVec(detail::Eq128Vec(d, a, b));
}

template <class D> OMNI_INLINE svbool_t Ne128(D d, const svuint64_t a, const svuint64_t b)
{
    return MaskFromVec(detail::Ne128Vec(d, a, b));
}

// ------------------------------ Eq128Upper, Ne128Upper

template <class D> OMNI_INLINE svbool_t Eq128Upper(D d, svuint64_t a, svuint64_t b)
{
    static_assert(IsSame<TFromD<D>, uint64_t>(), "D must be u64");
    const svbool_t eqHL = Eq(a, b);
    return detail::DupOddB(d, eqHL);
}

template <class D> OMNI_INLINE svbool_t Ne128Upper(D d, svuint64_t a, svuint64_t b)
{
    static_assert(IsSame<TFromD<D>, uint64_t>(), "D must be u64");
    const svbool_t neHL = Ne(a, b);
    return detail::DupOddB(d, neHL);
}

// ------------------------------ Min128, Max128 (Lt128)

template <class D> OMNI_INLINE svuint64_t Min128(D d, const svuint64_t a, const svuint64_t b)
{
    return IfVecThenElse(detail::Lt128Vec(d, a, b), a, b);
}

template <class D> OMNI_INLINE svuint64_t Max128(D d, const svuint64_t a, const svuint64_t b)
{
    return IfVecThenElse(detail::Lt128Vec(d, b, a), a, b);
}

template <class D> OMNI_INLINE svuint64_t Min128Upper(D d, const svuint64_t a, const svuint64_t b)
{
    return IfThenElse(Lt128Upper(d, a, b), a, b);
}

template <class D> OMNI_INLINE svuint64_t Max128Upper(D d, const svuint64_t a, const svuint64_t b)
{
    return IfThenElse(Lt128Upper(d, b, a), a, b);
}

// -------------------- LeadingZeroCount, TrailingZeroCount, HighestSetBitIndex

#ifdef OMNI_NATIVE_LEADING_ZERO_COUNT
#undef OMNI_NATIVE_LEADING_ZERO_COUNT
#else
#define OMNI_NATIVE_LEADING_ZERO_COUNT
#endif

#define OMNI_SVE_LEADING_ZERO_COUNT(BASE, CHAR, BITS, HALF, NAME, OP)      \
    OMNI_API OMNI_SVE_V(BASE, BITS) NAME(OMNI_SVE_V(BASE, BITS) v)         \
    {                                                                      \
        const DFromV<decltype(v)> d;                                       \
        return BitCast(d, sv##OP##_##CHAR##BITS##_x(detail::PTrue(d), v)); \
    }

OMNI_SVE_FOREACH_UI(OMNI_SVE_LEADING_ZERO_COUNT, LeadingZeroCount, clz)

#undef OMNI_SVE_LEADING_ZERO_COUNT

template <class V, OMNI_IF_NOT_FLOAT_NOR_SPECIAL_V(V)> OMNI_API V TrailingZeroCount(V v)
{
    return LeadingZeroCount(ReverseBits(v));
}

template <class V, OMNI_IF_NOT_FLOAT_NOR_SPECIAL_V(V)> OMNI_API V HighestSetBitIndex(V v)
{
    const DFromV<decltype(v)> d;
    using T = TFromD<decltype(d)>;
    return BitCast(d, Sub(Set(d, T{ sizeof(T) * 8 - 1 }), LeadingZeroCount(v)));
}

template <class T, size_t N> OMNI_API uint64_t LeadingValueCountMask(T value, const T *OMNI_RESTRICT in)
{
    CappedTag<T, N> d;
    auto vec = VecFromMask(d, detail::NeN(LoadU(d, in), value));
    T res[N];
    StoreU(vec, d, res);
    uint64_t nib = 0;
    for (int i = N - 1; i >= 0; --i) {
        nib = (nib << 4) | (res[i] & 0x0f);
    }
    return nib;
}

// -------------------- Match
template <typename T, size_t N> OMNI_API uint64_t FindMatchMask(T value, const T *OMNI_RESTRICT in)
{
    CappedTag<T, N> d;
    auto vec = VecFromMask(d, detail::EqN(LoadU(d, in), value));
    T res[N];
    StoreU(vec, d, res);
    uint64_t nib = 0;
    for (int i = N - 1; i >= 0; --i) {
        nib = (nib << 4) | (res[i] & 0x0f);
    }
    return nib;
}

template <typename T, size_t N>
OMNI_INLINE int32_t FindMatch(T value, const T* OMNI_RESTRICT in)
{
    svbool_t pg = svwhilelt_b8(0, (int)N);
    svuint8_t data = svld1(pg, (uint8_t*)in);
    svbool_t res = svcmpeq_n_u8(pg, data, value);
    int32_t mask = *(int*)&res;
    return mask;
}

template <typename T, size_t N>
OMNI_INLINE int32_t FindFirstMatch(T value, const T* OMNI_RESTRICT in)
{
    svbool_t pg = svwhilelt_b8(0, (int)N);
    svuint8_t data = svld1(pg, (uint8_t*)in);
    svbool_t res = svcmpeq_n_u8(pg, data, value);
    int32_t mask = *(int*)&res;
    return mask == 0 ? -1 : __builtin_ctz(mask);
}

template <typename T, size_t N>
OMNI_INLINE size_t CountLeadingValue(T value, const T* OMNI_RESTRICT in)
{
    svbool_t pg = svwhilelt_b8(0, (int)N);
    svint8_t data = svld1(pg, (int8_t*)in);
    svbool_t res = svcmpne_n_s8(pg, data, value);
    svbool_t pre = svbrkb_z(svptrue_b8(), res);
    return static_cast<intptr_t>(svcntp_b8(svptrue_b8(), pre));
}

namespace detail {
enum class SortOrder {
    ASCENDING,
    DESCENDING
};

using AddrVec = svuint64_t;
using AddrType = uint64_t;
template <typename T> using SortTag = ScalableTag<T>;

static const SortTag<AddrType> ADDR_TAG;

template <class Base> struct SharedTraits : public Base {
    using SharedTraitsForSortingNetwork = SharedTraits<typename Base::TraitsForSortingNetwork>;
    SortOrder CurrentOrder = Base::Order;
};

static svint64_t GetSmallestIntegerVec()
{
    return svdup_s64(INT64_MIN);
}
static svint64_t GetLargestIntegerVec()
{
    return svdup_s64(INT64_MAX);
}
static svfloat64_t GetSmallestDoubleVec()
{
    return svdup_f64(-DBL_MAX);
}
static svfloat64_t GetLargestDoubleVec()
{
    return svdup_f64(DBL_MAX);
}
template <class D> VFromD<D> GetVec(D d, double buf)
{
    return svdup_f64(buf);
}
template <class D> VFromD<D> GetVec(D d, int64_t buf)
{
    return svdup_s64(buf);
}

namespace QuickSortAscending {
template <class D, OMNI_IF_F64_D(D)> inline VFromD<D> GetSmallestVec(D d)
{
    return static_cast<VFromD<D>>(GetLargestDoubleVec());
}
template <class D, OMNI_IF_I64_D(D)> inline VFromD<D> GetSmallestVec(D d)
{
    return static_cast<VFromD<D>>(GetLargestIntegerVec());
}

template <class D, class V, OMNI_IF_F64_D(D)> inline TFromD<D> GetSmallest(D d, V v)
{
    return svminv_f64(svptrue_b64(), v);
}

template <class D, class V, OMNI_IF_F64_D(D)> inline TFromD<D> GetLargest(D d, V v)
{
    return svmaxv_f64(svptrue_b64(), v);
}

template <class D, class V, OMNI_IF_I64_D(D)> inline TFromD<D> GetSmallest(D d, V v)
{
    return svminv_s64(svptrue_b64(), v);
}

template <class D, class V, OMNI_IF_I64_D(D)> inline TFromD<D> GetLargest(D d, V v)
{
    return svmaxv_s64(svptrue_b64(), v);
}

template <class D, OMNI_IF_F64_D(D)> inline VFromD<D> GetLargestVec(D)
{
    return static_cast<VFromD<D>>(GetSmallestDoubleVec());
}

template <class D, OMNI_IF_I64_D(D)> inline VFromD<D> GetLargestVec(D)
{
    return static_cast<VFromD<D>>(GetSmallestIntegerVec());
}
template <class D, class V> static void UpdateMinMax(D, V value, V &smallest, V &largest)
{
    if constexpr (std::is_same_v<TFromD<D>, double>) {
        svbool_t cmp_less = svcmplt_f64(svptrue_b64(), value, smallest);
        svbool_t cmp_greater = svcmpgt_f64(svptrue_b64(), value, largest);

        smallest = svsel(cmp_less, value, smallest);
        largest = svsel(cmp_greater, value, largest);
    } else {
        svbool_t cmp_less = svcmplt_s64(svptrue_b64(), value, smallest);
        svbool_t cmp_greater = svcmpgt_s64(svptrue_b64(), value, largest);

        smallest = svsel(cmp_less, value, smallest);
        largest = svsel(cmp_greater, value, largest);
    }
}
template <class D, OMNI_IF_I64_D(D), class V> svbool_t Compare(D d, V v1, V v2)
{
    svbool_t less_than_mask = svcmplt_s64(svptrue_b64(), v1, v2);
    return less_than_mask;
}
template <class D, OMNI_IF_F64_D(D), class V> svbool_t Compare(D d, V v1, V v2)
{
    svbool_t less_than_mask = svcmplt_f64(svptrue_b64(), v1, v2);
    return less_than_mask;
}
};


namespace QuickSortDescending {
template <class D, OMNI_IF_F64_D(D)> inline VFromD<D> GetSmallestVec(D d)
{
    return GetSmallestDoubleVec();
}
template <class D, OMNI_IF_I64_D(D)> inline VFromD<D> GetSmallestVec(D d)
{
    return static_cast<VFromD<D>>(GetSmallestIntegerVec());
}
template <class D, OMNI_IF_F64_D(D)> inline VFromD<D> GetLargestVec(D d)
{
    return GetLargestDoubleVec();
}

template <class D, OMNI_IF_I64_D(D)> inline VFromD<D> GetLargestVec(D d)
{
    return static_cast<VFromD<D>>(GetLargestIntegerVec());
}
template <class D, class V> static void UpdateMinMax(D, V value, V &smallest, V &largest)
{
    svbool_t mask = svptrue_b64();
    if constexpr (std::is_same_v<TFromD<D>, double>) {
        svbool_t cmp_greater = svcmpgt_f64(mask, value, smallest);
        svbool_t cmp_less = svcmplt_f64(mask, value, largest);

        smallest = svsel(cmp_greater, value, smallest);
        largest = svsel(cmp_less, value, largest);
    } else {
        svbool_t cmp_greater = svcmpgt_s64(mask, value, smallest);
        svbool_t cmp_less = svcmplt_s64(mask, value, largest);

        smallest = svsel(cmp_greater, value, smallest);
        largest = svsel(cmp_less, value, largest);
    }
}
template <class D, OMNI_IF_I64_D(D), class V> svbool_t Compare(D d, V v1, V v2)
{
    svbool_t pg = svwhilelt_b64(0, 4);
    svbool_t maskV2LeV1 = svnot_b_z(pg, svcmplt_s64(pg, v1, v2));
    return maskV2LeV1;
}
template <class D, OMNI_IF_F64_D(D), class V> svbool_t Compare(D d, V v1, V v2)
{
    svbool_t pg = svwhilelt_b64(0, 4);
    svbool_t maskV2LeV1 = svnot_b_z(pg, svcmplt_f64(pg, v1, v2));
    return maskV2LeV1;
}
template <class D, class V, OMNI_IF_F64_D(D)> inline TFromD<D> GetSmallest(D d, V v)
{
    return svmaxv_f64(svptrue_b64(), v);
}
template <class D, class V, OMNI_IF_I64_D(D)> inline TFromD<D> GetSmallest(D d, V v)
{
    return svmaxv_s64(svptrue_b64(), v);
}

template <class D, class V, OMNI_IF_F64_D(D)> inline TFromD<D> GetLargest(D d, V v)
{
    return svminv_f64(svptrue_b64(), v);
}

template <class D, class V, OMNI_IF_I64_D(D)> inline TFromD<D> GetLargest(D d, V v)
{
    return svminv_s64(svptrue_b64(), v);
}
};
template <class V> OMNI_API V GetBlendedVec(const svbool_t &mask, const V &compressAddrVec, const V &tmpAddrVec)
{
    return svsel(mask, compressAddrVec, tmpAddrVec);
}
template <class Base> struct TraitsLane : public Base {
    using TraitsForSortingNetwork = TraitsLane<typename Base::OrderForSortingNetwork>;
    template <class D> inline void Sort2(D d, VFromD<D> &a, VFromD<D> &b) const
    {
        const Base *base = static_cast<const Base *>(this);

        const VFromD<D> a_copy = a;
        a = base->First(d, a, b);
        b = base->Last(d, a_copy, b);
    }
};
template <class V, class D, class RawType>
OMNI_API size_t CompressStore(const D d, const V v, const AddrVec a, const svbool_t mask, RawType *valuePtr,
    AddrType *address)
{
    StoreU(Compress(v, mask), d, valuePtr);
    StoreU(Compress(a, mask), ADDR_TAG, address);
    return CountTrue(d, mask);
}
template <class V, class D, class RawType>
OMNI_API size_t CompressBlendedStore(D d, V v, AddrVec addrVec, MFromD<D> mask, RawType *values, AddrType *addrPtr)
{
    const size_t count = CountTrue(d, mask);
    const svbool_t store_mask = FirstN(d, count);
    BlendedStore(Compress(v, mask), store_mask, d, values);
    BlendedStore(Compress(addrVec, mask), store_mask, ADDR_TAG, addrPtr);
    return count;
}
inline bool NotEqual(double pivot, double smallest)
{
    if (std::fabs(pivot - smallest) >= DBL_EPSILON) {
        return true;
    } else {
        return false;
    }
}

inline bool NotEqual(int64_t a, int64_t b)
{
    return a != b;
}
inline double GetAvg(double a, double b)
{
    return (a + b) / 2;
}
inline int64_t GetAvg(int64_t a, int64_t b)
{
    return (a & b) + ((a ^ b) >> 1);
}
template <class Traits, class V> inline V MedianOf3(Traits st, V v0, V v1, V v2)
{
    const DFromV<V> d;
    st.Sort2(d, v0, v2);
    v1 = st.Last(d, v0, v1);
    v1 = st.First(d, v1, v2);
    return v1;
}
template <class D, class M> OMNI_API MFromD<D> GetBlendedMask(D d, M a, M b)
{
    return svbic_b_z(svptrue_b64(), a, b);
}

template <class D, class M> inline MFromD<D> GetAndMask(D d, M a, M b)
{
    return svand_b_z(svptrue_b64(), a, b);
}
}
// ================================================== END MACROS
#undef OMNI_SVE_ALL_PTRUE
#undef OMNI_SVE_D
#undef OMNI_SVE_FOREACH
#undef OMNI_SVE_FOREACH_BF16
#undef OMNI_SVE_FOREACH_BF16_UNCONDITIONAL
#undef OMNI_SVE_FOREACH_F
#undef OMNI_SVE_FOREACH_F16
#undef OMNI_SVE_FOREACH_F32
#undef OMNI_SVE_FOREACH_F3264
#undef OMNI_SVE_FOREACH_F64
#undef OMNI_SVE_FOREACH_I
#undef OMNI_SVE_FOREACH_I08
#undef OMNI_SVE_FOREACH_I16
#undef OMNI_SVE_FOREACH_I32
#undef OMNI_SVE_FOREACH_I64
#undef OMNI_SVE_FOREACH_IF
#undef OMNI_SVE_FOREACH_U
#undef OMNI_SVE_FOREACH_U08
#undef OMNI_SVE_FOREACH_U16
#undef OMNI_SVE_FOREACH_U32
#undef OMNI_SVE_FOREACH_U64
#undef OMNI_SVE_FOREACH_UI
#undef OMNI_SVE_FOREACH_UI08
#undef OMNI_SVE_FOREACH_UI16
#undef OMNI_SVE_FOREACH_UI32
#undef OMNI_SVE_FOREACH_UI64
#undef OMNI_SVE_FOREACH_UIF3264
#undef OMNI_SVE_IF_EMULATED_D
#undef OMNI_SVE_IF_NOT_EMULATED_D
#undef OMNI_SVE_PTRUE
#undef OMNI_SVE_RETV_ARGMVV
#undef OMNI_SVE_RETV_ARGPV
#undef OMNI_SVE_RETV_ARGPVN
#undef OMNI_SVE_RETV_ARGPVV
#undef OMNI_SVE_RETV_ARGV
#undef OMNI_SVE_RETV_ARGVN
#undef OMNI_SVE_RETV_ARGVV
#undef OMNI_SVE_RETV_ARGVVV
#undef OMNI_SVE_T
#undef OMNI_SVE_UNDEFINED
#undef OMNI_SVE_V

} // namespace omni
#endif