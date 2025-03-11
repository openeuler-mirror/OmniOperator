/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
#ifndef ARM_NEON_INL_H
#define ARM_NEON_INL_H

#include <arm_neon.h>
#include <math.h>
#include <float.h>
#include "simd/instruction/shared-inl.h"

namespace simd {
namespace detail {
#define OMNI_NEON_BUILD_TPL_1
#define OMNI_NEON_BUILD_TPL_2
#define OMNI_NEON_BUILD_TPL_3

#define OMNI_NEON_BUILD_RET_1(type, size) Vec128<type##_t, size>
#define OMNI_NEON_BUILD_RET_2(type, size) Vec128<type##_t, size>
#define OMNI_NEON_BUILD_RET_3(type, size) Vec128<type##_t, size>

#define OMNI_NEON_BUILD_PARAM_1(type, size) const Vec128<type##_t, size> a
#define OMNI_NEON_BUILD_PARAM_2(type, size) const Vec128<type##_t, size> a, const Vec128<type##_t, size> b
#define OMNI_NEON_BUILD_PARAM_3(type, size) \
    const Vec128<type##_t, size> a, const Vec128<type##_t, size> b, const Vec128<type##_t, size> c

#define OMNI_NEON_BUILD_ARG_1 a.raw
#define OMNI_NEON_BUILD_ARG_2 a.raw, b.raw
#define OMNI_NEON_BUILD_ARG_3 a.raw, b.raw, c.raw

#define OMNI_NEON_EVAL(func, ...) func(__VA_ARGS__)

#define OMNI_NEON_DEF_FUNCTION(type, size, name, prefix, infix, suffix, args)         \
    OMNI_CONCAT(OMNI_NEON_BUILD_TPL_, args)                                           \
    OMNI_API OMNI_CONCAT(OMNI_NEON_BUILD_RET_, args)(type, size)                      \
        name(OMNI_CONCAT(OMNI_NEON_BUILD_PARAM_, args)(type, size))                   \
    {                                                                                 \
        return OMNI_CONCAT(OMNI_NEON_BUILD_RET_, args)(type,                          \
            size)(OMNI_NEON_EVAL(prefix##infix##suffix, OMNI_NEON_BUILD_ARG_##args)); \
    }

#define OMNI_NEON_DEF_FUNCTION_UINT_8(name, prefix, infix, args)        \
    OMNI_NEON_DEF_FUNCTION(uint8, 16, name, prefix##q, infix, u8, args) \
    OMNI_NEON_DEF_FUNCTION(uint8, 8, name, prefix, infix, u8, args)     \
    OMNI_NEON_DEF_FUNCTION(uint8, 4, name, prefix, infix, u8, args)     \
    OMNI_NEON_DEF_FUNCTION(uint8, 2, name, prefix, infix, u8, args)     \
    OMNI_NEON_DEF_FUNCTION(uint8, 1, name, prefix, infix, u8, args)

// int8_t
#define OMNI_NEON_DEF_FUNCTION_INT_8(name, prefix, infix, args)        \
    OMNI_NEON_DEF_FUNCTION(int8, 16, name, prefix##q, infix, s8, args) \
    OMNI_NEON_DEF_FUNCTION(int8, 8, name, prefix, infix, s8, args)     \
    OMNI_NEON_DEF_FUNCTION(int8, 4, name, prefix, infix, s8, args)     \
    OMNI_NEON_DEF_FUNCTION(int8, 2, name, prefix, infix, s8, args)     \
    OMNI_NEON_DEF_FUNCTION(int8, 1, name, prefix, infix, s8, args)

// uint16_t
#define OMNI_NEON_DEF_FUNCTION_UINT_16(name, prefix, infix, args)        \
    OMNI_NEON_DEF_FUNCTION(uint16, 8, name, prefix##q, infix, u16, args) \
    OMNI_NEON_DEF_FUNCTION(uint16, 4, name, prefix, infix, u16, args)    \
    OMNI_NEON_DEF_FUNCTION(uint16, 2, name, prefix, infix, u16, args)    \
    OMNI_NEON_DEF_FUNCTION(uint16, 1, name, prefix, infix, u16, args)

// int16_t
#define OMNI_NEON_DEF_FUNCTION_INT_16(name, prefix, infix, args)        \
    OMNI_NEON_DEF_FUNCTION(int16, 8, name, prefix##q, infix, s16, args) \
    OMNI_NEON_DEF_FUNCTION(int16, 4, name, prefix, infix, s16, args)    \
    OMNI_NEON_DEF_FUNCTION(int16, 2, name, prefix, infix, s16, args)    \
    OMNI_NEON_DEF_FUNCTION(int16, 1, name, prefix, infix, s16, args)

// uint32_t
#define OMNI_NEON_DEF_FUNCTION_UINT_32(name, prefix, infix, args)        \
    OMNI_NEON_DEF_FUNCTION(uint32, 4, name, prefix##q, infix, u32, args) \
    OMNI_NEON_DEF_FUNCTION(uint32, 2, name, prefix, infix, u32, args)    \
    OMNI_NEON_DEF_FUNCTION(uint32, 1, name, prefix, infix, u32, args)

// int32_t
#define OMNI_NEON_DEF_FUNCTION_INT_32(name, prefix, infix, args)        \
    OMNI_NEON_DEF_FUNCTION(int32, 4, name, prefix##q, infix, s32, args) \
    OMNI_NEON_DEF_FUNCTION(int32, 2, name, prefix, infix, s32, args)    \
    OMNI_NEON_DEF_FUNCTION(int32, 1, name, prefix, infix, s32, args)

// uint64_t
#define OMNI_NEON_DEF_FUNCTION_UINT_64(name, prefix, infix, args)        \
    OMNI_NEON_DEF_FUNCTION(uint64, 2, name, prefix##q, infix, u64, args) \
    OMNI_NEON_DEF_FUNCTION(uint64, 1, name, prefix, infix, u64, args)

// int64_t
#define OMNI_NEON_DEF_FUNCTION_INT_64(name, prefix, infix, args)        \
    OMNI_NEON_DEF_FUNCTION(int64, 2, name, prefix##q, infix, s64, args) \
    OMNI_NEON_DEF_FUNCTION(int64, 1, name, prefix, infix, s64, args)

#define OMNI_NEON_HAVE_BFLOAT16 0

#define OMNI_NEON_HAVE_F32_TO_BF16C 0

#define OMNI_NEON_DEF_FUNCTION_BFLOAT_16(name, prefix, infix, args)

// Used for conversion instructions if OMNI_NEON_HAVE_F16C.
#define OMNI_NEON_DEF_FUNCTION_FLOAT_16_UNCONDITIONAL(name, prefix, infix, args) \
    OMNI_NEON_DEF_FUNCTION(float16, 8, name, prefix##q, infix, f16, args)        \
    OMNI_NEON_DEF_FUNCTION(float16, 4, name, prefix, infix, f16, args)           \
    OMNI_NEON_DEF_FUNCTION(float16, 2, name, prefix, infix, f16, args)           \
    OMNI_NEON_DEF_FUNCTION(float16, 1, name, prefix, infix, f16, args)

#define OMNI_NEON_DEF_FUNCTION_FLOAT_16(name, prefix, infix, args)

// Enable generic functions for whichever of (f16, bf16) are not supported.
#if !OMNI_HAVE_FLOAT16 && !OMNI_NEON_HAVE_BFLOAT16
#define OMNI_NEON_IF_EMULATED_D(D) OMNI_IF_SPECIAL_FLOAT_D(D)
#define OMNI_GENERIC_IF_EMULATED_D(D) OMNI_IF_SPECIAL_FLOAT_D(D)
#define OMNI_NEON_IF_NOT_EMULATED_D(D) OMNI_IF_NOT_SPECIAL_FLOAT_D(D)
#endif

// float
#define OMNI_NEON_DEF_FUNCTION_FLOAT_32(name, prefix, infix, args)        \
    OMNI_NEON_DEF_FUNCTION(float32, 4, name, prefix##q, infix, f32, args) \
    OMNI_NEON_DEF_FUNCTION(float32, 2, name, prefix, infix, f32, args)    \
    OMNI_NEON_DEF_FUNCTION(float32, 1, name, prefix, infix, f32, args)

// double
#if OMNI_HAVE_FLOAT64
#define OMNI_NEON_DEF_FUNCTION_FLOAT_64(name, prefix, infix, args)        \
    OMNI_NEON_DEF_FUNCTION(float64, 2, name, prefix##q, infix, f64, args) \
    OMNI_NEON_DEF_FUNCTION(float64, 1, name, prefix, infix, f64, args)
#endif

#define OMNI_NEON_DEF_FUNCTION_UINT_8_16_32(name, prefix, infix, args) \
    OMNI_NEON_DEF_FUNCTION_UINT_8(name, prefix, infix, args)           \
    OMNI_NEON_DEF_FUNCTION_UINT_16(name, prefix, infix, args)          \
    OMNI_NEON_DEF_FUNCTION_UINT_32(name, prefix, infix, args)

// int8_t, int16_t and int32_t
#define OMNI_NEON_DEF_FUNCTION_INT_8_16_32(name, prefix, infix, args) \
    OMNI_NEON_DEF_FUNCTION_INT_8(name, prefix, infix, args)           \
    OMNI_NEON_DEF_FUNCTION_INT_16(name, prefix, infix, args)          \
    OMNI_NEON_DEF_FUNCTION_INT_32(name, prefix, infix, args)

// uint8_t, uint16_t, uint32_t and uint64_t
#define OMNI_NEON_DEF_FUNCTION_UINTS(name, prefix, infix, args)    \
    OMNI_NEON_DEF_FUNCTION_UINT_8_16_32(name, prefix, infix, args) \
    OMNI_NEON_DEF_FUNCTION_UINT_64(name, prefix, infix, args)

// int8_t, int16_t, int32_t and int64_t
#define OMNI_NEON_DEF_FUNCTION_INTS(name, prefix, infix, args)    \
    OMNI_NEON_DEF_FUNCTION_INT_8_16_32(name, prefix, infix, args) \
    OMNI_NEON_DEF_FUNCTION_INT_64(name, prefix, infix, args)

// All int*_t and uint*_t up to 64
#define OMNI_NEON_DEF_FUNCTION_INTS_UINTS(name, prefix, infix, args) \
    OMNI_NEON_DEF_FUNCTION_INTS(name, prefix, infix, args)           \
    OMNI_NEON_DEF_FUNCTION_UINTS(name, prefix, infix, args)

#define OMNI_NEON_DEF_FUNCTION_FLOAT_16_32(name, prefix, infix, args) \
    OMNI_NEON_DEF_FUNCTION_FLOAT_16(name, prefix, infix, args)        \
    OMNI_NEON_DEF_FUNCTION_FLOAT_32(name, prefix, infix, args)

#define OMNI_NEON_DEF_FUNCTION_ALL_FLOATS(name, prefix, infix, args) \
    OMNI_NEON_DEF_FUNCTION_FLOAT_16_32(name, prefix, infix, args)    \
    OMNI_NEON_DEF_FUNCTION_FLOAT_64(name, prefix, infix, args)

// All previous types.
#define OMNI_NEON_DEF_FUNCTION_ALL_TYPES(name, prefix, infix, args) \
    OMNI_NEON_DEF_FUNCTION_INTS_UINTS(name, prefix, infix, args)    \
    OMNI_NEON_DEF_FUNCTION_ALL_FLOATS(name, prefix, infix, args)

#define OMNI_NEON_DEF_FUNCTION_UI_8_16_32(name, prefix, infix, args) \
    OMNI_NEON_DEF_FUNCTION_UINT_8_16_32(name, prefix, infix, args)   \
    OMNI_NEON_DEF_FUNCTION_INT_8_16_32(name, prefix, infix, args)

#define OMNI_NEON_DEF_FUNCTION_UIF_8_16_32(name, prefix, infix, args) \
    OMNI_NEON_DEF_FUNCTION_UI_8_16_32(name, prefix, infix, args)      \
    OMNI_NEON_DEF_FUNCTION_FLOAT_16_32(name, prefix, infix, args)

#define OMNI_NEON_DEF_FUNCTION_UIF_64(name, prefix, infix, args) \
    OMNI_NEON_DEF_FUNCTION_UINT_64(name, prefix, infix, args)    \
    OMNI_NEON_DEF_FUNCTION_INT_64(name, prefix, infix, args)     \
    OMNI_NEON_DEF_FUNCTION_FLOAT_64(name, prefix, infix, args)

// For vzip1/2
#define OMNI_NEON_DEF_FUNCTION_FULL_UI_64(name, prefix, infix, args)     \
    OMNI_NEON_DEF_FUNCTION(uint64, 2, name, prefix##q, infix, u64, args) \
    OMNI_NEON_DEF_FUNCTION(int64, 2, name, prefix##q, infix, s64, args)
#define OMNI_NEON_DEF_FUNCTION_FULL_UIF_64(name, prefix, infix, args) \
    OMNI_NEON_DEF_FUNCTION_FULL_UI_64(name, prefix, infix, args)      \
    OMNI_NEON_DEF_FUNCTION(float64, 2, name, prefix##q, infix, f64, args)

// For eor3q, which is only defined for full vectors.
#define OMNI_NEON_DEF_FUNCTION_FULL_UI(name, prefix, infix, args)        \
    OMNI_NEON_DEF_FUNCTION(uint8, 16, name, prefix##q, infix, u8, args)  \
    OMNI_NEON_DEF_FUNCTION(uint16, 8, name, prefix##q, infix, u16, args) \
    OMNI_NEON_DEF_FUNCTION(uint32, 4, name, prefix##q, infix, u32, args) \
    OMNI_NEON_DEF_FUNCTION(int8, 16, name, prefix##q, infix, s8, args)   \
    OMNI_NEON_DEF_FUNCTION(int16, 8, name, prefix##q, infix, s16, args)  \
    OMNI_NEON_DEF_FUNCTION(int32, 4, name, prefix##q, infix, s32, args)  \
    OMNI_NEON_DEF_FUNCTION_FULL_UI_64(name, prefix, infix, args)

template <typename T, size_t N> struct Tuple2;
template <typename T, size_t N> struct Tuple3;
template <typename T, size_t N> struct Tuple4;

template <> struct Tuple2<uint8_t, 16> {
    uint8x16x2_t raw;
};
template <size_t N> struct Tuple2<uint8_t, N> {
    uint8x8x2_t raw;
};
template <> struct Tuple2<int8_t, 16> {
    int8x16x2_t raw;
};
template <size_t N> struct Tuple2<int8_t, N> {
    int8x8x2_t raw;
};
template <> struct Tuple2<uint16_t, 8> {
    uint16x8x2_t raw;
};
template <size_t N> struct Tuple2<uint16_t, N> {
    uint16x4x2_t raw;
};
template <> struct Tuple2<int16_t, 8> {
    int16x8x2_t raw;
};
template <size_t N> struct Tuple2<int16_t, N> {
    int16x4x2_t raw;
};
template <> struct Tuple2<uint32_t, 4> {
    uint32x4x2_t raw;
};
template <size_t N> struct Tuple2<uint32_t, N> {
    uint32x2x2_t raw;
};
template <> struct Tuple2<int32_t, 4> {
    int32x4x2_t raw;
};
template <size_t N> struct Tuple2<int32_t, N> {
    int32x2x2_t raw;
};
template <> struct Tuple2<uint64_t, 2> {
    uint64x2x2_t raw;
};
template <size_t N> struct Tuple2<uint64_t, N> {
    uint64x1x2_t raw;
};
template <> struct Tuple2<int64_t, 2> {
    int64x2x2_t raw;
};
template <size_t N> struct Tuple2<int64_t, N> {
    int64x1x2_t raw;
};

template <> struct Tuple2<float32_t, 4> {
    float32x4x2_t raw;
};
template <size_t N> struct Tuple2<float32_t, N> {
    float32x2x2_t raw;
};
#if OMNI_HAVE_FLOAT64
template <> struct Tuple2<float64_t, 2> {
    float64x2x2_t raw;
};
template <size_t N> struct Tuple2<float64_t, N> {
    float64x1x2_t raw;
};
#endif // OMNI_HAVE_FLOAT64

template <> struct Tuple3<uint8_t, 16> {
    uint8x16x3_t raw;
};
template <size_t N> struct Tuple3<uint8_t, N> {
    uint8x8x3_t raw;
};
template <> struct Tuple3<int8_t, 16> {
    int8x16x3_t raw;
};
template <size_t N> struct Tuple3<int8_t, N> {
    int8x8x3_t raw;
};
template <> struct Tuple3<uint16_t, 8> {
    uint16x8x3_t raw;
};
template <size_t N> struct Tuple3<uint16_t, N> {
    uint16x4x3_t raw;
};
template <> struct Tuple3<int16_t, 8> {
    int16x8x3_t raw;
};
template <size_t N> struct Tuple3<int16_t, N> {
    int16x4x3_t raw;
};
template <> struct Tuple3<uint32_t, 4> {
    uint32x4x3_t raw;
};
template <size_t N> struct Tuple3<uint32_t, N> {
    uint32x2x3_t raw;
};
template <> struct Tuple3<int32_t, 4> {
    int32x4x3_t raw;
};
template <size_t N> struct Tuple3<int32_t, N> {
    int32x2x3_t raw;
};
template <> struct Tuple3<uint64_t, 2> {
    uint64x2x3_t raw;
};
template <size_t N> struct Tuple3<uint64_t, N> {
    uint64x1x3_t raw;
};
template <> struct Tuple3<int64_t, 2> {
    int64x2x3_t raw;
};
template <size_t N> struct Tuple3<int64_t, N> {
    int64x1x3_t raw;
};

template <> struct Tuple3<float32_t, 4> {
    float32x4x3_t raw;
};
template <size_t N> struct Tuple3<float32_t, N> {
    float32x2x3_t raw;
};
#if OMNI_HAVE_FLOAT64
template <> struct Tuple3<float64_t, 2> {
    float64x2x3_t raw;
};
template <size_t N> struct Tuple3<float64_t, N> {
    float64x1x3_t raw;
};
#endif // OMNI_HAVE_FLOAT64

template <> struct Tuple4<uint8_t, 16> {
    uint8x16x4_t raw;
};
template <size_t N> struct Tuple4<uint8_t, N> {
    uint8x8x4_t raw;
};
template <> struct Tuple4<int8_t, 16> {
    int8x16x4_t raw;
};
template <size_t N> struct Tuple4<int8_t, N> {
    int8x8x4_t raw;
};
template <> struct Tuple4<uint16_t, 8> {
    uint16x8x4_t raw;
};
template <size_t N> struct Tuple4<uint16_t, N> {
    uint16x4x4_t raw;
};
template <> struct Tuple4<int16_t, 8> {
    int16x8x4_t raw;
};
template <size_t N> struct Tuple4<int16_t, N> {
    int16x4x4_t raw;
};
template <> struct Tuple4<uint32_t, 4> {
    uint32x4x4_t raw;
};
template <size_t N> struct Tuple4<uint32_t, N> {
    uint32x2x4_t raw;
};
template <> struct Tuple4<int32_t, 4> {
    int32x4x4_t raw;
};
template <size_t N> struct Tuple4<int32_t, N> {
    int32x2x4_t raw;
};
template <> struct Tuple4<uint64_t, 2> {
    uint64x2x4_t raw;
};
template <size_t N> struct Tuple4<uint64_t, N> {
    uint64x1x4_t raw;
};
template <> struct Tuple4<int64_t, 2> {
    int64x2x4_t raw;
};
template <size_t N> struct Tuple4<int64_t, N> {
    int64x1x4_t raw;
};

template <> struct Tuple4<float32_t, 4> {
    float32x4x4_t raw;
};
template <size_t N> struct Tuple4<float32_t, N> {
    float32x2x4_t raw;
};
#if OMNI_HAVE_FLOAT64
template <> struct Tuple4<float64_t, 2> {
    float64x2x4_t raw;
};
template <size_t N> struct Tuple4<float64_t, N> {
    float64x1x4_t raw;
};
#endif // OMNI_HAVE_FLOAT64

template <typename T, size_t N> struct Raw128;

template <> struct Raw128<uint8_t, 16> {
    using type = uint8x16_t;
};
template <size_t N> struct Raw128<uint8_t, N> {
    using type = uint8x8_t;
};

template <> struct Raw128<uint16_t, 8> {
    using type = uint16x8_t;
};
template <size_t N> struct Raw128<uint16_t, N> {
    using type = uint16x4_t;
};

template <> struct Raw128<uint32_t, 4> {
    using type = uint32x4_t;
};
template <size_t N> struct Raw128<uint32_t, N> {
    using type = uint32x2_t;
};

template <> struct Raw128<uint64_t, 2> {
    using type = uint64x2_t;
};
template <> struct Raw128<uint64_t, 1> {
    using type = uint64x1_t;
};

template <> struct Raw128<int8_t, 16> {
    using type = int8x16_t;
};
template <size_t N> struct Raw128<int8_t, N> {
    using type = int8x8_t;
};

template <> struct Raw128<int16_t, 8> {
    using type = int16x8_t;
};
template <size_t N> struct Raw128<int16_t, N> {
    using type = int16x4_t;
};

template <> struct Raw128<int32_t, 4> {
    using type = int32x4_t;
};
template <size_t N> struct Raw128<int32_t, N> {
    using type = int32x2_t;
};

template <> struct Raw128<int64_t, 2> {
    using type = int64x2_t;
};
template <> struct Raw128<int64_t, 1> {
    using type = int64x1_t;
};

template <> struct Raw128<float, 4> {
    using type = float32x4_t;
};
template <size_t N> struct Raw128<float, N> {
    using type = float32x2_t;
};

#if OMNI_HAVE_FLOAT64
template <> struct Raw128<double, 2> {
    using type = float64x2_t;
};
template <> struct Raw128<double, 1> {
    using type = float64x1_t;
};
#endif // OMNI_HAVE_FLOAT64


template <size_t N> struct Tuple2<float16_t, N> : public Tuple2<uint16_t, N> {};
template <size_t N> struct Tuple3<float16_t, N> : public Tuple3<uint16_t, N> {};
template <size_t N> struct Tuple4<float16_t, N> : public Tuple4<uint16_t, N> {};
template <size_t N> struct Raw128<float16_t, N> : public Raw128<uint16_t, N> {};


template <size_t N> struct Tuple2<bfloat16_t, N> : public Tuple2<uint16_t, N> {};
template <size_t N> struct Tuple3<bfloat16_t, N> : public Tuple3<uint16_t, N> {};
template <size_t N> struct Tuple4<bfloat16_t, N> : public Tuple4<uint16_t, N> {};
template <size_t N> struct Raw128<bfloat16_t, N> : public Raw128<uint16_t, N> {};
} // namespace detail

template <typename T, size_t N = 16 / sizeof(T)> class Vec128 {
public:
    using Raw = typename detail::Raw128<T, N>::type;
    using PrivateT = T;                    // only for DFromV
    static constexpr size_t kPrivateN = N; // only for DFromV

    OMNI_INLINE Vec128() {}

    Vec128(const Vec128 &) = default;

    Vec128 &operator = (const Vec128 &) = default;

    OMNI_INLINE explicit Vec128(const Raw raw) : raw(raw) {}

    OMNI_INLINE Vec128 &operator *= (const Vec128 other)
    {
        return *this = (*this * other);
    }

    OMNI_INLINE Vec128 &operator /= (const Vec128 other)
    {
        return *this = (*this / other);
    }

    OMNI_INLINE Vec128 &operator += (const Vec128 other)
    {
        return *this = (*this + other);
    }

    OMNI_INLINE Vec128 &operator -= (const Vec128 other)
    {
        return *this = (*this - other);
    }

    OMNI_INLINE Vec128 &operator %= (const Vec128 other)
    {
        return *this = (*this % other);
    }

    OMNI_INLINE Vec128 &operator &= (const Vec128 other)
    {
        return *this = (*this & other);
    }

    OMNI_INLINE Vec128 &operator |= (const Vec128 other)
    {
        return *this = (*this | other);
    }

    OMNI_INLINE Vec128 &operator ^= (const Vec128 other)
    {
        return *this = (*this ^ other);
    }

    Raw raw;
};

template <typename T> using Vec64 = Vec128<T, 8 / sizeof(T)>;

template <typename T> using Vec32 = Vec128<T, 4 / sizeof(T)>;

template <typename T> using Vec16 = Vec128<T, 2 / sizeof(T)>;

// FF..FF or 0.
template <typename T, size_t N = 16 / sizeof(T)> class Mask128 {
    using Raw = typename detail::Raw128<MakeUnsigned<T>, N>::type;

public:
    using PrivateT = T;                    // only for DFromM
    static constexpr size_t kPrivateN = N; // only for DFromM

    OMNI_INLINE Mask128() {}

    Mask128(const Mask128 &) = default;

    Mask128 &operator = (const Mask128 &) = default;

    OMNI_INLINE explicit Mask128(const Raw raw) : raw(raw) {}

    Raw raw;
};

template <typename T> using Mask64 = Mask128<T, 8 / sizeof(T)>;

template <class V> using DFromV = Simd<typename V::PrivateT, V::kPrivateN, 0>;

template <class M> using DFromM = Simd<typename M::PrivateT, M::kPrivateN, 0>;

template <class V> using TFromV = typename V::PrivateT;

// ------------------------------ Set

namespace detail {
#define OMNI_NEON_BUILD_TPL_OMNI_SET
#define OMNI_NEON_BUILD_RET_OMNI_SET(type, size) Vec128<type##_t, size>
#define OMNI_NEON_BUILD_PARAM_OMNI_SET(type, size) Simd<type##_t, size, 0> /* tag */, type##_t t
#define OMNI_NEON_BUILD_ARG_OMNI_SET t

OMNI_NEON_DEF_FUNCTION_ALL_TYPES(NativeSet, vdup, _n_, OMNI_SET)

OMNI_NEON_DEF_FUNCTION_BFLOAT_16(NativeSet, vdup, _n_, OMNI_SET)

template <class D, OMNI_NEON_IF_EMULATED_D(D)> OMNI_API Vec128<TFromD<D>, MaxLanes(D())> NativeSet(D d, TFromD<D> t)
{
    const uint16_t tu = BitCastScalar<uint16_t>(t);
    return Vec128<TFromD<D>, d.MaxLanes()>(Set(RebindToUnsigned<D>(), tu).raw);
}

#undef OMNI_NEON_BUILD_TPL_OMNI_SET
#undef OMNI_NEON_BUILD_RET_OMNI_SET
#undef OMNI_NEON_BUILD_PARAM_OMNI_SET
#undef OMNI_NEON_BUILD_ARG_OMNI_SET
} // namespace detail

template <class D, OMNI_IF_V_SIZE_D(D, 16), typename T> OMNI_INLINE Vec128<TFromD<D>> Set(D /* tag */, T t)
{
    return detail::NativeSet(Full128<TFromD<D>>(), static_cast<TFromD<D>>(t));
}

// Partial vector: create 64-bit and return wrapper.
template <class D, OMNI_IF_V_SIZE_LE_D(D, 8), typename T>
OMNI_API Vec128<TFromD<D>, MaxLanes(D())> Set(D /* tag */, T t)
{
    const Full64<TFromD<D>> dfull;
    return Vec128<TFromD<D>, MaxLanes(D())>(detail::NativeSet(dfull, static_cast<TFromD<D>>(t)).raw);
}

template <class D> using VFromD = decltype(Set(D(), TFromD<D>()));

template <class D> OMNI_API VFromD<D> Zero(D d)
{
    // Default ctor also works for bfloat16_t and float16_t.
    return Set(d, TFromD<D>{});
}

OMNI_DIAGNOSTICS(push)
OMNI_DIAGNOSTICS_OFF(disable : 4700, ignored "-Wuninitialized")
#if OMNI_COMPILER_GCC_ACTUAL
OMNI_DIAGNOSTICS_OFF(disable : 4701, ignored "-Wmaybe-uninitialized")
#endif

template <class D> OMNI_API VFromD<D> Undefined(D /* tag */)
{
    VFromD<D> v;
    return v;
}

OMNI_DIAGNOSTICS(pop)


template <class D, OMNI_IF_UI8_D(D), OMNI_IF_V_SIZE_LE_D(D, 8)>
OMNI_API VFromD<D> Dup128VecFromValues(D d, TFromD<D> t0, TFromD<D> t1, TFromD<D> t2, TFromD<D> t3, TFromD<D> t4,
    TFromD<D> t5, TFromD<D> t6, TFromD<D> t7, TFromD<D>, TFromD<D>, TFromD<D>, TFromD<D>, TFromD<D>, TFromD<D>,
    TFromD<D>, TFromD<D>)
{
    typedef int8_t GccI8RawVectType __attribute__((__vector_size__(8)));
    (void)d;
    const GccI8RawVectType raw = { static_cast<int8_t>(t0), static_cast<int8_t>(t1), static_cast<int8_t>(t2),
        static_cast<int8_t>(t3), static_cast<int8_t>(t4), static_cast<int8_t>(t5),
        static_cast<int8_t>(t6), static_cast<int8_t>(t7) };
    return VFromD<D>(reinterpret_cast<typename VFromD<D>::Raw>(raw));
}

template <class D, OMNI_IF_UI16_D(D), OMNI_IF_V_SIZE_LE_D(D, 8)>
OMNI_API VFromD<D> Dup128VecFromValues(D d, TFromD<D> t0, TFromD<D> t1, TFromD<D> t2, TFromD<D> t3, TFromD<D>,
    TFromD<D>, TFromD<D>, TFromD<D>)
{
    typedef int16_t GccI16RawVectType __attribute__((__vector_size__(8)));
    (void)d;
    const GccI16RawVectType raw = { static_cast<int16_t>(t0), static_cast<int16_t>(t1), static_cast<int16_t>(t2),
        static_cast<int16_t>(t3) };
    return VFromD<D>(reinterpret_cast<typename VFromD<D>::Raw>(raw));
}

template <class D, OMNI_IF_UI32_D(D), OMNI_IF_V_SIZE_LE_D(D, 8)>
OMNI_API VFromD<D> Dup128VecFromValues(D d, TFromD<D> t0, TFromD<D> t1, TFromD<D> /* t2 */, TFromD<D> /* t3 */)
{
    typedef int32_t GccI32RawVectType __attribute__((__vector_size__(8)));
    (void)d;
    const GccI32RawVectType raw = { static_cast<int32_t>(t0), static_cast<int32_t>(t1) };
    return VFromD<D>(reinterpret_cast<typename VFromD<D>::Raw>(raw));
}

template <class D, OMNI_IF_F32_D(D), OMNI_IF_V_SIZE_LE_D(D, 8)>
OMNI_API VFromD<D> Dup128VecFromValues(D d, TFromD<D> t0, TFromD<D> t1, TFromD<D> /* t2 */, TFromD<D> /* t3 */)
{
    typedef float GccF32RawVectType __attribute__((__vector_size__(8)));
    (void)d;
    const GccF32RawVectType raw = { t0, t1 };
    return VFromD<D>(reinterpret_cast<typename VFromD<D>::Raw>(raw));
}

template <class D, OMNI_IF_T_SIZE_D(D, 8), OMNI_IF_V_SIZE_D(D, 8)>
OMNI_API VFromD<D> Dup128VecFromValues(D d, TFromD<D> t0, TFromD<D> /* t1 */)
{
    return Set(d, t0);
}

template <class D, OMNI_IF_UI8_D(D), OMNI_IF_V_SIZE_D(D, 16)>
OMNI_API VFromD<D> Dup128VecFromValues(D d, TFromD<D> t0, TFromD<D> t1, TFromD<D> t2, TFromD<D> t3, TFromD<D> t4,
    TFromD<D> t5, TFromD<D> t6, TFromD<D> t7, TFromD<D> t8, TFromD<D> t9, TFromD<D> t10, TFromD<D> t11, TFromD<D> t12,
    TFromD<D> t13, TFromD<D> t14, TFromD<D> t15)
{
    typedef int8_t GccI8RawVectType __attribute__((__vector_size__(16)));
    (void)d;
    const GccI8RawVectType raw = { static_cast<int8_t>(t0),  static_cast<int8_t>(t1),  static_cast<int8_t>(t2),
        static_cast<int8_t>(t3),  static_cast<int8_t>(t4),  static_cast<int8_t>(t5),
        static_cast<int8_t>(t6),  static_cast<int8_t>(t7),  static_cast<int8_t>(t8),
        static_cast<int8_t>(t9),  static_cast<int8_t>(t10), static_cast<int8_t>(t11),
        static_cast<int8_t>(t12), static_cast<int8_t>(t13), static_cast<int8_t>(t14),
        static_cast<int8_t>(t15) };
    return VFromD<D>(reinterpret_cast<typename VFromD<D>::Raw>(raw));
}

template <class D, OMNI_IF_UI16_D(D), OMNI_IF_V_SIZE_D(D, 16)>
OMNI_API VFromD<D> Dup128VecFromValues(D d, TFromD<D> t0, TFromD<D> t1, TFromD<D> t2, TFromD<D> t3, TFromD<D> t4,
    TFromD<D> t5, TFromD<D> t6, TFromD<D> t7)
{
    typedef int16_t GccI16RawVectType __attribute__((__vector_size__(16)));
    (void)d;
    const GccI16RawVectType raw = { static_cast<int16_t>(t0), static_cast<int16_t>(t1), static_cast<int16_t>(t2),
        static_cast<int16_t>(t3), static_cast<int16_t>(t4), static_cast<int16_t>(t5),
        static_cast<int16_t>(t6), static_cast<int16_t>(t7) };
    return VFromD<D>(reinterpret_cast<typename VFromD<D>::Raw>(raw));
}

template <class D, OMNI_IF_UI32_D(D), OMNI_IF_V_SIZE_D(D, 16)>
OMNI_API VFromD<D> Dup128VecFromValues(D d, TFromD<D> t0, TFromD<D> t1, TFromD<D> t2, TFromD<D> t3)
{
    typedef int32_t GccI32RawVectType __attribute__((__vector_size__(16)));
    (void)d;
    const GccI32RawVectType raw = { static_cast<int32_t>(t0), static_cast<int32_t>(t1), static_cast<int32_t>(t2),
        static_cast<int32_t>(t3) };
    return VFromD<D>(reinterpret_cast<typename VFromD<D>::Raw>(raw));
}

template <class D, OMNI_IF_F32_D(D), OMNI_IF_V_SIZE_D(D, 16)>
OMNI_API VFromD<D> Dup128VecFromValues(D d, TFromD<D> t0, TFromD<D> t1, TFromD<D> t2, TFromD<D> t3)
{
    typedef float GccF32RawVectType __attribute__((__vector_size__(16)));
    (void)d;
    const GccF32RawVectType raw = { t0, t1, t2, t3 };
    return VFromD<D>(reinterpret_cast<typename VFromD<D>::Raw>(raw));
}

template <class D, OMNI_IF_UI64_D(D), OMNI_IF_V_SIZE_D(D, 16)>
OMNI_API VFromD<D> Dup128VecFromValues(D d, TFromD<D> t0, TFromD<D> t1)
{
    typedef int64_t GccI64RawVectType __attribute__((__vector_size__(16)));
    (void)d;
    const GccI64RawVectType raw = { static_cast<int64_t>(t0), static_cast<int64_t>(t1) };
    return VFromD<D>(reinterpret_cast<typename VFromD<D>::Raw>(raw));
}

#if OMNI_HAVE_FLOAT64

template <class D, OMNI_IF_F64_D(D), OMNI_IF_V_SIZE_D(D, 16)>
OMNI_API VFromD<D> Dup128VecFromValues(D d, TFromD<D> t0, TFromD<D> t1)
{
    typedef double GccF64RawVectType __attribute__((__vector_size__(16)));
    (void)d;
    const GccF64RawVectType raw = { t0, t1 };
    return VFromD<D>(reinterpret_cast<typename VFromD<D>::Raw>(raw));
}

#endif

// Generic for all vector lengths
template <class D, OMNI_IF_BF16_D(D)>
OMNI_API VFromD<D> Dup128VecFromValues(D d, TFromD<D> t0, TFromD<D> t1, TFromD<D> t2, TFromD<D> t3, TFromD<D> t4,
    TFromD<D> t5, TFromD<D> t6, TFromD<D> t7)
{
    const RebindToSigned<decltype(d)> di;
    return BitCast(d, Dup128VecFromValues(di, BitCastScalar<int16_t>(t0), BitCastScalar<int16_t>(t1),
        BitCastScalar<int16_t>(t2), BitCastScalar<int16_t>(t3), BitCastScalar<int16_t>(t4), BitCastScalar<int16_t>(t5),
        BitCastScalar<int16_t>(t6), BitCastScalar<int16_t>(t7)));
}

// Generic for all vector lengths if MSVC or !OMNI_NEON_HAVE_F16C
template <class D, OMNI_IF_F16_D(D)>
OMNI_API VFromD<D> Dup128VecFromValues(D d, TFromD<D> t0, TFromD<D> t1, TFromD<D> t2, TFromD<D> t3, TFromD<D> t4,
    TFromD<D> t5, TFromD<D> t6, TFromD<D> t7)
{
    const RebindToSigned<decltype(d)> di;
    return BitCast(d, Dup128VecFromValues(di, BitCastScalar<int16_t>(t0), BitCastScalar<int16_t>(t1),
        BitCastScalar<int16_t>(t2), BitCastScalar<int16_t>(t3), BitCastScalar<int16_t>(t4), BitCastScalar<int16_t>(t5),
        BitCastScalar<int16_t>(t6), BitCastScalar<int16_t>(t7)));
}

namespace detail {
template <class D, OMNI_IF_T_SIZE_D(D, 1)> OMNI_INLINE VFromD<D> Iota0(D d)
{
    return Dup128VecFromValues(d, TFromD<D>{ 0 }, TFromD<D>{ 1 }, TFromD<D>{ 2 }, TFromD<D>{ 3 }, TFromD<D>{ 4 },
        TFromD<D>{ 5 }, TFromD<D>{ 6 }, TFromD<D>{ 7 }, TFromD<D>{ 8 }, TFromD<D>{ 9 }, TFromD<D>{ 10 },
        TFromD<D>{ 11 }, TFromD<D>{ 12 }, TFromD<D>{ 13 }, TFromD<D>{ 14 }, TFromD<D>{ 15 });
}

template <class D, OMNI_IF_UI16_D(D)> OMNI_INLINE VFromD<D> Iota0(D d)
{
    return Dup128VecFromValues(d, TFromD<D>{ 0 }, TFromD<D>{ 1 }, TFromD<D>{ 2 }, TFromD<D>{ 3 }, TFromD<D>{ 4 },
        TFromD<D>{ 5 }, TFromD<D>{ 6 }, TFromD<D>{ 7 });
}

template <class D, OMNI_IF_F16_D(D)> OMNI_INLINE VFromD<D> Iota0(D d)
{
    const RebindToUnsigned<decltype(d)> du;
    return BitCast(d, Dup128VecFromValues(du, uint16_t{ 0 }, uint16_t{ 0x3C00 }, uint16_t{ 0x4000 }, uint16_t{ 0x4200 },
        uint16_t{ 0x4400 }, uint16_t{ 0x4500 }, uint16_t{ 0x4600 }, uint16_t{ 0x4700 }));
}

template <class D, OMNI_IF_T_SIZE_D(D, 4)> OMNI_INLINE VFromD<D> Iota0(D d)
{
    return Dup128VecFromValues(d, TFromD<D>{ 0 }, TFromD<D>{ 1 }, TFromD<D>{ 2 }, TFromD<D>{ 3 });
}

template <class D, OMNI_IF_T_SIZE_D(D, 8)> OMNI_INLINE VFromD<D> Iota0(D d)
{
    return Dup128VecFromValues(d, TFromD<D>{ 0 }, TFromD<D>{ 1 });
}
} // namespace detail

template <class D, typename T2> OMNI_API VFromD<D> Iota(D d, const T2 first)
{
    const auto result_iota = detail::Iota0(d) + Set(d, static_cast<TFromD<D>>(first));
    return result_iota;
}

// ------------------------------ Combine

// Full result
template <class D, OMNI_IF_U8_D(D)> OMNI_API Vec128<uint8_t> Combine(D /* tag */, Vec64<uint8_t> hi, Vec64<uint8_t> lo)
{
    return Vec128<uint8_t>(vcombine_u8(lo.raw, hi.raw));
}

template <class D, OMNI_IF_U16_D(D)>
OMNI_API Vec128<uint16_t> Combine(D /* tag */, Vec64<uint16_t> hi, Vec64<uint16_t> lo)
{
    return Vec128<uint16_t>(vcombine_u16(lo.raw, hi.raw));
}

template <class D, OMNI_IF_U32_D(D)>
OMNI_API Vec128<uint32_t> Combine(D /* tag */, Vec64<uint32_t> hi, Vec64<uint32_t> lo)
{
    return Vec128<uint32_t>(vcombine_u32(lo.raw, hi.raw));
}

template <class D, OMNI_IF_U64_D(D)>
OMNI_API Vec128<uint64_t> Combine(D /* tag */, Vec64<uint64_t> hi, Vec64<uint64_t> lo)
{
    return Vec128<uint64_t>(vcombine_u64(lo.raw, hi.raw));
}

template <class D, OMNI_IF_I8_D(D)> OMNI_API Vec128<int8_t> Combine(D /* tag */, Vec64<int8_t> hi, Vec64<int8_t> lo)
{
    return Vec128<int8_t>(vcombine_s8(lo.raw, hi.raw));
}

template <class D, OMNI_IF_I16_D(D)> OMNI_API Vec128<int16_t> Combine(D /* tag */, Vec64<int16_t> hi, Vec64<int16_t> lo)
{
    return Vec128<int16_t>(vcombine_s16(lo.raw, hi.raw));
}

template <class D, OMNI_IF_I32_D(D)> OMNI_API Vec128<int32_t> Combine(D /* tag */, Vec64<int32_t> hi, Vec64<int32_t> lo)
{
    return Vec128<int32_t>(vcombine_s32(lo.raw, hi.raw));
}

template <class D, OMNI_IF_I64_D(D)> OMNI_API Vec128<int64_t> Combine(D /* tag */, Vec64<int64_t> hi, Vec64<int64_t> lo)
{
    return Vec128<int64_t>(vcombine_s64(lo.raw, hi.raw));
}

template <class D, class DH = Half<D>, OMNI_NEON_IF_EMULATED_D(D)>
OMNI_API VFromD<D> Combine(D d, VFromD<DH> hi, VFromD<DH> lo)
{
    const RebindToUnsigned<D> du;
    const Half<decltype(du)> duh;
    return BitCast(d, Combine(du, BitCast(duh, hi), BitCast(duh, lo)));
}

template <class D, OMNI_IF_F32_D(D)> OMNI_API Vec128<float> Combine(D /* tag */, Vec64<float> hi, Vec64<float> lo)
{
    return Vec128<float>(vcombine_f32(lo.raw, hi.raw));
}

#if OMNI_HAVE_FLOAT64

template <class D, OMNI_IF_F64_D(D)> OMNI_API Vec128<double> Combine(D /* tag */, Vec64<double> hi, Vec64<double> lo)
{
    return Vec128<double>(vcombine_f64(lo.raw, hi.raw));
}

#endif // OMNI_HAVE_FLOAT64

// ------------------------------ BitCast

namespace detail {
#define OMNI_NEON_BUILD_TPL_OMNI_CAST_TO_U8
#define OMNI_NEON_BUILD_RET_OMNI_CAST_TO_U8(type, size) Vec128<uint8_t, size * sizeof(type##_t)>
#define OMNI_NEON_BUILD_PARAM_OMNI_CAST_TO_U8(type, size) Vec128<type##_t, size> v
#define OMNI_NEON_BUILD_ARG_OMNI_CAST_TO_U8 v.raw

// Special case of u8 to u8 since vreinterpret*_u8_u8 is obviously not defined.
template <size_t N> OMNI_INLINE Vec128<uint8_t, N> BitCastToByte(Vec128<uint8_t, N> v)
{
    return v;
}

OMNI_NEON_DEF_FUNCTION_ALL_FLOATS(BitCastToByte, vreinterpret, _u8_, OMNI_CAST_TO_U8)

OMNI_NEON_DEF_FUNCTION_BFLOAT_16(BitCastToByte, vreinterpret, _u8_, OMNI_CAST_TO_U8)

OMNI_NEON_DEF_FUNCTION_INTS(BitCastToByte, vreinterpret, _u8_, OMNI_CAST_TO_U8)

OMNI_NEON_DEF_FUNCTION_UINT_16(BitCastToByte, vreinterpret, _u8_, OMNI_CAST_TO_U8)

OMNI_NEON_DEF_FUNCTION_UINT_32(BitCastToByte, vreinterpret, _u8_, OMNI_CAST_TO_U8)

OMNI_NEON_DEF_FUNCTION_UINT_64(BitCastToByte, vreinterpret, _u8_, OMNI_CAST_TO_U8)

template <size_t N> OMNI_INLINE Vec128<uint8_t, N * 2> BitCastToByte(Vec128<float16_t, N> v)
{
    return BitCastToByte(Vec128<uint16_t, N>(v.raw));
}

#if !OMNI_NEON_HAVE_BFLOAT16

template <size_t N> OMNI_INLINE Vec128<uint8_t, N * 2> BitCastToByte(Vec128<bfloat16_t, N> v)
{
    return BitCastToByte(Vec128<uint16_t, N>(v.raw));
}

#endif // !OMNI_NEON_HAVE_BFLOAT16

#undef OMNI_NEON_BUILD_TPL_OMNI_CAST_TO_U8
#undef OMNI_NEON_BUILD_RET_OMNI_CAST_TO_U8
#undef OMNI_NEON_BUILD_PARAM_OMNI_CAST_TO_U8
#undef OMNI_NEON_BUILD_ARG_OMNI_CAST_TO_U8

template <class D, OMNI_IF_U8_D(D)> OMNI_INLINE VFromD<D> BitCastFromByte(D /* tag */, VFromD<D> v)
{
    return v;
}

// 64-bit or less:

template <class D, OMNI_IF_V_SIZE_LE_D(D, 8), OMNI_IF_I8_D(D)>
OMNI_INLINE VFromD<D> BitCastFromByte(D /* tag */, VFromD<RebindToUnsigned<D>> v)
{
    return VFromD<D>(vreinterpret_s8_u8(v.raw));
}

template <class D, OMNI_IF_V_SIZE_LE_D(D, 8), OMNI_IF_U16_D(D)>
OMNI_INLINE VFromD<D> BitCastFromByte(D /* tag */, VFromD<Repartition<uint8_t, D>> v)
{
    return VFromD<D>(vreinterpret_u16_u8(v.raw));
}

template <class D, OMNI_IF_V_SIZE_LE_D(D, 8), OMNI_IF_I16_D(D)>
OMNI_INLINE VFromD<D> BitCastFromByte(D /* tag */, VFromD<Repartition<uint8_t, D>> v)
{
    return VFromD<D>(vreinterpret_s16_u8(v.raw));
}

template <class D, OMNI_IF_V_SIZE_LE_D(D, 8), OMNI_IF_U32_D(D)>
OMNI_INLINE VFromD<D> BitCastFromByte(D /* tag */, VFromD<Repartition<uint8_t, D>> v)
{
    return VFromD<D>(vreinterpret_u32_u8(v.raw));
}

template <class D, OMNI_IF_V_SIZE_LE_D(D, 8), OMNI_IF_I32_D(D)>
OMNI_INLINE VFromD<D> BitCastFromByte(D /* tag */, VFromD<Repartition<uint8_t, D>> v)
{
    return VFromD<D>(vreinterpret_s32_u8(v.raw));
}

template <class D, OMNI_IF_V_SIZE_LE_D(D, 8), OMNI_IF_U64_D(D)>
OMNI_INLINE Vec64<uint64_t> BitCastFromByte(D /* tag */, Vec64<uint8_t> v)
{
    return Vec64<uint64_t>(vreinterpret_u64_u8(v.raw));
}

template <class D, OMNI_IF_V_SIZE_LE_D(D, 8), OMNI_IF_I64_D(D)>
OMNI_INLINE Vec64<int64_t> BitCastFromByte(D /* tag */, Vec64<uint8_t> v)
{
    return Vec64<int64_t>(vreinterpret_s64_u8(v.raw));
}

// Cannot use OMNI_NEON_IF_EMULATED_D due to the extra OMNI_NEON_HAVE_F16C.
template <class D, OMNI_IF_V_SIZE_LE_D(D, 8), OMNI_IF_F16_D(D)>
OMNI_INLINE VFromD<D> BitCastFromByte(D, VFromD<Repartition<uint8_t, D>> v)
{
    const RebindToUnsigned<D> du;
    return VFromD<D>(BitCastFromByte(du, v).raw);
}

template <class D, OMNI_IF_V_SIZE_LE_D(D, 8), OMNI_IF_BF16_D(D)>
OMNI_INLINE VFromD<D> BitCastFromByte(D, VFromD<Repartition<uint8_t, D>> v)
{
    const RebindToUnsigned<D> du;
    return VFromD<D>(BitCastFromByte(du, v).raw);
}

template <class D, OMNI_IF_V_SIZE_LE_D(D, 8), OMNI_IF_F32_D(D)>
OMNI_INLINE VFromD<D> BitCastFromByte(D /* tag */, VFromD<Repartition<uint8_t, D>> v)
{
    return VFromD<D>(vreinterpret_f32_u8(v.raw));
}

#if OMNI_HAVE_FLOAT64

template <class D, OMNI_IF_V_SIZE_LE_D(D, 8), OMNI_IF_F64_D(D)>
OMNI_INLINE Vec64<double> BitCastFromByte(D /* tag */, Vec64<uint8_t> v)
{
    return Vec64<double>(vreinterpret_f64_u8(v.raw));
}

#endif // OMNI_HAVE_FLOAT64

// 128-bit full:

template <class D, OMNI_IF_I8_D(D)> OMNI_INLINE Vec128<int8_t> BitCastFromByte(D /* tag */, Vec128<uint8_t> v)
{
    return Vec128<int8_t>(vreinterpretq_s8_u8(v.raw));
}

template <class D, OMNI_IF_U16_D(D)> OMNI_INLINE Vec128<uint16_t> BitCastFromByte(D /* tag */, Vec128<uint8_t> v)
{
    return Vec128<uint16_t>(vreinterpretq_u16_u8(v.raw));
}

template <class D, OMNI_IF_I16_D(D)> OMNI_INLINE Vec128<int16_t> BitCastFromByte(D /* tag */, Vec128<uint8_t> v)
{
    return Vec128<int16_t>(vreinterpretq_s16_u8(v.raw));
}

template <class D, OMNI_IF_U32_D(D)> OMNI_INLINE Vec128<uint32_t> BitCastFromByte(D /* tag */, Vec128<uint8_t> v)
{
    return Vec128<uint32_t>(vreinterpretq_u32_u8(v.raw));
}

template <class D, OMNI_IF_I32_D(D)> OMNI_INLINE Vec128<int32_t> BitCastFromByte(D /* tag */, Vec128<uint8_t> v)
{
    return Vec128<int32_t>(vreinterpretq_s32_u8(v.raw));
}

template <class D, OMNI_IF_U64_D(D)> OMNI_INLINE Vec128<uint64_t> BitCastFromByte(D /* tag */, Vec128<uint8_t> v)
{
    return Vec128<uint64_t>(vreinterpretq_u64_u8(v.raw));
}

template <class D, OMNI_IF_I64_D(D)> OMNI_INLINE Vec128<int64_t> BitCastFromByte(D /* tag */, Vec128<uint8_t> v)
{
    return Vec128<int64_t>(vreinterpretq_s64_u8(v.raw));
}

template <class D, OMNI_IF_F32_D(D)> OMNI_INLINE Vec128<float> BitCastFromByte(D /* tag */, Vec128<uint8_t> v)
{
    return Vec128<float>(vreinterpretq_f32_u8(v.raw));
}

#if OMNI_HAVE_FLOAT64

template <class D, OMNI_IF_F64_D(D)> OMNI_INLINE Vec128<double> BitCastFromByte(D /* tag */, Vec128<uint8_t> v)
{
    return Vec128<double>(vreinterpretq_f64_u8(v.raw));
}

#endif // OMNI_HAVE_FLOAT64

// Cannot use OMNI_NEON_IF_EMULATED_D due to the extra OMNI_NEON_HAVE_F16C.
template <class D, OMNI_IF_F16_D(D)> OMNI_INLINE VFromD<D> BitCastFromByte(D, Vec128<uint8_t> v)
{
    return VFromD<D>(BitCastFromByte(RebindToUnsigned<D>(), v).raw);
}

template <class D, OMNI_IF_BF16_D(D)> OMNI_INLINE VFromD<D> BitCastFromByte(D, Vec128<uint8_t> v)
{
    return VFromD<D>(BitCastFromByte(RebindToUnsigned<D>(), v).raw);
}
} // namespace detail

template <class D, class FromT> OMNI_API VFromD<D> BitCast(D d, Vec128<FromT, Repartition<FromT, D>().MaxLanes()> v)
{
    return detail::BitCastFromByte(d, detail::BitCastToByte(v));
}

// ------------------------------ ResizeBitCast

// <= 8 byte vector to <= 8 byte vector
template <class D, class FromV, OMNI_IF_V_SIZE_LE_V(FromV, 8), OMNI_IF_V_SIZE_LE_D(D, 8)>
OMNI_API VFromD<D> ResizeBitCast(D d, FromV v)
{
    const Repartition<uint8_t, decltype(d)> du8;
    return BitCast(d, VFromD<decltype(du8)>{ detail::BitCastToByte(v).raw });
}

// 16-byte vector to 16-byte vector: same as BitCast
template <class D, class FromV, OMNI_IF_V_SIZE_V(FromV, 16), OMNI_IF_V_SIZE_D(D, 16)>
OMNI_API VFromD<D> ResizeBitCast(D d, FromV v)
{
    return BitCast(d, v);
}

// 16-byte vector to <= 8-byte vector
template <class D, class FromV, OMNI_IF_V_SIZE_V(FromV, 16), OMNI_IF_V_SIZE_LE_D(D, 8)>
OMNI_API VFromD<D> ResizeBitCast(D d, FromV v)
{
    const DFromV<decltype(v)> d_from;
    const Half<decltype(d_from)> dh_from;
    return ResizeBitCast(d, LowerHalf(dh_from, v));
}

// <= 8-bit vector to 16-byte vector
template <class D, class FromV, OMNI_IF_V_SIZE_LE_V(FromV, 8), OMNI_IF_V_SIZE_D(D, 16)>
OMNI_API VFromD<D> ResizeBitCast(D d, FromV v)
{
    const Full64<TFromV<FromV>> d_full64_from;
    const Full128<TFromV<FromV>> d_full128_from;
    return BitCast(d, Combine(d_full128_from, Zero(d_full64_from), ResizeBitCast(d_full64_from, v)));
}

// ------------------------------ GetLane

namespace detail {
#define OMNI_NEON_BUILD_TPL_OMNI_GET template <size_t kLane>
#define OMNI_NEON_BUILD_RET_OMNI_GET(type, size) type##_t
#define OMNI_NEON_BUILD_PARAM_OMNI_GET(type, size) Vec128<type##_t, size> v
#define OMNI_NEON_BUILD_ARG_OMNI_GET v.raw, kLane

OMNI_NEON_DEF_FUNCTION_ALL_TYPES(GetLane, vget, _lane_, OMNI_GET)

// OMNI_NEON_DEF_FUNCTION_BFLOAT_16(GetLane, vget, _lane_, OMNI_GET)

template <size_t kLane, class V, OMNI_NEON_IF_EMULATED_D(DFromV<V>)>
static OMNI_INLINE OMNI_MAYBE_UNUSED TFromV<V> GetLane(V v)
{
    const DFromV<decltype(v)> d;
    const RebindToUnsigned<decltype(d)> du;
    return BitCastScalar<TFromV<V>>(GetLane<kLane>(BitCast(du, v)));
}

#undef OMNI_NEON_BUILD_TPL_OMNI_GET
#undef OMNI_NEON_BUILD_RET_OMNI_GET
#undef OMNI_NEON_BUILD_PARAM_OMNI_GET
#undef OMNI_NEON_BUILD_ARG_OMNI_GET
} // namespace detail

template <class V> OMNI_API TFromV<V> GetLane(const V v)
{
    return detail::GetLane<0>(v);
}

// ------------------------------ ExtractLane

// Requires one overload per vector length because GetLane<3> is a compile error
// if v is a uint32x2_t.
template <typename T> OMNI_API T ExtractLane(const Vec128<T, 1> v, size_t i)
{
    (void)i;
    return detail::GetLane<0>(v);
}

template <typename T> OMNI_API T ExtractLane(const Vec128<T, 2> v, size_t i)
{
    alignas(16) T lanes[2];
    Store(v, DFromV<decltype(v)>(), lanes);
    return lanes[i];
}

template <typename T> OMNI_API T ExtractLane(const Vec128<T, 4> v, size_t i)
{
    alignas(16) T lanes[4];
    Store(v, DFromV<decltype(v)>(), lanes);
    return lanes[i];
}

template <typename T> OMNI_API T ExtractLane(const Vec128<T, 8> v, size_t i)
{
    alignas(16) T lanes[8];
    Store(v, DFromV<decltype(v)>(), lanes);
    return lanes[i];
}

template <typename T> OMNI_API T ExtractLane(const Vec128<T, 16> v, size_t i)
{
    alignas(16) T lanes[16];
    Store(v, DFromV<decltype(v)>(), lanes);
    return lanes[i];
}

// ------------------------------ InsertLane

namespace detail {
#define OMNI_NEON_BUILD_TPL_OMNI_INSERT template <size_t kLane>
#define OMNI_NEON_BUILD_RET_OMNI_INSERT(type, size) Vec128<type##_t, size>
#define OMNI_NEON_BUILD_PARAM_OMNI_INSERT(type, size) Vec128<type##_t, size> v, type##_t t
#define OMNI_NEON_BUILD_ARG_OMNI_INSERT t, v.raw, kLane

OMNI_NEON_DEF_FUNCTION_ALL_TYPES(InsertLane, vset, _lane_, OMNI_INSERT)

OMNI_NEON_DEF_FUNCTION_BFLOAT_16(InsertLane, vset, _lane_, OMNI_INSERT)

#undef OMNI_NEON_BUILD_TPL_OMNI_INSERT
#undef OMNI_NEON_BUILD_RET_OMNI_INSERT
#undef OMNI_NEON_BUILD_PARAM_OMNI_INSERT
#undef OMNI_NEON_BUILD_ARG_OMNI_INSERT

template <size_t kLane, class V, class D = DFromV<V>, OMNI_NEON_IF_EMULATED_D(D)>
OMNI_API V InsertLane(const V v, TFromD<D> t)
{
    const D d;
    const RebindToUnsigned<D> du;
    const uint16_t tu = BitCastScalar<uint16_t>(t);
    return BitCast(d, InsertLane<kLane>(BitCast(du, v), tu));
}
} // namespace detail

template <typename T> OMNI_API Vec128<T, 1> InsertLane(const Vec128<T, 1> v, size_t i, T t)
{
    (void)i;
    return Set(DFromV<decltype(v)>(), t);
}

template <typename T> OMNI_API Vec128<T, 2> InsertLane(const Vec128<T, 2> v, size_t i, T t)
{
    const DFromV<decltype(v)> d;
    alignas(16) T lanes[2];
    Store(v, d, lanes);
    lanes[i] = t;
    return Load(d, lanes);
}

template <typename T> OMNI_API Vec128<T, 4> InsertLane(const Vec128<T, 4> v, size_t i, T t)
{
    const DFromV<decltype(v)> d;
    alignas(16) T lanes[4];
    Store(v, d, lanes);
    lanes[i] = t;
    return Load(d, lanes);
}

template <typename T> OMNI_API Vec128<T, 8> InsertLane(const Vec128<T, 8> v, size_t i, T t)
{
    const DFromV<decltype(v)> d;
    alignas(16) T lanes[8];
    Store(v, d, lanes);
    lanes[i] = t;
    return Load(d, lanes);
}

template <typename T> OMNI_API Vec128<T, 16> InsertLane(const Vec128<T, 16> v, size_t i, T t)
{
    const DFromV<decltype(v)> d;
    alignas(16) T lanes[16];
    Store(v, d, lanes);
    lanes[i] = t;
    return Load(d, lanes);
}

// ================================================== ARITHMETIC

// ------------------------------ Addition
OMNI_NEON_DEF_FUNCTION_ALL_TYPES(operator +, vadd, _, 2)

// ------------------------------ Subtraction
OMNI_NEON_DEF_FUNCTION_ALL_TYPES(operator -, vsub, _, 2)

// ------------------------------ SumsOf8

OMNI_API Vec128<uint64_t> SumsOf8(const Vec128<uint8_t> v)
{
    return Vec128<uint64_t>(vpaddlq_u32(vpaddlq_u16(vpaddlq_u8(v.raw))));
}

OMNI_API Vec64<uint64_t> SumsOf8(const Vec64<uint8_t> v)
{
    return Vec64<uint64_t>(vpaddl_u32(vpaddl_u16(vpaddl_u8(v.raw))));
}

OMNI_API Vec128<int64_t> SumsOf8(const Vec128<int8_t> v)
{
    return Vec128<int64_t>(vpaddlq_s32(vpaddlq_s16(vpaddlq_s8(v.raw))));
}

OMNI_API Vec64<int64_t> SumsOf8(const Vec64<int8_t> v)
{
    return Vec64<int64_t>(vpaddl_s32(vpaddl_s16(vpaddl_s8(v.raw))));
}

// ------------------------------ SumsOf2
namespace detail {
template <class V, OMNI_IF_V_SIZE_LE_V(V, 8)>
OMNI_INLINE VFromD<RepartitionToWide<DFromV<V>>> SumsOf2(simd::SignedTag, simd::SizeTag<1> /* lane_size_tag */, V v)
{
    return VFromD<RepartitionToWide<DFromV<V>>>(vpaddl_s8(v.raw));
}

template <class V, OMNI_IF_V_SIZE_V(V, 16)>
OMNI_INLINE VFromD<RepartitionToWide<DFromV<V>>> SumsOf2(simd::SignedTag, simd::SizeTag<1> /* lane_size_tag */, V v)
{
    return VFromD<RepartitionToWide<DFromV<V>>>(vpaddlq_s8(v.raw));
}

template <class V, OMNI_IF_V_SIZE_LE_V(V, 8)>
OMNI_INLINE VFromD<RepartitionToWide<DFromV<V>>> SumsOf2(simd::UnsignedTag, simd::SizeTag<1> /* lane_size_tag */, V v)
{
    return VFromD<RepartitionToWide<DFromV<V>>>(vpaddl_u8(v.raw));
}

template <class V, OMNI_IF_V_SIZE_V(V, 16)>
OMNI_INLINE VFromD<RepartitionToWide<DFromV<V>>> SumsOf2(simd::UnsignedTag, simd::SizeTag<1> /* lane_size_tag */, V v)
{
    return VFromD<RepartitionToWide<DFromV<V>>>(vpaddlq_u8(v.raw));
}

template <class V, OMNI_IF_V_SIZE_LE_V(V, 8)>
OMNI_INLINE VFromD<RepartitionToWide<DFromV<V>>> SumsOf2(simd::SignedTag, simd::SizeTag<2> /* lane_size_tag */, V v)
{
    return VFromD<RepartitionToWide<DFromV<V>>>(vpaddl_s16(v.raw));
}

template <class V, OMNI_IF_V_SIZE_V(V, 16)>
OMNI_INLINE VFromD<RepartitionToWide<DFromV<V>>> SumsOf2(simd::SignedTag, simd::SizeTag<2> /* lane_size_tag */, V v)
{
    return VFromD<RepartitionToWide<DFromV<V>>>(vpaddlq_s16(v.raw));
}

template <class V, OMNI_IF_V_SIZE_LE_V(V, 8)>
OMNI_INLINE VFromD<RepartitionToWide<DFromV<V>>> SumsOf2(simd::UnsignedTag, simd::SizeTag<2> /* lane_size_tag */, V v)
{
    return VFromD<RepartitionToWide<DFromV<V>>>(vpaddl_u16(v.raw));
}

template <class V, OMNI_IF_V_SIZE_V(V, 16)>
OMNI_INLINE VFromD<RepartitionToWide<DFromV<V>>> SumsOf2(simd::UnsignedTag, simd::SizeTag<2> /* lane_size_tag */, V v)
{
    return VFromD<RepartitionToWide<DFromV<V>>>(vpaddlq_u16(v.raw));
}

template <class V, OMNI_IF_V_SIZE_LE_V(V, 8)>
OMNI_INLINE VFromD<RepartitionToWide<DFromV<V>>> SumsOf2(simd::SignedTag, simd::SizeTag<4> /* lane_size_tag */, V v)
{
    return VFromD<RepartitionToWide<DFromV<V>>>(vpaddl_s32(v.raw));
}

template <class V, OMNI_IF_V_SIZE_V(V, 16)>
OMNI_INLINE VFromD<RepartitionToWide<DFromV<V>>> SumsOf2(simd::SignedTag, simd::SizeTag<4> /* lane_size_tag */, V v)
{
    return VFromD<RepartitionToWide<DFromV<V>>>(vpaddlq_s32(v.raw));
}

template <class V, OMNI_IF_V_SIZE_LE_V(V, 8)>
OMNI_INLINE VFromD<RepartitionToWide<DFromV<V>>> SumsOf2(simd::UnsignedTag, simd::SizeTag<4> /* lane_size_tag */, V v)
{
    return VFromD<RepartitionToWide<DFromV<V>>>(vpaddl_u32(v.raw));
}

template <class V, OMNI_IF_V_SIZE_V(V, 16)>
OMNI_INLINE VFromD<RepartitionToWide<DFromV<V>>> SumsOf2(simd::UnsignedTag, simd::SizeTag<4> /* lane_size_tag */, V v)
{
    return VFromD<RepartitionToWide<DFromV<V>>>(vpaddlq_u32(v.raw));
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

// Returns a + b clamped to the destination range.
OMNI_NEON_DEF_FUNCTION_INTS_UINTS(SaturatedAdd, vqadd, _, 2)

// ------------------------------ SaturatedSub

// Returns a - b clamped to the destination range.
OMNI_NEON_DEF_FUNCTION_INTS_UINTS(SaturatedSub, vqsub, _, 2)

// ------------------------------ Average

// Returns (a + b + 1) / 2

#ifdef OMNI_NATIVE_AVERAGE_ROUND_UI32
#undef OMNI_NATIVE_AVERAGE_ROUND_UI32
#else
#define OMNI_NATIVE_AVERAGE_ROUND_UI32
#endif

OMNI_NEON_DEF_FUNCTION_UI_8_16_32(AverageRound, vrhadd, _, 2)

// ------------------------------ Neg

OMNI_NEON_DEF_FUNCTION_ALL_FLOATS(Neg, vneg, _, 1)

OMNI_NEON_DEF_FUNCTION_INT_8_16_32(Neg, vneg, _, 1) // i64 implemented below


// There is no vneg for bf16, but we can cast to f16 (emulated or native).
template <size_t N> OMNI_API Vec128<bfloat16_t, N> Neg(const Vec128<bfloat16_t, N> v)
{
    const DFromV<decltype(v)> d;
    const Rebind<float16_t, decltype(d)> df16;
    return BitCast(d, Neg(BitCast(df16, v)));
}

OMNI_API Vec64<int64_t> Neg(const Vec64<int64_t> v)
{
    return Vec64<int64_t>(vneg_s64(v.raw));
}

OMNI_API Vec128<int64_t> Neg(const Vec128<int64_t> v)
{
    return Vec128<int64_t>(vnegq_s64(v.raw));
}

// ------------------------------ SaturatedNeg
#ifdef OMNI_NATIVE_SATURATED_NEG_8_16_32
#undef OMNI_NATIVE_SATURATED_NEG_8_16_32
#else
#define OMNI_NATIVE_SATURATED_NEG_8_16_32
#endif

OMNI_NEON_DEF_FUNCTION_INT_8_16_32(SaturatedNeg, vqneg, _, 1)

#if OMNI_ARCH_ARM_A64
#ifdef OMNI_NATIVE_SATURATED_NEG_64
#undef OMNI_NATIVE_SATURATED_NEG_64
#else
#define OMNI_NATIVE_SATURATED_NEG_64
#endif

OMNI_API Vec64<int64_t> SaturatedNeg(const Vec64<int64_t> v)
{
    return Vec64<int64_t>(vqneg_s64(v.raw));
}

OMNI_API Vec128<int64_t> SaturatedNeg(const Vec128<int64_t> v)
{
    return Vec128<int64_t>(vqnegq_s64(v.raw));
}

#endif


// Customize OMNI_NEON_DEF_FUNCTION to special-case count=0 (not supported).
#pragma push_macro("OMNI_NEON_DEF_FUNCTION")
#undef OMNI_NEON_DEF_FUNCTION
#define OMNI_NEON_DEF_FUNCTION(type, size, name, prefix, infix, suffix, args)                                         \
    template <int kBits> OMNI_API Vec128<type##_t, size> name(const Vec128<type##_t, size> v)                         \
    {                                                                                                                 \
        return kBits == 0 ? v :                                                                                       \
                            Vec128<type##_t, size>(OMNI_NEON_EVAL(prefix##infix##suffix, v.raw, OMNI_MAX(1, kBits))); \
    }

OMNI_NEON_DEF_FUNCTION_INTS_UINTS(ShiftLeft, vshl, _n_, ignored)

OMNI_NEON_DEF_FUNCTION_UINTS(ShiftRight, vshr, _n_, ignored)

OMNI_NEON_DEF_FUNCTION_INTS(ShiftRight, vshr, _n_, ignored)

OMNI_NEON_DEF_FUNCTION_UINTS(RoundingShiftRight, vrshr, _n_, ignored)

OMNI_NEON_DEF_FUNCTION_INTS(RoundingShiftRight, vrshr, _n_, ignored)

#pragma pop_macro("OMNI_NEON_DEF_FUNCTION")

// ------------------------------ RotateRight (ShiftRight, Or)
template <int kBits, typename T, size_t N, OMNI_IF_NOT_FLOAT_NOR_SPECIAL(T)>
OMNI_API Vec128<T, N> RotateRight(const Vec128<T, N> v)
{
    const DFromV<decltype(v)> d;
    const RebindToUnsigned<decltype(d)> du;

    constexpr size_t kSizeInBits = sizeof(T) * 8;
    static_assert(0 <= kBits && kBits < kSizeInBits, "Invalid shift count");
    if (kBits == 0)
        return v;

    return Or(BitCast(d, ShiftRight<kBits>(BitCast(du, v))),
        ShiftLeft<OMNI_MIN(kSizeInBits - 1, kSizeInBits - kBits)>(v));
}

// NOTE: vxarq_u64 can be applied to uint64_t, but we do not yet have a
// mechanism for checking for extensions to Armv8.

// ------------------------------ Shl

OMNI_API Vec128<uint8_t> operator << (Vec128<uint8_t> v, Vec128<uint8_t> bits)
{
    return Vec128<uint8_t>(vshlq_u8(v.raw, vreinterpretq_s8_u8(bits.raw)));
}

template <size_t N, OMNI_IF_V_SIZE_LE(uint8_t, N, 8)>
OMNI_API Vec128<uint8_t, N> operator << (Vec128<uint8_t, N> v, Vec128<uint8_t, N> bits)
{
    return Vec128<uint8_t, N>(vshl_u8(v.raw, vreinterpret_s8_u8(bits.raw)));
}

OMNI_API Vec128<uint16_t> operator << (Vec128<uint16_t> v, Vec128<uint16_t> bits)
{
    return Vec128<uint16_t>(vshlq_u16(v.raw, vreinterpretq_s16_u16(bits.raw)));
}

template <size_t N, OMNI_IF_V_SIZE_LE(uint16_t, N, 8)>
OMNI_API Vec128<uint16_t, N> operator << (Vec128<uint16_t, N> v, Vec128<uint16_t, N> bits)
{
    return Vec128<uint16_t, N>(vshl_u16(v.raw, vreinterpret_s16_u16(bits.raw)));
}

OMNI_API Vec128<uint32_t> operator << (Vec128<uint32_t> v, Vec128<uint32_t> bits)
{
    return Vec128<uint32_t>(vshlq_u32(v.raw, vreinterpretq_s32_u32(bits.raw)));
}

template <size_t N, OMNI_IF_V_SIZE_LE(uint32_t, N, 8)>
OMNI_API Vec128<uint32_t, N> operator << (Vec128<uint32_t, N> v, Vec128<uint32_t, N> bits)
{
    return Vec128<uint32_t, N>(vshl_u32(v.raw, vreinterpret_s32_u32(bits.raw)));
}

OMNI_API Vec128<uint64_t> operator << (Vec128<uint64_t> v, Vec128<uint64_t> bits)
{
    return Vec128<uint64_t>(vshlq_u64(v.raw, vreinterpretq_s64_u64(bits.raw)));
}

OMNI_API Vec64<uint64_t> operator << (Vec64<uint64_t> v, Vec64<uint64_t> bits)
{
    return Vec64<uint64_t>(vshl_u64(v.raw, vreinterpret_s64_u64(bits.raw)));
}

OMNI_API Vec128<int8_t> operator << (Vec128<int8_t> v, Vec128<int8_t> bits)
{
    return Vec128<int8_t>(vshlq_s8(v.raw, bits.raw));
}

template <size_t N, OMNI_IF_V_SIZE_LE(int8_t, N, 8)>
OMNI_API Vec128<int8_t, N> operator << (Vec128<int8_t, N> v, Vec128<int8_t, N> bits)
{
    return Vec128<int8_t, N>(vshl_s8(v.raw, bits.raw));
}

OMNI_API Vec128<int16_t> operator << (Vec128<int16_t> v, Vec128<int16_t> bits)
{
    return Vec128<int16_t>(vshlq_s16(v.raw, bits.raw));
}

template <size_t N, OMNI_IF_V_SIZE_LE(int16_t, N, 8)>
OMNI_API Vec128<int16_t, N> operator << (Vec128<int16_t, N> v, Vec128<int16_t, N> bits)
{
    return Vec128<int16_t, N>(vshl_s16(v.raw, bits.raw));
}

OMNI_API Vec128<int32_t> operator << (Vec128<int32_t> v, Vec128<int32_t> bits)
{
    return Vec128<int32_t>(vshlq_s32(v.raw, bits.raw));
}

template <size_t N, OMNI_IF_V_SIZE_LE(int32_t, N, 8)>
OMNI_API Vec128<int32_t, N> operator << (Vec128<int32_t, N> v, Vec128<int32_t, N> bits)
{
    return Vec128<int32_t, N>(vshl_s32(v.raw, bits.raw));
}

OMNI_API Vec128<int64_t> operator << (Vec128<int64_t> v, Vec128<int64_t> bits)
{
    return Vec128<int64_t>(vshlq_s64(v.raw, bits.raw));
}

OMNI_API Vec64<int64_t> operator << (Vec64<int64_t> v, Vec64<int64_t> bits)
{
    return Vec64<int64_t>(vshl_s64(v.raw, bits.raw));
}

// ------------------------------ Shr (Neg)

OMNI_API Vec128<uint8_t> operator >> (Vec128<uint8_t> v, Vec128<uint8_t> bits)
{
    const RebindToSigned<DFromV<decltype(v)>> di;
    const int8x16_t neg_bits = Neg(BitCast(di, bits)).raw;
    return Vec128<uint8_t>(vshlq_u8(v.raw, neg_bits));
}

template <size_t N, OMNI_IF_V_SIZE_LE(uint8_t, N, 8)>
OMNI_API Vec128<uint8_t, N> operator >> (Vec128<uint8_t, N> v, Vec128<uint8_t, N> bits)
{
    const RebindToSigned<DFromV<decltype(v)>> di;
    const int8x8_t neg_bits = Neg(BitCast(di, bits)).raw;
    return Vec128<uint8_t, N>(vshl_u8(v.raw, neg_bits));
}

OMNI_API Vec128<uint16_t> operator >> (Vec128<uint16_t> v, Vec128<uint16_t> bits)
{
    const RebindToSigned<DFromV<decltype(v)>> di;
    const int16x8_t neg_bits = Neg(BitCast(di, bits)).raw;
    return Vec128<uint16_t>(vshlq_u16(v.raw, neg_bits));
}

template <size_t N, OMNI_IF_V_SIZE_LE(uint16_t, N, 8)>
OMNI_API Vec128<uint16_t, N> operator >> (Vec128<uint16_t, N> v, Vec128<uint16_t, N> bits)
{
    const RebindToSigned<DFromV<decltype(v)>> di;
    const int16x4_t neg_bits = Neg(BitCast(di, bits)).raw;
    return Vec128<uint16_t, N>(vshl_u16(v.raw, neg_bits));
}

OMNI_API Vec128<uint32_t> operator >> (Vec128<uint32_t> v, Vec128<uint32_t> bits)
{
    const RebindToSigned<DFromV<decltype(v)>> di;
    const int32x4_t neg_bits = Neg(BitCast(di, bits)).raw;
    return Vec128<uint32_t>(vshlq_u32(v.raw, neg_bits));
}

template <size_t N, OMNI_IF_V_SIZE_LE(uint32_t, N, 8)>
OMNI_API Vec128<uint32_t, N> operator >> (Vec128<uint32_t, N> v, Vec128<uint32_t, N> bits)
{
    const RebindToSigned<DFromV<decltype(v)>> di;
    const int32x2_t neg_bits = Neg(BitCast(di, bits)).raw;
    return Vec128<uint32_t, N>(vshl_u32(v.raw, neg_bits));
}

OMNI_API Vec128<uint64_t> operator >> (Vec128<uint64_t> v, Vec128<uint64_t> bits)
{
    const RebindToSigned<DFromV<decltype(v)>> di;
    const int64x2_t neg_bits = Neg(BitCast(di, bits)).raw;
    return Vec128<uint64_t>(vshlq_u64(v.raw, neg_bits));
}

OMNI_API Vec64<uint64_t> operator >> (Vec64<uint64_t> v, Vec64<uint64_t> bits)
{
    const RebindToSigned<DFromV<decltype(v)>> di;
    const int64x1_t neg_bits = Neg(BitCast(di, bits)).raw;
    return Vec64<uint64_t>(vshl_u64(v.raw, neg_bits));
}

OMNI_API Vec128<int8_t> operator >> (Vec128<int8_t> v, Vec128<int8_t> bits)
{
    return Vec128<int8_t>(vshlq_s8(v.raw, Neg(bits).raw));
}

template <size_t N, OMNI_IF_V_SIZE_LE(int8_t, N, 8)>
OMNI_API Vec128<int8_t, N> operator >> (Vec128<int8_t, N> v, Vec128<int8_t, N> bits)
{
    return Vec128<int8_t, N>(vshl_s8(v.raw, Neg(bits).raw));
}

OMNI_API Vec128<int16_t> operator >> (Vec128<int16_t> v, Vec128<int16_t> bits)
{
    return Vec128<int16_t>(vshlq_s16(v.raw, Neg(bits).raw));
}

template <size_t N, OMNI_IF_V_SIZE_LE(int16_t, N, 8)>
OMNI_API Vec128<int16_t, N> operator >> (Vec128<int16_t, N> v, Vec128<int16_t, N> bits)
{
    return Vec128<int16_t, N>(vshl_s16(v.raw, Neg(bits).raw));
}

OMNI_API Vec128<int32_t> operator >> (Vec128<int32_t> v, Vec128<int32_t> bits)
{
    return Vec128<int32_t>(vshlq_s32(v.raw, Neg(bits).raw));
}

template <size_t N, OMNI_IF_V_SIZE_LE(int32_t, N, 8)>
OMNI_API Vec128<int32_t, N> operator >> (Vec128<int32_t, N> v, Vec128<int32_t, N> bits)
{
    return Vec128<int32_t, N>(vshl_s32(v.raw, Neg(bits).raw));
}

OMNI_API Vec128<int64_t> operator >> (Vec128<int64_t> v, Vec128<int64_t> bits)
{
    return Vec128<int64_t>(vshlq_s64(v.raw, Neg(bits).raw));
}

OMNI_API Vec64<int64_t> operator >> (Vec64<int64_t> v, Vec64<int64_t> bits)
{
    return Vec64<int64_t>(vshl_s64(v.raw, Neg(bits).raw));
}

// ------------------------------ RoundingShr (Neg)

OMNI_API Vec128<uint8_t> RoundingShr(Vec128<uint8_t> v, Vec128<uint8_t> bits)
{
    const RebindToSigned<DFromV<decltype(v)>> di;
    const int8x16_t neg_bits = Neg(BitCast(di, bits)).raw;
    return Vec128<uint8_t>(vrshlq_u8(v.raw, neg_bits));
}

template <size_t N, OMNI_IF_V_SIZE_LE(uint8_t, N, 8)>
OMNI_API Vec128<uint8_t, N> RoundingShr(Vec128<uint8_t, N> v, Vec128<uint8_t, N> bits)
{
    const RebindToSigned<DFromV<decltype(v)>> di;
    const int8x8_t neg_bits = Neg(BitCast(di, bits)).raw;
    return Vec128<uint8_t, N>(vrshl_u8(v.raw, neg_bits));
}

OMNI_API Vec128<uint16_t> RoundingShr(Vec128<uint16_t> v, Vec128<uint16_t> bits)
{
    const RebindToSigned<DFromV<decltype(v)>> di;
    const int16x8_t neg_bits = Neg(BitCast(di, bits)).raw;
    return Vec128<uint16_t>(vrshlq_u16(v.raw, neg_bits));
}

template <size_t N, OMNI_IF_V_SIZE_LE(uint16_t, N, 8)>
OMNI_API Vec128<uint16_t, N> RoundingShr(Vec128<uint16_t, N> v, Vec128<uint16_t, N> bits)
{
    const RebindToSigned<DFromV<decltype(v)>> di;
    const int16x4_t neg_bits = Neg(BitCast(di, bits)).raw;
    return Vec128<uint16_t, N>(vrshl_u16(v.raw, neg_bits));
}

OMNI_API Vec128<uint32_t> RoundingShr(Vec128<uint32_t> v, Vec128<uint32_t> bits)
{
    const RebindToSigned<DFromV<decltype(v)>> di;
    const int32x4_t neg_bits = Neg(BitCast(di, bits)).raw;
    return Vec128<uint32_t>(vrshlq_u32(v.raw, neg_bits));
}

template <size_t N, OMNI_IF_V_SIZE_LE(uint32_t, N, 8)>
OMNI_API Vec128<uint32_t, N> RoundingShr(Vec128<uint32_t, N> v, Vec128<uint32_t, N> bits)
{
    const RebindToSigned<DFromV<decltype(v)>> di;
    const int32x2_t neg_bits = Neg(BitCast(di, bits)).raw;
    return Vec128<uint32_t, N>(vrshl_u32(v.raw, neg_bits));
}

OMNI_API Vec128<uint64_t> RoundingShr(Vec128<uint64_t> v, Vec128<uint64_t> bits)
{
    const RebindToSigned<DFromV<decltype(v)>> di;
    const int64x2_t neg_bits = Neg(BitCast(di, bits)).raw;
    return Vec128<uint64_t>(vrshlq_u64(v.raw, neg_bits));
}

OMNI_API Vec64<uint64_t> RoundingShr(Vec64<uint64_t> v, Vec64<uint64_t> bits)
{
    const RebindToSigned<DFromV<decltype(v)>> di;
    const int64x1_t neg_bits = Neg(BitCast(di, bits)).raw;
    return Vec64<uint64_t>(vrshl_u64(v.raw, neg_bits));
}

OMNI_API Vec128<int8_t> RoundingShr(Vec128<int8_t> v, Vec128<int8_t> bits)
{
    return Vec128<int8_t>(vrshlq_s8(v.raw, Neg(bits).raw));
}

template <size_t N, OMNI_IF_V_SIZE_LE(int8_t, N, 8)>
OMNI_API Vec128<int8_t, N> RoundingShr(Vec128<int8_t, N> v, Vec128<int8_t, N> bits)
{
    return Vec128<int8_t, N>(vrshl_s8(v.raw, Neg(bits).raw));
}

OMNI_API Vec128<int16_t> RoundingShr(Vec128<int16_t> v, Vec128<int16_t> bits)
{
    return Vec128<int16_t>(vrshlq_s16(v.raw, Neg(bits).raw));
}

template <size_t N, OMNI_IF_V_SIZE_LE(int16_t, N, 8)>
OMNI_API Vec128<int16_t, N> RoundingShr(Vec128<int16_t, N> v, Vec128<int16_t, N> bits)
{
    return Vec128<int16_t, N>(vrshl_s16(v.raw, Neg(bits).raw));
}

OMNI_API Vec128<int32_t> RoundingShr(Vec128<int32_t> v, Vec128<int32_t> bits)
{
    return Vec128<int32_t>(vrshlq_s32(v.raw, Neg(bits).raw));
}

template <size_t N, OMNI_IF_V_SIZE_LE(int32_t, N, 8)>
OMNI_API Vec128<int32_t, N> RoundingShr(Vec128<int32_t, N> v, Vec128<int32_t, N> bits)
{
    return Vec128<int32_t, N>(vrshl_s32(v.raw, Neg(bits).raw));
}

OMNI_API Vec128<int64_t> RoundingShr(Vec128<int64_t> v, Vec128<int64_t> bits)
{
    return Vec128<int64_t>(vrshlq_s64(v.raw, Neg(bits).raw));
}

OMNI_API Vec64<int64_t> RoundingShr(Vec64<int64_t> v, Vec64<int64_t> bits)
{
    return Vec64<int64_t>(vrshl_s64(v.raw, Neg(bits).raw));
}

// ------------------------------ ShiftLeftSame (Shl)

template <typename T, size_t N> OMNI_API Vec128<T, N> ShiftLeftSame(const Vec128<T, N> v, int bits)
{
    return v << Set(DFromV<decltype(v)>(), static_cast<T>(bits));
}

template <typename T, size_t N> OMNI_API Vec128<T, N> ShiftRightSame(const Vec128<T, N> v, int bits)
{
    return v >> Set(DFromV<decltype(v)>(), static_cast<T>(bits));
}

// ------------------------------ RoundingShiftRightSame (RoundingShr)

template <typename T, size_t N> OMNI_API Vec128<T, N> RoundingShiftRightSame(const Vec128<T, N> v, int bits)
{
    return RoundingShr(v, Set(DFromV<decltype(v)>(), static_cast<T>(bits)));
}

// ------------------------------ Int/float multiplication

OMNI_NEON_DEF_FUNCTION_UINT_8_16_32(operator*, vmul, _, 2)

OMNI_NEON_DEF_FUNCTION_INT_8_16_32(operator*, vmul, _, 2)

OMNI_NEON_DEF_FUNCTION_ALL_FLOATS(operator*, vmul, _, 2)

// Approximate reciprocal
OMNI_NEON_DEF_FUNCTION_ALL_FLOATS(ApproximateReciprocal, vrecpe, _, 1)

#ifdef OMNI_NATIVE_F64_APPROX_RECIP
#undef OMNI_NATIVE_F64_APPROX_RECIP
#else
#define OMNI_NATIVE_F64_APPROX_RECIP
#endif

OMNI_NEON_DEF_FUNCTION_ALL_FLOATS(operator /, vdiv, _, 2)


OMNI_NEON_DEF_FUNCTION_ALL_FLOATS(AbsDiff, vabd, _, 2)

OMNI_NEON_DEF_FUNCTION_UI_8_16_32(AbsDiff, vabd, _, 2) // no UI64

#ifdef OMNI_NATIVE_INTEGER_ABS_DIFF
#undef OMNI_NATIVE_INTEGER_ABS_DIFF
#else
#define OMNI_NATIVE_INTEGER_ABS_DIFF
#endif

// ------------------------------ Integer multiply-add

// Per-target flag to prevent generic_ops-inl.h from defining int MulAdd.
#ifdef OMNI_NATIVE_INT_FMA
#undef OMNI_NATIVE_INT_FMA
#else
#define OMNI_NATIVE_INT_FMA
#endif

// Wrappers for changing argument order to what intrinsics expect.
namespace detail {
// All except ui64
OMNI_NEON_DEF_FUNCTION_UINT_8_16_32(MulAdd, vmla, _, 3)

OMNI_NEON_DEF_FUNCTION_INT_8_16_32(MulAdd, vmla, _, 3)

OMNI_NEON_DEF_FUNCTION_UINT_8_16_32(NegMulAdd, vmls, _, 3)

OMNI_NEON_DEF_FUNCTION_INT_8_16_32(NegMulAdd, vmls, _, 3)
} // namespace detail

template <typename T, size_t N, OMNI_IF_NOT_FLOAT(T), OMNI_IF_NOT_T_SIZE(T, 8)>
OMNI_API Vec128<T, N> MulAdd(Vec128<T, N> mul, Vec128<T, N> x, Vec128<T, N> add)
{
    return detail::MulAdd(add, mul, x);
}

template <typename T, size_t N, OMNI_IF_NOT_FLOAT(T), OMNI_IF_NOT_T_SIZE(T, 8)>
OMNI_API Vec128<T, N> NegMulAdd(Vec128<T, N> mul, Vec128<T, N> x, Vec128<T, N> add)
{
    return detail::NegMulAdd(add, mul, x);
}

// 64-bit integer
template <typename T, size_t N, OMNI_IF_NOT_FLOAT(T), OMNI_IF_T_SIZE(T, 8)>
OMNI_API Vec128<T, N> MulAdd(Vec128<T, N> mul, Vec128<T, N> x, Vec128<T, N> add)
{
    return Add(Mul(mul, x), add);
}

template <typename T, size_t N, OMNI_IF_NOT_FLOAT(T), OMNI_IF_T_SIZE(T, 8)>
OMNI_API Vec128<T, N> NegMulAdd(Vec128<T, N> mul, Vec128<T, N> x, Vec128<T, N> add)
{
    return Sub(add, Mul(mul, x));
}

// ------------------------------ Floating-point multiply-add variants

namespace detail {
OMNI_NEON_DEF_FUNCTION_ALL_FLOATS(MulAdd, vfma, _, 3)

OMNI_NEON_DEF_FUNCTION_ALL_FLOATS(NegMulAdd, vfms, _, 3)
} // namespace detail

template <typename T, size_t N, OMNI_IF_FLOAT(T)>
OMNI_API Vec128<T, N> MulAdd(Vec128<T, N> mul, Vec128<T, N> x, Vec128<T, N> add)
{
    return detail::MulAdd(add, mul, x);
}

template <typename T, size_t N, OMNI_IF_FLOAT(T)>
OMNI_API Vec128<T, N> NegMulAdd(Vec128<T, N> mul, Vec128<T, N> x, Vec128<T, N> add)
{
    return detail::NegMulAdd(add, mul, x);
}

template <typename T, size_t N, OMNI_IF_FLOAT(T)>
OMNI_API Vec128<T, N> MulSub(Vec128<T, N> mul, Vec128<T, N> x, Vec128<T, N> sub)
{
    return MulAdd(mul, x, Neg(sub));
}

template <typename T, size_t N, OMNI_IF_FLOAT(T)>
OMNI_API Vec128<T, N> NegMulSub(Vec128<T, N> mul, Vec128<T, N> x, Vec128<T, N> sub)
{
    return Neg(MulAdd(mul, x, sub));
}

// Approximate reciprocal square root
OMNI_NEON_DEF_FUNCTION_ALL_FLOATS(ApproximateReciprocalSqrt, vrsqrte, _, 1)

#ifdef OMNI_NATIVE_F64_APPROX_RSQRT
#undef OMNI_NATIVE_F64_APPROX_RSQRT
#else
#define OMNI_NATIVE_F64_APPROX_RSQRT
#endif

OMNI_NEON_DEF_FUNCTION_ALL_FLOATS(Sqrt, vsqrt, _, 1)

template <typename T> OMNI_API Vec128<T> Not(const Vec128<T> v)
{
    const DFromV<decltype(v)> d;
    const Repartition<uint8_t, decltype(d)> d8;
    return BitCast(d, Vec128<uint8_t>(vmvnq_u8(BitCast(d8, v).raw)));
}

template <typename T, size_t N, OMNI_IF_V_SIZE_LE(T, N, 8)> OMNI_API Vec128<T, N> Not(const Vec128<T, N> v)
{
    const DFromV<decltype(v)> d;
    const Repartition<uint8_t, decltype(d)> d8;
    using V8 = decltype(Zero(d8));
    return BitCast(d, V8(vmvn_u8(BitCast(d8, v).raw)));
}

// ------------------------------ And
OMNI_NEON_DEF_FUNCTION_INTS_UINTS(And, vand, _, 2)

// Uses the u32/64 defined above.
template <typename T, size_t N, OMNI_IF_FLOAT(T)> OMNI_API Vec128<T, N> And(const Vec128<T, N> a, const Vec128<T, N> b)
{
    const DFromV<decltype(a)> d;
    const RebindToUnsigned<decltype(d)> du;
    return BitCast(d, BitCast(du, a) & BitCast(du, b));
}

// ------------------------------ AndNot

namespace detail {
// reversed_andnot returns a & ~b.
OMNI_NEON_DEF_FUNCTION_INTS_UINTS(reversed_andnot, vbic, _, 2)
} // namespace detail

// Returns ~not_mask & mask.
template <typename T, size_t N, OMNI_IF_NOT_FLOAT(T)>
OMNI_API Vec128<T, N> AndNot(const Vec128<T, N> not_mask, const Vec128<T, N> mask)
{
    return detail::reversed_andnot(mask, not_mask);
}

// Uses the u32/64 defined above.
template <typename T, size_t N, OMNI_IF_FLOAT(T)>
OMNI_API Vec128<T, N> AndNot(const Vec128<T, N> not_mask, const Vec128<T, N> mask)
{
    const DFromV<decltype(mask)> d;
    const RebindToUnsigned<decltype(d)> du;
    VFromD<decltype(du)> ret = detail::reversed_andnot(BitCast(du, mask), BitCast(du, not_mask));
    return BitCast(d, ret);
}

// ------------------------------ Or

OMNI_NEON_DEF_FUNCTION_INTS_UINTS(Or, vorr, _, 2)

// Uses the u32/64 defined above.
template <typename T, size_t N, OMNI_IF_FLOAT(T)> OMNI_API Vec128<T, N> Or(const Vec128<T, N> a, const Vec128<T, N> b)
{
    const DFromV<decltype(a)> d;
    const RebindToUnsigned<decltype(d)> du;
    return BitCast(d, BitCast(du, a) | BitCast(du, b));
}

// ------------------------------ Xor

OMNI_NEON_DEF_FUNCTION_INTS_UINTS(Xor, veor, _, 2)

// Uses the u32/64 defined above.
template <typename T, size_t N, OMNI_IF_FLOAT(T)> OMNI_API Vec128<T, N> Xor(const Vec128<T, N> a, const Vec128<T, N> b)
{
    const DFromV<decltype(a)> d;
    const RebindToUnsigned<decltype(d)> du;
    return BitCast(d, BitCast(du, a) ^ BitCast(du, b));
}


template <typename T, size_t N> OMNI_API Vec128<T, N> Xor3(Vec128<T, N> x1, Vec128<T, N> x2, Vec128<T, N> x3)
{
    return Xor(x1, Xor(x2, x3));
}

// ------------------------------ Or3
template <typename T, size_t N> OMNI_API Vec128<T, N> Or3(Vec128<T, N> o1, Vec128<T, N> o2, Vec128<T, N> o3)
{
    return Or(o1, Or(o2, o3));
}

// ------------------------------ OrAnd
template <typename T, size_t N> OMNI_API Vec128<T, N> OrAnd(Vec128<T, N> o, Vec128<T, N> a1, Vec128<T, N> a2)
{
    return Or(o, And(a1, a2));
}

// ------------------------------ IfVecThenElse
template <typename T, size_t N>
OMNI_API Vec128<T, N> IfVecThenElse(Vec128<T, N> mask, Vec128<T, N> yes, Vec128<T, N> no)
{
    return IfThenElse(MaskFromVec(mask), yes, no);
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

// ------------------------------ Operator overloads (internal-only if float)

template <typename T, size_t N> OMNI_API Vec128<T, N> operator&(const Vec128<T, N> a, const Vec128<T, N> b)
{
    return And(a, b);
}

template <typename T, size_t N> OMNI_API Vec128<T, N> operator | (const Vec128<T, N> a, const Vec128<T, N> b)
{
    return Or(a, b);
}

template <typename T, size_t N> OMNI_API Vec128<T, N> operator ^ (const Vec128<T, N> a, const Vec128<T, N> b)
{
    return Xor(a, b);
}

// ------------------------------ I64/U64 AbsDiff

template <size_t N> OMNI_API Vec128<int64_t, N> AbsDiff(const Vec128<int64_t, N> a, const Vec128<int64_t, N> b)
{
    return Max(a, b) - Min(a, b);
}

template <size_t N> OMNI_API Vec128<uint64_t, N> AbsDiff(const Vec128<uint64_t, N> a, const Vec128<uint64_t, N> b)
{
    return Or(SaturatedSub(a, b), SaturatedSub(b, a));
}

// ================================================== SIGN

// ------------------------------ Abs
// i64 is implemented after BroadcastSignBit.
OMNI_NEON_DEF_FUNCTION_INT_8_16_32(Abs, vabs, _, 1)

OMNI_NEON_DEF_FUNCTION_ALL_FLOATS(Abs, vabs, _, 1)

// ------------------------------ SaturatedAbs
#ifdef OMNI_NATIVE_SATURATED_ABS
#undef OMNI_NATIVE_SATURATED_ABS
#else
#define OMNI_NATIVE_SATURATED_ABS
#endif

OMNI_NEON_DEF_FUNCTION_INT_8_16_32(SaturatedAbs, vqabs, _, 1)

// ------------------------------ CopySign
template <typename T, size_t N> OMNI_API Vec128<T, N> CopySign(Vec128<T, N> magn, Vec128<T, N> sign)
{
    static_assert(IsFloat<T>(), "Only makes sense for floating-point");
    const DFromV<decltype(magn)> d;
    return BitwiseIfThenElse(SignBit(d), sign, magn);
}

// ------------------------------ BroadcastSignBit

template <typename T, size_t N, OMNI_IF_SIGNED(T)> OMNI_API Vec128<T, N> BroadcastSignBit(const Vec128<T, N> v)
{
    return ShiftRight<sizeof(T) * 8 - 1>(v);
}

template <typename T, size_t N> OMNI_API Mask128<T, N> MaskFromVec(const Vec128<T, N> v)
{
    const Simd<MakeUnsigned<T>, N, 0> du;
    return Mask128<T, N>(BitCast(du, v).raw);
}

template <class D> using MFromD = decltype(MaskFromVec(VFromD<D>()));

template <class D, class M> OMNI_API VFromD<D> VecFromMask(D d, const M m)
{
    // Raw type of masks is unsigned.
    const RebindToUnsigned<D> du;
    return BitCast(d, VFromD<decltype(du)>(m.raw));
}

// ------------------------------ RebindMask (MaskFromVec)

template <typename TFrom, size_t NFrom, class DTo>
OMNI_API MFromD<DTo> RebindMask(DTo /* tag */, Mask128<TFrom, NFrom> m)
{
    static_assert(sizeof(TFrom) == sizeof(TFromD<DTo>), "Must have same size");
    return MFromD<DTo>(m.raw);
}

// ------------------------------ IfThenElse

#define OMNI_NEON_BUILD_TPL_OMNI_IF
#define OMNI_NEON_BUILD_RET_OMNI_IF(type, size) Vec128<type##_t, size>
#define OMNI_NEON_BUILD_PARAM_OMNI_IF(type, size) \
    const Mask128<type##_t, size> mask, const Vec128<type##_t, size> yes, const Vec128<type##_t, size> no
#define OMNI_NEON_BUILD_ARG_OMNI_IF mask.raw, yes.raw, no.raw

OMNI_NEON_DEF_FUNCTION_ALL_TYPES(IfThenElse, vbsl, _, OMNI_IF)

#if OMNI_HAVE_FLOAT16
#define OMNI_NEON_IF_EMULATED_IF_THEN_ELSE(V) OMNI_IF_BF16(TFromV<V>)
#else
#define OMNI_NEON_IF_EMULATED_IF_THEN_ELSE(V) OMNI_IF_SPECIAL_FLOAT_V(V)
#endif

template <class V, OMNI_NEON_IF_EMULATED_IF_THEN_ELSE(V)> OMNI_API V IfThenElse(MFromD<DFromV<V>> mask, V yes, V no)
{
    const DFromV<decltype(yes)> d;
    const RebindToUnsigned<decltype(d)> du;
    return BitCast(d, IfThenElse(RebindMask(du, mask), BitCast(du, yes), BitCast(du, no)));
}

#undef OMNI_NEON_IF_EMULATED_IF_THEN_ELSE
#undef OMNI_NEON_BUILD_TPL_OMNI_IF
#undef OMNI_NEON_BUILD_RET_OMNI_IF
#undef OMNI_NEON_BUILD_PARAM_OMNI_IF
#undef OMNI_NEON_BUILD_ARG_OMNI_IF

// mask ? yes : 0
template <typename T, size_t N, OMNI_IF_NOT_SPECIAL_FLOAT(T)>
OMNI_API Vec128<T, N> IfThenElseZero(Mask128<T, N> mask, Vec128<T, N> yes)
{
    return yes & VecFromMask(DFromV<decltype(yes)>(), mask);
}

template <typename T, size_t N, OMNI_IF_SPECIAL_FLOAT(T)>
OMNI_API Vec128<T, N> IfThenElseZero(Mask128<T, N> mask, Vec128<T, N> yes)
{
    const DFromV<decltype(yes)> d;
    const RebindToUnsigned<decltype(d)> du;
    return BitCast(d, IfThenElseZero(RebindMask(du, mask), BitCast(du, yes)));
}

// mask ? 0 : no
template <typename T, size_t N, OMNI_IF_NOT_SPECIAL_FLOAT(T)>
OMNI_API Vec128<T, N> IfThenZeroElse(Mask128<T, N> mask, Vec128<T, N> no)
{
    return AndNot(VecFromMask(DFromV<decltype(no)>(), mask), no);
}

template <typename T, size_t N, OMNI_IF_SPECIAL_FLOAT(T)>
OMNI_API Vec128<T, N> IfThenZeroElse(Mask128<T, N> mask, Vec128<T, N> no)
{
    const DFromV<decltype(no)> d;
    const RebindToUnsigned<decltype(d)> du;
    return BitCast(d, IfThenZeroElse(RebindMask(du, mask), BitCast(du, no)));
}

template <typename T, size_t N>
OMNI_API Vec128<T, N> IfNegativeThenElse(Vec128<T, N> v, Vec128<T, N> yes, Vec128<T, N> no)
{
    static_assert(IsSigned<T>(), "Only works for signed/float");
    const DFromV<decltype(no)> d;
    const RebindToSigned<decltype(d)> di;

    Mask128<T, N> m = MaskFromVec(BitCast(d, BroadcastSignBit(BitCast(di, v))));
    return IfThenElse(m, yes, no);
}

// ------------------------------ Mask logical

template <typename T, size_t N> OMNI_API Mask128<T, N> Not(const Mask128<T, N> m)
{
    return MaskFromVec(Not(VecFromMask(DFromM<decltype(m)>(), m)));
}

template <typename T, size_t N> OMNI_API Mask128<T, N> And(const Mask128<T, N> a, Mask128<T, N> b)
{
    const DFromM<decltype(a)> d;
    return MaskFromVec(And(VecFromMask(d, a), VecFromMask(d, b)));
}

template <typename T, size_t N> OMNI_API Mask128<T, N> AndNot(const Mask128<T, N> a, Mask128<T, N> b)
{
    const DFromM<decltype(a)> d;
    return MaskFromVec(AndNot(VecFromMask(d, a), VecFromMask(d, b)));
}

template <typename T, size_t N> OMNI_API Mask128<T, N> Or(const Mask128<T, N> a, Mask128<T, N> b)
{
    const DFromM<decltype(a)> d;
    return MaskFromVec(Or(VecFromMask(d, a), VecFromMask(d, b)));
}

template <typename T, size_t N> OMNI_API Mask128<T, N> Xor(const Mask128<T, N> a, Mask128<T, N> b)
{
    const DFromM<decltype(a)> d;
    return MaskFromVec(Xor(VecFromMask(d, a), VecFromMask(d, b)));
}

OMNI_API Vec64<uint32_t> Shuffle2301(const Vec64<uint32_t> v)
{
    return Vec64<uint32_t>(vrev64_u32(v.raw));
}

OMNI_API Vec64<int32_t> Shuffle2301(const Vec64<int32_t> v)
{
    return Vec64<int32_t>(vrev64_s32(v.raw));
}

OMNI_API Vec64<float> Shuffle2301(const Vec64<float> v)
{
    return Vec64<float>(vrev64_f32(v.raw));
}

OMNI_API Vec128<uint32_t> Shuffle2301(const Vec128<uint32_t> v)
{
    return Vec128<uint32_t>(vrev64q_u32(v.raw));
}

OMNI_API Vec128<int32_t> Shuffle2301(const Vec128<int32_t> v)
{
    return Vec128<int32_t>(vrev64q_s32(v.raw));
}

OMNI_API Vec128<float> Shuffle2301(const Vec128<float> v)
{
    return Vec128<float>(vrev64q_f32(v.raw));
}

#define OMNI_NEON_BUILD_TPL_OMNI_COMPARE
#define OMNI_NEON_BUILD_RET_OMNI_COMPARE(type, size) Mask128<type##_t, size>
#define OMNI_NEON_BUILD_PARAM_OMNI_COMPARE(type, size) const Vec128<type##_t, size> a, const Vec128<type##_t, size> b
#define OMNI_NEON_BUILD_ARG_OMNI_COMPARE a.raw, b.raw

// ------------------------------ Equality
OMNI_API Vec128<uint64_t, 1> EqU64(const Vec128<int64_t, 1> a, const Vec128<int64_t, 1> b)
{
    return Vec128<uint64_t, 1>(vceq_s64(a.raw, b.raw));
}

OMNI_API Vec128<uint64_t, 1> EqU64(const Vec128<double, 1> a, const Vec128<double, 1> b)
{
    return Vec128<uint64_t, 1>(vceq_f64(a.raw, b.raw));
}

OMNI_NEON_DEF_FUNCTION_ALL_FLOATS(operator ==, vceq, _, OMNI_COMPARE)

OMNI_NEON_DEF_FUNCTION_INTS_UINTS(operator ==, vceq, _, OMNI_COMPARE)


OMNI_NEON_DEF_FUNCTION_INTS_UINTS(operator <, vclt, _, OMNI_COMPARE)


OMNI_NEON_DEF_FUNCTION_ALL_FLOATS(operator <, vclt, _, OMNI_COMPARE)

OMNI_NEON_DEF_FUNCTION_INTS_UINTS(operator <=, vcle, _, OMNI_COMPARE)


OMNI_NEON_DEF_FUNCTION_ALL_FLOATS(operator <=, vcle, _, OMNI_COMPARE)

#undef OMNI_NEON_BUILD_TPL_OMNI_COMPARE
#undef OMNI_NEON_BUILD_RET_OMNI_COMPARE
#undef OMNI_NEON_BUILD_PARAM_OMNI_COMPARE
#undef OMNI_NEON_BUILD_ARG_OMNI_COMPARE

#pragma push_macro("OMNI_NEON_DEF_FUNCTION")
#undef OMNI_NEON_DEF_FUNCTION
#define OMNI_NEON_DEF_FUNCTION(type, size, name, prefix, infix, suffix, args)                 \
    OMNI_API Mask128<type##_t, size> name(Vec128<type##_t, size> a, Vec128<type##_t, size> b) \
    {                                                                                         \
        return Not(a == b);                                                                   \
    }

OMNI_NEON_DEF_FUNCTION_ALL_TYPES(operator !=, ignored, ignored, ignored)

#pragma pop_macro("OMNI_NEON_DEF_FUNCTION")

// ------------------------------ Reversed comparisons

template <typename T, size_t N> OMNI_API Mask128<T, N> operator > (Vec128<T, N> a, Vec128<T, N> b)
{
    return operator < (b, a);
}

template <typename T, size_t N> OMNI_API Mask128<T, N> operator >= (Vec128<T, N> a, Vec128<T, N> b)
{
    return operator <= (b, a);
}

// ------------------------------ FirstN (Iota, Lt)

template <class D> OMNI_API MFromD<D> FirstN(D d, size_t num)
{
    const RebindToSigned<decltype(d)> di; // Signed comparisons are cheaper.
    using TI = TFromD<decltype(di)>;
    return RebindMask(d, detail::Iota0(di) < Set(di, static_cast<TI>(num)));
}

// ------------------------------ TestBit (Eq)

#define OMNI_NEON_BUILD_TPL_OMNI_TESTBIT
#define OMNI_NEON_BUILD_RET_OMNI_TESTBIT(type, size) Mask128<type##_t, size>
#define OMNI_NEON_BUILD_PARAM_OMNI_TESTBIT(type, size) Vec128<type##_t, size> v, Vec128<type##_t, size> bit
#define OMNI_NEON_BUILD_ARG_OMNI_TESTBIT v.raw, bit.raw

OMNI_NEON_DEF_FUNCTION_INTS_UINTS(TestBit, vtst, _, OMNI_TESTBIT)

#undef OMNI_NEON_BUILD_TPL_OMNI_TESTBIT
#undef OMNI_NEON_BUILD_RET_OMNI_TESTBIT
#undef OMNI_NEON_BUILD_PARAM_OMNI_TESTBIT
#undef OMNI_NEON_BUILD_ARG_OMNI_TESTBIT

// ------------------------------ Abs i64 (IfThenElse, BroadcastSignBit)
OMNI_API Vec128<int64_t> Abs(const Vec128<int64_t> v)
{
    return Vec128<int64_t>(vabsq_s64(v.raw));
}

OMNI_API Vec64<int64_t> Abs(const Vec64<int64_t> v)
{
    return Vec64<int64_t>(vabs_s64(v.raw));
}

OMNI_API Vec128<int64_t> SaturatedAbs(const Vec128<int64_t> v)
{
    return Vec128<int64_t>(vqabsq_s64(v.raw));
}

OMNI_API Vec64<int64_t> SaturatedAbs(const Vec64<int64_t> v)
{
    return Vec64<int64_t>(vqabs_s64(v.raw));
}

// ------------------------------ Min (IfThenElse, BroadcastSignBit)

// Unsigned
OMNI_NEON_DEF_FUNCTION_UINT_8_16_32(Min, vmin, _, 2)

template <size_t N> OMNI_API Vec128<uint64_t, N> Min(Vec128<uint64_t, N> a, Vec128<uint64_t, N> b)
{
    return IfThenElse(b < a, b, a);
}

// Signed
OMNI_NEON_DEF_FUNCTION_INT_8_16_32(Min, vmin, _, 2)

template <size_t N> OMNI_API Vec128<int64_t, N> Min(Vec128<int64_t, N> a, Vec128<int64_t, N> b)
{
    return IfThenElse(b < a, b, a);
}


OMNI_NEON_DEF_FUNCTION_FLOAT_16_32(Min, vminnm, _, 2)


OMNI_API Vec64<double> Min(Vec64<double> a, Vec64<double> b)
{
    return Vec64<double>(vminnm_f64(a.raw, b.raw));
}

OMNI_API Vec128<double> Min(Vec128<double> a, Vec128<double> b)
{
    return Vec128<double>(vminnmq_f64(a.raw, b.raw));
}

OMNI_NEON_DEF_FUNCTION_UINT_8_16_32(Max, vmax, _, 2)

template <size_t N> OMNI_API Vec128<uint64_t, N> Max(Vec128<uint64_t, N> a, Vec128<uint64_t, N> b)
{
    return IfThenElse(b < a, a, b);
}

// Signed (no i64)
OMNI_NEON_DEF_FUNCTION_INT_8_16_32(Max, vmax, _, 2)

template <size_t N> OMNI_API Vec128<int64_t, N> Max(Vec128<int64_t, N> a, Vec128<int64_t, N> b)
{
    return IfThenElse(b < a, a, b);
}


OMNI_NEON_DEF_FUNCTION_FLOAT_16_32(Max, vmaxnm, _, 2)


OMNI_API Vec64<double> Max(Vec64<double> a, Vec64<double> b)
{
    return Vec64<double>(vmaxnm_f64(a.raw, b.raw));
}

OMNI_API Vec128<double> Max(Vec128<double> a, Vec128<double> b)
{
    return Vec128<double>(vmaxnmq_f64(a.raw, b.raw));
}


// ================================================== MEMORY
// UGet
template <class D, OMNI_IF_V_SIZE_D(D, 8), OMNI_IF_I64_D(D)> OMNI_API TFromD<D> UGet(D /* tag */, Vec64<int64_t> v)
{
    return vget_lane_s64(v.raw, 0);
}

template <class D, OMNI_IF_V_SIZE_D(D, 8), OMNI_IF_U64_D(D)> OMNI_API TFromD<D> UGet(D /* tag */, Vec64<uint64_t> v)
{
    return vget_lane_u64(v.raw, 0);
}

template <int i = 0, class D, OMNI_IF_V_SIZE_D(D, 16), OMNI_IF_I64_D(D)>
OMNI_API TFromD<D> UGet(D /* tag */, Vec128<int64_t> v)
{
    return vgetq_lane_s64(v.raw, i);
}

template <int i = 0, class D, OMNI_IF_V_SIZE_D(D, 16), OMNI_IF_U64_D(D)>
OMNI_API TFromD<D> UGet(D /* tag */, Vec128<uint64_t> v)
{
    return vgetq_lane_u64(v.raw, i);
}

template <int i = 0, class D, OMNI_IF_V_SIZE_D(D, 16), OMNI_IF_F64_D(D)>
OMNI_API TFromD<D> UGet(D /* tag */, Vec128<double> v)
{
    return vgetq_lane_f64(v.raw, i);
}

template <int i = 0, class D, OMNI_IF_V_SIZE_D(D, 16), OMNI_IF_F64_D(D)>
OMNI_API void USet(D /* tag */, Vec128<double> &v, TFromD<D> value)
{
    v.raw = vsetq_lane_f64(value, v.raw, i);
}

template <int i = 0, class D, OMNI_IF_V_SIZE_D(D, 16), OMNI_IF_U64_D(D)>
OMNI_API void USet(D /* tag */, Vec128<uint64_t> &v, TFromD<D> value)
{
    v.raw = vsetq_lane_u64(value, v.raw, i);
}

template <int i = 0, class D, OMNI_IF_V_SIZE_D(D, 16), OMNI_IF_I64_D(D)>
OMNI_API void USet(D /* tag */, Vec128<int64_t> &v, TFromD<D> value)
{
    v.raw = vsetq_lane_s64(value, v.raw, i);
}

template <class D, OMNI_IF_V_SIZE_D(D, 16), OMNI_IF_U8_D(D)>
OMNI_API Vec128<uint8_t> LoadU(D /* tag */, const uint8_t *OMNI_RESTRICT unaligned)
{
    return Vec128<uint8_t>(vld1q_u8(unaligned));
}

template <class D, OMNI_IF_V_SIZE_D(D, 16), OMNI_IF_U16_D(D)>
OMNI_API Vec128<uint16_t> LoadU(D /* tag */, const uint16_t *OMNI_RESTRICT unaligned)
{
    return Vec128<uint16_t>(vld1q_u16(unaligned));
}

template <class D, OMNI_IF_V_SIZE_D(D, 16), OMNI_IF_U32_D(D)>
OMNI_API Vec128<uint32_t> LoadU(D /* tag */, const uint32_t *OMNI_RESTRICT unaligned)
{
    return Vec128<uint32_t>(vld1q_u32(unaligned));
}

template <class D, OMNI_IF_V_SIZE_D(D, 16), OMNI_IF_U64_D(D)>
OMNI_API Vec128<uint64_t> LoadU(D /* tag */, const uint64_t *OMNI_RESTRICT unaligned)
{
    return Vec128<uint64_t>(vld1q_u64(unaligned));
}

template <class D, OMNI_IF_V_SIZE_D(D, 16), OMNI_IF_I8_D(D)>
OMNI_API Vec128<int8_t> LoadU(D /* tag */, const int8_t *OMNI_RESTRICT unaligned)
{
    return Vec128<int8_t>(vld1q_s8(unaligned));
}

template <class D, OMNI_IF_V_SIZE_D(D, 16), OMNI_IF_I16_D(D)>
OMNI_API Vec128<int16_t> LoadU(D /* tag */, const int16_t *OMNI_RESTRICT unaligned)
{
    return Vec128<int16_t>(vld1q_s16(unaligned));
}

template <class D, OMNI_IF_V_SIZE_D(D, 16), OMNI_IF_I32_D(D)>
OMNI_API Vec128<int32_t> LoadU(D /* tag */, const int32_t *OMNI_RESTRICT unaligned)
{
    return Vec128<int32_t>(vld1q_s32(unaligned));
}

template <class D, OMNI_IF_V_SIZE_D(D, 16), OMNI_IF_I64_D(D)>
OMNI_API Vec128<int64_t> LoadU(D /* tag */, const int64_t *OMNI_RESTRICT unaligned)
{
    return Vec128<int64_t>(vld1q_s64(unaligned));
}

template <class D, OMNI_IF_V_SIZE_D(D, 16), OMNI_IF_F32_D(D)>
OMNI_API Vec128<float> LoadU(D /* tag */, const float *OMNI_RESTRICT unaligned)
{
    return Vec128<float>(vld1q_f32(unaligned));
}

#if OMNI_HAVE_FLOAT64

template <class D, OMNI_IF_V_SIZE_D(D, 16), OMNI_IF_F64_D(D)>
OMNI_API Vec128<double> LoadU(D /* tag */, const double *OMNI_RESTRICT unaligned)
{
    return Vec128<double>(vld1q_f64(unaligned));
}

#endif // OMNI_HAVE_FLOAT64

// ------------------------------ Load 64

template <class D, OMNI_IF_V_SIZE_D(D, 8), OMNI_IF_U8_D(D)>
OMNI_API Vec64<uint8_t> LoadU(D /* tag */, const uint8_t *OMNI_RESTRICT p)
{
    return Vec64<uint8_t>(vld1_u8(p));
}

template <class D, OMNI_IF_V_SIZE_D(D, 8), OMNI_IF_U16_D(D)>
OMNI_API Vec64<uint16_t> LoadU(D /* tag */, const uint16_t *OMNI_RESTRICT p)
{
    return Vec64<uint16_t>(vld1_u16(p));
}

template <class D, OMNI_IF_V_SIZE_D(D, 8), OMNI_IF_U32_D(D)>
OMNI_API Vec64<uint32_t> LoadU(D /* tag */, const uint32_t *OMNI_RESTRICT p)
{
    return Vec64<uint32_t>(vld1_u32(p));
}

template <class D, OMNI_IF_V_SIZE_D(D, 8), OMNI_IF_U64_D(D)>
OMNI_API Vec64<uint64_t> LoadU(D /* tag */, const uint64_t *OMNI_RESTRICT p)
{
    return Vec64<uint64_t>(vld1_u64(p));
}

template <class D, OMNI_IF_V_SIZE_D(D, 8), OMNI_IF_I8_D(D)>
OMNI_API Vec64<int8_t> LoadU(D /* tag */, const int8_t *OMNI_RESTRICT p)
{
    return Vec64<int8_t>(vld1_s8(p));
}

template <class D, OMNI_IF_V_SIZE_D(D, 8), OMNI_IF_I16_D(D)>
OMNI_API Vec64<int16_t> LoadU(D /* tag */, const int16_t *OMNI_RESTRICT p)
{
    return Vec64<int16_t>(vld1_s16(p));
}

template <class D, OMNI_IF_V_SIZE_D(D, 8), OMNI_IF_I32_D(D)>
OMNI_API Vec64<int32_t> LoadU(D /* tag */, const int32_t *OMNI_RESTRICT p)
{
    return Vec64<int32_t>(vld1_s32(p));
}

template <class D, OMNI_IF_V_SIZE_D(D, 8), OMNI_IF_I64_D(D)>
OMNI_API Vec64<int64_t> LoadU(D /* tag */, const int64_t *OMNI_RESTRICT p)
{
    return Vec64<int64_t>(vld1_s64(p));
}

template <class D, OMNI_IF_V_SIZE_D(D, 8), OMNI_IF_F32_D(D)>
OMNI_API Vec64<float> LoadU(D /* tag */, const float *OMNI_RESTRICT p)
{
    return Vec64<float>(vld1_f32(p));
}

#if OMNI_HAVE_FLOAT64

template <class D, OMNI_IF_V_SIZE_D(D, 8), OMNI_IF_F64_D(D)>
OMNI_API Vec64<double> LoadU(D /* tag */, const double *OMNI_RESTRICT p)
{
    return Vec64<double>(vld1_f64(p));
}

#endif // OMNI_HAVE_FLOAT64

template <class D, OMNI_IF_V_SIZE_D(D, 4), OMNI_IF_U32_D(D)>
OMNI_API Vec32<uint32_t> LoadU(D /* tag */, const uint32_t *OMNI_RESTRICT p)
{
    return Vec32<uint32_t>(vld1_dup_u32(p));
}

template <class D, OMNI_IF_V_SIZE_D(D, 4), OMNI_IF_I32_D(D)>
OMNI_API Vec32<int32_t> LoadU(D /* tag */, const int32_t *OMNI_RESTRICT p)
{
    return Vec32<int32_t>(vld1_dup_s32(p));
}

template <class D, OMNI_IF_V_SIZE_D(D, 4), OMNI_IF_F32_D(D)>
OMNI_API Vec32<float> LoadU(D /* tag */, const float *OMNI_RESTRICT p)
{
    return Vec32<float>(vld1_dup_f32(p));
}

// {u,i}{8,16}
template <class D, OMNI_IF_V_SIZE_D(D, 4), OMNI_IF_T_SIZE_LE_D(D, 2), OMNI_IF_NOT_SPECIAL_FLOAT_D(D)>
OMNI_API VFromD<D> LoadU(D d, const TFromD<D> *OMNI_RESTRICT p)
{
    const Repartition<uint32_t, decltype(d)> d32;
    uint32_t buf;
    CopyBytes<4>(p, &buf);
    return BitCast(d, LoadU(d32, &buf));
}

template <class D, OMNI_IF_LANES_D(D, 1), OMNI_IF_U16_D(D)>
OMNI_API VFromD<D> LoadU(D /* tag */, const uint16_t *OMNI_RESTRICT p)
{
    return VFromD<D>(vld1_dup_u16(p));
}

template <class D, OMNI_IF_LANES_D(D, 1), OMNI_IF_I16_D(D)>
OMNI_API VFromD<D> LoadU(D /* tag */, const int16_t *OMNI_RESTRICT p)
{
    return VFromD<D>(vld1_dup_s16(p));
}

template <class D, OMNI_IF_LANES_D(D, 2), OMNI_IF_T_SIZE_D(D, 1)>
OMNI_API VFromD<D> LoadU(D d, const TFromD<D> *OMNI_RESTRICT p)
{
    const Repartition<uint16_t, decltype(d)> d16;
    uint16_t buf;
    CopyBytes<2>(p, &buf);
    return BitCast(d, LoadU(d16, &buf));
}

// ------------------------------ Load 8
template <class D, OMNI_IF_LANES_D(D, 1), OMNI_IF_U8_D(D)>
OMNI_API VFromD<D> LoadU(D /* tag */, const uint8_t *OMNI_RESTRICT p)
{
    return VFromD<D>(vld1_dup_u8(p));
}

template <class D, OMNI_IF_LANES_D(D, 1), OMNI_IF_I8_D(D)>
OMNI_API VFromD<D> LoadU(D /* tag */, const int8_t *OMNI_RESTRICT p)
{
    return VFromD<D>(vld1_dup_s8(p));
}

// ------------------------------ Load misc

template <class D, OMNI_NEON_IF_EMULATED_D(D)> OMNI_API VFromD<D> LoadU(D d, const TFromD<D> *OMNI_RESTRICT p)
{
    const RebindToUnsigned<decltype(d)> du;
    return BitCast(d, LoadU(du, detail::U16LanePointer(p)));
}

// On Arm, Load is the same as LoadU.
template <class D> OMNI_API VFromD<D> Load(D d, const TFromD<D> *OMNI_RESTRICT p)
{
    return LoadU(d, p);
}

template <class D> OMNI_API VFromD<D> MaskedLoad(MFromD<D> m, D d, const TFromD<D> *OMNI_RESTRICT aligned)
{
    return IfThenElseZero(m, Load(d, aligned));
}

template <class D>
OMNI_API VFromD<D> MaskedLoadOr(VFromD<D> v, MFromD<D> m, D d, const TFromD<D> *OMNI_RESTRICT aligned)
{
    return IfThenElse(m, Load(d, aligned), v);
}

// 128-bit SIMD => nothing to duplicate, same as an unaligned load.
template <class D, OMNI_IF_V_SIZE_LE_D(D, 16)> OMNI_API VFromD<D> LoadDup128(D d, const TFromD<D> *OMNI_RESTRICT p)
{
    return LoadU(d, p);
}

// ------------------------------ Store 128

template <class D, OMNI_IF_V_SIZE_D(D, 16), OMNI_IF_U8_D(D)>
OMNI_API void StoreU(Vec128<uint8_t> v, D /* tag */, uint8_t *OMNI_RESTRICT unaligned)
{
    vst1q_u8(unaligned, v.raw);
}

template <class D, OMNI_IF_V_SIZE_D(D, 16), OMNI_IF_U16_D(D)>
OMNI_API void StoreU(Vec128<uint16_t> v, D /* tag */, uint16_t *OMNI_RESTRICT unaligned)
{
    vst1q_u16(unaligned, v.raw);
}

template <class D, OMNI_IF_V_SIZE_D(D, 16), OMNI_IF_U32_D(D)>
OMNI_API void StoreU(Vec128<uint32_t> v, D /* tag */, uint32_t *OMNI_RESTRICT unaligned)
{
    vst1q_u32(unaligned, v.raw);
}

template <class D, OMNI_IF_V_SIZE_D(D, 16), OMNI_IF_U64_D(D)>
OMNI_API void StoreU(Vec128<uint64_t> v, D /* tag */, uint64_t *OMNI_RESTRICT unaligned)
{
    vst1q_u64(unaligned, v.raw);
}

template <class D, OMNI_IF_V_SIZE_D(D, 16), OMNI_IF_I8_D(D)>
OMNI_API void StoreU(Vec128<int8_t> v, D /* tag */, int8_t *OMNI_RESTRICT unaligned)
{
    vst1q_s8(unaligned, v.raw);
}

template <class D, OMNI_IF_V_SIZE_D(D, 16), OMNI_IF_I16_D(D)>
OMNI_API void StoreU(Vec128<int16_t> v, D /* tag */, int16_t *OMNI_RESTRICT unaligned)
{
    vst1q_s16(unaligned, v.raw);
}

template <class D, OMNI_IF_V_SIZE_D(D, 16), OMNI_IF_I32_D(D)>
OMNI_API void StoreU(Vec128<int32_t> v, D /* tag */, int32_t *OMNI_RESTRICT unaligned)
{
    vst1q_s32(unaligned, v.raw);
}

template <class D, OMNI_IF_V_SIZE_D(D, 16), OMNI_IF_I64_D(D)>
OMNI_API void StoreU(Vec128<int64_t> v, D /* tag */, int64_t *OMNI_RESTRICT unaligned)
{
    vst1q_s64(unaligned, v.raw);
}

template <class D, OMNI_IF_V_SIZE_D(D, 16), OMNI_IF_F32_D(D)>
OMNI_API void StoreU(Vec128<float> v, D /* tag */, float *OMNI_RESTRICT unaligned)
{
    vst1q_f32(unaligned, v.raw);
}

#if OMNI_HAVE_FLOAT64

template <class D, OMNI_IF_V_SIZE_D(D, 16), OMNI_IF_F64_D(D)>
OMNI_API void StoreU(Vec128<double> v, D /* tag */, double *OMNI_RESTRICT unaligned)
{
    vst1q_f64(unaligned, v.raw);
}

#endif // OMNI_HAVE_FLOAT64

// ------------------------------ Store 64

template <class D, OMNI_IF_V_SIZE_D(D, 8), OMNI_IF_U8_D(D)>
OMNI_API void StoreU(Vec64<uint8_t> v, D /* tag */, uint8_t *OMNI_RESTRICT p)
{
    vst1_u8(p, v.raw);
}

template <class D, OMNI_IF_V_SIZE_D(D, 8), OMNI_IF_U16_D(D)>
OMNI_API void StoreU(Vec64<uint16_t> v, D /* tag */, uint16_t *OMNI_RESTRICT p)
{
    vst1_u16(p, v.raw);
}

template <class D, OMNI_IF_V_SIZE_D(D, 8), OMNI_IF_U32_D(D)>
OMNI_API void StoreU(Vec64<uint32_t> v, D /* tag */, uint32_t *OMNI_RESTRICT p)
{
    vst1_u32(p, v.raw);
}

template <class D, OMNI_IF_V_SIZE_D(D, 8), OMNI_IF_U64_D(D)>
OMNI_API void StoreU(Vec64<uint64_t> v, D /* tag */, uint64_t *OMNI_RESTRICT p)
{
    vst1_u64(p, v.raw);
}

template <class D, OMNI_IF_V_SIZE_D(D, 8), OMNI_IF_I8_D(D)>
OMNI_API void StoreU(Vec64<int8_t> v, D /* tag */, int8_t *OMNI_RESTRICT p)
{
    vst1_s8(p, v.raw);
}

template <class D, OMNI_IF_V_SIZE_D(D, 8), OMNI_IF_I16_D(D)>
OMNI_API void StoreU(Vec64<int16_t> v, D /* tag */, int16_t *OMNI_RESTRICT p)
{
    vst1_s16(p, v.raw);
}

template <class D, OMNI_IF_V_SIZE_D(D, 8), OMNI_IF_I32_D(D)>
OMNI_API void StoreU(Vec64<int32_t> v, D /* tag */, int32_t *OMNI_RESTRICT p)
{
    vst1_s32(p, v.raw);
}

template <class D, OMNI_IF_V_SIZE_D(D, 8), OMNI_IF_I64_D(D)>
OMNI_API void StoreU(Vec64<int64_t> v, D /* tag */, int64_t *OMNI_RESTRICT p)
{
    vst1_s64(p, v.raw);
}

template <class D, OMNI_IF_V_SIZE_D(D, 8), OMNI_IF_F32_D(D)>
OMNI_API void StoreU(Vec64<float> v, D /* tag */, float *OMNI_RESTRICT p)
{
    vst1_f32(p, v.raw);
}

#if OMNI_HAVE_FLOAT64

template <class D, OMNI_IF_V_SIZE_D(D, 8), OMNI_IF_F64_D(D)>
OMNI_API void StoreU(Vec64<double> v, D /* tag */, double *OMNI_RESTRICT p)
{
    vst1_f64(p, v.raw);
}

#endif // OMNI_HAVE_FLOAT64

// ------------------------------ Store 32

template <class D, OMNI_IF_V_SIZE_D(D, 4), OMNI_IF_U32_D(D)>
OMNI_API void StoreU(Vec32<uint32_t> v, D, uint32_t *OMNI_RESTRICT p)
{
    vst1_lane_u32(p, v.raw, 0);
}

template <class D, OMNI_IF_V_SIZE_D(D, 4), OMNI_IF_I32_D(D)>
OMNI_API void StoreU(Vec32<int32_t> v, D, int32_t *OMNI_RESTRICT p)
{
    vst1_lane_s32(p, v.raw, 0);
}

template <class D, OMNI_IF_V_SIZE_D(D, 4), OMNI_IF_F32_D(D)>
OMNI_API void StoreU(Vec32<float> v, D, float *OMNI_RESTRICT p)
{
    vst1_lane_f32(p, v.raw, 0);
}

// {u,i}{8,16}
template <class D, OMNI_IF_V_SIZE_D(D, 4), OMNI_IF_T_SIZE_LE_D(D, 2), OMNI_IF_NOT_SPECIAL_FLOAT_D(D)>
OMNI_API void StoreU(VFromD<D> v, D d, TFromD<D> *OMNI_RESTRICT p)
{
    Repartition<uint32_t, decltype(d)> d32;
    uint32_t buf = GetLane(BitCast(d32, v));
    CopyBytes<4>(&buf, p);
}


template <class D, OMNI_IF_V_SIZE_D(D, 2), OMNI_IF_U16_D(D)>
OMNI_API void StoreU(Vec16<uint16_t> v, D, uint16_t *OMNI_RESTRICT p)
{
    vst1_lane_u16(p, v.raw, 0);
}

template <class D, OMNI_IF_V_SIZE_D(D, 2), OMNI_IF_I16_D(D)>
OMNI_API void StoreU(Vec16<int16_t> v, D, int16_t *OMNI_RESTRICT p)
{
    vst1_lane_s16(p, v.raw, 0);
}

template <class D, OMNI_IF_V_SIZE_D(D, 2), OMNI_IF_T_SIZE_D(D, 1)>
OMNI_API void StoreU(VFromD<D> v, D d, TFromD<D> *OMNI_RESTRICT p)
{
    const Repartition<uint16_t, decltype(d)> d16;
    const uint16_t buf = GetLane(BitCast(d16, v));
    CopyBytes<2>(&buf, p);
}

// ------------------------------ Store 8

template <class D, OMNI_IF_V_SIZE_D(D, 1), OMNI_IF_U8_D(D)>
OMNI_API void StoreU(Vec128<uint8_t, 1> v, D, uint8_t *OMNI_RESTRICT p)
{
    vst1_lane_u8(p, v.raw, 0);
}

template <class D, OMNI_IF_V_SIZE_D(D, 1), OMNI_IF_I8_D(D)>
OMNI_API void StoreU(Vec128<int8_t, 1> v, D, int8_t *OMNI_RESTRICT p)
{
    vst1_lane_s8(p, v.raw, 0);
}

// ------------------------------ Store misc

template <class D, OMNI_NEON_IF_EMULATED_D(D)> OMNI_API void StoreU(VFromD<D> v, D d, TFromD<D> *OMNI_RESTRICT p)
{
    const RebindToUnsigned<decltype(d)> du;
    return StoreU(BitCast(du, v), du, detail::U16LanePointer(p));
}

OMNI_DIAGNOSTICS(push)
#if OMNI_COMPILER_GCC_ACTUAL
OMNI_DIAGNOSTICS_OFF(disable : 4701, ignored "-Wmaybe-uninitialized")
#endif

// On Arm, Store is the same as StoreU.
template <class D> OMNI_API void Store(VFromD<D> v, D d, TFromD<D> *OMNI_RESTRICT aligned)
{
    StoreU(v, d, aligned);
}

OMNI_DIAGNOSTICS(pop)

template <class D> OMNI_API void BlendedStore(VFromD<D> v, MFromD<D> m, D d, TFromD<D> *OMNI_RESTRICT p)
{
    // Treat as unsigned so that we correctly support float16.
    const RebindToUnsigned<decltype(d)> du;
    const auto blended = IfThenElse(RebindMask(du, m), BitCast(du, v), BitCast(du, LoadU(d, p)));
    StoreU(BitCast(d, blended), d, p);
}

template <class D> OMNI_API void Stream(const VFromD<D> v, D d, TFromD<D> *OMNI_RESTRICT aligned)
{
    __builtin_prefetch(aligned, 1, 0);
    Store(v, d, aligned);
}

template <class D, OMNI_IF_F32_D(D)> OMNI_API Vec128<float> ConvertTo(D /* tag */, Vec128<int32_t> v)
{
    return Vec128<float>(vcvtq_f32_s32(v.raw));
}

template <class D, OMNI_IF_V_SIZE_LE_D(D, 8), OMNI_IF_F32_D(D)>
OMNI_API VFromD<D> ConvertTo(D /* tag */, VFromD<RebindToSigned<D>> v)
{
    return VFromD<D>(vcvt_f32_s32(v.raw));
}

template <class D, OMNI_IF_F32_D(D)> OMNI_API Vec128<float> ConvertTo(D /* tag */, Vec128<uint32_t> v)
{
    return Vec128<float>(vcvtq_f32_u32(v.raw));
}

template <class D, OMNI_IF_V_SIZE_LE_D(D, 8), OMNI_IF_F32_D(D)>
OMNI_API VFromD<D> ConvertTo(D /* tag */, VFromD<RebindToUnsigned<D>> v)
{
    return VFromD<D>(vcvt_f32_u32(v.raw));
}

#if OMNI_HAVE_FLOAT64

template <class D, OMNI_IF_F64_D(D)> OMNI_API Vec128<double> ConvertTo(D /* tag */, Vec128<int64_t> v)
{
    return Vec128<double>(vcvtq_f64_s64(v.raw));
}

template <class D, OMNI_IF_F64_D(D)> OMNI_API Vec64<double> ConvertTo(D /* tag */, Vec64<int64_t> v)
{
    return Vec64<double>(vcvt_f64_s64(v.raw));
}

template <class D, OMNI_IF_F64_D(D)> OMNI_API Vec128<double> ConvertTo(D /* tag */, Vec128<uint64_t> v)
{
    return Vec128<double>(vcvtq_f64_u64(v.raw));
}

template <class D, OMNI_IF_F64_D(D)> OMNI_API Vec64<double> ConvertTo(D /* tag */, Vec64<uint64_t> v)
{
    return Vec64<double>(vcvt_f64_u64(v.raw));
}

#endif // OMNI_HAVE_FLOAT64

namespace detail {
// Truncates (rounds toward zero).
template <class D, OMNI_IF_V_SIZE_D(D, 16), OMNI_IF_I32_D(D)>
OMNI_INLINE Vec128<int32_t> ConvertFToI(D /* tag */, Vec128<float> v)
{
    return Vec128<int32_t>(vcvtq_s32_f32(v.raw));
}

template <class D, OMNI_IF_V_SIZE_LE_D(D, 8), OMNI_IF_I32_D(D)>
OMNI_INLINE VFromD<D> ConvertFToI(D /* tag */, VFromD<RebindToFloat<D>> v)
{
    return VFromD<D>(vcvt_s32_f32(v.raw));
}

template <class D, OMNI_IF_V_SIZE_D(D, 16), OMNI_IF_U32_D(D)>
OMNI_INLINE Vec128<uint32_t> ConvertFToU(D /* tag */, Vec128<float> v)
{
    return Vec128<uint32_t>(vcvtq_u32_f32(v.raw));
}

template <class D, OMNI_IF_V_SIZE_LE_D(D, 8), OMNI_IF_U32_D(D)>
OMNI_INLINE VFromD<D> ConvertFToU(D /* tag */, VFromD<RebindToFloat<D>> v)
{
    return VFromD<D>(vcvt_u32_f32(v.raw));
}

#if OMNI_HAVE_FLOAT64

// Truncates (rounds toward zero).
template <class D, OMNI_IF_V_SIZE_D(D, 16), OMNI_IF_I64_D(D)>
OMNI_INLINE Vec128<int64_t> ConvertFToI(D /* tag */, Vec128<double> v)
{
    return Vec128<int64_t>(vcvtq_s64_f64(v.raw));
}

template <class D, OMNI_IF_V_SIZE_D(D, 8), OMNI_IF_I64_D(D)>
OMNI_INLINE Vec64<int64_t> ConvertFToI(D /* tag */, Vec64<double> v)
{
    return Vec64<int64_t>(vcvt_s64_f64(v.raw));
}

template <class D, OMNI_IF_V_SIZE_D(D, 16), OMNI_IF_U64_D(D)>
OMNI_INLINE Vec128<uint64_t> ConvertFToU(D /* tag */, Vec128<double> v)
{
    return Vec128<uint64_t>(vcvtq_u64_f64(v.raw));
}

template <class D, OMNI_IF_V_SIZE_D(D, 8), OMNI_IF_U64_D(D)>
OMNI_INLINE Vec64<uint64_t> ConvertFToU(D /* tag */, Vec64<double> v)
{
    return Vec64<uint64_t>(vcvt_u64_f64(v.raw));
}

#endif // OMNI_HAVE_FLOAT64
} // namespace detail

template <class D, OMNI_IF_SIGNED_D(D),
    OMNI_IF_T_SIZE_ONE_OF_D(D,
    (1 << 4) | ((OMNI_ARCH_ARM_A64 && OMNI_HAVE_FLOAT16) ? (1 << 2) : 0) | (OMNI_HAVE_FLOAT64 ? (1 << 8) : 0))>
OMNI_API VFromD<D> ConvertTo(D di, VFromD<RebindToFloat<D>> v)
{
    return detail::ConvertFToI(di, v);
}

template <class D, OMNI_IF_UNSIGNED_D(D),
    OMNI_IF_T_SIZE_ONE_OF_D(D,
    (1 << 4) | ((OMNI_ARCH_ARM_A64 && OMNI_HAVE_FLOAT16) ? (1 << 2) : 0) | (OMNI_HAVE_FLOAT64 ? (1 << 8) : 0))>
OMNI_API VFromD<D> ConvertTo(D du, VFromD<RebindToFloat<D>> v)
{
    return detail::ConvertFToU(du, v);
}

// ------------------------------ PromoteTo (ConvertTo)

// Unsigned: zero-extend to full vector.
template <class D, OMNI_IF_I8_D(D)> OMNI_API Vec128<int8_t> PromoteTo(D /* tag */, Vec128<uint8_t> v)
{
    return Vec128<int8_t>(vreinterpretq_s8_u8(v.raw));
}

template <class D, OMNI_IF_U16_D(D)> OMNI_API Vec128<uint16_t> PromoteTo(D /* tag */, Vec64<uint8_t> v)
{
    return Vec128<uint16_t>(vmovl_u8(v.raw));
}

template <class D, OMNI_IF_U32_D(D)> OMNI_API Vec128<uint32_t> PromoteTo(D /* tag */, Vec32<uint8_t> v)
{
    uint16x8_t a = vmovl_u8(v.raw);
    return Vec128<uint32_t>(vmovl_u16(vget_low_u16(a)));
}

template <class D, OMNI_IF_U32_D(D)> OMNI_API Vec128<uint32_t> PromoteTo(D /* tag */, Vec64<uint16_t> v)
{
    return Vec128<uint32_t>(vmovl_u16(v.raw));
}

template <class D, OMNI_IF_U64_D(D)> OMNI_API Vec128<uint64_t> PromoteTo(D /* tag */, Vec64<uint32_t> v)
{
    return Vec128<uint64_t>(vmovl_u32(v.raw));
}

template <class D, OMNI_IF_I16_D(D)> OMNI_API Vec128<int16_t> PromoteTo(D d, Vec64<uint8_t> v)
{
    return BitCast(d, Vec128<uint16_t>(vmovl_u8(v.raw)));
}

template <class D, OMNI_IF_I32_D(D)> OMNI_API Vec128<int32_t> PromoteTo(D d, Vec32<uint8_t> v)
{
    uint16x8_t a = vmovl_u8(v.raw);
    return BitCast(d, Vec128<uint32_t>(vmovl_u16(vget_low_u16(a))));
}

template <class D, OMNI_IF_I32_D(D)> OMNI_API Vec128<int32_t> PromoteTo(D d, Vec64<uint16_t> v)
{
    return BitCast(d, Vec128<uint32_t>(vmovl_u16(v.raw)));
}

template <class D, OMNI_IF_I64_D(D)> OMNI_API Vec128<int64_t> PromoteTo(D d, Vec64<uint32_t> v)
{
    return BitCast(d, Vec128<uint64_t>(vmovl_u32(v.raw)));
}

// Unsigned: zero-extend to half vector.
template <class D, OMNI_IF_V_SIZE_LE_D(D, 8), OMNI_IF_U16_D(D)>
OMNI_API VFromD<D> PromoteTo(D /* tag */, VFromD<Rebind<uint8_t, D>> v)
{
    return VFromD<D>(vget_low_u16(vmovl_u8(v.raw)));
}

template <class D, OMNI_IF_V_SIZE_LE_D(D, 8), OMNI_IF_U32_D(D)>
OMNI_API VFromD<D> PromoteTo(D /* tag */, VFromD<Rebind<uint8_t, D>> v)
{
    return VFromD<D>(vget_low_u32(vmovl_u16(vget_low_u16(vmovl_u8(v.raw)))));
}

template <class D, OMNI_IF_V_SIZE_LE_D(D, 8), OMNI_IF_U32_D(D)>
OMNI_API VFromD<D> PromoteTo(D /* tag */, VFromD<Rebind<uint16_t, D>> v)
{
    return VFromD<D>(vget_low_u32(vmovl_u16(v.raw)));
}

template <class D, OMNI_IF_V_SIZE_LE_D(D, 8), OMNI_IF_U64_D(D)>
OMNI_API VFromD<D> PromoteTo(D /* tag */, VFromD<Rebind<uint32_t, D>> v)
{
    return VFromD<D>(vget_low_u64(vmovl_u32(v.raw)));
}

template <class D, OMNI_IF_V_SIZE_LE_D(D, 8), OMNI_IF_I16_D(D)>
OMNI_API VFromD<D> PromoteTo(D d, VFromD<Rebind<uint8_t, D>> v)
{
    using VU16 = VFromD<RebindToUnsigned<D>>;
    return BitCast(d, VU16(vget_low_u16(vmovl_u8(v.raw))));
}

template <class D, OMNI_IF_V_SIZE_LE_D(D, 8), OMNI_IF_I32_D(D)>
OMNI_API VFromD<D> PromoteTo(D /* tag */, VFromD<Rebind<uint8_t, D>> v)
{
    const uint32x4_t u32 = vmovl_u16(vget_low_u16(vmovl_u8(v.raw)));
    return VFromD<D>(vget_low_s32(vreinterpretq_s32_u32(u32)));
}

template <class D, OMNI_IF_V_SIZE_LE_D(D, 8), OMNI_IF_I32_D(D)>
OMNI_API VFromD<D> PromoteTo(D /* tag */, VFromD<Rebind<uint16_t, D>> v)
{
    return VFromD<D>(vget_low_s32(vreinterpretq_s32_u32(vmovl_u16(v.raw))));
}

template <class D, OMNI_IF_V_SIZE_LE_D(D, 8), OMNI_IF_I64_D(D)>
OMNI_API VFromD<D> PromoteTo(D d, VFromD<Rebind<uint32_t, D>> v)
{
    using DU = RebindToUnsigned<D>;
    return BitCast(d, VFromD<DU>(vget_low_u64(vmovl_u32(v.raw))));
}

// U8/U16 to U64/I64: First, zero-extend to U32, and then zero-extend to
// TFromD<D>
template <class D, class V, OMNI_IF_UI64_D(D), OMNI_IF_LANES_D(D, OMNI_MAX_LANES_V(V)), OMNI_IF_UNSIGNED_V(V),
    OMNI_IF_T_SIZE_ONE_OF_V(V, (1 << 1) | (1 << 2))>
OMNI_API VFromD<D> PromoteTo(D d, V v)
{
    const Rebind<uint32_t, decltype(d)> du32;
    return PromoteTo(d, PromoteTo(du32, v));
}

// Signed: replicate sign bit to full vector.
template <class D, OMNI_IF_I16_D(D)> OMNI_API Vec128<int16_t> PromoteTo(D /* tag */, Vec64<int8_t> v)
{
    return Vec128<int16_t>(vmovl_s8(v.raw));
}

template <class D, OMNI_IF_I32_D(D)> OMNI_API Vec128<int32_t> PromoteTo(D /* tag */, Vec32<int8_t> v)
{
    int16x8_t a = vmovl_s8(v.raw);
    return Vec128<int32_t>(vmovl_s16(vget_low_s16(a)));
}

template <class D, OMNI_IF_I32_D(D)> OMNI_API Vec128<int32_t> PromoteTo(D /* tag */, Vec64<int16_t> v)
{
    return Vec128<int32_t>(vmovl_s16(v.raw));
}

template <class D, OMNI_IF_I64_D(D)> OMNI_API Vec128<int64_t> PromoteTo(D /* tag */, Vec64<int32_t> v)
{
    return Vec128<int64_t>(vmovl_s32(v.raw));
}

// Signed: replicate sign bit to half vector.
template <class D, OMNI_IF_V_SIZE_LE_D(D, 8), OMNI_IF_I16_D(D)>
OMNI_API VFromD<D> PromoteTo(D /* tag */, VFromD<Rebind<int8_t, D>> v)
{
    return VFromD<D>(vget_low_s16(vmovl_s8(v.raw)));
}

template <class D, OMNI_IF_V_SIZE_LE_D(D, 8), OMNI_IF_I32_D(D)>
OMNI_API VFromD<D> PromoteTo(D /* tag */, VFromD<Rebind<int8_t, D>> v)
{
    return VFromD<D>(vget_low_s32(vmovl_s16(vget_low_s16(vmovl_s8(v.raw)))));
}

template <class D, OMNI_IF_V_SIZE_LE_D(D, 8), OMNI_IF_I32_D(D)>
OMNI_API VFromD<D> PromoteTo(D /* tag */, VFromD<Rebind<int16_t, D>> v)
{
    return VFromD<D>(vget_low_s32(vmovl_s16(v.raw)));
}

template <class D, OMNI_IF_V_SIZE_LE_D(D, 8), OMNI_IF_I64_D(D)>
OMNI_API VFromD<D> PromoteTo(D /* tag */, VFromD<Rebind<int32_t, D>> v)
{
    return VFromD<D>(vget_low_s64(vmovl_s32(v.raw)));
}

// I8/I16 to I64: First, promote to I32, and then promote to I64
template <class D, class V, OMNI_IF_I64_D(D), OMNI_IF_LANES_D(D, OMNI_MAX_LANES_V(V)), OMNI_IF_SIGNED_V(V),
    OMNI_IF_T_SIZE_ONE_OF_V(V, (1 << 1) | (1 << 2))>
OMNI_API VFromD<D> PromoteTo(D d, V v)
{
    const Rebind<int32_t, decltype(d)> di32;
    return PromoteTo(d, PromoteTo(di32, v));
}

template <class D, OMNI_IF_F64_D(D)> OMNI_API Vec128<double> PromoteTo(D /* tag */, Vec64<float> v)
{
    return Vec128<double>(vcvt_f64_f32(v.raw));
}

template <class D, OMNI_IF_F64_D(D)> OMNI_API Vec64<double> PromoteTo(D /* tag */, Vec32<float> v)
{
    return Vec64<double>(vget_low_f64(vcvt_f64_f32(v.raw)));
}

template <class D, OMNI_IF_F64_D(D)> OMNI_API Vec128<double> PromoteTo(D /* tag */, Vec64<int32_t> v)
{
    const int64x2_t i64 = vmovl_s32(v.raw);
    return Vec128<double>(vcvtq_f64_s64(i64));
}

template <class D, OMNI_IF_F64_D(D)> OMNI_API Vec64<double> PromoteTo(D d, Vec32<int32_t> v)
{
    return ConvertTo(d, Vec64<int64_t>(vget_low_s64(vmovl_s32(v.raw))));
}

template <class D, OMNI_IF_F64_D(D)> OMNI_API Vec128<double> PromoteTo(D /* tag */, Vec64<uint32_t> v)
{
    const uint64x2_t u64 = vmovl_u32(v.raw);
    return Vec128<double>(vcvtq_f64_u64(u64));
}

template <class D, OMNI_IF_F64_D(D)> OMNI_API Vec64<double> PromoteTo(D d, Vec32<uint32_t> v)
{
    return ConvertTo(d, Vec64<uint64_t>(vget_low_u64(vmovl_u32(v.raw))));
}

template <class D, OMNI_IF_UI64_D(D)> OMNI_API VFromD<D> PromoteTo(D d64, VFromD<Rebind<float, D>> v)
{
    const RebindToFloat<decltype(d64)> df64;
    return ConvertTo(d64, PromoteTo(df64, v));
}

// ------------------------------ PromoteEvenTo/PromoteOddTo

#include "inside-inl.h"

// ------------------------------ PromoteUpperTo

#if OMNI_ARCH_ARM_A64

// Per-target flag to prevent generic_ops-inl.h from defining PromoteUpperTo.
#ifdef OMNI_NATIVE_PROMOTE_UPPER_TO
#undef OMNI_NATIVE_PROMOTE_UPPER_TO
#else
#define OMNI_NATIVE_PROMOTE_UPPER_TO
#endif

// Unsigned: zero-extend to full vector.
template <class D, OMNI_IF_U16_D(D)> OMNI_API Vec128<uint16_t> PromoteUpperTo(D /* tag */, Vec128<uint8_t> v)
{
    return Vec128<uint16_t>(vmovl_high_u8(v.raw));
}

template <class D, OMNI_IF_U32_D(D)> OMNI_API Vec128<uint32_t> PromoteUpperTo(D /* tag */, Vec128<uint16_t> v)
{
    return Vec128<uint32_t>(vmovl_high_u16(v.raw));
}

template <class D, OMNI_IF_U64_D(D)> OMNI_API Vec128<uint64_t> PromoteUpperTo(D /* tag */, Vec128<uint32_t> v)
{
    return Vec128<uint64_t>(vmovl_high_u32(v.raw));
}

template <class D, OMNI_IF_I16_D(D)> OMNI_API Vec128<int16_t> PromoteUpperTo(D d, Vec128<uint8_t> v)
{
    return BitCast(d, Vec128<uint16_t>(vmovl_high_u8(v.raw)));
}

template <class D, OMNI_IF_I32_D(D)> OMNI_API Vec128<int32_t> PromoteUpperTo(D d, Vec128<uint16_t> v)
{
    return BitCast(d, Vec128<uint32_t>(vmovl_high_u16(v.raw)));
}

template <class D, OMNI_IF_I64_D(D)> OMNI_API Vec128<int64_t> PromoteUpperTo(D d, Vec128<uint32_t> v)
{
    return BitCast(d, Vec128<uint64_t>(vmovl_high_u32(v.raw)));
}

// Signed: replicate sign bit to full vector.
template <class D, OMNI_IF_I16_D(D)> OMNI_API Vec128<int16_t> PromoteUpperTo(D /* tag */, Vec128<int8_t> v)
{
    return Vec128<int16_t>(vmovl_high_s8(v.raw));
}

template <class D, OMNI_IF_I32_D(D)> OMNI_API Vec128<int32_t> PromoteUpperTo(D /* tag */, Vec128<int16_t> v)
{
    return Vec128<int32_t>(vmovl_high_s16(v.raw));
}

template <class D, OMNI_IF_I64_D(D)> OMNI_API Vec128<int64_t> PromoteUpperTo(D /* tag */, Vec128<int32_t> v)
{
    return Vec128<int64_t>(vmovl_high_s32(v.raw));
}

template <class D, OMNI_IF_V_SIZE_D(D, 16), OMNI_IF_F32_D(D)>
OMNI_API VFromD<D> PromoteUpperTo(D df32, VFromD<Repartition<bfloat16_t, D>> v)
{
    const Repartition<uint16_t, decltype(df32)> du16;
    const RebindToSigned<decltype(df32)> di32;
    return BitCast(df32, ShiftLeft<16>(PromoteUpperTo(di32, BitCast(du16, v))));
}

#if OMNI_HAVE_FLOAT64

template <class D, OMNI_IF_F64_D(D)> OMNI_API Vec128<double> PromoteUpperTo(D /* tag */, Vec128<float> v)
{
    return Vec128<double>(vcvt_high_f64_f32(v.raw));
}

template <class D, OMNI_IF_F64_D(D)> OMNI_API Vec128<double> PromoteUpperTo(D /* tag */, Vec128<int32_t> v)
{
    const int64x2_t i64 = vmovl_high_s32(v.raw);
    return Vec128<double>(vcvtq_f64_s64(i64));
}

template <class D, OMNI_IF_F64_D(D)> OMNI_API Vec128<double> PromoteUpperTo(D /* tag */, Vec128<uint32_t> v)
{
    const uint64x2_t u64 = vmovl_high_u32(v.raw);
    return Vec128<double>(vcvtq_f64_u64(u64));
}

#endif // OMNI_HAVE_FLOAT64

template <class D, OMNI_IF_UI64_D(D)> OMNI_API VFromD<D> PromoteUpperTo(D d64, Vec128<float> v)
{
#if OMNI_HAVE_FLOAT64
    const RebindToFloat<decltype(d64)> df64;
    return ConvertTo(d64, PromoteUpperTo(df64, v));
#else
    const Rebind<float, decltype(d)> dh;
    return PromoteTo(d, UpperHalf(dh, v));
#endif
}

// Generic version for <=64 bit input/output (_high is only for full vectors).
template <class D, OMNI_IF_V_SIZE_LE_D(D, 8), class V> OMNI_API VFromD<D> PromoteUpperTo(D d, V v)
{
    const Rebind<TFromV<V>, decltype(d)> dh;
    return PromoteTo(d, UpperHalf(dh, v));
}

#endif // OMNI_ARCH_ARM_A64

// ------------------------------ DemoteTo (ConvertTo)

// From full vector to half or quarter
template <class D, OMNI_IF_U16_D(D)> OMNI_API Vec64<uint16_t> DemoteTo(D /* tag */, Vec128<int32_t> v)
{
    return Vec64<uint16_t>(vqmovun_s32(v.raw));
}

template <class D, OMNI_IF_I16_D(D)> OMNI_API Vec64<int16_t> DemoteTo(D /* tag */, Vec128<int32_t> v)
{
    return Vec64<int16_t>(vqmovn_s32(v.raw));
}

template <class D, OMNI_IF_U8_D(D)> OMNI_API Vec32<uint8_t> DemoteTo(D /* tag */, Vec128<int32_t> v)
{
    const uint16x4_t a = vqmovun_s32(v.raw);
    return Vec32<uint8_t>(vqmovn_u16(vcombine_u16(a, a)));
}

template <class D, OMNI_IF_U8_D(D)> OMNI_API Vec64<uint8_t> DemoteTo(D /* tag */, Vec128<int16_t> v)
{
    return Vec64<uint8_t>(vqmovun_s16(v.raw));
}

template <class D, OMNI_IF_I8_D(D)> OMNI_API Vec32<int8_t> DemoteTo(D /* tag */, Vec128<int32_t> v)
{
    const int16x4_t a = vqmovn_s32(v.raw);
    return Vec32<int8_t>(vqmovn_s16(vcombine_s16(a, a)));
}

template <class D, OMNI_IF_I8_D(D)> OMNI_API Vec64<int8_t> DemoteTo(D /* tag */, Vec128<int16_t> v)
{
    return Vec64<int8_t>(vqmovn_s16(v.raw));
}

template <class D, OMNI_IF_U16_D(D)> OMNI_API Vec64<uint16_t> DemoteTo(D /* tag */, Vec128<uint32_t> v)
{
    return Vec64<uint16_t>(vqmovn_u32(v.raw));
}

template <class D, OMNI_IF_U8_D(D)> OMNI_API Vec32<uint8_t> DemoteTo(D /* tag */, Vec128<uint32_t> v)
{
    const uint16x4_t a = vqmovn_u32(v.raw);
    return Vec32<uint8_t>(vqmovn_u16(vcombine_u16(a, a)));
}

template <class D, OMNI_IF_U8_D(D)> OMNI_API Vec64<uint8_t> DemoteTo(D /* tag */, Vec128<uint16_t> v)
{
    return Vec64<uint8_t>(vqmovn_u16(v.raw));
}

// From half vector to partial half
template <class D, OMNI_IF_V_SIZE_LE_D(D, 4), OMNI_IF_U16_D(D)>
OMNI_API VFromD<D> DemoteTo(D /* tag */, VFromD<Rebind<int32_t, D>> v)
{
    return VFromD<D>(vqmovun_s32(vcombine_s32(v.raw, v.raw)));
}

template <class D, OMNI_IF_V_SIZE_LE_D(D, 4), OMNI_IF_I16_D(D)>
OMNI_API VFromD<D> DemoteTo(D /* tag */, VFromD<Rebind<int32_t, D>> v)
{
    return VFromD<D>(vqmovn_s32(vcombine_s32(v.raw, v.raw)));
}

template <class D, OMNI_IF_V_SIZE_LE_D(D, 2), OMNI_IF_U8_D(D)>
OMNI_API VFromD<D> DemoteTo(D /* tag */, VFromD<Rebind<int32_t, D>> v)
{
    const uint16x4_t a = vqmovun_s32(vcombine_s32(v.raw, v.raw));
    return VFromD<D>(vqmovn_u16(vcombine_u16(a, a)));
}

template <class D, OMNI_IF_V_SIZE_LE_D(D, 4), OMNI_IF_U8_D(D)>
OMNI_API VFromD<D> DemoteTo(D /* tag */, VFromD<Rebind<int16_t, D>> v)
{
    return VFromD<D>(vqmovun_s16(vcombine_s16(v.raw, v.raw)));
}

template <class D, OMNI_IF_V_SIZE_LE_D(D, 2), OMNI_IF_I8_D(D)>
OMNI_API VFromD<D> DemoteTo(D /* tag */, VFromD<Rebind<int32_t, D>> v)
{
    const int16x4_t a = vqmovn_s32(vcombine_s32(v.raw, v.raw));
    return VFromD<D>(vqmovn_s16(vcombine_s16(a, a)));
}

template <class D, OMNI_IF_V_SIZE_LE_D(D, 4), OMNI_IF_I8_D(D)>
OMNI_API VFromD<D> DemoteTo(D /* tag */, VFromD<Rebind<int16_t, D>> v)
{
    return VFromD<D>(vqmovn_s16(vcombine_s16(v.raw, v.raw)));
}

template <class D, OMNI_IF_V_SIZE_LE_D(D, 4), OMNI_IF_U16_D(D)>
OMNI_API VFromD<D> DemoteTo(D /* tag */, VFromD<Rebind<uint32_t, D>> v)
{
    return VFromD<D>(vqmovn_u32(vcombine_u32(v.raw, v.raw)));
}

template <class D, OMNI_IF_V_SIZE_LE_D(D, 2), OMNI_IF_U8_D(D)>
OMNI_API VFromD<D> DemoteTo(D /* tag */, VFromD<Rebind<uint32_t, D>> v)
{
    const uint16x4_t a = vqmovn_u32(vcombine_u32(v.raw, v.raw));
    return VFromD<D>(vqmovn_u16(vcombine_u16(a, a)));
}

template <class D, OMNI_IF_V_SIZE_LE_D(D, 4), OMNI_IF_U8_D(D)>
OMNI_API VFromD<D> DemoteTo(D /* tag */, VFromD<Rebind<uint16_t, D>> v)
{
    return VFromD<D>(vqmovn_u16(vcombine_u16(v.raw, v.raw)));
}

template <class D, OMNI_IF_I32_D(D)> OMNI_API Vec64<int32_t> DemoteTo(D /* tag */, Vec128<int64_t> v)
{
    return Vec64<int32_t>(vqmovn_s64(v.raw));
}

template <class D, OMNI_IF_U32_D(D)> OMNI_API Vec64<uint32_t> DemoteTo(D /* tag */, Vec128<int64_t> v)
{
    return Vec64<uint32_t>(vqmovun_s64(v.raw));
}

template <class D, OMNI_IF_U32_D(D)> OMNI_API Vec64<uint32_t> DemoteTo(D /* tag */, Vec128<uint64_t> v)
{
    return Vec64<uint32_t>(vqmovn_u64(v.raw));
}

template <class D, OMNI_IF_T_SIZE_ONE_OF_D(D, (1 << 1) | (1 << 2)), OMNI_IF_SIGNED_D(D)>
OMNI_API VFromD<D> DemoteTo(D d, Vec128<int64_t> v)
{
    const Rebind<int32_t, D> di32;
    return DemoteTo(d, DemoteTo(di32, v));
}

template <class D, OMNI_IF_T_SIZE_ONE_OF_D(D, (1 << 1) | (1 << 2)), OMNI_IF_UNSIGNED_D(D)>
OMNI_API VFromD<D> DemoteTo(D d, Vec128<int64_t> v)
{
    const Rebind<uint32_t, D> du32;
    return DemoteTo(d, DemoteTo(du32, v));
}

template <class D, OMNI_IF_T_SIZE_ONE_OF_D(D, (1 << 1) | (1 << 2)), OMNI_IF_UNSIGNED_D(D)>
OMNI_API VFromD<D> DemoteTo(D d, Vec128<uint64_t> v)
{
    const Rebind<uint32_t, D> du32;
    return DemoteTo(d, DemoteTo(du32, v));
}

template <class D, OMNI_IF_I32_D(D)> OMNI_API Vec32<int32_t> DemoteTo(D /* tag */, Vec64<int64_t> v)
{
    return Vec32<int32_t>(vqmovn_s64(vcombine_s64(v.raw, v.raw)));
}

template <class D, OMNI_IF_U32_D(D)> OMNI_API Vec32<uint32_t> DemoteTo(D /* tag */, Vec64<int64_t> v)
{
    return Vec32<uint32_t>(vqmovun_s64(vcombine_s64(v.raw, v.raw)));
}

template <class D, OMNI_IF_U32_D(D)> OMNI_API Vec32<uint32_t> DemoteTo(D /* tag */, Vec64<uint64_t> v)
{
    return Vec32<uint32_t>(vqmovn_u64(vcombine_u64(v.raw, v.raw)));
}

template <class D, OMNI_IF_SIGNED_D(D), OMNI_IF_T_SIZE_ONE_OF_D(D, (1 << 1) | (1 << 2))>
OMNI_API VFromD<D> DemoteTo(D d, Vec64<int64_t> v)
{
    const Rebind<int32_t, D> di32;
    return DemoteTo(d, DemoteTo(di32, v));
}

template <class D, OMNI_IF_UNSIGNED_D(D), OMNI_IF_T_SIZE_ONE_OF_D(D, (1 << 1) | (1 << 2))>
OMNI_API VFromD<D> DemoteTo(D d, Vec64<int64_t> v)
{
    const Rebind<uint32_t, D> du32;
    return DemoteTo(d, DemoteTo(du32, v));
}

template <class D, OMNI_IF_LANES_D(D, 1), OMNI_IF_UNSIGNED_D(D), OMNI_IF_T_SIZE_ONE_OF_D(D, (1 << 1) | (1 << 2))>
OMNI_API VFromD<D> DemoteTo(D d, Vec64<uint64_t> v)
{
    const Rebind<uint32_t, D> du32;
    return DemoteTo(d, DemoteTo(du32, v));
}

#if OMNI_HAVE_FLOAT64

template <class D, OMNI_IF_F32_D(D)> OMNI_API Vec64<float> DemoteTo(D /* tag */, Vec128<double> v)
{
    return Vec64<float>(vcvt_f32_f64(v.raw));
}

template <class D, OMNI_IF_F32_D(D)> OMNI_API Vec32<float> DemoteTo(D /* tag */, Vec64<double> v)
{
    return Vec32<float>(vcvt_f32_f64(vcombine_f64(v.raw, v.raw)));
}

template <class D, OMNI_IF_UI32_D(D)> OMNI_API VFromD<D> DemoteTo(D d32, VFromD<Rebind<double, D>> v)
{
    const Rebind<MakeWide<TFromD<D>>, D> d64;
    return DemoteTo(d32, ConvertTo(d64, v));
}

#endif // OMNI_HAVE_FLOAT64

template <class D, OMNI_IF_F32_D(D)> OMNI_API VFromD<D> DemoteTo(D df32, VFromD<Rebind<int64_t, D>> v)
{
    const Rebind<int64_t, decltype(df32)> di64;
    const RebindToUnsigned<decltype(di64)> du64;

    const RebindToFloat<decltype(du64)> df64;

    const auto k2p64_63 = Set(df64, 27670116110564327424.0);
    const auto f64_hi52 = Xor(BitCast(df64, ShiftRight<12>(BitCast(du64, v))), k2p64_63) - k2p64_63;
    const auto f64_lo12 = ConvertTo(df64, And(BitCast(du64, v), Set(du64, uint64_t{ 0x00000FFF })));

    const auto f64_sum = f64_hi52 + f64_lo12;
    const auto f64_carry = (f64_hi52 - f64_sum) + f64_lo12;

    const auto f64_sum_is_inexact = ShiftRight<63>(BitCast(du64, VecFromMask(df64, f64_carry != Zero(df64))));
    const auto f64_bits_decrement = And(ShiftRight<63>(BitCast(du64, Xor(f64_sum, f64_carry))), f64_sum_is_inexact);

    const auto adj_f64_val = BitCast(df64, Or(BitCast(du64, f64_sum) - f64_bits_decrement, f64_sum_is_inexact));

    return DemoteTo(df32, adj_f64_val);
}

template <class D, OMNI_IF_F32_D(D)> OMNI_API VFromD<D> DemoteTo(D df32, VFromD<Rebind<uint64_t, D>> v)
{
    const Rebind<uint64_t, decltype(df32)> du64;
    const RebindToFloat<decltype(du64)> df64;

    const auto k2p64 = Set(df64, 18446744073709551616.0);
    const auto f64_hi52 = Or(BitCast(df64, ShiftRight<12>(v)), k2p64) - k2p64;
    const auto f64_lo12 = ConvertTo(df64, And(v, Set(du64, uint64_t{ 0x00000FFF })));

    const auto f64_sum = f64_hi52 + f64_lo12;
    const auto f64_carry = (f64_hi52 - f64_sum) + f64_lo12;
    const auto f64_sum_is_inexact = ShiftRight<63>(BitCast(du64, VecFromMask(df64, f64_carry != Zero(df64))));

    const auto adj_f64_val =
        BitCast(df64, Or(BitCast(du64, f64_sum) - ShiftRight<63>(BitCast(du64, f64_carry)), f64_sum_is_inexact));

    return DemoteTo(df32, adj_f64_val);
}

OMNI_API Vec32<uint8_t> U8FromU32(Vec128<uint32_t> v)
{
    const uint8x16_t org_v = detail::BitCastToByte(v).raw;
    const uint8x16_t w = vuzp1q_u8(org_v, org_v);
    return Vec32<uint8_t>(vget_low_u8(vuzp1q_u8(w, w)));
}

template <size_t N, OMNI_IF_V_SIZE_LE(uint32_t, N, 8)> OMNI_API Vec128<uint8_t, N> U8FromU32(Vec128<uint32_t, N> v)
{
    const uint8x8_t org_v = detail::BitCastToByte(v).raw;
    const uint8x8_t w = vuzp1_u8(org_v, org_v);
    return Vec128<uint8_t, N>(vuzp1_u8(w, w));
}

// ------------------------------ Round (IfThenElse, mask, logical)

// Toward nearest integer
OMNI_NEON_DEF_FUNCTION_ALL_FLOATS(Round, vrndn, _, 1)

// Toward zero, aka truncate
OMNI_NEON_DEF_FUNCTION_ALL_FLOATS(Trunc, vrnd, _, 1)

// Toward +infinity, aka ceiling
OMNI_NEON_DEF_FUNCTION_ALL_FLOATS(Ceil, vrndp, _, 1)

// Toward -infinity, aka floor
OMNI_NEON_DEF_FUNCTION_ALL_FLOATS(Floor, vrndm, _, 1)

// ------------------------------ CeilInt/FloorInt
#if OMNI_ARCH_ARM_A64

OMNI_API Vec128<int32_t> FloorInt(const Vec128<float> v)
{
    return Vec128<int32_t>(vcvtmq_s32_f32(v.raw));
}

template <size_t N, OMNI_IF_V_SIZE_LE(float, N, 8)> OMNI_API Vec128<int32_t, N> FloorInt(const Vec128<float, N> v)
{
    return Vec128<int32_t, N>(vcvtm_s32_f32(v.raw));
}

OMNI_API Vec128<int64_t> FloorInt(const Vec128<double> v)
{
    return Vec128<int64_t>(vcvtmq_s64_f64(v.raw));
}

template <size_t N, OMNI_IF_V_SIZE_LE(double, N, 8)> OMNI_API Vec128<int64_t, N> FloorInt(const Vec128<double, N> v)
{
    return Vec128<int64_t, N>(vcvtm_s64_f64(v.raw));
}

#endif // OMNI_ARCH_ARM_A64

// ------------------------------ Floating-point classification
template <typename T, size_t N> OMNI_API Mask128<T, N> IsNaN(const Vec128<T, N> v)
{
    return v != v;
}

template <typename T, size_t N, OMNI_IF_V_SIZE_LE(T, N, 8)> OMNI_API Vec128<T, N / 2> LowerHalf(Vec128<T, N> v)
{
    return Vec128<T, N / 2>(v.raw);
}

OMNI_API Vec64<uint8_t> LowerHalf(Vec128<uint8_t> v)
{
    return Vec64<uint8_t>(vget_low_u8(v.raw));
}

OMNI_API Vec64<uint16_t> LowerHalf(Vec128<uint16_t> v)
{
    return Vec64<uint16_t>(vget_low_u16(v.raw));
}

OMNI_API Vec64<uint32_t> LowerHalf(Vec128<uint32_t> v)
{
    return Vec64<uint32_t>(vget_low_u32(v.raw));
}

OMNI_API Vec64<uint64_t> LowerHalf(Vec128<uint64_t> v)
{
    return Vec64<uint64_t>(vget_low_u64(v.raw));
}

OMNI_API Vec64<int8_t> LowerHalf(Vec128<int8_t> v)
{
    return Vec64<int8_t>(vget_low_s8(v.raw));
}

OMNI_API Vec64<int16_t> LowerHalf(Vec128<int16_t> v)
{
    return Vec64<int16_t>(vget_low_s16(v.raw));
}

OMNI_API Vec64<int32_t> LowerHalf(Vec128<int32_t> v)
{
    return Vec64<int32_t>(vget_low_s32(v.raw));
}

OMNI_API Vec64<int64_t> LowerHalf(Vec128<int64_t> v)
{
    return Vec64<int64_t>(vget_low_s64(v.raw));
}

OMNI_API Vec64<float> LowerHalf(Vec128<float> v)
{
    return Vec64<float>(vget_low_f32(v.raw));
}

#if OMNI_HAVE_FLOAT64

OMNI_API Vec64<double> LowerHalf(Vec128<double> v)
{
    return Vec64<double>(vget_low_f64(v.raw));
}

#endif // OMNI_HAVE_FLOAT64

template <class V, OMNI_NEON_IF_EMULATED_D(DFromV<V>), OMNI_IF_V_SIZE_V(V, 16)>
OMNI_API VFromD<Half<DFromV<V>>> LowerHalf(V v)
{
    const Full128<uint16_t> du;
    const Half<DFromV<V>> dh;
    return BitCast(dh, LowerHalf(BitCast(du, v)));
}

template <class DH> OMNI_API VFromD<DH> LowerHalf(DH /* tag */, VFromD<Twice<DH>> v)
{
    return LowerHalf(v);
}

// ------------------------------ CombineShiftRightBytes

// 128-bit
template <int kBytes, class D, typename T = TFromD<D>>
OMNI_API Vec128<T> CombineShiftRightBytes(D d, Vec128<T> hi, Vec128<T> lo)
{
    static_assert(0 < kBytes && kBytes < 16, "kBytes must be in [1, 15]");
    const Repartition<uint8_t, decltype(d)> d8;
    uint8x16_t v8 = vextq_u8(BitCast(d8, lo).raw, BitCast(d8, hi).raw, kBytes);
    return BitCast(d, Vec128<uint8_t>(v8));
}

// 64-bit
template <int kBytes, class D, typename T = TFromD<D>>
OMNI_API Vec64<T> CombineShiftRightBytes(D d, Vec64<T> hi, Vec64<T> lo)
{
    static_assert(0 < kBytes && kBytes < 8, "kBytes must be in [1, 7]");
    const Repartition<uint8_t, decltype(d)> d8;
    uint8x8_t v8 = vext_u8(BitCast(d8, lo).raw, BitCast(d8, hi).raw, kBytes);
    return BitCast(d, VFromD<decltype(d8)>(v8));
}


namespace detail {
template <int kBytes> struct ShiftLeftBytesT {
    // Full
    template <class T> OMNI_INLINE Vec128<T> operator () (const Vec128<T> v)
    {
        const Full128<T> d;
        return CombineShiftRightBytes<16 - kBytes>(d, v, Zero(d));
    }

    // Partial
    template <class T, size_t N, OMNI_IF_V_SIZE_LE(T, N, 8)> OMNI_INLINE Vec128<T, N> operator () (const Vec128<T, N> v)
    {
        // Expand to 64-bit so we only use the native EXT instruction.
        const Full64<T> d64;
        const auto zero64 = Zero(d64);
        const decltype(zero64)v64(v.raw);
        return Vec128<T, N>(CombineShiftRightBytes<8 - kBytes>(d64, v64, zero64).raw);
    }
};

template <> struct ShiftLeftBytesT<0> {
    template <class T, size_t N> OMNI_INLINE Vec128<T, N> operator () (const Vec128<T, N> v)
    {
        return v;
    }
};

template <> struct ShiftLeftBytesT<0xFF> {
    template <class T, size_t N> OMNI_INLINE Vec128<T, N> operator () (const Vec128<T, N> v)
    {
        return Xor(v, v);
    }
};

template <int kBytes> struct ShiftRightBytesT {
    template <class T, size_t N> OMNI_INLINE Vec128<T, N> operator () (Vec128<T, N> v)
    {
        const DFromV<decltype(v)> d;
        // For < 64-bit vectors, zero undefined lanes so we shift in zeros.
        if (d.MaxBytes() < 8) {
            constexpr size_t kReg = d.MaxBytes() == 16 ? 16 : 8;
            const Simd<T, kReg / sizeof(T), 0> dreg;
            v = Vec128<T, N>(IfThenElseZero(FirstN(dreg, N), VFromD<decltype(dreg)>(v.raw)).raw);
        }
        return CombineShiftRightBytes<kBytes>(d, Zero(d), v);
    }
};

template <> struct ShiftRightBytesT<0> {
    template <class T, size_t N> OMNI_INLINE Vec128<T, N> operator () (const Vec128<T, N> v)
    {
        return v;
    }
};

template <> struct ShiftRightBytesT<0xFF> {
    template <class T, size_t N> OMNI_INLINE Vec128<T, N> operator () (const Vec128<T, N> v)
    {
        return Xor(v, v);
    }
};
} // namespace detail

template <int kBytes, class D> OMNI_API VFromD<D> ShiftLeftBytes(D d, VFromD<D> v)
{
    return detail::ShiftLeftBytesT<(kBytes >= d.MaxBytes() ? 0xFF : kBytes)>()(v);
}

template <int kBytes, typename T, size_t N> OMNI_API Vec128<T, N> ShiftLeftBytes(Vec128<T, N> v)
{
    return ShiftLeftBytes<kBytes>(DFromV<decltype(v)>(), v);
}

template <int kLanes, class D> OMNI_API VFromD<D> ShiftLeftLanes(D d, VFromD<D> v)
{
    const Repartition<uint8_t, decltype(d)> d8;
    return BitCast(d, ShiftLeftBytes<kLanes * sizeof(TFromD<D>)>(BitCast(d8, v)));
}

template <int kLanes, typename T, size_t N> OMNI_API Vec128<T, N> ShiftLeftLanes(Vec128<T, N> v)
{
    return ShiftLeftLanes<kLanes>(DFromV<decltype(v)>(), v);
}

// 0x01..0F, kBytes = 1 => 0x0001..0E
template <int kBytes, class D> OMNI_API VFromD<D> ShiftRightBytes(D d, VFromD<D> v)
{
    return detail::ShiftRightBytesT<(kBytes >= d.MaxBytes() ? 0xFF : kBytes)>()(v);
}

template <int kLanes, class D> OMNI_API VFromD<D> ShiftRightLanes(D d, VFromD<D> v)
{
    const Repartition<uint8_t, decltype(d)> d8;
    return BitCast(d, ShiftRightBytes<kLanes * sizeof(TFromD<D>)>(d8, BitCast(d8, v)));
}

// Calls ShiftLeftBytes
template <int kBytes, class D, OMNI_IF_V_SIZE_LE_D(D, 4)>
OMNI_API VFromD<D> CombineShiftRightBytes(D d, VFromD<D> hi, VFromD<D> lo)
{
    constexpr size_t kSize = d.MaxBytes();
    static_assert(0 < kBytes && kBytes < kSize, "kBytes invalid");
    const Repartition<uint8_t, decltype(d)> d8;
    const Full64<uint8_t> d_full8;
    const Repartition<TFromD<D>, decltype(d_full8)> d_full;
    using V64 = VFromD<decltype(d_full8)>;
    const V64 hi64(BitCast(d8, hi).raw);
    // Move into most-significant bytes
    const V64 lo64 = ShiftLeftBytes<8 - kSize>(V64(BitCast(d8, lo).raw));
    const V64 r = CombineShiftRightBytes<8 - kSize + kBytes>(d_full8, hi64, lo64);
    // After casting to full 64-bit vector of correct type, shrink to 32-bit
    return VFromD<D>(BitCast(d_full, r).raw);
}

// ------------------------------ UpperHalf (ShiftRightBytes)

// Full input
template <class D, OMNI_IF_U8_D(D)> OMNI_API Vec64<uint8_t> UpperHalf(D /* tag */, Vec128<uint8_t> v)
{
    return Vec64<uint8_t>(vget_high_u8(v.raw));
}

template <class D, OMNI_IF_U16_D(D)> OMNI_API Vec64<uint16_t> UpperHalf(D /* tag */, Vec128<uint16_t> v)
{
    return Vec64<uint16_t>(vget_high_u16(v.raw));
}

template <class D, OMNI_IF_U32_D(D)> OMNI_API Vec64<uint32_t> UpperHalf(D /* tag */, Vec128<uint32_t> v)
{
    return Vec64<uint32_t>(vget_high_u32(v.raw));
}

template <class D, OMNI_IF_U64_D(D)> OMNI_API Vec64<uint64_t> UpperHalf(D /* tag */, Vec128<uint64_t> v)
{
    return Vec64<uint64_t>(vget_high_u64(v.raw));
}

template <class D, OMNI_IF_I8_D(D)> OMNI_API Vec64<int8_t> UpperHalf(D /* tag */, Vec128<int8_t> v)
{
    return Vec64<int8_t>(vget_high_s8(v.raw));
}

template <class D, OMNI_IF_I16_D(D)> OMNI_API Vec64<int16_t> UpperHalf(D /* tag */, Vec128<int16_t> v)
{
    return Vec64<int16_t>(vget_high_s16(v.raw));
}

template <class D, OMNI_IF_I32_D(D)> OMNI_API Vec64<int32_t> UpperHalf(D /* tag */, Vec128<int32_t> v)
{
    return Vec64<int32_t>(vget_high_s32(v.raw));
}

template <class D, OMNI_IF_I64_D(D)> OMNI_API Vec64<int64_t> UpperHalf(D /* tag */, Vec128<int64_t> v)
{
    return Vec64<int64_t>(vget_high_s64(v.raw));
}

template <class D, OMNI_IF_F32_D(D)> OMNI_API Vec64<float> UpperHalf(D /* tag */, Vec128<float> v)
{
    return Vec64<float>(vget_high_f32(v.raw));
}

#if OMNI_HAVE_FLOAT64

template <class D, OMNI_IF_F64_D(D)> OMNI_API Vec64<double> UpperHalf(D /* tag */, Vec128<double> v)
{
    return Vec64<double>(vget_high_f64(v.raw));
}

#endif // OMNI_HAVE_FLOAT64

template <class D, OMNI_NEON_IF_EMULATED_D(D), OMNI_IF_V_SIZE_GT_D(D, 4)>
OMNI_API VFromD<D> UpperHalf(D dh, VFromD<Twice<D>> v)
{
    const RebindToUnsigned<Twice<decltype(dh)>> du;
    const Half<decltype(du)> duh;
    return BitCast(dh, UpperHalf(duh, BitCast(du, v)));
}

// Partial
template <class DH, OMNI_IF_V_SIZE_LE_D(DH, 4)> OMNI_API VFromD<DH> UpperHalf(DH dh, VFromD<Twice<DH>> v)
{
    const Twice<DH> d;
    const RebindToUnsigned<decltype(d)> du;
    const VFromD<decltype(du)> upper = ShiftRightBytes<dh.MaxBytes()>(du, BitCast(du, v));
    return VFromD<DH>(BitCast(d, upper).raw);
}

// ------------------------------ Broadcast/splat any lane

template <int kLane, typename T> OMNI_API Vec128<T, 1> Broadcast(Vec128<T, 1> v)
{
    return v;
}

// Unsigned
template <int kLane> OMNI_API Vec128<uint8_t> Broadcast(Vec128<uint8_t> v)
{
    static_assert(0 <= kLane && kLane < 16, "Invalid lane");
    return Vec128<uint8_t>(vdupq_laneq_u8(v.raw, kLane));
}

template <int kLane, size_t N, OMNI_IF_V_SIZE_LE(uint8_t, N, 8), OMNI_IF_LANES_GT(N, 1)>
OMNI_API Vec128<uint8_t, N> Broadcast(Vec128<uint8_t, N> v)
{
    static_assert(0 <= kLane && kLane < N, "Invalid lane");
    return Vec128<uint8_t, N>(vdup_lane_u8(v.raw, kLane));
}

template <int kLane> OMNI_API Vec128<uint16_t> Broadcast(Vec128<uint16_t> v)
{
    static_assert(0 <= kLane && kLane < 8, "Invalid lane");
    return Vec128<uint16_t>(vdupq_laneq_u16(v.raw, kLane));
}

template <int kLane, size_t N, OMNI_IF_V_SIZE_LE(uint16_t, N, 8), OMNI_IF_LANES_GT(N, 1)>
OMNI_API Vec128<uint16_t, N> Broadcast(Vec128<uint16_t, N> v)
{
    static_assert(0 <= kLane && kLane < N, "Invalid lane");
    return Vec128<uint16_t, N>(vdup_lane_u16(v.raw, kLane));
}

template <int kLane> OMNI_API Vec128<uint32_t> Broadcast(Vec128<uint32_t> v)
{
    static_assert(0 <= kLane && kLane < 4, "Invalid lane");
    return Vec128<uint32_t>(vdupq_laneq_u32(v.raw, kLane));
}

template <int kLane, size_t N, OMNI_IF_V_SIZE_LE(uint32_t, N, 8), OMNI_IF_LANES_GT(N, 1)>
OMNI_API Vec128<uint32_t, N> Broadcast(Vec128<uint32_t, N> v)
{
    static_assert(0 <= kLane && kLane < N, "Invalid lane");
    return Vec128<uint32_t, N>(vdup_lane_u32(v.raw, kLane));
}

template <int kLane> OMNI_API Vec128<uint64_t> Broadcast(Vec128<uint64_t> v)
{
    static_assert(0 <= kLane && kLane < 2, "Invalid lane");
    return Vec128<uint64_t>(vdupq_laneq_u64(v.raw, kLane));
}

// Signed
template <int kLane> OMNI_API Vec128<int8_t> Broadcast(Vec128<int8_t> v)
{
    static_assert(0 <= kLane && kLane < 16, "Invalid lane");
    return Vec128<int8_t>(vdupq_laneq_s8(v.raw, kLane));
}

template <int kLane, size_t N, OMNI_IF_V_SIZE_LE(int8_t, N, 8), OMNI_IF_LANES_GT(N, 1)>
OMNI_API Vec128<int8_t, N> Broadcast(Vec128<int8_t, N> v)
{
    static_assert(0 <= kLane && kLane < N, "Invalid lane");
    return Vec128<int8_t, N>(vdup_lane_s8(v.raw, kLane));
}

template <int kLane> OMNI_API Vec128<int16_t> Broadcast(Vec128<int16_t> v)
{
    static_assert(0 <= kLane && kLane < 8, "Invalid lane");
    return Vec128<int16_t>(vdupq_laneq_s16(v.raw, kLane));
}

template <int kLane, size_t N, OMNI_IF_V_SIZE_LE(int16_t, N, 8), OMNI_IF_LANES_GT(N, 1)>
OMNI_API Vec128<int16_t, N> Broadcast(Vec128<int16_t, N> v)
{
    static_assert(0 <= kLane && kLane < N, "Invalid lane");
    return Vec128<int16_t, N>(vdup_lane_s16(v.raw, kLane));
}

template <int kLane> OMNI_API Vec128<int32_t> Broadcast(Vec128<int32_t> v)
{
    static_assert(0 <= kLane && kLane < 4, "Invalid lane");
    return Vec128<int32_t>(vdupq_laneq_s32(v.raw, kLane));
}

template <int kLane, size_t N, OMNI_IF_V_SIZE_LE(int32_t, N, 8), OMNI_IF_LANES_GT(N, 1)>
OMNI_API Vec128<int32_t, N> Broadcast(Vec128<int32_t, N> v)
{
    static_assert(0 <= kLane && kLane < N, "Invalid lane");
    return Vec128<int32_t, N>(vdup_lane_s32(v.raw, kLane));
}

template <int kLane> OMNI_API Vec128<int64_t> Broadcast(Vec128<int64_t> v)
{
    static_assert(0 <= kLane && kLane < 2, "Invalid lane");
    return Vec128<int64_t>(vdupq_laneq_s64(v.raw, kLane));
}

template <int kLane> OMNI_API Vec128<float> Broadcast(Vec128<float> v)
{
    static_assert(0 <= kLane && kLane < 4, "Invalid lane");
    return Vec128<float>(vdupq_laneq_f32(v.raw, kLane));
}

template <int kLane, size_t N, OMNI_IF_V_SIZE_LE(float, N, 8), OMNI_IF_LANES_GT(N, 1)>
OMNI_API Vec128<float, N> Broadcast(Vec128<float, N> v)
{
    static_assert(0 <= kLane && kLane < N, "Invalid lane");
    return Vec128<float, N>(vdup_lane_f32(v.raw, kLane));
}

template <int kLane> OMNI_API Vec128<double> Broadcast(Vec128<double> v)
{
    static_assert(0 <= kLane && kLane < 2, "Invalid lane");
    return Vec128<double>(vdupq_laneq_f64(v.raw, kLane));
}

template <int kLane, typename V, OMNI_NEON_IF_EMULATED_D(DFromV<V>), OMNI_IF_LANES_GT_D(DFromV<V>, 1)>
OMNI_API V Broadcast(V v)
{
    const DFromV<V> d;
    const RebindToUnsigned<decltype(d)> du;
    return BitCast(d, Broadcast<kLane>(BitCast(du, v)));
}

// ------------------------------ TableLookupLanes

// Returned by SetTableIndices for use by TableLookupLanes.
template <typename T, size_t N> struct Indices128 {
    typename detail::Raw128<T, N>::type raw;
};

namespace detail {
template <class D, OMNI_IF_T_SIZE_D(D, 1)>
OMNI_INLINE VFromD<Repartition<uint8_t, D>> IndicesFromVecBroadcastLaneBytes(D d)
{
    const Repartition<uint8_t, decltype(d)> d8;
    return Iota(d8, 0);
}

template <class D, OMNI_IF_T_SIZE_D(D, 2)>
OMNI_INLINE VFromD<Repartition<uint8_t, D>> IndicesFromVecBroadcastLaneBytes(D d)
{
    const Repartition<uint8_t, decltype(d)> d8;
    alignas(16) static constexpr uint8_t kBroadcastLaneBytes[16] = { 0, 0, 2, 2, 4, 4, 6, 6, 8, 8, 10, 10, 12, 12, 14,
            14 };
    return Load(d8, kBroadcastLaneBytes);
}

template <class D, OMNI_IF_T_SIZE_D(D, 4)>
OMNI_INLINE VFromD<Repartition<uint8_t, D>> IndicesFromVecBroadcastLaneBytes(D d)
{
    const Repartition<uint8_t, decltype(d)> d8;
    alignas(16) static constexpr uint8_t kBroadcastLaneBytes[16] = { 0, 0, 0, 0, 4, 4, 4, 4, 8, 8, 8, 8, 12, 12, 12,
            12 };
    return Load(d8, kBroadcastLaneBytes);
}

template <class D, OMNI_IF_T_SIZE_D(D, 8)>
OMNI_INLINE VFromD<Repartition<uint8_t, D>> IndicesFromVecBroadcastLaneBytes(D d)
{
    const Repartition<uint8_t, decltype(d)> d8;
    alignas(16) static constexpr uint8_t kBroadcastLaneBytes[16] = { 0, 0, 0, 0, 0, 0, 0, 0, 8, 8, 8, 8, 8, 8, 8, 8 };
    return Load(d8, kBroadcastLaneBytes);
}

template <class D, OMNI_IF_T_SIZE_D(D, 1)> OMNI_INLINE VFromD<Repartition<uint8_t, D>> IndicesFromVecByteOffsets(D d)
{
    const Repartition<uint8_t, decltype(d)> d8;
    return Zero(d8);
}

template <class D, OMNI_IF_T_SIZE_D(D, 2)> OMNI_INLINE VFromD<Repartition<uint8_t, D>> IndicesFromVecByteOffsets(D d)
{
    const Repartition<uint8_t, decltype(d)> d8;
    alignas(16) static constexpr uint8_t kByteOffsets[16] = { 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1 };
    return Load(d8, kByteOffsets);
}

template <class D, OMNI_IF_T_SIZE_D(D, 4)> OMNI_INLINE VFromD<Repartition<uint8_t, D>> IndicesFromVecByteOffsets(D d)
{
    const Repartition<uint8_t, decltype(d)> d8;
    alignas(16) static constexpr uint8_t kByteOffsets[16] = { 0, 1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3 };
    return Load(d8, kByteOffsets);
}

template <class D, OMNI_IF_T_SIZE_D(D, 8)> OMNI_INLINE VFromD<Repartition<uint8_t, D>> IndicesFromVecByteOffsets(D d)
{
    const Repartition<uint8_t, decltype(d)> d8;
    alignas(16) static constexpr uint8_t kByteOffsets[16] = { 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7 };
    return Load(d8, kByteOffsets);
}
} // namespace detail

template <class D, typename TI, OMNI_IF_T_SIZE_D(D, 1)>
OMNI_API Indices128<TFromD<D>, MaxLanes(D())> IndicesFromVec(D d, Vec128<TI, MaxLanes(D())> vec)
{
    using T = TFromD<D>;
    static_assert(sizeof(T) == sizeof(TI), "Index size must match lane");

    (void)d;
    return Indices128<TFromD<D>, MaxLanes(D())>{ BitCast(d, vec).raw };
}

template <class D, typename TI, OMNI_IF_T_SIZE_ONE_OF_D(D, (1 << 2) | (1 << 4) | (1 << 8))>
OMNI_API Indices128<TFromD<D>, MaxLanes(D())> IndicesFromVec(D d, Vec128<TI, MaxLanes(D())> vec)
{
    using T = TFromD<D>;
    static_assert(sizeof(T) == sizeof(TI), "Index size must match lane");

    const Repartition<uint8_t, decltype(d)> d8;
    using V8 = VFromD<decltype(d8)>;

    // Broadcast each lane index to all bytes of T and shift to bytes
    const V8 lane_indices = TableLookupBytes(BitCast(d8, vec), detail::IndicesFromVecBroadcastLaneBytes(d));
    constexpr int kIndexShiftAmt = static_cast<int>(FloorLog2(sizeof(T)));
    const V8 byte_indices = ShiftLeft<kIndexShiftAmt>(lane_indices);
    const V8 sum = Add(byte_indices, detail::IndicesFromVecByteOffsets(d));
    return Indices128<TFromD<D>, MaxLanes(D())>{ BitCast(d, sum).raw };
}

template <class D, typename TI> OMNI_API Indices128<TFromD<D>, MaxLanes(D())> SetTableIndices(D d, const TI *idx)
{
    const Rebind<TI, decltype(d)> di;
    return IndicesFromVec(d, LoadU(di, idx));
}

template <typename T, size_t N> OMNI_API Vec128<T, N> TableLookupLanes(Vec128<T, N> v, Indices128<T, N> idx)
{
    const DFromV<decltype(v)> d;
    const RebindToSigned<decltype(d)> di;
    return BitCast(d, TableLookupBytes(BitCast(di, v), BitCast(di, Vec128<T, N>{ idx.raw })));
}

template <typename T, size_t N, OMNI_IF_V_SIZE_LE(T, N, 4)>
OMNI_API Vec128<T, N> TwoTablesLookupLanes(Vec128<T, N> a, Vec128<T, N> b, Indices128<T, N> idx)
{
    const DFromV<decltype(a)> d;
    const Twice<decltype(d)> dt;
    // We only keep LowerHalf of the result, which is valid in idx.
    const Indices128<T, N * 2> idx2{ idx.raw };
}

template <typename T> OMNI_API Vec64<T> TwoTablesLookupLanes(Vec64<T> a, Vec64<T> b, Indices128<T, 8 / sizeof(T)> idx)
{
    const DFromV<decltype(a)> d;
    const Repartition<uint8_t, decltype(d)> du8;
    const auto a_u8 = BitCast(du8, a);
    const auto b_u8 = BitCast(du8, b);
    const auto idx_u8 = BitCast(du8, Vec64<T>{ idx.raw });

    const Twice<decltype(du8)> dt_u8;
    return BitCast(d, Vec64<uint8_t>{ vqtbl1_u8(Combine(dt_u8, b_u8, a_u8).raw, idx_u8.raw) });
}

template <typename T>
OMNI_API Vec128<T> TwoTablesLookupLanes(Vec128<T> a, Vec128<T> b, Indices128<T, 16 / sizeof(T)> idx)
{
    const DFromV<decltype(a)> d;
    const Repartition<uint8_t, decltype(d)> du8;
    const auto a_u8 = BitCast(du8, a);
    const auto b_u8 = BitCast(du8, b);
    const auto idx_u8 = BitCast(du8, Vec128<T>{ idx.raw });

    detail::Tuple2<uint8_t, du8.MaxLanes()> tup = { { { a_u8.raw, b_u8.raw } } };
    return BitCast(d, Vec128<uint8_t>{ vqtbl2q_u8(tup.raw, idx_u8.raw) });
}

// ------------------------------ Reverse2 (CombineShiftRightBytes)

// Per-target flag to prevent generic_ops-inl.h defining 8-bit Reverse2/4/8.
#ifdef OMNI_NATIVE_REVERSE2_8
#undef OMNI_NATIVE_REVERSE2_8
#else
#define OMNI_NATIVE_REVERSE2_8
#endif

template <class D, OMNI_IF_V_SIZE_LE_D(D, 8), OMNI_IF_T_SIZE_D(D, 1)> OMNI_API VFromD<D> Reverse2(D d, VFromD<D> v)
{
    const RebindToUnsigned<decltype(d)> du;
    return BitCast(d, VFromD<decltype(du)>(vrev16_u8(BitCast(du, v).raw)));
}

template <class D, typename T = TFromD<D>, OMNI_IF_T_SIZE(T, 1)> OMNI_API Vec128<T> Reverse2(D d, Vec128<T> v)
{
    const RebindToUnsigned<decltype(d)> du;
    return BitCast(d, Vec128<uint8_t>(vrev16q_u8(BitCast(du, v).raw)));
}

template <class D, OMNI_IF_V_SIZE_LE_D(D, 8), OMNI_IF_T_SIZE_D(D, 2)> OMNI_API VFromD<D> Reverse2(D d, VFromD<D> v)
{
    const RebindToUnsigned<decltype(d)> du;
    return BitCast(d, VFromD<decltype(du)>(vrev32_u16(BitCast(du, v).raw)));
}

template <class D, typename T = TFromD<D>, OMNI_IF_T_SIZE(T, 2)> OMNI_API Vec128<T> Reverse2(D d, Vec128<T> v)
{
    const RebindToUnsigned<decltype(d)> du;
    return BitCast(d, Vec128<uint16_t>(vrev32q_u16(BitCast(du, v).raw)));
}

template <class D, OMNI_IF_V_SIZE_LE_D(D, 8), OMNI_IF_T_SIZE_D(D, 4)> OMNI_API VFromD<D> Reverse2(D d, VFromD<D> v)
{
    const RebindToUnsigned<decltype(d)> du;
    return BitCast(d, VFromD<decltype(du)>(vrev64_u32(BitCast(du, v).raw)));
}

template <class D, typename T = TFromD<D>, OMNI_IF_T_SIZE(T, 4)> OMNI_API Vec128<T> Reverse2(D d, Vec128<T> v)
{
    const RebindToUnsigned<decltype(d)> du;
    return BitCast(d, Vec128<uint32_t>(vrev64q_u32(BitCast(du, v).raw)));
}

template <class D, OMNI_IF_T_SIZE_D(D, 8)> OMNI_API VFromD<D> Reverse2(D d, VFromD<D> v)
{
    return CombineShiftRightBytes<8>(d, v, v);
}

// ------------------------------ Reverse4 (Reverse2)

template <class D, OMNI_IF_V_SIZE_LE_D(D, 8), OMNI_IF_T_SIZE_D(D, 1)> OMNI_API VFromD<D> Reverse4(D d, VFromD<D> v)
{
    const RebindToUnsigned<decltype(d)> du;
    return BitCast(d, VFromD<decltype(du)>(vrev32_u8(BitCast(du, v).raw)));
}

template <class D, typename T = TFromD<D>, OMNI_IF_T_SIZE(T, 1)> OMNI_API Vec128<T> Reverse4(D d, Vec128<T> v)
{
    const RebindToUnsigned<decltype(d)> du;
    return BitCast(d, Vec128<uint8_t>(vrev32q_u8(BitCast(du, v).raw)));
}

template <class D, OMNI_IF_V_SIZE_LE_D(D, 8), OMNI_IF_T_SIZE_D(D, 2)> OMNI_API VFromD<D> Reverse4(D d, VFromD<D> v)
{
    const RebindToUnsigned<decltype(d)> du;
    return BitCast(d, VFromD<decltype(du)>(vrev64_u16(BitCast(du, v).raw)));
}

template <class D, typename T = TFromD<D>, OMNI_IF_T_SIZE(T, 2)> OMNI_API Vec128<T> Reverse4(D d, Vec128<T> v)
{
    const RebindToUnsigned<decltype(d)> du;
    return BitCast(d, Vec128<uint16_t>(vrev64q_u16(BitCast(du, v).raw)));
}

template <class D, OMNI_IF_T_SIZE_D(D, 4)> OMNI_API VFromD<D> Reverse4(D d, VFromD<D> v)
{
    const RepartitionToWide<RebindToUnsigned<decltype(d)>> duw;
    return BitCast(d, Reverse2(duw, BitCast(duw, Reverse2(d, v))));
}

template <class D, OMNI_IF_T_SIZE_D(D, 8)> OMNI_API VFromD<D> Reverse4(D /* tag */, VFromD<D>)
{
    OMNI_ASSERT(0); // don't have 8 u64 lanes
}

// ------------------------------ Reverse8 (Reverse2, Reverse4)

template <class D, OMNI_IF_V_SIZE_LE_D(D, 8), OMNI_IF_T_SIZE_D(D, 1)> OMNI_API VFromD<D> Reverse8(D d, VFromD<D> v)
{
    const RebindToUnsigned<decltype(d)> du;
    return BitCast(d, VFromD<decltype(du)>(vrev64_u8(BitCast(du, v).raw)));
}

template <class D, typename T = TFromD<D>, OMNI_IF_T_SIZE(T, 1)> OMNI_API Vec128<T> Reverse8(D d, Vec128<T> v)
{
    const RebindToUnsigned<decltype(d)> du;
    return BitCast(d, Vec128<uint8_t>(vrev64q_u8(BitCast(du, v).raw)));
}

template <class D, OMNI_IF_T_SIZE_D(D, 2)> OMNI_API VFromD<D> Reverse8(D d, VFromD<D> v)
{
    const Repartition<uint64_t, decltype(d)> du64;
    return BitCast(d, Reverse2(du64, BitCast(du64, Reverse4(d, v))));
}

template <class D, OMNI_IF_T_SIZE_ONE_OF_D(D, (1 << 4) | (1 << 8))> OMNI_API VFromD<D> Reverse8(D, VFromD<D>)
{
    OMNI_ASSERT(0); // don't have 8 lanes if larger than 16-bit
}

// ------------------------------ Reverse (Reverse2, Reverse4, Reverse8)

template <class D, typename T = TFromD<D>, OMNI_IF_LANES_D(D, 1)>
OMNI_API Vec128<T, 1> Reverse(D /* tag */, Vec128<T, 1> v)
{
    return v;
}

template <class D, typename T = TFromD<D>, OMNI_IF_LANES_D(D, 2)> OMNI_API Vec128<T, 2> Reverse(D d, Vec128<T, 2> v)
{
    return Reverse2(d, v);
}

template <class D, typename T = TFromD<D>, OMNI_IF_LANES_D(D, 4)> OMNI_API Vec128<T, 4> Reverse(D d, Vec128<T, 4> v)
{
    return Reverse4(d, v);
}

template <class D, typename T = TFromD<D>, OMNI_IF_LANES_D(D, 8)> OMNI_API Vec128<T, 8> Reverse(D d, Vec128<T, 8> v)
{
    return Reverse8(d, v);
}

template <class D, typename T = TFromD<D>, OMNI_IF_LANES_D(D, 16)> OMNI_API Vec128<T> Reverse(D d, Vec128<T> v)
{
    const Repartition<uint64_t, decltype(d)> du64;
    return BitCast(d, Reverse2(du64, BitCast(du64, Reverse8(d, v))));
}

// ------------------------------ ReverseBits

#if OMNI_ARCH_ARM_A64

#ifdef OMNI_NATIVE_REVERSE_BITS_UI8
#undef OMNI_NATIVE_REVERSE_BITS_UI8
#else
#define OMNI_NATIVE_REVERSE_BITS_UI8
#endif

OMNI_NEON_DEF_FUNCTION_INT_8(ReverseBits, vrbit, _, 1)

OMNI_NEON_DEF_FUNCTION_UINT_8(ReverseBits, vrbit, _, 1)

#endif // OMNI_ARCH_ARM_A64

template <typename T> OMNI_API Vec128<T> Shuffle1032(Vec128<T> v)
{
    return CombineShiftRightBytes<8>(DFromV<decltype(v)>(), v, v);
}

template <typename T> OMNI_API Vec128<T> Shuffle01(Vec128<T> v)
{
    return CombineShiftRightBytes<8>(DFromV<decltype(v)>(), v, v);
}

// Rotate right 32 bits
template <typename T> OMNI_API Vec128<T> Shuffle0321(Vec128<T> v)
{
    return CombineShiftRightBytes<4>(DFromV<decltype(v)>(), v, v);
}

// Rotate left 32 bits
template <typename T> OMNI_API Vec128<T> Shuffle2103(Vec128<T> v)
{
    return CombineShiftRightBytes<12>(DFromV<decltype(v)>(), v, v);
}

// Reverse
template <typename T> OMNI_API Vec128<T> Shuffle0123(Vec128<T> v)
{
    return Reverse4(DFromV<decltype(v)>(), v);
}

OMNI_NEON_DEF_FUNCTION_UIF_8_16_32(InterleaveLower, vzip1, _, 2)

// N=1 makes no sense (in that case, there would be no upper/lower).
OMNI_NEON_DEF_FUNCTION_FULL_UIF_64(InterleaveLower, vzip1, _, 2)

// < 64 bit parts
template <typename T, size_t N, OMNI_IF_V_SIZE_LE(T, N, 4)>
OMNI_API Vec128<T, N> InterleaveLower(Vec128<T, N> a, Vec128<T, N> b)
{
    return Vec128<T, N>(InterleaveLower(Vec64<T>(a.raw), Vec64<T>(b.raw)).raw);
}

// Additional overload for the optional Simd<> tag.
template <class D> OMNI_API VFromD<D> InterleaveLower(D /* tag */, VFromD<D> a, VFromD<D> b)
{
    return InterleaveLower(a, b);
}

// ------------------------------ InterleaveUpper (UpperHalf)

// All functions inside detail lack the required D parameter.
namespace detail {
OMNI_NEON_DEF_FUNCTION_UIF_8_16_32(InterleaveUpper, vzip2, _, 2)

OMNI_NEON_DEF_FUNCTION_FULL_UIF_64(InterleaveUpper, vzip2, _, 2)
} // namespace detail

// Full register
template <class D, OMNI_IF_V_SIZE_GT_D(D, 4)> OMNI_API VFromD<D> InterleaveUpper(D /* tag */, VFromD<D> a, VFromD<D> b)
{
    return detail::InterleaveUpper(a, b);
}

// Partial
template <class D, OMNI_IF_V_SIZE_LE_D(D, 4)> OMNI_API VFromD<D> InterleaveUpper(D d, VFromD<D> a, VFromD<D> b)
{
    const Half<decltype(d)> d2;
    const VFromD<D> a2(UpperHalf(d2, a).raw);
    const VFromD<D> b2(UpperHalf(d2, b).raw);
    return InterleaveLower(d, a2, b2);
}

// ------------------------------ ZipLower/ZipUpper (InterleaveLower)

// Same as Interleave*, except that the return lanes are double-width integers;
// this is necessary because the single-lane scalar cannot return two values.
template <class V, class DW = RepartitionToWide<DFromV<V>>> OMNI_API VFromD<DW> ZipLower(V a, V b)
{
    return BitCast(DW(), InterleaveLower(a, b));
}

template <class V, class D = DFromV<V>, class DW = RepartitionToWide<D>> OMNI_API VFromD<DW> ZipLower(DW dw, V a, V b)
{
    return BitCast(dw, InterleaveLower(D(), a, b));
}

template <class V, class D = DFromV<V>, class DW = RepartitionToWide<D>> OMNI_API VFromD<DW> ZipUpper(DW dw, V a, V b)
{
    return BitCast(dw, InterleaveUpper(D(), a, b));
}

// ------------------------------ Per4LaneBlockShuffle
namespace detail {
#if OMNI_COMPILER_GCC || OMNI_COMPILER_CLANG

#ifdef OMNI_NATIVE_PER4LANEBLKSHUF_DUP32
#undef OMNI_NATIVE_PER4LANEBLKSHUF_DUP32
#else
#define OMNI_NATIVE_PER4LANEBLKSHUF_DUP32
#endif

template <class D, OMNI_IF_V_SIZE_LE_D(D, 8)>
OMNI_INLINE VFromD<D> Per4LaneBlkShufDupSet4xU32(D d, const uint32_t /* x3 */, const uint32_t /* x2 */,
    const uint32_t x1, const uint32_t x0)
{
    typedef uint32_t GccU32RawVectType __attribute__((__vector_size__(8)));
    const GccU32RawVectType raw = { x0, x1 };
    return ResizeBitCast(d, Vec64<uint32_t>(reinterpret_cast<uint32x2_t>(raw)));
}

template <class D, OMNI_IF_V_SIZE_D(D, 16)>
OMNI_INLINE VFromD<D> Per4LaneBlkShufDupSet4xU32(D d, const uint32_t x3, const uint32_t x2, const uint32_t x1,
    const uint32_t x0)
{
    typedef uint32_t GccU32RawVectType __attribute__((__vector_size__(16)));
    const GccU32RawVectType raw = { x0, x1, x2, x3 };
    return ResizeBitCast(d, Vec128<uint32_t>(reinterpret_cast<uint32x4_t>(raw)));
}

#endif // OMNI_COMPILER_GCC || OMNI_COMPILER_CLANG

template <size_t kLaneSize, size_t kVectSize, class V, OMNI_IF_LANES_GT_D(DFromV<V>, 4)>
OMNI_INLINE V Per4LaneBlockShuffle(simd::SizeTag<0x88> /* idx_3210_tag */, simd::SizeTag<kLaneSize> /* lane_size_tag */,
    simd::SizeTag<kVectSize> /* vect_size_tag */, V v)
{
    const DFromV<decltype(v)> d;
    const RebindToUnsigned<decltype(d)> du;
    const RepartitionToWide<decltype(du)> dw;

    const auto evens = BitCast(dw, ConcatEven(d, v, v));
    return BitCast(d, InterleaveLower(dw, evens, evens));
}

template <size_t kLaneSize, size_t kVectSize, class V, OMNI_IF_LANES_GT_D(DFromV<V>, 4)>
OMNI_INLINE V Per4LaneBlockShuffle(simd::SizeTag<0xDD> /* idx_3210_tag */, simd::SizeTag<kLaneSize> /* lane_size_tag */,
    simd::SizeTag<kVectSize> /* vect_size_tag */, V v)
{
    const DFromV<decltype(v)> d;
    const RebindToUnsigned<decltype(d)> du;
    const RepartitionToWide<decltype(du)> dw;

    const auto odds = BitCast(dw, ConcatOdd(d, v, v));
    return BitCast(d, InterleaveLower(dw, odds, odds));
}

template <class V>
OMNI_INLINE V Per4LaneBlockShuffle(simd::SizeTag<0xFA> /* idx_3210_tag */, simd::SizeTag<2> /* lane_size_tag */,
    simd::SizeTag<8> /* vect_size_tag */, V v)
{
    const DFromV<decltype(v)> d;
    return InterleaveUpper(d, v, v);
}
} // namespace detail

template <class DI32, OMNI_IF_I32_D(DI32), OMNI_IF_V_SIZE_D(DI32, 16)>
OMNI_API VFromD<DI32> SatWidenMulAccumFixedPoint(DI32 /* di32 */, VFromD<Rebind<int16_t, DI32>> a,
    VFromD<Rebind<int16_t, DI32>> b, VFromD<DI32> sum)
{
    return VFromD<DI32>(vqdmlal_s16(sum.raw, a.raw, b.raw));
}

template <class DI32, OMNI_IF_I32_D(DI32), OMNI_IF_V_SIZE_LE_D(DI32, 8)>
OMNI_API VFromD<DI32> SatWidenMulAccumFixedPoint(DI32 di32, VFromD<Rebind<int16_t, DI32>> a,
    VFromD<Rebind<int16_t, DI32>> b, VFromD<DI32> sum)
{
    const Full128<TFromD<DI32>> di32_full;
    const Rebind<int16_t, decltype(di32_full)> di16_full64;
    return ResizeBitCast(di32, SatWidenMulAccumFixedPoint(di32_full, ResizeBitCast(di16_full64, a),
        ResizeBitCast(di16_full64, b), ResizeBitCast(di32_full, sum)));
}

template <class D, OMNI_IF_I32_D(D)>
OMNI_API Vec128<int32_t> ReorderWidenMulAccumulate(D /* d32 */, Vec128<int16_t> a, Vec128<int16_t> b,
    const Vec128<int32_t> sum0, Vec128<int32_t> &sum1)
{
    sum1 = Vec128<int32_t>(vmlal_high_s16(sum1.raw, a.raw, b.raw));
    return Vec128<int32_t>(vmlal_s16(sum0.raw, LowerHalf(a).raw, LowerHalf(b).raw));
}

template <class D, OMNI_IF_I32_D(D)>
OMNI_API Vec64<int32_t> ReorderWidenMulAccumulate(D d32, Vec64<int16_t> a, Vec64<int16_t> b, const Vec64<int32_t> sum0,
    Vec64<int32_t> &sum1)
{
    const Vec128<int32_t> mul_3210(vmull_s16(a.raw, b.raw));
    const Vec64<int32_t> mul_32 = UpperHalf(d32, mul_3210);
    sum1 += mul_32;
    return sum0 + LowerHalf(mul_3210);
}

template <class D, OMNI_IF_I32_D(D)>
OMNI_API Vec32<int32_t> ReorderWidenMulAccumulate(D d32, Vec32<int16_t> a, Vec32<int16_t> b, const Vec32<int32_t> sum0,
    Vec32<int32_t> &sum1)
{
    const Vec128<int32_t> mul_xx10(vmull_s16(a.raw, b.raw));
    const Vec64<int32_t> mul_10(LowerHalf(mul_xx10));
    const Vec32<int32_t> mul0 = LowerHalf(d32, mul_10);
    const Vec32<int32_t> mul1 = UpperHalf(d32, mul_10);
    sum1 += mul1;
    return sum0 + mul0;
}

template <class D, OMNI_IF_U32_D(D)>
OMNI_API Vec128<uint32_t> ReorderWidenMulAccumulate(D /* d32 */, Vec128<uint16_t> a, Vec128<uint16_t> b,
    const Vec128<uint32_t> sum0, Vec128<uint32_t> &sum1)
{
    sum1 = Vec128<uint32_t>(vmlal_high_u16(sum1.raw, a.raw, b.raw));
    return Vec128<uint32_t>(vmlal_u16(sum0.raw, LowerHalf(a).raw, LowerHalf(b).raw));
}

template <class D, OMNI_IF_U32_D(D)>
OMNI_API Vec64<uint32_t> ReorderWidenMulAccumulate(D d32, Vec64<uint16_t> a, Vec64<uint16_t> b,
    const Vec64<uint32_t> sum0, Vec64<uint32_t> &sum1)
{
    const Vec128<uint32_t> mul_3210(vmull_u16(a.raw, b.raw));
    const Vec64<uint32_t> mul_32 = UpperHalf(d32, mul_3210);
    sum1 += mul_32;
    return sum0 + LowerHalf(mul_3210);
}

template <class D, OMNI_IF_U32_D(D)>
OMNI_API Vec32<uint32_t> ReorderWidenMulAccumulate(D du32, Vec32<uint16_t> a, Vec32<uint16_t> b,
    const Vec32<uint32_t> sum0, Vec32<uint32_t> &sum1)
{
    const Vec128<uint32_t> mul_xx10(vmull_u16(a.raw, b.raw));
    const Vec64<uint32_t> mul_10(LowerHalf(mul_xx10));
    const Vec32<uint32_t> mul0 = LowerHalf(du32, mul_10);
    const Vec32<uint32_t> mul1 = UpperHalf(du32, mul_10);
    sum1 += mul1;
    return sum0 + mul0;
}

// ------------------------------ Combine partial (InterleaveLower)
// < 64bit input, <= 64 bit result
template <class D, OMNI_IF_V_SIZE_LE_D(D, 8)> OMNI_API VFromD<D> Combine(D d, VFromD<Half<D>> hi, VFromD<Half<D>> lo)
{
    // First double N (only lower halves will be used).
    const VFromD<D> hi2(hi.raw);
    const VFromD<D> lo2(lo.raw);
    // Repartition to two unsigned lanes (each the size of the valid input).
    const Simd<UnsignedFromSize<d.MaxBytes() / 2>, 2, 0> du;
    return BitCast(d, InterleaveLower(BitCast(du, lo2), BitCast(du, hi2)));
}

// ------------------------------ RearrangeToOddPlusEven (Combine)

template <size_t N> OMNI_API Vec128<float, N> RearrangeToOddPlusEven(Vec128<float, N> sum0, Vec128<float, N> sum1)
{
    return Add(sum0, sum1);
}

OMNI_API Vec128<int32_t> RearrangeToOddPlusEven(Vec128<int32_t> sum0, Vec128<int32_t> sum1)
{
    return Vec128<int32_t>(vpaddq_s32(sum0.raw, sum1.raw));
}

OMNI_API Vec64<int32_t> RearrangeToOddPlusEven(Vec64<int32_t> sum0, Vec64<int32_t> sum1)
{
    // vmlal_s16 multiplied the lower half into sum0 and upper into sum1.
    return Vec64<int32_t>(vpadd_s32(sum0.raw, sum1.raw));
}

OMNI_API Vec32<int32_t> RearrangeToOddPlusEven(Vec32<int32_t> sum0, Vec32<int32_t> sum1)
{
    // Only one widened sum per register, so add them for sum of odd and even.
    return sum0 + sum1;
}

OMNI_API Vec128<uint32_t> RearrangeToOddPlusEven(Vec128<uint32_t> sum0, Vec128<uint32_t> sum1)
{
    return Vec128<uint32_t>(vpaddq_u32(sum0.raw, sum1.raw));
}

OMNI_API Vec64<uint32_t> RearrangeToOddPlusEven(Vec64<uint32_t> sum0, Vec64<uint32_t> sum1)
{
    // vmlal_u16 multiplied the lower half into sum0 and upper into sum1.
    return Vec64<uint32_t>(vpadd_u32(sum0.raw, sum1.raw));
}

OMNI_API Vec32<uint32_t> RearrangeToOddPlusEven(Vec32<uint32_t> sum0, Vec32<uint32_t> sum1)
{
    // Only one widened sum per register, so add them for sum of odd and even.
    return sum0 + sum1;
}

template <class DF, OMNI_IF_F32_D(DF)>
OMNI_API VFromD<DF> WidenMulPairwiseAdd(DF df, VFromD<Repartition<bfloat16_t, DF>> a,
    VFromD<Repartition<bfloat16_t, DF>> b)
{
    return MulAdd(PromoteEvenTo(df, a), PromoteEvenTo(df, b), Mul(PromoteOddTo(df, a), PromoteOddTo(df, b)));
}

template <class D, OMNI_IF_I32_D(D)>
OMNI_API Vec128<int32_t> WidenMulPairwiseAdd(D /* d32 */, Vec128<int16_t> a, Vec128<int16_t> b)
{
    Vec128<int32_t> sum1;
    sum1 = Vec128<int32_t>(vmull_high_s16(a.raw, b.raw));
    Vec128<int32_t> sum0 = Vec128<int32_t>(vmull_s16(LowerHalf(a).raw, LowerHalf(b).raw));
    return RearrangeToOddPlusEven(sum0, sum1);
}

template <class D, OMNI_IF_I32_D(D)>
OMNI_API Vec64<int32_t> WidenMulPairwiseAdd(D d32, Vec64<int16_t> a, Vec64<int16_t> b)
{
    // vmlal writes into the upper half, which the caller cannot use, so
    // split into two halves.
    const Vec128<int32_t> mul_3210(vmull_s16(a.raw, b.raw));
    const Vec64<int32_t> mul0 = LowerHalf(mul_3210);
    const Vec64<int32_t> mul1 = UpperHalf(d32, mul_3210);
    return RearrangeToOddPlusEven(mul0, mul1);
}

template <class D, OMNI_IF_I32_D(D)>
OMNI_API Vec32<int32_t> WidenMulPairwiseAdd(D d32, Vec32<int16_t> a, Vec32<int16_t> b)
{
    const Vec128<int32_t> mul_xx10(vmull_s16(a.raw, b.raw));
    const Vec64<int32_t> mul_10(LowerHalf(mul_xx10));
    const Vec32<int32_t> mul0 = LowerHalf(d32, mul_10);
    const Vec32<int32_t> mul1 = UpperHalf(d32, mul_10);
    return RearrangeToOddPlusEven(mul0, mul1);
}

template <class D, OMNI_IF_U32_D(D)>
OMNI_API Vec128<uint32_t> WidenMulPairwiseAdd(D /* d32 */, Vec128<uint16_t> a, Vec128<uint16_t> b)
{
    Vec128<uint32_t> sum1;
    sum1 = Vec128<uint32_t>(vmull_high_u16(a.raw, b.raw));
    Vec128<uint32_t> sum0 = Vec128<uint32_t>(vmull_u16(LowerHalf(a).raw, LowerHalf(b).raw));
    return RearrangeToOddPlusEven(sum0, sum1);
}

template <class D, OMNI_IF_U32_D(D)>
OMNI_API Vec64<uint32_t> WidenMulPairwiseAdd(D d32, Vec64<uint16_t> a, Vec64<uint16_t> b)
{
    // vmlal writes into the upper half, which the caller cannot use, so
    // split into two halves.
    const Vec128<uint32_t> mul_3210(vmull_u16(a.raw, b.raw));
    const Vec64<uint32_t> mul0 = LowerHalf(mul_3210);
    const Vec64<uint32_t> mul1 = UpperHalf(d32, mul_3210);
    return RearrangeToOddPlusEven(mul0, mul1);
}

template <class D, OMNI_IF_U32_D(D)>
OMNI_API Vec32<uint32_t> WidenMulPairwiseAdd(D d32, Vec32<uint16_t> a, Vec32<uint16_t> b)
{
    const Vec128<uint32_t> mul_xx10(vmull_u16(a.raw, b.raw));
    const Vec64<uint32_t> mul_10(LowerHalf(mul_xx10));
    const Vec32<uint32_t> mul0 = LowerHalf(d32, mul_10);
    const Vec32<uint32_t> mul1 = UpperHalf(d32, mul_10);
    return RearrangeToOddPlusEven(mul0, mul1);
}

// ------------------------------ ZeroExtendVector (Combine)

template <class D> OMNI_API VFromD<D> ZeroExtendVector(D d, VFromD<Half<D>> lo)
{
    return Combine(d, Zero(Half<decltype(d)>()), lo);
}

// ------------------------------ ConcatLowerLower

// 64 or 128-bit input: just interleave
template <class D, OMNI_IF_V_SIZE_GT_D(D, 4)> OMNI_API VFromD<D> ConcatLowerLower(D d, VFromD<D> hi, VFromD<D> lo)
{
    // Treat half-width input as a single lane and interleave them.
    const Repartition<UnsignedFromSize<d.MaxBytes() / 2>, decltype(d)> du;
    return BitCast(d, InterleaveLower(BitCast(du, lo), BitCast(du, hi)));
}

namespace detail {
OMNI_NEON_DEF_FUNCTION_UIF_8_16_32(InterleaveEven, vtrn1, _, 2)

OMNI_NEON_DEF_FUNCTION_UIF_8_16_32(InterleaveOdd, vtrn2, _, 2)
} // namespace detail

// <= 32-bit input/output
template <class D, OMNI_IF_V_SIZE_LE_D(D, 4)> OMNI_API VFromD<D> ConcatLowerLower(D d, VFromD<D> hi, VFromD<D> lo)
{
    // Treat half-width input as two lanes and take every second one.
    const Repartition<UnsignedFromSize<d.MaxBytes() / 2>, decltype(d)> du;
    return BitCast(d, detail::InterleaveEven(BitCast(du, lo), BitCast(du, hi)));
}
// ------------------------------ ConcatLowerUpper (ShiftLeftBytes)

// 64 or 128-bit input: extract from concatenated
template <class D, OMNI_IF_V_SIZE_GT_D(D, 4)> OMNI_API VFromD<D> ConcatLowerUpper(D d, VFromD<D> hi, VFromD<D> lo)
{
    return CombineShiftRightBytes<d.MaxBytes() / 2>(d, hi, lo);
}

// <= 32-bit input/output
template <class D, OMNI_IF_V_SIZE_LE_D(D, 4)> OMNI_API VFromD<D> ConcatLowerUpper(D d, VFromD<D> hi, VFromD<D> lo)
{
    constexpr size_t kSize = d.MaxBytes();
    const Repartition<uint8_t, decltype(d)> d8;
    const Full64<uint8_t> d8x8;
    const Full64<TFromD<D>> d64;
    using V8x8 = VFromD<decltype(d8x8)>;
    const V8x8 hi8x8(BitCast(d8, hi).raw);
    // Move into most-significant bytes
    const V8x8 lo8x8 = ShiftLeftBytes<8 - kSize>(V8x8(BitCast(d8, lo).raw));
    const V8x8 r = CombineShiftRightBytes<8 - kSize / 2>(d8x8, hi8x8, lo8x8);
    // Back to original lane type, then shrink N.
    return VFromD<D>(BitCast(d64, r).raw);
}

// ------------------------------ ConcatUpperLower

// Works for all N.
template <class D> OMNI_API VFromD<D> ConcatUpperLower(D d, VFromD<D> hi, VFromD<D> lo)
{
    return IfThenElse(FirstN(d, Lanes(d) / 2), lo, hi);
}

// ------------------------------ ConcatOdd (InterleaveUpper)

namespace detail {
// There is no vuzpq_u64.
OMNI_NEON_DEF_FUNCTION_UIF_8_16_32(ConcatEven, vuzp1, _, 2)

OMNI_NEON_DEF_FUNCTION_UIF_8_16_32(ConcatOdd, vuzp2, _, 2)

#if !OMNI_HAVE_FLOAT16

template <size_t N> OMNI_INLINE Vec128<float16_t, N> ConcatEven(Vec128<float16_t, N> hi, Vec128<float16_t, N> lo)
{
    const DFromV<decltype(hi)> d;
    const RebindToUnsigned<decltype(d)> du;
    return BitCast(d, ConcatEven(BitCast(du, hi), BitCast(du, lo)));
}

template <size_t N> OMNI_INLINE Vec128<float16_t, N> ConcatOdd(Vec128<float16_t, N> hi, Vec128<float16_t, N> lo)
{
    const DFromV<decltype(hi)> d;
    const RebindToUnsigned<decltype(d)> du;
    return BitCast(d, ConcatOdd(BitCast(du, hi), BitCast(du, lo)));
}

#endif // !OMNI_HAVE_FLOAT16
} // namespace detail

// Full/half vector
template <class D, OMNI_IF_V_SIZE_GT_D(D, 4)> OMNI_API VFromD<D> ConcatOdd(D /* tag */, VFromD<D> hi, VFromD<D> lo)
{
    return detail::ConcatOdd(lo, hi);
}

// 8-bit x4
template <class D, typename T = TFromD<D>, OMNI_IF_T_SIZE(T, 1)>
OMNI_API Vec32<T> ConcatOdd(D d, Vec32<T> hi, Vec32<T> lo)
{
    const Twice<decltype(d)> d2;
    const Repartition<uint16_t, decltype(d2)> dw2;
    const VFromD<decltype(d2)> hi2(hi.raw);
    const VFromD<decltype(d2)> lo2(lo.raw);
    const VFromD<decltype(dw2)> Hx1Lx1 = BitCast(dw2, ConcatOdd(d2, hi2, lo2));
    // Compact into two pairs of u8, skipping the invalid x lanes. Could also use
    // vcopy_lane_u16, but that's A64-only.
    return Vec32<T>(BitCast(d2, ConcatEven(dw2, Hx1Lx1, Hx1Lx1)).raw);
}

// Any type x2
template <class D, OMNI_IF_LANES_D(D, 2), typename T = TFromD<D>>
OMNI_API Vec128<T, 2> ConcatOdd(D d, Vec128<T, 2> hi, Vec128<T, 2> lo)
{
    return InterleaveUpper(d, lo, hi);
}

// ------------------------------ ConcatEven (InterleaveLower)

// Full/half vector
template <class D, OMNI_IF_V_SIZE_GT_D(D, 4)> OMNI_API VFromD<D> ConcatEven(D /* tag */, VFromD<D> hi, VFromD<D> lo)
{
    return detail::ConcatEven(lo, hi);
}

// 8-bit x4
template <class D, typename T = TFromD<D>, OMNI_IF_T_SIZE(T, 1)>
OMNI_API Vec32<T> ConcatEven(D d, Vec32<T> hi, Vec32<T> lo)
{
    const Twice<decltype(d)> d2;
    const Repartition<uint16_t, decltype(d2)> dw2;
    const VFromD<decltype(d2)> hi2(hi.raw);
    const VFromD<decltype(d2)> lo2(lo.raw);
    const VFromD<decltype(dw2)> Hx0Lx0 = BitCast(dw2, ConcatEven(d2, hi2, lo2));
    // Compact into two pairs of u8, skipping the invalid x lanes. Could also use
    // vcopy_lane_u16, but that's A64-only.
    return Vec32<T>(BitCast(d2, ConcatEven(dw2, Hx0Lx0, Hx0Lx0)).raw);
}

// Any type x2
template <class D, OMNI_IF_LANES_D(D, 2), typename T = TFromD<D>>
OMNI_API Vec128<T, 2> ConcatEven(D d, Vec128<T, 2> hi, Vec128<T, 2> lo)
{
    return InterleaveLower(d, lo, hi);
}

// ------------------------------ DupEven (InterleaveLower)

template <typename T, size_t N, OMNI_IF_T_SIZE_ONE_OF(T, (1 << 1) | (1 << 2) | (1 << 4))>
OMNI_API Vec128<T, N> DupEven(Vec128<T, N> v)
{
    return detail::InterleaveEven(v, v);
}

template <typename T, size_t N, OMNI_IF_T_SIZE(T, 8)> OMNI_API Vec128<T, N> DupEven(Vec128<T, N> v)
{
    return InterleaveLower(DFromV<decltype(v)>(), v, v);
}

// ------------------------------ DupOdd (InterleaveUpper)

template <typename T, size_t N, OMNI_IF_T_SIZE_ONE_OF(T, (1 << 1) | (1 << 2) | (1 << 4))>
OMNI_API Vec128<T, N> DupOdd(Vec128<T, N> v)
{
    return detail::InterleaveOdd(v, v);
}

template <typename T, size_t N, OMNI_IF_T_SIZE(T, 8)> OMNI_API Vec128<T, N> DupOdd(Vec128<T, N> v)
{
    return InterleaveUpper(DFromV<decltype(v)>(), v, v);
}

// ------------------------------ OddEven (IfThenElse)

template <typename T, size_t N> OMNI_API Vec128<T, N> OddEven(const Vec128<T, N> a, const Vec128<T, N> b)
{
    const DFromV<decltype(a)> d;
    const Repartition<uint8_t, decltype(d)> d8;
    alignas(16) static constexpr uint8_t kBytes[16] = { ((0 / sizeof(T)) & 1) ? 0 : 0xFF,
            ((1 / sizeof(T)) & 1) ? 0 : 0xFF, ((2 / sizeof(T)) & 1) ? 0 : 0xFF, ((3 / sizeof(T)) & 1) ? 0 : 0xFF,
            ((4 / sizeof(T)) & 1) ? 0 : 0xFF, ((5 / sizeof(T)) & 1) ? 0 : 0xFF, ((6 / sizeof(T)) & 1) ? 0 : 0xFF,
            ((7 / sizeof(T)) & 1) ? 0 : 0xFF, ((8 / sizeof(T)) & 1) ? 0 : 0xFF, ((9 / sizeof(T)) & 1) ? 0 : 0xFF,
            ((10 / sizeof(T)) & 1) ? 0 : 0xFF, ((11 / sizeof(T)) & 1) ? 0 : 0xFF, ((12 / sizeof(T)) & 1) ? 0 : 0xFF,
            ((13 / sizeof(T)) & 1) ? 0 : 0xFF, ((14 / sizeof(T)) & 1) ? 0 : 0xFF, ((15 / sizeof(T)) & 1) ? 0 : 0xFF, };
    const auto vec = BitCast(d, Load(d8, kBytes));
    return IfThenElse(MaskFromVec(vec), b, a);
}

// ------------------------------ InterleaveEven
template <class D, OMNI_IF_T_SIZE_ONE_OF_D(D, (1 << 1) | (1 << 2) | (1 << 4))>
OMNI_API VFromD<D> InterleaveEven(D /* d */, VFromD<D> a, VFromD<D> b)
{
    return detail::InterleaveEven(a, b);
}

template <class D, OMNI_IF_T_SIZE_D(D, 8)> OMNI_API VFromD<D> InterleaveEven(D /* d */, VFromD<D> a, VFromD<D> b)
{
    return InterleaveLower(a, b);
}

// ------------------------------ InterleaveOdd
template <class D, OMNI_IF_T_SIZE_ONE_OF_D(D, (1 << 1) | (1 << 2) | (1 << 4))>
OMNI_API VFromD<D> InterleaveOdd(D /* d */, VFromD<D> a, VFromD<D> b)
{
    return detail::InterleaveOdd(a, b);
}

template <class D, OMNI_IF_T_SIZE_D(D, 8)> OMNI_API VFromD<D> InterleaveOdd(D d, VFromD<D> a, VFromD<D> b)
{
    return InterleaveUpper(d, a, b);
}

// ------------------------------ OddEvenBlocks
template <typename T, size_t N> OMNI_API Vec128<T, N> OddEvenBlocks(Vec128<T, N> /* odd */, Vec128<T, N> even)
{
    return even;
}

// ------------------------------ SwapAdjacentBlocks
template <typename T, size_t N> OMNI_API Vec128<T, N> SwapAdjacentBlocks(Vec128<T, N> v)
{
    return v;
}

// ------------------------------ ReverseBlocks
// Single block: no change
template <class D, OMNI_IF_V_SIZE_LE_D(D, 16)> OMNI_API VFromD<D> ReverseBlocks(D /* tag */, VFromD<D> v)
{
    return v;
}

template <class D, OMNI_IF_I32_D(D)>
OMNI_API Vec128<int32_t> ReorderDemote2To(D d32, Vec128<int64_t> a, Vec128<int64_t> b)
{
    const Vec64<int32_t> a32(vqmovn_s64(a.raw));
    (void)d32;
    return Vec128<int32_t>(vqmovn_high_s64(a32.raw, b.raw));
}

template <class D, OMNI_IF_I32_D(D), OMNI_IF_V_SIZE_LE_D(D, 8)>
OMNI_API VFromD<D> ReorderDemote2To(D d32, VFromD<Repartition<int64_t, D>> a, VFromD<Repartition<int64_t, D>> b)
{
    const Rebind<int64_t, decltype(d32)> dt;
    return DemoteTo(d32, Combine(dt, b, a));
}

template <class D, OMNI_IF_U32_D(D)>
OMNI_API Vec128<uint32_t> ReorderDemote2To(D d32, Vec128<int64_t> a, Vec128<int64_t> b)
{
    const Vec64<uint32_t> a32(vqmovun_s64(a.raw));
    (void)d32;
    return Vec128<uint32_t>(vqmovun_high_s64(a32.raw, b.raw));
}

template <class D, OMNI_IF_U32_D(D), OMNI_IF_V_SIZE_LE_D(D, 8)>
OMNI_API VFromD<D> ReorderDemote2To(D d32, VFromD<Repartition<int64_t, D>> a, VFromD<Repartition<int64_t, D>> b)
{
    const Rebind<int64_t, decltype(d32)> dt;
    return DemoteTo(d32, Combine(dt, b, a));
}

template <class D, OMNI_IF_U32_D(D)>
OMNI_API Vec128<uint32_t> ReorderDemote2To(D d32, Vec128<uint64_t> a, Vec128<uint64_t> b)
{
    const Vec64<uint32_t> a32(vqmovn_u64(a.raw));
    (void)d32;
    return Vec128<uint32_t>(vqmovn_high_u64(a32.raw, b.raw));
}

template <class D, OMNI_IF_U32_D(D), OMNI_IF_V_SIZE_LE_D(D, 8)>
OMNI_API VFromD<D> ReorderDemote2To(D d32, VFromD<Repartition<uint64_t, D>> a, VFromD<Repartition<uint64_t, D>> b)
{
    const Rebind<uint64_t, decltype(d32)> dt;
    return DemoteTo(d32, Combine(dt, b, a));
}

template <class D, OMNI_IF_I16_D(D)>
OMNI_API Vec128<int16_t> ReorderDemote2To(D d16, Vec128<int32_t> a, Vec128<int32_t> b)
{
    const Vec64<int16_t> a16(vqmovn_s32(a.raw));
    (void)d16;
    return Vec128<int16_t>(vqmovn_high_s32(a16.raw, b.raw));
}

template <class D, OMNI_IF_I16_D(D)>
OMNI_API Vec64<int16_t> ReorderDemote2To(D /* d16 */, Vec64<int32_t> a, Vec64<int32_t> b)
{
    const Full128<int32_t> d32;
    const Vec128<int32_t> ab = Combine(d32, b, a);
    return Vec64<int16_t>(vqmovn_s32(ab.raw));
}

template <class D, OMNI_IF_I16_D(D)>
OMNI_API Vec32<int16_t> ReorderDemote2To(D /* d16 */, Vec32<int32_t> a, Vec32<int32_t> b)
{
    const Full128<int32_t> d32;
    const Vec64<int32_t> ab(vzip1_s32(a.raw, b.raw));
    return Vec32<int16_t>(vqmovn_s32(Combine(d32, ab, ab).raw));
}

template <class D, OMNI_IF_U16_D(D)>
OMNI_API Vec128<uint16_t> ReorderDemote2To(D d16, Vec128<int32_t> a, Vec128<int32_t> b)
{
    const Vec64<uint16_t> a16(vqmovun_s32(a.raw));
    (void)d16;
    return Vec128<uint16_t>(vqmovun_high_s32(a16.raw, b.raw));
}

template <class D, OMNI_IF_U16_D(D)>
OMNI_API Vec64<uint16_t> ReorderDemote2To(D /* d16 */, Vec64<int32_t> a, Vec64<int32_t> b)
{
    const Full128<int32_t> d32;
    const Vec128<int32_t> ab = Combine(d32, b, a);
    return Vec64<uint16_t>(vqmovun_s32(ab.raw));
}

template <class D, OMNI_IF_U16_D(D)>
OMNI_API Vec32<uint16_t> ReorderDemote2To(D /* d16 */, Vec32<int32_t> a, Vec32<int32_t> b)
{
    const Full128<int32_t> d32;
    const Vec64<int32_t> ab(vzip1_s32(a.raw, b.raw));
    return Vec32<uint16_t>(vqmovun_s32(Combine(d32, ab, ab).raw));
}

template <class D, OMNI_IF_U16_D(D)>
OMNI_API Vec128<uint16_t> ReorderDemote2To(D d16, Vec128<uint32_t> a, Vec128<uint32_t> b)
{
    const Vec64<uint16_t> a16(vqmovn_u32(a.raw));
    (void)d16;
    return Vec128<uint16_t>(vqmovn_high_u32(a16.raw, b.raw));
}

template <class D, OMNI_IF_U16_D(D)>
OMNI_API Vec64<uint16_t> ReorderDemote2To(D /* d16 */, Vec64<uint32_t> a, Vec64<uint32_t> b)
{
    const Full128<uint32_t> d32;
    const Vec128<uint32_t> ab = Combine(d32, b, a);
    return Vec64<uint16_t>(vqmovn_u32(ab.raw));
}

template <class D, OMNI_IF_U16_D(D)>
OMNI_API Vec32<uint16_t> ReorderDemote2To(D /* d16 */, Vec32<uint32_t> a, Vec32<uint32_t> b)
{
    const Full128<uint32_t> d32;
    const Vec64<uint32_t> ab(vzip1_u32(a.raw, b.raw));
    return Vec32<uint16_t>(vqmovn_u32(Combine(d32, ab, ab).raw));
}

template <class D, OMNI_IF_I8_D(D)> OMNI_API Vec128<int8_t> ReorderDemote2To(D d8, Vec128<int16_t> a, Vec128<int16_t> b)
{
    const Vec64<int8_t> a8(vqmovn_s16(a.raw));
    (void)d8;
    return Vec128<int8_t>(vqmovn_high_s16(a8.raw, b.raw));
}

template <class D, OMNI_IF_I8_D(D), OMNI_IF_V_SIZE_LE_D(D, 8)>
OMNI_API VFromD<D> ReorderDemote2To(D d8, VFromD<Repartition<int16_t, D>> a, VFromD<Repartition<int16_t, D>> b)
{
    const Rebind<int16_t, decltype(d8)> dt;
    return DemoteTo(d8, Combine(dt, b, a));
}

template <class D, OMNI_IF_U8_D(D)>
OMNI_API Vec128<uint8_t> ReorderDemote2To(D d8, Vec128<int16_t> a, Vec128<int16_t> b)
{
    const Vec64<uint8_t> a8(vqmovun_s16(a.raw));
    (void)d8;
    return Vec128<uint8_t>(vqmovun_high_s16(a8.raw, b.raw));
}

template <class D, OMNI_IF_U8_D(D), OMNI_IF_V_SIZE_LE_D(D, 8)>
OMNI_API VFromD<D> ReorderDemote2To(D d8, VFromD<Repartition<int16_t, D>> a, VFromD<Repartition<int16_t, D>> b)
{
    const Rebind<int16_t, decltype(d8)> dt;
    return DemoteTo(d8, Combine(dt, b, a));
}

template <class D, OMNI_IF_U8_D(D)>
OMNI_API Vec128<uint8_t> ReorderDemote2To(D d8, Vec128<uint16_t> a, Vec128<uint16_t> b)
{
    const Vec64<uint8_t> a8(vqmovn_u16(a.raw));
    (void)d8;
    return Vec128<uint8_t>(vqmovn_high_u16(a8.raw, b.raw));
}

template <class D, OMNI_IF_U8_D(D), OMNI_IF_V_SIZE_LE_D(D, 8)>
OMNI_API VFromD<D> ReorderDemote2To(D d8, VFromD<Repartition<uint16_t, D>> a, VFromD<Repartition<uint16_t, D>> b)
{
    const Rebind<uint16_t, decltype(d8)> dt;
    return DemoteTo(d8, Combine(dt, b, a));
}

template <class D, class V, OMNI_IF_NOT_FLOAT_NOR_SPECIAL_V(V), OMNI_IF_NOT_FLOAT_NOR_SPECIAL(TFromD<D>),
    OMNI_IF_LANES_D(D, OMNI_MAX_LANES_D(DFromV<V>) * 2)>
OMNI_API VFromD<D> OrderedDemote2To(D d, V a, V b)
{
    return ReorderDemote2To(d, a, b);
}

template <class D, OMNI_IF_F32_D(D)> OMNI_API VFromD<D> PromoteTo(D df32, VFromD<Rebind<bfloat16_t, D>> v)
{
    const Rebind<uint16_t, decltype(df32)> du16;
    const RebindToSigned<decltype(df32)> di32;
    return BitCast(df32, ShiftLeft<16>(PromoteTo(di32, BitCast(du16, v))));
}

// ------------------------------ Truncations

template <class DTo, typename TTo = TFromD<DTo>, typename TFrom, OMNI_IF_UNSIGNED(TFrom), OMNI_IF_UNSIGNED(TTo),
    simd::EnableIf<(sizeof(TTo) < sizeof(TFrom))> * = nullptr>
OMNI_API Vec128<TTo, 1> TruncateTo(DTo /* tag */, Vec128<TFrom, 1> v)
{
    const Repartition<TTo, DFromV<decltype(v)>> d;
    return Vec128<TTo, 1>{ BitCast(d, v).raw };
}

template <class D, OMNI_IF_U8_D(D)> OMNI_API Vec16<uint8_t> TruncateTo(D /* tag */, Vec128<uint64_t> v)
{
    const Repartition<uint8_t, DFromV<decltype(v)>> d;
    const auto v1 = BitCast(d, v);
    const auto v2 = detail::ConcatEven(v1, v1);
    const auto v3 = detail::ConcatEven(v2, v2);
    const auto v4 = detail::ConcatEven(v3, v3);
    return LowerHalf(LowerHalf(LowerHalf(v4)));
}

template <class D, OMNI_IF_U16_D(D)> OMNI_API Vec32<uint16_t> TruncateTo(D /* tag */, Vec128<uint64_t> v)
{
    const Repartition<uint16_t, DFromV<decltype(v)>> d;
    const auto v1 = BitCast(d, v);
    const auto v2 = detail::ConcatEven(v1, v1);
    const auto v3 = detail::ConcatEven(v2, v2);
    return LowerHalf(LowerHalf(v3));
}

template <class D, OMNI_IF_U32_D(D)> OMNI_API Vec64<uint32_t> TruncateTo(D /* tag */, Vec128<uint64_t> v)
{
    const Repartition<uint32_t, DFromV<decltype(v)>> d;
    const auto v1 = BitCast(d, v);
    const auto v2 = detail::ConcatEven(v1, v1);
    return LowerHalf(v2);
}

template <class D, OMNI_IF_U8_D(D), OMNI_IF_LANES_GT_D(D, 1)>
OMNI_API VFromD<D> TruncateTo(D /* tag */, VFromD<Rebind<uint32_t, D>> v)
{
    const Repartition<uint8_t, DFromV<decltype(v)>> d;
    const auto v1 = BitCast(d, v);
    const auto v2 = detail::ConcatEven(v1, v1);
    const auto v3 = detail::ConcatEven(v2, v2);
    return LowerHalf(LowerHalf(v3));
}

template <class D, OMNI_IF_U16_D(D), OMNI_IF_LANES_GT_D(D, 1)>
OMNI_API VFromD<D> TruncateTo(D /* tag */, VFromD<Rebind<uint32_t, D>> v)
{
    const Repartition<uint16_t, DFromV<decltype(v)>> d;
    const auto v1 = BitCast(d, v);
    const auto v2 = detail::ConcatEven(v1, v1);
    return LowerHalf(v2);
}

template <class D, OMNI_IF_U8_D(D), OMNI_IF_LANES_GT_D(D, 1)>
OMNI_API VFromD<D> TruncateTo(D /* tag */, VFromD<Rebind<uint16_t, D>> v)
{
    const Repartition<uint8_t, DFromV<decltype(v)>> d;
    const auto v1 = BitCast(d, v);
    const auto v2 = detail::ConcatEven(v1, v1);
    return LowerHalf(v2);
}

OMNI_API Vec128<int16_t> MulEven(Vec128<int8_t> a, Vec128<int8_t> b)
{
    const DFromV<decltype(a)> d;
    int8x16_t a_packed = ConcatEven(d, a, a).raw;
    int8x16_t b_packed = ConcatEven(d, b, b).raw;
    return Vec128<int16_t>(vmull_s8(vget_low_s8(a_packed), vget_low_s8(b_packed)));
}

OMNI_API Vec128<uint16_t> MulEven(Vec128<uint8_t> a, Vec128<uint8_t> b)
{
    const DFromV<decltype(a)> d;
    uint8x16_t a_packed = ConcatEven(d, a, a).raw;
    uint8x16_t b_packed = ConcatEven(d, b, b).raw;
    return Vec128<uint16_t>(vmull_u8(vget_low_u8(a_packed), vget_low_u8(b_packed)));
}

OMNI_API Vec128<int32_t> MulEven(Vec128<int16_t> a, Vec128<int16_t> b)
{
    const DFromV<decltype(a)> d;
    int16x8_t a_packed = ConcatEven(d, a, a).raw;
    int16x8_t b_packed = ConcatEven(d, b, b).raw;
    return Vec128<int32_t>(vmull_s16(vget_low_s16(a_packed), vget_low_s16(b_packed)));
}

OMNI_API Vec128<uint32_t> MulEven(Vec128<uint16_t> a, Vec128<uint16_t> b)
{
    const DFromV<decltype(a)> d;
    uint16x8_t a_packed = ConcatEven(d, a, a).raw;
    uint16x8_t b_packed = ConcatEven(d, b, b).raw;
    return Vec128<uint32_t>(vmull_u16(vget_low_u16(a_packed), vget_low_u16(b_packed)));
}

OMNI_API Vec128<int64_t> MulEven(Vec128<int32_t> a, Vec128<int32_t> b)
{
    const DFromV<decltype(a)> d;
    int32x4_t a_packed = ConcatEven(d, a, a).raw;
    int32x4_t b_packed = ConcatEven(d, b, b).raw;
    return Vec128<int64_t>(vmull_s32(vget_low_s32(a_packed), vget_low_s32(b_packed)));
}

OMNI_API Vec128<uint64_t> MulEven(Vec128<uint32_t> a, Vec128<uint32_t> b)
{
    const DFromV<decltype(a)> d;
    uint32x4_t a_packed = ConcatEven(d, a, a).raw;
    uint32x4_t b_packed = ConcatEven(d, b, b).raw;
    return Vec128<uint64_t>(vmull_u32(vget_low_u32(a_packed), vget_low_u32(b_packed)));
}

template <size_t N> OMNI_API Vec128<int16_t, (N + 1) / 2> MulEven(Vec128<int8_t, N> a, Vec128<int8_t, N> b)
{
    const DFromV<decltype(a)> d;
    int8x8_t a_packed = ConcatEven(d, a, a).raw;
    int8x8_t b_packed = ConcatEven(d, b, b).raw;
    return Vec128<int16_t, (N + 1) / 2>(vget_low_s16(vmull_s8(a_packed, b_packed)));
}

template <size_t N> OMNI_API Vec128<uint16_t, (N + 1) / 2> MulEven(Vec128<uint8_t, N> a, Vec128<uint8_t, N> b)
{
    const DFromV<decltype(a)> d;
    uint8x8_t a_packed = ConcatEven(d, a, a).raw;
    uint8x8_t b_packed = ConcatEven(d, b, b).raw;
    return Vec128<uint16_t, (N + 1) / 2>(vget_low_u16(vmull_u8(a_packed, b_packed)));
}

template <size_t N> OMNI_API Vec128<int32_t, (N + 1) / 2> MulEven(Vec128<int16_t, N> a, Vec128<int16_t, N> b)
{
    const DFromV<decltype(a)> d;
    int16x4_t a_packed = ConcatEven(d, a, a).raw;
    int16x4_t b_packed = ConcatEven(d, b, b).raw;
    return Vec128<int32_t, (N + 1) / 2>(vget_low_s32(vmull_s16(a_packed, b_packed)));
}

template <size_t N> OMNI_API Vec128<uint32_t, (N + 1) / 2> MulEven(Vec128<uint16_t, N> a, Vec128<uint16_t, N> b)
{
    const DFromV<decltype(a)> d;
    uint16x4_t a_packed = ConcatEven(d, a, a).raw;
    uint16x4_t b_packed = ConcatEven(d, b, b).raw;
    return Vec128<uint32_t, (N + 1) / 2>(vget_low_u32(vmull_u16(a_packed, b_packed)));
}

template <size_t N> OMNI_API Vec128<int64_t, (N + 1) / 2> MulEven(Vec128<int32_t, N> a, Vec128<int32_t, N> b)
{
    const DFromV<decltype(a)> d;
    int32x2_t a_packed = ConcatEven(d, a, a).raw;
    int32x2_t b_packed = ConcatEven(d, b, b).raw;
    return Vec128<int64_t, (N + 1) / 2>(vget_low_s64(vmull_s32(a_packed, b_packed)));
}

template <size_t N> OMNI_API Vec128<uint64_t, (N + 1) / 2> MulEven(Vec128<uint32_t, N> a, Vec128<uint32_t, N> b)
{
    const DFromV<decltype(a)> d;
    uint32x2_t a_packed = ConcatEven(d, a, a).raw;
    uint32x2_t b_packed = ConcatEven(d, b, b).raw;
    return Vec128<uint64_t, (N + 1) / 2>(vget_low_u64(vmull_u32(a_packed, b_packed)));
}

template <class T, OMNI_IF_UI64(T)> OMNI_INLINE Vec128<T> MulEven(Vec128<T> a, Vec128<T> b)
{
    T hi;
    T lo = Mul128(GetLane(a), GetLane(b), &hi);
    return Dup128VecFromValues(Full128<T>(), lo, hi);
}

OMNI_API Vec128<int16_t> MulOdd(Vec128<int8_t> a, Vec128<int8_t> b)
{
    const DFromV<decltype(a)> d;
    int8x16_t a_packed = ConcatOdd(d, a, a).raw;
    int8x16_t b_packed = ConcatOdd(d, b, b).raw;
    return Vec128<int16_t>(vmull_s8(vget_low_s8(a_packed), vget_low_s8(b_packed)));
}

OMNI_API Vec128<uint16_t> MulOdd(Vec128<uint8_t> a, Vec128<uint8_t> b)
{
    const DFromV<decltype(a)> d;
    uint8x16_t a_packed = ConcatOdd(d, a, a).raw;
    uint8x16_t b_packed = ConcatOdd(d, b, b).raw;
    return Vec128<uint16_t>(vmull_u8(vget_low_u8(a_packed), vget_low_u8(b_packed)));
}

OMNI_API Vec128<int32_t> MulOdd(Vec128<int16_t> a, Vec128<int16_t> b)
{
    const DFromV<decltype(a)> d;
    int16x8_t a_packed = ConcatOdd(d, a, a).raw;
    int16x8_t b_packed = ConcatOdd(d, b, b).raw;
    return Vec128<int32_t>(vmull_s16(vget_low_s16(a_packed), vget_low_s16(b_packed)));
}

OMNI_API Vec128<uint32_t> MulOdd(Vec128<uint16_t> a, Vec128<uint16_t> b)
{
    const DFromV<decltype(a)> d;
    uint16x8_t a_packed = ConcatOdd(d, a, a).raw;
    uint16x8_t b_packed = ConcatOdd(d, b, b).raw;
    return Vec128<uint32_t>(vmull_u16(vget_low_u16(a_packed), vget_low_u16(b_packed)));
}

OMNI_API Vec128<int64_t> MulOdd(Vec128<int32_t> a, Vec128<int32_t> b)
{
    const DFromV<decltype(a)> d;
    int32x4_t a_packed = ConcatOdd(d, a, a).raw;
    int32x4_t b_packed = ConcatOdd(d, b, b).raw;
    return Vec128<int64_t>(vmull_s32(vget_low_s32(a_packed), vget_low_s32(b_packed)));
}

OMNI_API Vec128<uint64_t> MulOdd(Vec128<uint32_t> a, Vec128<uint32_t> b)
{
    const DFromV<decltype(a)> d;
    uint32x4_t a_packed = ConcatOdd(d, a, a).raw;
    uint32x4_t b_packed = ConcatOdd(d, b, b).raw;
    return Vec128<uint64_t>(vmull_u32(vget_low_u32(a_packed), vget_low_u32(b_packed)));
}

template <size_t N> OMNI_API Vec128<int16_t, (N + 1) / 2> MulOdd(Vec128<int8_t, N> a, Vec128<int8_t, N> b)
{
    const DFromV<decltype(a)> d;
    int8x8_t a_packed = ConcatOdd(d, a, a).raw;
    int8x8_t b_packed = ConcatOdd(d, b, b).raw;
    return Vec128<int16_t, (N + 1) / 2>(vget_low_s16(vmull_s8(a_packed, b_packed)));
}

template <size_t N> OMNI_API Vec128<uint16_t, (N + 1) / 2> MulOdd(Vec128<uint8_t, N> a, Vec128<uint8_t, N> b)
{
    const DFromV<decltype(a)> d;
    uint8x8_t a_packed = ConcatOdd(d, a, a).raw;
    uint8x8_t b_packed = ConcatOdd(d, b, b).raw;
    return Vec128<uint16_t, (N + 1) / 2>(vget_low_u16(vmull_u8(a_packed, b_packed)));
}

template <size_t N> OMNI_API Vec128<int32_t, (N + 1) / 2> MulOdd(Vec128<int16_t, N> a, Vec128<int16_t, N> b)
{
    const DFromV<decltype(a)> d;
    int16x4_t a_packed = ConcatOdd(d, a, a).raw;
    int16x4_t b_packed = ConcatOdd(d, b, b).raw;
    return Vec128<int32_t, (N + 1) / 2>(vget_low_s32(vmull_s16(a_packed, b_packed)));
}

template <size_t N> OMNI_API Vec128<uint32_t, (N + 1) / 2> MulOdd(Vec128<uint16_t, N> a, Vec128<uint16_t, N> b)
{
    const DFromV<decltype(a)> d;
    uint16x4_t a_packed = ConcatOdd(d, a, a).raw;
    uint16x4_t b_packed = ConcatOdd(d, b, b).raw;
    return Vec128<uint32_t, (N + 1) / 2>(vget_low_u32(vmull_u16(a_packed, b_packed)));
}

template <size_t N> OMNI_API Vec128<int64_t, (N + 1) / 2> MulOdd(Vec128<int32_t, N> a, Vec128<int32_t, N> b)
{
    const DFromV<decltype(a)> d;
    int32x2_t a_packed = ConcatOdd(d, a, a).raw;
    int32x2_t b_packed = ConcatOdd(d, b, b).raw;
    return Vec128<int64_t, (N + 1) / 2>(vget_low_s64(vmull_s32(a_packed, b_packed)));
}

template <size_t N> OMNI_API Vec128<uint64_t, (N + 1) / 2> MulOdd(Vec128<uint32_t, N> a, Vec128<uint32_t, N> b)
{
    const DFromV<decltype(a)> d;
    uint32x2_t a_packed = ConcatOdd(d, a, a).raw;
    uint32x2_t b_packed = ConcatOdd(d, b, b).raw;
    return Vec128<uint64_t, (N + 1) / 2>(vget_low_u64(vmull_u32(a_packed, b_packed)));
}

template <class T, OMNI_IF_UI64(T)> OMNI_INLINE Vec128<T> MulOdd(Vec128<T> a, Vec128<T> b)
{
    T hi;
    T lo = Mul128(detail::GetLane<1>(a), detail::GetLane<1>(b), &hi);
    return Dup128VecFromValues(Full128<T>(), lo, hi);
}

template <typename T, typename TI> OMNI_API Vec128<TI> TableLookupBytes(Vec128<T> bytes, Vec128<TI> from)
{
    const DFromV<decltype(from)> d;
    const Repartition<uint8_t, decltype(d)> d8;
    return BitCast(d, Vec128<uint8_t>(vqtbl1q_u8(BitCast(d8, bytes).raw, BitCast(d8, from).raw)));
}

// Partial index vector
template <typename T, typename TI, size_t NI, OMNI_IF_V_SIZE_LE(TI, NI, 8)>
OMNI_API Vec128<TI, NI> TableLookupBytes(Vec128<T> bytes, Vec128<TI, NI> from)
{
    const Full128<TI> d_full;
    const Vec64<TI> from64(from.raw);
    const auto idx_full = Combine(d_full, from64, from64);
    const auto out_full = TableLookupBytes(bytes, idx_full);
    return Vec128<TI, NI>(LowerHalf(Half<decltype(d_full)>(), out_full).raw);
}

// Partial table vector
template <typename T, size_t N, typename TI, OMNI_IF_V_SIZE_LE(T, N, 8)>
OMNI_API Vec128<TI> TableLookupBytes(Vec128<T, N> bytes, Vec128<TI> from)
{
    const Full128<T> d_full;
    return TableLookupBytes(Combine(d_full, bytes, bytes), from);
}

// Partial both
template <typename T, size_t N, typename TI, size_t NI, OMNI_IF_V_SIZE_LE(T, N, 8), OMNI_IF_V_SIZE_LE(TI, NI, 8)>
OMNI_API Vec128<TI, NI> TableLookupBytes(Vec128<T, N> bytes, Vec128<TI, NI> from)
{
    const DFromV<decltype(bytes)> d;
    const Simd<TI, NI, 0> d_idx;
    const Repartition<uint8_t, decltype(d_idx)> d_idx8;
    // uint8x8
    const auto bytes8 = BitCast(Repartition<uint8_t, decltype(d)>(), bytes);
    const auto from8 = BitCast(d_idx8, from);
    const VFromD<decltype(d_idx8)> v8(vtbl1_u8(bytes8.raw, from8.raw));
    return BitCast(d_idx, v8);
}

// For all vector widths; Arm anyway zeroes if >= 0x10.
template <class V, class VI> OMNI_API VI TableLookupBytesOr0(V bytes, VI from)
{
    return TableLookupBytes(bytes, from);
}

#if OMNI_ARCH_ARM_A64

#ifdef OMNI_NATIVE_REDUCE_SCALAR
#undef OMNI_NATIVE_REDUCE_SCALAR
#else
#define OMNI_NATIVE_REDUCE_SCALAR
#endif

#define OMNI_NEON_DEF_REDUCTION(type, size, name, prefix, infix, suffix)                                       \
    template <class D, OMNI_IF_LANES_D(D, size)> OMNI_API type##_t name(D /* tag */, Vec128<type##_t, size> v) \
    {                                                                                                          \
        return OMNI_NEON_EVAL(prefix##infix##suffix, v.raw);                                                   \
    }

// Excludes u64/s64 (missing minv/maxv) and f16 (missing addv).
#define OMNI_NEON_DEF_REDUCTION_CORE_TYPES(name, prefix)         \
    OMNI_NEON_DEF_REDUCTION(uint8, 8, name, prefix, _, u8)       \
    OMNI_NEON_DEF_REDUCTION(uint8, 16, name, prefix##q, _, u8)   \
    OMNI_NEON_DEF_REDUCTION(uint16, 4, name, prefix, _, u16)     \
    OMNI_NEON_DEF_REDUCTION(uint16, 8, name, prefix##q, _, u16)  \
    OMNI_NEON_DEF_REDUCTION(uint32, 2, name, prefix, _, u32)     \
    OMNI_NEON_DEF_REDUCTION(uint32, 4, name, prefix##q, _, u32)  \
    OMNI_NEON_DEF_REDUCTION(int8, 8, name, prefix, _, s8)        \
    OMNI_NEON_DEF_REDUCTION(int8, 16, name, prefix##q, _, s8)    \
    OMNI_NEON_DEF_REDUCTION(int16, 4, name, prefix, _, s16)      \
    OMNI_NEON_DEF_REDUCTION(int16, 8, name, prefix##q, _, s16)   \
    OMNI_NEON_DEF_REDUCTION(int32, 2, name, prefix, _, s32)      \
    OMNI_NEON_DEF_REDUCTION(int32, 4, name, prefix##q, _, s32)   \
    OMNI_NEON_DEF_REDUCTION(float32, 2, name, prefix, _, f32)    \
    OMNI_NEON_DEF_REDUCTION(float32, 4, name, prefix##q, _, f32) \
    OMNI_NEON_DEF_REDUCTION(float64, 2, name, prefix##q, _, f64)

// Different interface than OMNI_NEON_DEF_FUNCTION_FULL_UI_64.
#define OMNI_NEON_DEF_REDUCTION_UI64(name, prefix)              \
    OMNI_NEON_DEF_REDUCTION(uint64, 2, name, prefix##q, _, u64) \
    OMNI_NEON_DEF_REDUCTION(int64, 2, name, prefix##q, _, s64)

#define OMNI_NEON_DEF_REDUCTION_F16(name, prefix)

OMNI_NEON_DEF_REDUCTION_CORE_TYPES(ReduceMin, vminv)

OMNI_NEON_DEF_REDUCTION_CORE_TYPES(ReduceMax, vmaxv)

OMNI_NEON_DEF_REDUCTION_F16(ReduceMin, vminv)
OMNI_NEON_DEF_REDUCTION_F16(ReduceMax, vmaxv)

OMNI_NEON_DEF_REDUCTION_CORE_TYPES(ReduceSum, vaddv)

OMNI_NEON_DEF_REDUCTION_UI64(ReduceSum, vaddv)

// Emulate missing UI64 and partial N=2.
template <class D, OMNI_IF_LANES_D(D, 2), OMNI_IF_T_SIZE_ONE_OF_D(D, (1 << 1) | (1 << 2))>
OMNI_API TFromD<D> ReduceSum(D /* tag */, VFromD<D> v10)
{
    return GetLane(v10) + ExtractLane(v10, 1);
}

template <class D, OMNI_IF_LANES_D(D, 2), OMNI_IF_NOT_FLOAT_D(D),
    OMNI_IF_T_SIZE_ONE_OF_D(D, (1 << 1) | (1 << 2) | (1 << 8))>
OMNI_API TFromD<D> ReduceMin(D /* tag */, VFromD<D> v10)
{
    return OMNI_MIN(GetLane(v10), ExtractLane(v10, 1));
}

template <class D, OMNI_IF_LANES_D(D, 2), OMNI_IF_NOT_FLOAT_D(D),
    OMNI_IF_T_SIZE_ONE_OF_D(D, (1 << 1) | (1 << 2) | (1 << 8))>
OMNI_API TFromD<D> ReduceMax(D /* tag */, VFromD<D> v10)
{
    return OMNI_MAX(GetLane(v10), ExtractLane(v10, 1));
}

#undef OMNI_NEON_DEF_REDUCTION_CORE_TYPES
#undef OMNI_NEON_DEF_REDUCTION_F16
#undef OMNI_NEON_DEF_REDUCTION_UI64
#undef OMNI_NEON_DEF_REDUCTION

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

#else // !OMNI_ARCH_ARM_A64


#endif // OMNI_ARCH_ARM_A64

// ------------------------------ LoadMaskBits (TestBit)

namespace detail {
// Helper function to set 64 bits and potentially return a smaller vector. The
// overload is required to call the q vs non-q intrinsics. Note that 8-bit
// LoadMaskBits only requires 16 bits, but 64 avoids casting.
template <class D, OMNI_IF_V_SIZE_LE_D(D, 8)> OMNI_INLINE VFromD<D> Set64(D /* tag */, uint64_t mask_bits)
{
    const auto v64 = Vec64<uint64_t>(vdup_n_u64(mask_bits));
    return VFromD<D>(BitCast(Full64<TFromD<D>>(), v64).raw);
}

template <typename T> OMNI_INLINE Vec128<T> Set64(Full128<T> d, uint64_t mask_bits)
{
    return BitCast(d, Vec128<uint64_t>(vdupq_n_u64(mask_bits)));
}

template <class D, OMNI_IF_T_SIZE_D(D, 1)> OMNI_INLINE MFromD<D> LoadMaskBits(D d, uint64_t mask_bits)
{
    const RebindToUnsigned<decltype(d)> du;
    // Easier than Set(), which would require an >8-bit type, which would not
    // compile for T=uint8_t, N=1.
    const auto vmask_bits = Set64(du, mask_bits);

    // Replicate bytes 8x such that each byte contains the bit that governs it.
    alignas(16) static constexpr uint8_t kRep8[16] = { 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1 };
    const auto rep8 = TableLookupBytes(vmask_bits, Load(du, kRep8));

    alignas(16) static constexpr uint8_t kBit[16] = { 1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128 };
    return RebindMask(d, TestBit(rep8, LoadDup128(du, kBit)));
}

template <class D, OMNI_IF_T_SIZE_D(D, 2)> OMNI_INLINE MFromD<D> LoadMaskBits(D d, uint64_t mask_bits)
{
    const RebindToUnsigned<decltype(d)> du;
    alignas(16) static constexpr uint16_t kBit[8] = { 1, 2, 4, 8, 16, 32, 64, 128 };
    const auto vmask_bits = Set(du, static_cast<uint16_t>(mask_bits));
    return RebindMask(d, TestBit(vmask_bits, Load(du, kBit)));
}

template <class D, OMNI_IF_T_SIZE_D(D, 4)> OMNI_INLINE MFromD<D> LoadMaskBits(D d, uint64_t mask_bits)
{
    const RebindToUnsigned<decltype(d)> du;
    alignas(16) static constexpr uint32_t kBit[8] = { 1, 2, 4, 8 };
    const auto vmask_bits = Set(du, static_cast<uint32_t>(mask_bits));
    return RebindMask(d, TestBit(vmask_bits, Load(du, kBit)));
}

template <class D, OMNI_IF_T_SIZE_D(D, 8)> OMNI_INLINE MFromD<D> LoadMaskBits(D d, uint64_t mask_bits)
{
    const RebindToUnsigned<decltype(d)> du;
    alignas(16) static constexpr uint64_t kBit[8] = { 1, 2 };
    return RebindMask(d, TestBit(Set(du, mask_bits), Load(du, kBit)));
}
} // namespace detail

// `p` points to at least 8 readable bytes, not all of which need be valid.
template <class D, OMNI_IF_V_SIZE_LE_D(D, 16)> OMNI_API MFromD<D> LoadMaskBits(D d, const uint8_t *OMNI_RESTRICT bits)
{
    uint64_t mask_bits = 0;
    CopyBytes<(d.MaxLanes() + 7) / 8>(bits, &mask_bits);
    return detail::LoadMaskBits(d, mask_bits);
}

// ------------------------------ Dup128MaskFromMaskBits

template <class D> OMNI_API MFromD<D> Dup128MaskFromMaskBits(D d, unsigned mask_bits)
{
    constexpr size_t kN = MaxLanes(d);
    if (kN < 8)
        mask_bits &= (1u << kN) - 1;
    return detail::LoadMaskBits(d, mask_bits);
}

// ------------------------------ Mask

namespace detail {
// Returns mask[i]? 0xF : 0 in each nibble. This is more efficient than
// BitsFromMask for use in (partial) CountTrue, FindFirstTrue and AllFalse.
template <class D, OMNI_IF_V_SIZE_D(D, 16)> OMNI_INLINE uint64_t NibblesFromMask(D d, MFromD<D> mask)
{
    const Full128<uint16_t> du16;
    const Vec128<uint16_t> vu16 = BitCast(du16, VecFromMask(d, mask));
    const Vec64<uint8_t> nib(vshrn_n_u16(vu16.raw, 4));
    return GetLane(BitCast(Full64<uint64_t>(), nib));
}

template <class D, OMNI_IF_V_SIZE_D(D, 8)> OMNI_INLINE uint64_t NibblesFromMask(D d, MFromD<D> mask)
{
    // There is no vshrn_n_u16 for uint16x4, so zero-extend.
    const Twice<decltype(d)> d2;
    const VFromD<decltype(d2)> v128 = ZeroExtendVector(d2, VecFromMask(d, mask));
    // No need to mask, upper half is zero thanks to ZeroExtendVector.
    return NibblesFromMask(d2, MaskFromVec(v128));
}

template <class D, OMNI_IF_V_SIZE_LE_D(D, 4)> OMNI_INLINE uint64_t NibblesFromMask(D d, MFromD<D> mask)
{
    const Mask64<TFromD<D>> mask64(mask.raw);
    const uint64_t nib = NibblesFromMask(Full64<TFromD<D>>(), mask64);
    // Clear nibbles from upper half of 64-bits
    return nib & ((1ull << (d.MaxBytes() * 4)) - 1);
}

template <typename T> OMNI_INLINE uint64_t BitsFromMask(simd::SizeTag<1> /* tag */, Mask128<T> mask)
{
    alignas(16) static constexpr uint8_t kSliceLanes[16] = { 1, 2, 4, 8, 0x10, 0x20, 0x40, 0x80, 1, 2, 4, 8, 0x10, 0x20,
            0x40, 0x80, };
    const Full128<uint8_t> du;
    const Vec128<uint8_t> values = BitCast(du, VecFromMask(Full128<T>(), mask)) & Load(du, kSliceLanes);

    // Can't vaddv - we need two separate bytes (16 bits).
    const uint8x8_t x2 = vget_low_u8(vpaddq_u8(values.raw, values.raw));
    const uint8x8_t x4 = vpadd_u8(x2, x2);
    const uint8x8_t x8 = vpadd_u8(x4, x4);
    return vget_lane_u64(vreinterpret_u64_u8(x8), 0) & 0xFFFF;
}

template <typename T, size_t N, OMNI_IF_V_SIZE_LE(T, N, 8)>
OMNI_INLINE uint64_t BitsFromMask(simd::SizeTag<1> /* tag */, Mask128<T, N> mask)
{
    // Upper lanes of partial loads are undefined. OnlyActive will fix this if
    // we load all kSliceLanes so the upper lanes do not pollute the valid bits.
    alignas(8) static constexpr uint8_t kSliceLanes[8] = { 1, 2, 4, 8, 0x10, 0x20, 0x40, 0x80 };
    const DFromM<decltype(mask)> d;
    const RebindToUnsigned<decltype(d)> du;
    const Vec128<uint8_t, N> slice(Load(Full64<uint8_t>(), kSliceLanes).raw);
    const Vec128<uint8_t, N> values = BitCast(du, VecFromMask(d, mask)) & slice;

    return vaddv_u8(values.raw);
}

template <typename T> OMNI_INLINE uint64_t BitsFromMask(simd::SizeTag<2> /* tag */, Mask128<T> mask)
{
    alignas(16) static constexpr uint16_t kSliceLanes[8] = { 1, 2, 4, 8, 0x10, 0x20, 0x40, 0x80 };
    const Full128<T> d;
    const Full128<uint16_t> du;
    const Vec128<uint16_t> values = BitCast(du, VecFromMask(d, mask)) & Load(du, kSliceLanes);
    return vaddvq_u16(values.raw);
}

template <typename T, size_t N, OMNI_IF_V_SIZE_LE(T, N, 8)>
OMNI_INLINE uint64_t BitsFromMask(simd::SizeTag<2> /* tag */, Mask128<T, N> mask)
{
    // Upper lanes of partial loads are undefined. OnlyActive will fix this if
    // we load all kSliceLanes so the upper lanes do not pollute the valid bits.
    alignas(8) static constexpr uint16_t kSliceLanes[4] = { 1, 2, 4, 8 };
    const DFromM<decltype(mask)> d;
    const RebindToUnsigned<decltype(d)> du;
    const Vec128<uint16_t, N> slice(Load(Full64<uint16_t>(), kSliceLanes).raw);
    const Vec128<uint16_t, N> values = BitCast(du, VecFromMask(d, mask)) & slice;
    return vaddv_u16(values.raw);
}

template <typename T> OMNI_INLINE uint64_t BitsFromMask(simd::SizeTag<4> /* tag */, Mask128<T> mask)
{
    alignas(16) static constexpr uint32_t kSliceLanes[4] = { 1, 2, 4, 8 };
    const Full128<T> d;
    const Full128<uint32_t> du;
    const Vec128<uint32_t> values = BitCast(du, VecFromMask(d, mask)) & Load(du, kSliceLanes);
    return vaddvq_u32(values.raw);
}

template <typename T, size_t N, OMNI_IF_V_SIZE_LE(T, N, 8)>
OMNI_INLINE uint64_t BitsFromMask(simd::SizeTag<4> /* tag */, Mask128<T, N> mask)
{
    // Upper lanes of partial loads are undefined. OnlyActive will fix this if
    // we load all kSliceLanes so the upper lanes do not pollute the valid bits.
    alignas(8) static constexpr uint32_t kSliceLanes[2] = { 1, 2 };
    const DFromM<decltype(mask)> d;
    const RebindToUnsigned<decltype(d)> du;
    const Vec128<uint32_t, N> slice(Load(Full64<uint32_t>(), kSliceLanes).raw);
    const Vec128<uint32_t, N> values = BitCast(du, VecFromMask(d, mask)) & slice;
    return vaddv_u32(values.raw);
}

template <typename T> OMNI_INLINE uint64_t BitsFromMask(simd::SizeTag<8> /* tag */, Mask128<T> m)
{
    alignas(16) static constexpr uint64_t kSliceLanes[2] = { 1, 2 };
    const Full128<T> d;
    const Full128<uint64_t> du;
    const Vec128<uint64_t> values = BitCast(du, VecFromMask(d, m)) & Load(du, kSliceLanes);
    return vaddvq_u64(values.raw);
}

template <typename T> OMNI_INLINE uint64_t BitsFromMask(simd::SizeTag<8> /* tag */, Mask128<T, 1> m)
{
    const Full64<T> d;
    const Full64<uint64_t> du;
    const Vec64<uint64_t> values = BitCast(du, VecFromMask(d, m)) & Set(du, 1);
    return vget_lane_u64(values.raw, 0);
}

// Returns the lowest N for the BitsFromMask result.
template <typename T, size_t N> constexpr uint64_t OnlyActive(uint64_t bits)
{
    return ((N * sizeof(T)) >= 8) ? bits : (bits & ((1ull << N) - 1));
}

template <typename T, size_t N> OMNI_INLINE uint64_t BitsFromMask(Mask128<T, N> mask)
{
    return OnlyActive<T, N>(BitsFromMask(simd::SizeTag<sizeof(T)>(), mask));
}

template <typename T> OMNI_INLINE size_t CountTrue(simd::SizeTag<1> /* tag */, Mask128<T> mask)
{
    const Full128<int8_t> di;
    const int8x16_t ones = vnegq_s8(BitCast(di, VecFromMask(Full128<T>(), mask)).raw);
    return static_cast<size_t>(vaddvq_s8(ones));
}

template <typename T> OMNI_INLINE size_t CountTrue(simd::SizeTag<2> /* tag */, Mask128<T> mask)
{
    const Full128<int16_t> di;
    const int16x8_t ones = vnegq_s16(BitCast(di, VecFromMask(Full128<T>(), mask)).raw);

    return static_cast<size_t>(vaddvq_s16(ones));
}

template <typename T> OMNI_INLINE size_t CountTrue(simd::SizeTag<4> /* tag */, Mask128<T> mask)
{
    const Full128<int32_t> di;
    const int32x4_t ones = vnegq_s32(BitCast(di, VecFromMask(Full128<T>(), mask)).raw);
    return static_cast<size_t>(vaddvq_s32(ones));
}

template <typename T> OMNI_INLINE size_t CountTrue(simd::SizeTag<8> /* tag */, Mask128<T> mask)
{
    const Full128<int64_t> di;
    const int64x2_t ones = vnegq_s64(BitCast(di, VecFromMask(Full128<T>(), mask)).raw);
    return static_cast<size_t>(vaddvq_s64(ones));
}
} // namespace detail

// Full
template <class D, typename T = TFromD<D>> OMNI_API size_t CountTrue(D /* tag */, Mask128<T> mask)
{
    return detail::CountTrue(simd::SizeTag<sizeof(T)>(), mask);
}

// Partial
template <class D, OMNI_IF_V_SIZE_LE_D(D, 8)> OMNI_API size_t CountTrue(D d, MFromD<D> mask)
{
    constexpr int kDiv = 4 * sizeof(TFromD<D>);
    return PopCount(detail::NibblesFromMask(d, mask)) / kDiv;
}

template <class D> OMNI_API size_t FindKnownFirstTrue(D d, MFromD<D> mask)
{
    const uint64_t nib = detail::NibblesFromMask(d, mask);
    constexpr size_t kDiv = 4 * sizeof(TFromD<D>);
    return Num0BitsBelowLS1Bit_Nonzero64(nib) / kDiv;
}

template <class D> OMNI_API intptr_t FindFirstTrue(D d, MFromD<D> mask)
{
    const uint64_t nib = detail::NibblesFromMask(d, mask);
    if (nib == 0)
        return -1;
    constexpr size_t kDiv = 4 * sizeof(TFromD<D>);
    return static_cast<intptr_t>(Num0BitsBelowLS1Bit_Nonzero64(nib) / kDiv);
}

template <class D> OMNI_API size_t FindKnownLastTrue(D d, MFromD<D> mask)
{
    const uint64_t nib = detail::NibblesFromMask(d, mask);
    constexpr size_t kDiv = 4 * sizeof(TFromD<D>);
    return (63 - Num0BitsAboveMS1Bit_Nonzero64(nib)) / kDiv;
}

template <class D> OMNI_API intptr_t FindLastTrue(D d, MFromD<D> mask)
{
    const uint64_t nib = detail::NibblesFromMask(d, mask);
    if (nib == 0)
        return -1;
    constexpr size_t kDiv = 4 * sizeof(TFromD<D>);
    return static_cast<intptr_t>((63 - Num0BitsAboveMS1Bit_Nonzero64(nib)) / kDiv);
}

// `p` points to at least 8 writable bytes.
template <class D> OMNI_API size_t StoreMaskBits(D d, MFromD<D> mask, uint8_t *bits)
{
    const uint64_t mask_bits = detail::BitsFromMask(mask);
    const size_t kNumBytes = (d.MaxLanes() + 7) / 8;
    CopyBytes<kNumBytes>(&mask_bits, bits);
    return kNumBytes;
}

template <class D> OMNI_API bool AllFalse(D d, MFromD<D> m)
{
    return detail::NibblesFromMask(d, m) == 0;
}

// Full
template <class D, typename T = TFromD<D>> OMNI_API bool AllTrue(D d, Mask128<T> m)
{
    return detail::NibblesFromMask(d, m) == ~0ull;
}

// Partial
template <class D, OMNI_IF_V_SIZE_LE_D(D, 8)> OMNI_API bool AllTrue(D d, MFromD<D> m)
{
    return detail::NibblesFromMask(d, m) == (1ull << (d.MaxBytes() * 4)) - 1;
}

// ------------------------------ Compress

template <typename T> struct CompressIsPartition {
    enum {
        value = (sizeof(T) != 1)
    };
};

namespace detail {
// Load 8 bytes, replicate into upper half so ZipLower can use the lower half.
template <class D, OMNI_IF_V_SIZE_D(D, 16)> OMNI_INLINE Vec128<uint8_t> Load8Bytes(D /* tag */, const uint8_t *bytes)
{
    return Vec128<uint8_t>(vreinterpretq_u8_u64(vld1q_dup_u64(OMNI_RCAST_ALIGNED(const uint64_t *, bytes))));
}

// Load 8 bytes and return half-reg with N <= 8 bytes.
template <class D, OMNI_IF_V_SIZE_LE_D(D, 8)> OMNI_INLINE VFromD<D> Load8Bytes(D d, const uint8_t *bytes)
{
    return Load(d, bytes);
}

template <typename T, size_t N> OMNI_INLINE Vec128<T, N> IdxFromBits(simd::SizeTag<4> /* tag */, uint64_t mask_bits)
{
    // There are only 4 lanes, so we can afford to load the index vector directly.
    alignas(16) static constexpr uint8_t u8_indices[16 * 16] = {
        // PrintCompress32x4Tables
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, //
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, //
            4, 5, 6, 7, 0, 1, 2, 3, 8, 9, 10, 11, 12, 13, 14, 15, //
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, //
            8, 9, 10, 11, 0, 1, 2, 3, 4, 5, 6, 7, 12, 13, 14, 15, //
            0, 1, 2, 3, 8, 9, 10, 11, 4, 5, 6, 7, 12, 13, 14, 15, //
            4, 5, 6, 7, 8, 9, 10, 11, 0, 1, 2, 3, 12, 13, 14, 15, //
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, //
            12, 13, 14, 15, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, //
            0, 1, 2, 3, 12, 13, 14, 15, 4, 5, 6, 7, 8, 9, 10, 11, //
            4, 5, 6, 7, 12, 13, 14, 15, 0, 1, 2, 3, 8, 9, 10, 11, //
            0, 1, 2, 3, 4, 5, 6, 7, 12, 13, 14, 15, 8, 9, 10, 11, //
            8, 9, 10, 11, 12, 13, 14, 15, 0, 1, 2, 3, 4, 5, 6, 7,  //
            0, 1, 2, 3, 8, 9, 10, 11, 12, 13, 14, 15, 4, 5, 6, 7,  //
            4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 0, 1, 2, 3,  //
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
    const Simd<T, N, 0> d;
    const Repartition<uint8_t, decltype(d)> d8;
    return BitCast(d, Load(d8, u8_indices + 16 * mask_bits));
}

template <typename T, size_t N> OMNI_INLINE Vec128<T, N> IdxFromNotBits(simd::SizeTag<4> /* tag */, uint64_t mask_bits)
{
    // There are only 4 lanes, so we can afford to load the index vector directly.
    alignas(16) static constexpr uint8_t u8_indices[16 * 16] = {
        // PrintCompressNot32x4Tables
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 0, 1, 2, 3,
            0, 1, 2, 3, 8, 9, 10, 11, 12, 13, 14, 15, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 0, 1, 2, 3, 4, 5, 6, 7,
            0, 1, 2, 3, 4, 5, 6, 7, 12, 13, 14, 15, 8, 9, 10, 11, 4, 5, 6, 7, 12, 13, 14, 15, 0, 1, 2, 3, 8, 9, 10, 11,
            0, 1, 2, 3, 12, 13, 14, 15, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 4, 5, 6, 7, 8, 9, 10, 11, 0, 1, 2, 3, 12, 13, 14, 15,
            0, 1, 2, 3, 8, 9, 10, 11, 4, 5, 6, 7, 12, 13, 14, 15, 8, 9, 10, 11, 0, 1, 2, 3, 4, 5, 6, 7, 12, 13, 14, 15,
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 4, 5, 6, 7, 0, 1, 2, 3, 8, 9, 10, 11, 12, 13, 14, 15,
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
            15 };
    const Simd<T, N, 0> d;
    const Repartition<uint8_t, decltype(d)> d8;
    return BitCast(d, Load(d8, u8_indices + 16 * mask_bits));
}

#if OMNI_HAVE_INTEGER64 || OMNI_HAVE_FLOAT64

template <typename T, size_t N> OMNI_INLINE Vec128<T, N> IdxFromBits(simd::SizeTag<8> /* tag */, uint64_t mask_bits)
{
    // There are only 2 lanes, so we can afford to load the index vector directly.
    alignas(16) static constexpr uint8_t u8_indices[64] = { // PrintCompress64x2Tables
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
            8, 9, 10, 11, 12, 13, 14, 15, 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
            15 };

    const Simd<T, N, 0> d;
    const Repartition<uint8_t, decltype(d)> d8;
    return BitCast(d, Load(d8, u8_indices + 16 * mask_bits));
}

#endif

// Helper function called by both Compress and CompressStore - avoids a
// redundant BitsFromMask in the latter.
template <typename T, size_t N> OMNI_INLINE Vec128<T, N> Compress(Vec128<T, N> v, uint64_t mask_bits)
{
    const auto idx = detail::IdxFromBits<T, N>(simd::SizeTag<sizeof(T)>(), mask_bits);
    using D = DFromV<decltype(v)>;
    const RebindToSigned<D> di;
    return BitCast(D(), TableLookupBytes(BitCast(di, v), BitCast(di, idx)));
}

template <typename T, size_t N> OMNI_INLINE Vec128<T, N> CompressNot(Vec128<T, N> v, uint64_t mask_bits)
{
    const auto idx = detail::IdxFromNotBits<T, N>(simd::SizeTag<sizeof(T)>(), mask_bits);
    using D = DFromV<decltype(v)>;
    const RebindToSigned<D> di;
    return BitCast(D(), TableLookupBytes(BitCast(di, v), BitCast(di, idx)));
}
} // namespace detail

// Single lane: no-op
template <typename T> OMNI_API Vec128<T, 1> Compress(Vec128<T, 1> v, Mask128<T, 1> /* m */)
{
    return v;
}

// Two lanes: conditional swap
template <typename T, size_t N, OMNI_IF_T_SIZE(T, 8)> OMNI_API Vec128<T, N> Compress(Vec128<T, N> v, Mask128<T, N> mask)
{
    // If mask[1] = 1 and mask[0] = 0, then swap both halves, else keep.
    const DFromV<decltype(v)> d;
    const Vec128<T, N> m = VecFromMask(d, mask);
    const Vec128<T, N> maskL = DupEven(m);
    const Vec128<T, N> maskH = DupOdd(m);
    const Vec128<T, N> swap = AndNot(maskL, maskH);
    return IfVecThenElse(swap, Shuffle01(v), v);
}

// General case, 2 or 4 byte lanes
template <typename T, size_t N, OMNI_IF_T_SIZE_ONE_OF(T, (1 << 2) | (1 << 4))>
OMNI_API Vec128<T, N> Compress(Vec128<T, N> v, Mask128<T, N> mask)
{
    return detail::Compress(v, detail::BitsFromMask(mask));
}

// Single lane: no-op
template <typename T> OMNI_API Vec128<T, 1> CompressNot(Vec128<T, 1> v, Mask128<T, 1> /* m */)
{
    return v;
}

// Two lanes: conditional swap
template <typename T, OMNI_IF_T_SIZE(T, 8)> OMNI_API Vec128<T> CompressNot(Vec128<T> v, Mask128<int64_t> mask)
{
    // If mask[1] = 0 and mask[0] = 1, then swap both halves, else keep.
    const DFromV<decltype(v)> d;
    const Vec128<T> m = VecFromMask(d, mask);
    const Vec128<T> maskL = DupEven(m);
    const Vec128<T> maskH = DupOdd(m);
    const Vec128<T> swap = AndNot(maskH, maskL);
    return IfVecThenElse(swap, Shuffle01(v), v);
}

template <typename T, OMNI_IF_T_SIZE(T, 8)> OMNI_API Vec128<T> CompressNot(Vec128<T> v, Mask128<uint64_t> mask)
{
    // If mask[1] = 0 and mask[0] = 1, then swap both halves, else keep.
    const DFromV<decltype(v)> d;
    const Vec128<T> m = VecFromMask(d, mask);
    const Vec128<T> maskL = DupEven(m);
    const Vec128<T> maskH = DupOdd(m);
    const Vec128<T> swap = AndNot(maskH, maskL);
    return IfVecThenElse(swap, Shuffle01(v), v);
}

template <typename T, OMNI_IF_T_SIZE(T, 8)> OMNI_API Vec128<T> CompressNot(Vec128<T> v, Mask128<double> mask)
{
    // If mask[1] = 0 and mask[0] = 1, then swap both halves, else keep.
    const DFromV<decltype(v)> d;
    const Vec128<T> m = VecFromMask(d, mask);
    const Vec128<T> maskL = DupEven(m);
    const Vec128<T> maskH = DupOdd(m);
    const Vec128<T> swap = AndNot(maskH, maskL);
    return IfVecThenElse(swap, Shuffle01(v), v);
}

// General case, 2 or 4 byte lanes
template <typename T, size_t N, OMNI_IF_T_SIZE_ONE_OF(T, (1 << 2) | (1 << 4))>
OMNI_API Vec128<T, N> CompressNot(Vec128<T, N> v, Mask128<T, N> mask)
{
    // For partial vectors, we cannot pull the Not() into the table because
    // BitsFromMask clears the upper bits.
    if (N < 16 / sizeof(T)) {
        return detail::Compress(v, detail::BitsFromMask(Not(mask)));
    }
    return detail::CompressNot(v, detail::BitsFromMask(mask));
}

// ------------------------------ CompressBlocksNot
OMNI_API Vec128<uint64_t> CompressBlocksNot(Vec128<uint64_t> v, Mask128<uint64_t> /* m */)
{
    return v;
}

// ------------------------------ CompressBits

template <typename T, size_t N, OMNI_IF_NOT_T_SIZE(T, 1)>
OMNI_INLINE Vec128<T, N> CompressBits(Vec128<T, N> v, const uint8_t *OMNI_RESTRICT bits)
{
    uint64_t mask_bits = 0;
    constexpr size_t kNumBytes = (N + 7) / 8;
    CopyBytes<kNumBytes>(bits, &mask_bits);
    if (N < 8) {
        mask_bits &= (1ull << N) - 1;
    }

    return detail::Compress(v, mask_bits);
}

// ------------------------------ CompressStore
template <class V, class M, class D, OMNI_IF_NOT_T_SIZE_D(D, 1)>
OMNI_API size_t CompressStore(V v, M mask, D d, TFromD<D> *unaligned)
{
    const uint64_t mask_bits = detail::BitsFromMask(mask);
    StoreU(detail::Compress(v, mask_bits), d, unaligned);
    return PopCount(mask_bits);
}

// ------------------------------ CompressBlendedStore
template <class D, class V, class M, OMNI_IF_NOT_T_SIZE_D(D, 1)>
OMNI_API size_t CompressBlendedStore(V v, M m, D d, TFromD<D> *OMNI_RESTRICT unaligned)
{
    const RebindToUnsigned<decltype(d)> du; // so we can support fp16/bf16
    const uint64_t mask_bits = detail::BitsFromMask(m);
    const size_t count = PopCount(mask_bits);
    const MFromD<D> store_mask = RebindMask(d, FirstN(du, count));
    const VFromD<decltype(du)> compressed = detail::Compress(BitCast(du, v), mask_bits);
    BlendedStore(BitCast(d, compressed), store_mask, d, unaligned);
    return count;
}

// ------------------------------ CompressBitsStore

template <class D, OMNI_IF_NOT_T_SIZE_D(D, 1)>
OMNI_API size_t CompressBitsStore(VFromD<D> v, const uint8_t *OMNI_RESTRICT bits, D d,
    TFromD<D> *OMNI_RESTRICT unaligned)
{
    uint64_t mask_bits = 0;
    constexpr size_t kNumBytes = (d.MaxLanes() + 7) / 8;
    CopyBytes<kNumBytes>(bits, &mask_bits);
    if (d.MaxLanes() < 8) {
        mask_bits &= (1ull << d.MaxLanes()) - 1;
    }

    StoreU(detail::Compress(v, mask_bits), d, unaligned);
    return PopCount(mask_bits);
}

// ------------------------------ LoadInterleaved2

// Per-target flag to prevent generic_ops-inl.h from defining LoadInterleaved2.
#ifdef OMNI_NATIVE_LOAD_STORE_INTERLEAVED
#undef OMNI_NATIVE_LOAD_STORE_INTERLEAVED
#else
#define OMNI_NATIVE_LOAD_STORE_INTERLEAVED
#endif

namespace detail {
#define OMNI_NEON_BUILD_TPL_OMNI_LOAD_INT
#define OMNI_NEON_BUILD_ARG_OMNI_LOAD_INT from

#define OMNI_IF_LOAD_INT(D) OMNI_IF_V_SIZE_GT_D(D, 4), OMNI_NEON_IF_NOT_EMULATED_D(D)
#define OMNI_NEON_DEF_FUNCTION_LOAD_INT(name, prefix, infix, args) \
    OMNI_NEON_DEF_FUNCTION_ALL_TYPES(name, prefix, infix, args)    \
    OMNI_NEON_DEF_FUNCTION_BFLOAT_16(name, prefix, infix, args)

#define OMNI_NEON_BUILD_RET_OMNI_LOAD_INT(type, size) decltype(Tuple2<type##_t, size>().raw)
// Tuple tag arg allows overloading (cannot just overload on return type)
#define OMNI_NEON_BUILD_PARAM_OMNI_LOAD_INT(type, size) const NativeLaneType<type##_t> *from, Tuple2<type##_t, size>

OMNI_NEON_DEF_FUNCTION_LOAD_INT(LoadInterleaved2, vld2, _, OMNI_LOAD_INT)

#undef OMNI_NEON_BUILD_RET_OMNI_LOAD_INT
#undef OMNI_NEON_BUILD_PARAM_OMNI_LOAD_INT

#define OMNI_NEON_BUILD_RET_OMNI_LOAD_INT(type, size) decltype(Tuple3<type##_t, size>().raw)
#define OMNI_NEON_BUILD_PARAM_OMNI_LOAD_INT(type, size) const NativeLaneType<type##_t> *from, Tuple3<type##_t, size>

OMNI_NEON_DEF_FUNCTION_LOAD_INT(LoadInterleaved3, vld3, _, OMNI_LOAD_INT)

#undef OMNI_NEON_BUILD_PARAM_OMNI_LOAD_INT
#undef OMNI_NEON_BUILD_RET_OMNI_LOAD_INT

#define OMNI_NEON_BUILD_RET_OMNI_LOAD_INT(type, size) decltype(Tuple4<type##_t, size>().raw)
#define OMNI_NEON_BUILD_PARAM_OMNI_LOAD_INT(type, size) const NativeLaneType<type##_t> *from, Tuple4<type##_t, size>

OMNI_NEON_DEF_FUNCTION_LOAD_INT(LoadInterleaved4, vld4, _, OMNI_LOAD_INT)

#undef OMNI_NEON_BUILD_PARAM_OMNI_LOAD_INT
#undef OMNI_NEON_BUILD_RET_OMNI_LOAD_INT

#undef OMNI_NEON_DEF_FUNCTION_LOAD_INT
#undef OMNI_NEON_BUILD_TPL_OMNI_LOAD_INT
#undef OMNI_NEON_BUILD_ARG_OMNI_LOAD_INT
} // namespace detail

template <class D, OMNI_IF_LOAD_INT(D), typename T = TFromD<D>>
OMNI_API void LoadInterleaved2(D d, const T *OMNI_RESTRICT unaligned, VFromD<D> &v0, VFromD<D> &v1)
{
    auto raw = detail::LoadInterleaved2(detail::NativeLanePointer(unaligned), detail::Tuple2<T, d.MaxLanes()>());
    v0 = VFromD<D>(raw.val[0]);
    v1 = VFromD<D>(raw.val[1]);
}

// <= 32 bits: avoid loading more than N bytes by copying to buffer
template <class D, OMNI_IF_V_SIZE_LE_D(D, 4), OMNI_NEON_IF_NOT_EMULATED_D(D), typename T = TFromD<D>>
OMNI_API void LoadInterleaved2(D d, const T *OMNI_RESTRICT unaligned, VFromD<D> &v0, VFromD<D> &v1)
{
    // The smallest vector registers are 64-bits and we want space for two.
    alignas(16) T buf[2 * 8 / sizeof(T)] = {};
    CopyBytes<d.MaxBytes() * 2>(unaligned, buf);
    auto raw = detail::LoadInterleaved2(detail::NativeLanePointer(buf), detail::Tuple2<T, d.MaxLanes()>());
    v0 = VFromD<D>(raw.val[0]);
    v1 = VFromD<D>(raw.val[1]);
}

template <class D, OMNI_IF_LOAD_INT(D), typename T = TFromD<D>>
OMNI_API void LoadInterleaved3(D d, const T *OMNI_RESTRICT unaligned, VFromD<D> &v0, VFromD<D> &v1, VFromD<D> &v2)
{
    auto raw = detail::LoadInterleaved3(detail::NativeLanePointer(unaligned), detail::Tuple3<T, d.MaxLanes()>());
    v0 = VFromD<D>(raw.val[0]);
    v1 = VFromD<D>(raw.val[1]);
    v2 = VFromD<D>(raw.val[2]);
}

// <= 32 bits: avoid writing more than N bytes by copying to buffer
template <class D, OMNI_IF_V_SIZE_LE_D(D, 4), OMNI_NEON_IF_NOT_EMULATED_D(D), typename T = TFromD<D>>
OMNI_API void LoadInterleaved3(D d, const T *OMNI_RESTRICT unaligned, VFromD<D> &v0, VFromD<D> &v1, VFromD<D> &v2)
{
    // The smallest vector registers are 64-bits and we want space for three.
    alignas(16) T buf[3 * 8 / sizeof(T)] = {};
    CopyBytes<d.MaxBytes() * 3>(unaligned, buf);
    auto raw = detail::LoadInterleaved3(detail::NativeLanePointer(buf), detail::Tuple3<T, d.MaxLanes()>());
    v0 = VFromD<D>(raw.val[0]);
    v1 = VFromD<D>(raw.val[1]);
    v2 = VFromD<D>(raw.val[2]);
}


template <class D, OMNI_IF_LOAD_INT(D), typename T = TFromD<D>>
OMNI_API void LoadInterleaved4(D d, const T *OMNI_RESTRICT unaligned, VFromD<D> &v0, VFromD<D> &v1, VFromD<D> &v2,
    VFromD<D> &v3)
{
    auto raw = detail::LoadInterleaved4(detail::NativeLanePointer(unaligned), detail::Tuple4<T, d.MaxLanes()>());
    v0 = VFromD<D>(raw.val[0]);
    v1 = VFromD<D>(raw.val[1]);
    v2 = VFromD<D>(raw.val[2]);
    v3 = VFromD<D>(raw.val[3]);
}

// <= 32 bits: avoid writing more than N bytes by copying to buffer
template <class D, OMNI_IF_V_SIZE_LE_D(D, 4), OMNI_NEON_IF_NOT_EMULATED_D(D), typename T = TFromD<D>>
OMNI_API void LoadInterleaved4(D d, const T *OMNI_RESTRICT unaligned, VFromD<D> &v0, VFromD<D> &v1, VFromD<D> &v2,
    VFromD<D> &v3)
{
    alignas(16) T buf[4 * 8 / sizeof(T)] = {};
    CopyBytes<d.MaxBytes() * 4>(unaligned, buf);
    auto raw = detail::LoadInterleaved4(detail::NativeLanePointer(buf), detail::Tuple4<T, d.MaxLanes()>());
    v0 = VFromD<D>(raw.val[0]);
    v1 = VFromD<D>(raw.val[1]);
    v2 = VFromD<D>(raw.val[2]);
    v3 = VFromD<D>(raw.val[3]);
}

#undef OMNI_IF_LOAD_INT

// ------------------------------ StoreInterleaved2

namespace detail {
#define OMNI_NEON_BUILD_TPL_OMNI_STORE_INT
#define OMNI_NEON_BUILD_RET_OMNI_STORE_INT(type, size) void
#define OMNI_NEON_BUILD_ARG_OMNI_STORE_INT to, tup.raw

#define OMNI_IF_STORE_INT(D) OMNI_IF_V_SIZE_GT_D(D, 4), OMNI_NEON_IF_NOT_EMULATED_D(D)
#define OMNI_NEON_DEF_FUNCTION_STORE_INT(name, prefix, infix, args) \
    OMNI_NEON_DEF_FUNCTION_ALL_TYPES(name, prefix, infix, args)     \
    OMNI_NEON_DEF_FUNCTION_BFLOAT_16(name, prefix, infix, args)

#define OMNI_NEON_BUILD_PARAM_OMNI_STORE_INT(type, size) Tuple2<type##_t, size> tup, NativeLaneType<type##_t> *to

#undef OMNI_NEON_BUILD_PARAM_OMNI_STORE_INT

#define OMNI_NEON_BUILD_PARAM_OMNI_STORE_INT(type, size) Tuple3<type##_t, size> tup, NativeLaneType<type##_t> *to

#undef OMNI_NEON_BUILD_PARAM_OMNI_STORE_INT

#define OMNI_NEON_BUILD_PARAM_OMNI_STORE_INT(type, size) Tuple4<type##_t, size> tup, NativeLaneType<type##_t> *to

#undef OMNI_NEON_BUILD_PARAM_OMNI_STORE_INT

#undef OMNI_NEON_DEF_FUNCTION_STORE_INT
#undef OMNI_NEON_BUILD_TPL_OMNI_STORE_INT
#undef OMNI_NEON_BUILD_RET_OMNI_STORE_INT
#undef OMNI_NEON_BUILD_ARG_OMNI_STORE_INT
} // namespace detail

#undef OMNI_IF_STORE_INT

template <class T, size_t N> OMNI_API uint64_t LeadingValueCountMask(T value, const T *OMNI_RESTRICT in)
{
    ScalableTag<T> d;
    const auto broadCasted = Set(d, value);
    auto mask = Ne(broadCasted, LoadU(d, in));
    uint64_t nib = detail::NibblesFromMask(d, mask);
    return nib;
}

// -------------------- Match
template <typename T, size_t N> OMNI_API uint64_t FindMatchMask(T value, const T *OMNI_RESTRICT in)
{
    ScalableTag<T> d;
    const auto broadCasted = Set(d, value);
    auto mask = Eq(broadCasted, LoadU(d, in));
    uint64_t nib = detail::NibblesFromMask(d, mask);
    return nib;
}

template <typename T, size_t N>
OMNI_INLINE int32_t FindMatch(T value, const T* OMNI_RESTRICT in)
{
    int8x16_t data = vld1q_s8(in);
    int8x16_t value_vec = vdupq_n_s8(value);
    uint8x16_t mask = vceqq_s8(data, value_vec);
    static const int8_t kShift[] = {
        -7, -6, -5, -4, -3, -2, -1, 0, -7, -6, -5, -4, -3, -2, -1, 0};
    int8x16_t vshift = vld1q_s8(kShift);
    uint8x16_t vmask = vshlq_u8(vandq_u8(mask, vdupq_n_u8(0x80)), vshift);
    return (vaddv_u8(vget_high_u8(vmask)) << 8) | vaddv_u8(vget_low_u8(vmask));
}

template <typename T, size_t N>
OMNI_INLINE int32_t FindFirstMatch(T value, const T* OMNI_RESTRICT in)
{
    int8x16_t data = vld1q_s8(in);
    int8x16_t value_vec = vdupq_n_s8(value);
    uint8x16_t mask = vceqq_s8(data, value_vec);
    static const int8_t kShift[] = {
        -7, -6, -5, -4, -3, -2, -1, 0, -7, -6, -5, -4, -3, -2, -1, 0};
    int8x16_t vshift = vld1q_s8(kShift);
    uint8x16_t vmask = vshlq_u8(vandq_u8(mask, vdupq_n_u8(0x80)), vshift);
    auto res = (vaddv_u8(vget_high_u8(vmask)) << 8) | vaddv_u8(vget_low_u8(vmask));
    return res == 0 ? -1 : __builtin_ctz(res);
}

template <typename T>
OMNI_INLINE static unsigned FindFirstSetNonZeroNeon(T mask)
{
    if (sizeof(mask) == sizeof(unsigned)) {
        return __builtin_ctz(static_cast<unsigned>(mask));
    } else {
        return __builtin_ctzll(mask);
    }
}

template <typename T, size_t N>
OMNI_INLINE size_t CountLeadingValue(T value, const T* OMNI_RESTRICT in)
{
    uint64_t nib = LeadingValueCountMask<T, N>(value, in);
    return static_cast<size_t>(FindFirstSetNonZeroNeon(nib) >> 2);
}

namespace detail {
using AddrVec = Vec128<uint64_t>;
using AddrType = uint64_t;
template <typename T> using SortTag = ScalableTag<T>;
static const SortTag<AddrType> ADDR_TAG;
static int64x2_t SMALLEST_VEC1 = { INT64_MIN, INT64_MIN };
static int64x2_t LARGEST_VEC1 = { INT64_MAX, INT64_MAX };
static float64x2_t SMALLEST_DOUBLE_VEC1 = { -DBL_MAX, -DBL_MAX };
static float64x2_t LARGEST_DOUBLE_VEC1 = { DBL_MAX, DBL_MAX };
enum class SortOrder {
    ASCENDING,
    DESCENDING
};

template <typename TI> constexpr size_t FloorLog2(TI x)
{
    return x == TI{ 1 } ? 0 : static_cast<size_t>(FloorLog2(static_cast<TI>(x >> 1)) + 1);
}

template <typename TI> constexpr size_t CeilLog2(TI x)
{
    return x == TI{ 1 } ? 0 : static_cast<size_t>(FloorLog2(static_cast<TI>(x - 1)) + 1);
}

OMNI_API Vec128<uint64_t> GetBlendedVec(const Mask128<uint64_t, 2> &mask, const Vec128<uint64_t> &compressAddrVec,
    const Vec128<uint64_t> &tmpAddrVec)
{
    return static_cast<Vec128<uint64_t>>(vbslq_u64(mask.raw, compressAddrVec.raw, tmpAddrVec.raw));
}

OMNI_API Vec128<uint64_t> GetBlendedVec(const Mask128<double, 2> &mask, const Vec128<uint64_t> &compressAddrVec,
    const Vec128<uint64_t> &tmpAddrVec)
{
    return static_cast<Vec128<uint64_t>>(vbslq_u64(mask.raw, compressAddrVec.raw, tmpAddrVec.raw));
}

OMNI_API Vec128<uint64_t> GetBlendedVec(const Mask128<int64_t, 2> &mask, const Vec128<uint64_t> &compressAddrVec,
    const Vec128<uint64_t> &tmpAddrVec)
{
    return static_cast<Vec128<uint64_t>>(vbslq_u64(mask.raw, compressAddrVec.raw, tmpAddrVec.raw));
}

namespace QuickSortAscending {
template <class D, OMNI_IF_F64_D(D)> inline VFromD<D> GetSmallestVec(D d)
{
    return VFromD<D>(LARGEST_DOUBLE_VEC1);
}

template <class D, OMNI_IF_I64_D(D)> inline VFromD<D> GetSmallestVec(D d)
{
    return VFromD<D>(LARGEST_VEC1);
}

template <class D, class V, OMNI_IF_F64_D(D)> inline TFromD<D> GetSmallest(D d, V v)
{
    float64_t fVal0 = vgetq_lane_f64(v.raw, 0);
    float64_t fVal1 = vgetq_lane_f64(v.raw, 1);
    bool compare = fVal0 < fVal1;
    return compare ? fVal0 : fVal1;
}

template <class D, class V, OMNI_IF_F64_D(D)> inline TFromD<D> GetLargest(D d, V v)
{
    float64_t fVal0 = vgetq_lane_f64(v.raw, 0);
    float64_t fVal1 = vgetq_lane_f64(v.raw, 1);
    bool compare = fVal0 > fVal1;
    return compare ? fVal0 : fVal1;
}

template <class D, class V, OMNI_IF_I64_D(D)> inline TFromD<D> GetSmallest(D d, V v)
{
    int64_t val0 = vgetq_lane_s64(v.raw, 0);
    int64_t val1 = vgetq_lane_s64(v.raw, 1);
    bool compare = val0 < val1;
    return compare ? val0 : val1;
}

template <class D, class V, OMNI_IF_I64_D(D)> inline TFromD<D> GetLargest(D d, V v)
{
    int64_t val0 = vgetq_lane_s64(v.raw, 0);
    int64_t val1 = vgetq_lane_s64(v.raw, 1);
    bool compare = val0 > val1;
    return compare ? val0 : val1;
}

template <class D, OMNI_IF_F64_D(D)> inline VFromD<D> GetLargestVec(D)
{
    return VFromD<D>(SMALLEST_DOUBLE_VEC1);
}

template <class D, OMNI_IF_I64_D(D)> inline VFromD<D> GetLargestVec(D)
{
    return VFromD<D>(SMALLEST_VEC1);
}

template <class D, class V> static void UpdateMinMax(D, V value, V &smallest, V &largest)
{
    if constexpr (std::is_same_v<TFromD<D>, double>) {
        uint64x2_t compare1 = vcltq_f64(value.raw, smallest.raw);
        uint64x2_t compare2 = vcgtq_f64(value.raw, largest.raw);
        smallest = static_cast<V>(vbslq_f64(compare1, value.raw, smallest.raw));
        largest = static_cast<V>(vbslq_f64(compare2, value.raw, largest.raw));
    } else {
        auto compare1 = vcltq_s64(value.raw, smallest.raw);
        auto compare2 = vcgtq_s64(value.raw, largest.raw);
        smallest = static_cast<V>(vbslq_s64(compare1, value.raw, smallest.raw));
        largest = static_cast<V>(vbslq_s64(compare2, value.raw, largest.raw));
    }
}

template <class D, OMNI_IF_I64_D(D), class V> MFromD<D> Compare(D d, V v1, V v2)
{
    return static_cast<MFromD<D>>(vcltq_s64(v1.raw, v2.raw));
}

template <class D, OMNI_IF_F64_D(D), class V> MFromD<D> Compare(D d, V v1, V v2)
{
    return static_cast<MFromD<D>>(vcltq_f64(v1.raw, v2.raw));
}
}

namespace QuickSortDescending {
template <class D, OMNI_IF_F64_D(D)> inline VFromD<D> GetSmallestVec(D d)
{
    return VFromD<D>(SMALLEST_DOUBLE_VEC1);
}

template <class D, OMNI_IF_I64_D(D)> inline VFromD<D> GetSmallestVec(D d)
{
    return VFromD<D>(SMALLEST_VEC1);
}

template <class D, OMNI_IF_F64_D(D)> inline VFromD<D> GetLargestVec(D d)
{
    return VFromD<D>(LARGEST_DOUBLE_VEC1);
}

template <class D, OMNI_IF_I64_D(D)> inline VFromD<D> GetLargestVec(D d)
{
    return static_cast<VFromD<D>>(LARGEST_VEC1);
}

template <class D, class V> static void UpdateMinMax(D, V value, V &smallest, V &largest)
{
    if constexpr (std::is_same_v<TFromD<D>, double>) {
        auto compare1 = vcgtq_f64(value.raw, smallest.raw);
        auto compare2 = vcltq_f64(value.raw, largest.raw);
        smallest = static_cast<V>(vbslq_f64(compare1, value.raw, smallest.raw));
        largest = static_cast<V>(vbslq_f64(compare2, value.raw, largest.raw));
    } else {
        auto compare1 = vcgtq_s64(value.raw, smallest.raw);
        auto compare2 = vcltq_s64(value.raw, largest.raw);
        smallest = static_cast<V>(vbslq_s64(compare1, value.raw, smallest.raw));
        largest = static_cast<V>(vbslq_s64(compare2, value.raw, largest.raw));
    }
}

template <class D, OMNI_IF_I64_D(D), class V> MFromD<D> Compare(D d, V v1, V v2)
{
    return Not(static_cast<MFromD<D>>(vcltq_s64(v1.raw, v2.raw)));
}

template <class D, OMNI_IF_F64_D(D), class V> MFromD<D> Compare(D d, V v1, V v2)
{
    return Not(static_cast<MFromD<D>>(vcltq_f64(v1.raw, v2.raw)));
}

template <class D, class V, OMNI_IF_F64_D(D)> inline TFromD<D> GetSmallest(D d, V v)
{
    float64_t fVal0 = vgetq_lane_f64(v.raw, 0);
    float64_t fVal1 = vgetq_lane_f64(v.raw, 1);
    bool compare = fVal0 < fVal1;
    return compare ? fVal1 : fVal0;
}

template <class D, class V, OMNI_IF_I64_D(D)> inline TFromD<D> GetSmallest(D d, V v)
{
    int64_t val0 = vgetq_lane_s64(v.raw, 0);
    int64_t val1 = vgetq_lane_s64(v.raw, 1);
    bool compare = val0 < val1;
    return compare ? val1 : val0;
}

template <class D, class V, OMNI_IF_F64_D(D)> inline TFromD<D> GetLargest(D d, V v)
{
    float64_t fVal0 = vgetq_lane_f64(v.raw, 0);
    float64_t fVal1 = vgetq_lane_f64(v.raw, 1);
    bool compare = fVal0 > fVal1;
    return compare ? fVal1 : fVal0;
}

template <class D, class V, OMNI_IF_I64_D(D)> inline TFromD<D> GetLargest(D d, V v)
{
    int64_t val0 = vgetq_lane_s64(v.raw, 0);
    int64_t val1 = vgetq_lane_s64(v.raw, 1);
    bool compare = val0 > val1;
    return compare ? val1 : val0;
}
};

template <class D, OMNI_IF_UI64_D(D)> inline Vec128<uint64_t> BitCastFromByte(D /* tag */, Vec128<uint8_t> v)
{
    return Vec128<uint64_t>(vreinterpretq_u64_u8(v.raw));
}

template <class D, OMNI_IF_UI8_D(D)> inline VFromD<D> BitCastFromByte(D /* tag */, VFromD<D> v)
{
    return v;
}

template <typename T, size_t N> OMNI_API Mask128<T, N> MaskFromVec(const Vec128<T, N> v)
{
    const Simd<MakeUnsigned<T>, N, 0> du;
    return Mask128<T, N>(BitCast(du, v).raw);
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

template <class Base> struct SharedTraits : public Base {
    using SharedTraitsForSortingNetwork = SharedTraits<typename Base::TraitsForSortingNetwork>;
    SortOrder CurrentOrder = Base::Order;

    template <class V, class M> V CompressKeys(V keys, M mask)
    {
        return CompressNot(keys, mask);
    }
};

template <class Traits, class V> inline V MedianOf3(Traits st, V v0, V v1, V v2)
{
    const DFromV<V> d;
    st.Sort2(d, v0, v2);
    v1 = st.Last(d, v0, v1);
    v1 = st.First(d, v1, v2);
    return v1;
}

inline bool NotEqual(const double &pivot, const double &smallest)
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

template <class D> Vec128<TFromD<D>> GetVec(D d, double buf)
{
    return static_cast<Vec128<TFromD<D>>>(vdupq_n_f64(buf));
}

template <class D> Vec128<TFromD<D>> GetVec(D d, int64_t buf)
{
    return static_cast<Vec128<TFromD<D>>>(vdupq_n_s64(buf));
}

static Mask128<int64_t> inline __attribute__((flatten)) __attribute__((unused)) Not(Mask128<int64_t> value)
{
    uint8x16_t result = vreinterpretq_u8_u64(value.raw);
    uint8x16_t negResult = vmvnq_u8(result);
    return static_cast<Mask128<int64_t>>(vreinterpretq_u64_u8(negResult));
}

static Mask128<double> inline __attribute__((flatten)) __attribute__((unused)) Not(Mask128<double> value)
{
    uint8x16_t result = vreinterpretq_u8_u64(value.raw);
    uint8x16_t negResult = vmvnq_u8(result);
    return static_cast<Mask128<double>>(vreinterpretq_u64_u8(negResult));
}

static uint64_t inline __attribute__((flatten)) __attribute__((unused)) BitsFromMask(Mask128<int64_t> maskVec)
{
    alignas(16) static constexpr uint64_t kSliceLanes[2] = { 1, 2 };
    auto sliceLaneVec = vld1q_u64(kSliceLanes);
    uint64x2_t values = vandq_u64(maskVec.raw, sliceLaneVec);
    auto result = vaddvq_u64(values);
    return result;
}

static uint64_t inline __attribute__((flatten)) __attribute__((unused)) BitsFromMask(Mask128<double> maskVec)
{
    alignas(16) static constexpr uint64_t kSliceLanes[2] = { 1, 2 };
    auto sliceLaneVec = vld1q_u64(kSliceLanes);
    uint64x2_t values = vandq_u64(maskVec.raw, sliceLaneVec);
    auto result = vaddvq_u64(values);
    return result;
}

static Mask128<int64_t> inline SwapMask(Mask128<int64_t> mask)
{
    uint64x2_t tmpMaskL = vzip1q_u64(mask.raw, mask.raw);
    uint64x2_t tmpMaskH = vzip2q_u64(mask.raw, mask.raw);
    uint64x2_t tmpSwap = vbicq_u64(tmpMaskL, tmpMaskH);
    return static_cast<Mask128<int64_t>>(tmpSwap);
}

static Mask128<double> inline SwapMask(Mask128<double> mask)
{
    uint64x2_t tmpMaskL = vzip1q_u64(mask.raw, mask.raw);
    uint64x2_t tmpMaskH = vzip2q_u64(mask.raw, mask.raw);
    uint64x2_t tmpSwap = vbicq_u64(tmpMaskL, tmpMaskH);
    return static_cast<Mask128<double>>(tmpSwap);
}

static Vec128<double> inline CompressNot(Vec128<double> value, Mask128<double> swapMask)
{
    uint8x16_t valueBytes = vreinterpretq_u8_f64(value.raw);
    uint8x16_t shuffleBytes = vextq_u8(valueBytes, valueBytes, 8);
    float64x2_t compressVec = vbslq_f64(swapMask.raw, vreinterpretq_f64_u8(shuffleBytes), value.raw);
    return static_cast<Vec128<double>>(compressVec);
}


static Vec128<int64_t> inline CompressNot(Vec128<int64_t> value, Mask128<int64_t> swapMask)
{
    uint8x16_t valueBytes = vreinterpretq_u8_s64(value.raw);
    uint8x16_t shuffleBytes = vextq_u8(valueBytes, valueBytes, 8);
    int64x2_t compressVec = vbslq_s64(swapMask.raw, vreinterpretq_s64_u8(shuffleBytes), value.raw);
    return static_cast<Vec128<int64_t>>(compressVec);
}

static Vec128<uint64_t> inline CompressNot(Vec128<uint64_t> value, Mask128<int64_t> swapMask)
{
    uint8x16_t valueBytes = vreinterpretq_u8_u64(value.raw);
    uint8x16_t shuffleBytes = vextq_u8(valueBytes, valueBytes, 8);
    uint64x2_t compressVec = vbslq_u64(swapMask.raw, vreinterpretq_u64_u8(shuffleBytes), value.raw);
    return static_cast<Vec128<uint64_t>>(compressVec);
}

static Vec128<uint64_t> inline CompressNot(Vec128<uint64_t> value, Mask128<double> swapMask)
{
    uint8x16_t valueBytes = vreinterpretq_u8_u64(value.raw);
    uint8x16_t shuffleBytes = vextq_u8(valueBytes, valueBytes, 8);
    uint64x2_t compressVec = vbslq_u64(swapMask.raw, vreinterpretq_u64_u8(shuffleBytes), value.raw);
    return static_cast<Vec128<uint64_t>>(compressVec);
}

static int32_t inline CountTrue(uint64x2_t mask)
{
    auto sMask = vreinterpretq_s64_u64(mask);
    auto ones = vnegq_s64(sMask);
    return static_cast<int32_t>(vaddvq_s64(ones));
}

template <class D, class V, class RawType, class Vmask>
static int32_t inline CompressStore(D d, V valueVec, AddrVec addrVec, Vmask mask, RawType *valuePtr, AddrType *addrPtr)
{
    uint64_t maskBits = BitsFromMask(mask);
    auto compressValueVec = Compress(valueVec, maskBits);
    StoreU(compressValueVec, d, valuePtr);
    auto compressAddrVec = Compress(addrVec, maskBits);
    StoreU(compressAddrVec, ADDR_TAG, addrPtr);
    int32_t result = __builtin_popcountll(maskBits);
    return result;
}

template <class D>
OMNI_API size_t CompressBlendedStore(D d, VFromD<D> v, AddrVec addrVec, MFromD<D> m, TFromD<D> *__restrict__ unaligned,
    AddrType *addrPtr)
{
    const RebindToUnsigned<decltype(d)> du;
    const uint64_t mask_bits = BitsFromMask(m);
    const size_t count = __builtin_popcountll(mask_bits);
    const MFromD<D> store_mask = RebindMask(d, FirstN(du, count));
    const VFromD<decltype(du)> compressed = Compress(BitCast(du, v), mask_bits);
    BlendedStore(BitCast(d, compressed), store_mask, d, unaligned);
    auto compressAddrVec = Compress(addrVec, mask_bits);
    auto tmpAddrVec = LoadU(ADDR_TAG, addrPtr);
    auto blendedAddrVec = GetBlendedVec(store_mask, compressAddrVec, tmpAddrVec);
    StoreU(blendedAddrVec, ADDR_TAG, addrPtr);
    return count;
}

template <class D, class M> inline MFromD<D> GetBlendedMask(D d, M a, M b)
{
    return static_cast<MFromD<D>>(vbicq_u64(a.raw, b.raw));
}

template <class D, class M> inline MFromD<D> GetAndMask(D d, M a, M b)
{
    return static_cast<MFromD<D>>(vandq_u64(a.raw, b.raw));
}
}

namespace detail { // for code folding

#undef OMNI_NEON_BUILD_ARG_1
#undef OMNI_NEON_BUILD_ARG_2
#undef OMNI_NEON_BUILD_ARG_3
#undef OMNI_NEON_BUILD_PARAM_1
#undef OMNI_NEON_BUILD_PARAM_2
#undef OMNI_NEON_BUILD_PARAM_3
#undef OMNI_NEON_BUILD_RET_1
#undef OMNI_NEON_BUILD_RET_2
#undef OMNI_NEON_BUILD_RET_3
#undef OMNI_NEON_BUILD_TPL_1
#undef OMNI_NEON_BUILD_TPL_2
#undef OMNI_NEON_BUILD_TPL_3
#undef OMNI_NEON_DEF_FUNCTION
#undef OMNI_NEON_DEF_FUNCTION_ALL_FLOATS
#undef OMNI_NEON_DEF_FUNCTION_ALL_TYPES
#undef OMNI_NEON_DEF_FUNCTION_BFLOAT_16
#undef OMNI_NEON_DEF_FUNCTION_FLOAT_16
#undef OMNI_NEON_DEF_FUNCTION_FLOAT_16_32
#undef OMNI_NEON_DEF_FUNCTION_FLOAT_32
#undef OMNI_NEON_DEF_FUNCTION_FLOAT_64
#undef OMNI_NEON_DEF_FUNCTION_FULL_UI
#undef OMNI_NEON_DEF_FUNCTION_FULL_UI_64
#undef OMNI_NEON_DEF_FUNCTION_FULL_UIF_64
#undef OMNI_NEON_DEF_FUNCTION_INT_16
#undef OMNI_NEON_DEF_FUNCTION_INT_32
#undef OMNI_NEON_DEF_FUNCTION_INT_64
#undef OMNI_NEON_DEF_FUNCTION_INT_8
#undef OMNI_NEON_DEF_FUNCTION_INT_8_16_32
#undef OMNI_NEON_DEF_FUNCTION_INTS
#undef OMNI_NEON_DEF_FUNCTION_INTS_UINTS
#undef OMNI_NEON_DEF_FUNCTION_UI_8_16_32
#undef OMNI_NEON_DEF_FUNCTION_UIF_64
#undef OMNI_NEON_DEF_FUNCTION_UIF_8_16_32
#undef OMNI_NEON_DEF_FUNCTION_UINT_16
#undef OMNI_NEON_DEF_FUNCTION_UINT_32
#undef OMNI_NEON_DEF_FUNCTION_UINT_64
#undef OMNI_NEON_DEF_FUNCTION_UINT_8
#undef OMNI_NEON_DEF_FUNCTION_UINT_8_16_32
#undef OMNI_NEON_DEF_FUNCTION_UINTS
#undef OMNI_NEON_EVAL
#undef OMNI_NEON_IF_EMULATED_D
#undef OMNI_NEON_IF_NOT_EMULATED_D
} // namespace detail

} // namespace omni
#endif
