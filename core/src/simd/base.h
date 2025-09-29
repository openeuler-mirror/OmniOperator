// Copyright 2020 Google LLC
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
#ifndef OMNI_BASE_H_
#define OMNI_BASE_H_

// Target-independent definitions.

// IWYU pragma: begin_exports
#include <stddef.h>
#include <stdint.h>

#if !defined(OMNI_NO_LIBCXX)

#include <ostream>

#endif

#if !defined(OMNI_NO_LIBCXX)
#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS // before inttypes.h
#endif

#include <inttypes.h>

#endif

// ------------------------------------------------------------------------------
// Compiler-specific definitions

#define OMNI_RESTRICT __restrict__
// force inlining without optimization enabled creates very inefficient code
// that can cause compiler timeout
#define OMNI_INLINE inline __attribute__((always_inline))
#define OMNI_FLATTEN __attribute__((flatten))
#define OMNI_UNLIKELY(expr) __builtin_expect(!!(expr), 0)
#define OMNI_PRAGMA(tokens) _Pragma(#tokens)
#define OMNI_DIAGNOSTICS(tokens) OMNI_PRAGMA(GCC diagnostic tokens)
#define OMNI_DIAGNOSTICS_OFF(msc, gcc) OMNI_DIAGNOSTICS(gcc)
// Encountered "attribute list cannot appear here" when using the C++17
// [[maybe_unused]], so only use the old style attribute for now.
#define OMNI_MAYBE_UNUSED __attribute__((unused))

// ------------------------------------------------------------------------------
// Builtin/attributes (no more #include after this point due to namespace!)

namespace simd {
// Returns a void* pointer which the compiler then assumes is N-byte aligned.
// Example: float* OMNI_RESTRICT aligned = (float*)OMNI_ASSUME_ALIGNED(in, 32);
//
// The assignment semantics are required by GCC/Clang. ICC provides an in-place
// __assume_aligned, whereas MSVC's __assume appears unsuitable.

#define OMNI_ASSUME_ALIGNED(ptr, align) (ptr) /* not supported */


// Returns a pointer whose type is `type` (T*), while allowing the compiler to
// assume that the untyped pointer `ptr` is aligned to a multiple of sizeof(T).
#define OMNI_RCAST_ALIGNED(type, ptr) reinterpret_cast<type>(OMNI_ASSUME_ALIGNED((ptr), alignof(RemovePtr<type>)))

// Clang and GCC require attributes on each function into which SIMD intrinsics
// are inlined. Support both per-function annotation (OMNI_ATTR) for lambdas and
// automatic annotation via pragmas.
#define OMNI_PUSH_ATTRIBUTES(targets_str) OMNI_PRAGMA(GCC push_options) OMNI_PRAGMA(GCC target targets_str)
#define OMNI_POP_ATTRIBUTES OMNI_PRAGMA(GCC pop_options)

// ------------------------------------------------------------------------------
// Macros

#define OMNI_API static OMNI_INLINE OMNI_FLATTEN OMNI_MAYBE_UNUSED

#define OMNI_CONCAT_IMPL(a, b) a##b
#define OMNI_CONCAT(a, b) OMNI_CONCAT_IMPL(a, b)

#define OMNI_MIN(a, b) ((a) < (b) ? (a) : (b))
#define OMNI_MAX(a, b) ((a) > (b) ? (a) : (b))

// nielskm: GCC does not support '#pragma GCC unroll' without the factor.

#define OMNI_ABORT(format, ...) 1

// Always enabled.
#define OMNI_ASSERT_M(condition, msg)                     \
    do {                                                  \
        if (!(condition)) {                               \
            OMNI_ABORT("Assert %s: %s", #condition, msg); \
        }                                                 \
    } while (0)
#define OMNI_ASSERT(condition) OMNI_ASSERT_M(condition, "")

template <size_t kBytes, typename From, typename To>
OMNI_API void CopyBytes(const From *OMNI_RESTRICT from, To *OMNI_RESTRICT to)
{
    __builtin_memcpy(to, from, kBytes);
}

OMNI_API void CopyBytes(const void *OMNI_RESTRICT from, void *OMNI_RESTRICT to, size_t num_of_bytes_to_copy)
{
    __builtin_memcpy(to, from, num_of_bytes_to_copy);
}

// Same as CopyBytes, but for same-sized objects; avoids a size argument.
template <typename From, typename To> OMNI_API void CopySameSize(const From *OMNI_RESTRICT from, To *OMNI_RESTRICT to)
{
    static_assert(sizeof(From) == sizeof(To), "");
    CopyBytes<sizeof(From)>(from, to);
}

// ------------------------------------------------------------------------------
// kMaxVectorSize (undocumented, pending removal)
static constexpr OMNI_MAYBE_UNUSED size_t kMaxVectorSize = 16;

// ------------------------------------------------------------------------------
// Alignment

// Potentially useful for LoadDup128 and capped vectors. In other cases, arrays
// should be allocated dynamically via aligned_allocator.h because Lanes() may
// exceed the stack size.
#define OMNI_ALIGN_MAX alignas(16)

struct float16_t;
struct bfloat16_t;

using float32_t = float;
using float64_t = double;

#pragma pack(push, 1)

struct alignas(16) uint128_t {
    uint64_t lo; // little-endian layout
    uint64_t hi;
};

struct alignas(16) K64V64 {
    uint64_t value; // little-endian layout
    uint64_t key;
};

struct alignas(8) K32V32 {
    uint32_t value; // little-endian layout
    uint32_t key;
};

#pragma pack(pop)

static inline OMNI_MAYBE_UNUSED bool operator < (const uint128_t &a, const uint128_t &b)
{
    return (a.hi == b.hi) ? a.lo < b.lo : a.hi < b.hi;
}

// Required for std::greater.
static inline OMNI_MAYBE_UNUSED bool operator > (const uint128_t &a, const uint128_t &b)
{
    return b < a;
}

static inline OMNI_MAYBE_UNUSED bool operator == (const uint128_t &a, const uint128_t &b)
{
    return a.lo == b.lo && a.hi == b.hi;
}

#if !defined(OMNI_NO_LIBCXX)

static inline OMNI_MAYBE_UNUSED std::ostream &operator << (std::ostream &os, const uint128_t &n)
{
    return os << "[hi=" << n.hi << ",lo=" << n.lo << "]";
}

#endif

static inline OMNI_MAYBE_UNUSED bool operator < (const K64V64 &a, const K64V64 &b)
{
    return a.key < b.key;
}

// Required for std::greater.
static inline OMNI_MAYBE_UNUSED bool operator > (const K64V64 &a, const K64V64 &b)
{
    return b < a;
}

static inline OMNI_MAYBE_UNUSED bool operator == (const K64V64 &a, const K64V64 &b)
{
    return a.key == b.key;
}

#if !defined(OMNI_NO_LIBCXX)

static inline OMNI_MAYBE_UNUSED std::ostream &operator << (std::ostream &os, const K64V64 &n)
{
    return os << "[k=" << n.key << ",v=" << n.value << "]";
}

#endif

static inline OMNI_MAYBE_UNUSED bool operator < (const K32V32 &a, const K32V32 &b)
{
    return a.key < b.key;
}

// Required for std::greater.
static inline OMNI_MAYBE_UNUSED bool operator > (const K32V32 &a, const K32V32 &b)
{
    return b < a;
}

static inline OMNI_MAYBE_UNUSED bool operator == (const K32V32 &a, const K32V32 &b)
{
    return a.key == b.key;
}

static inline OMNI_MAYBE_UNUSED std::ostream &operator << (std::ostream &os, const K32V32 &n)
{
    return os << "[k=" << n.key << ",v=" << n.value << "]";
}

// ------------------------------------------------------------------------------
// Controlling overload resolution (SFINAE)

template <bool Condition> struct EnableIfT {};
template <> struct EnableIfT<true> {
    using type = void;
};

template <bool Condition> using EnableIf = typename EnableIfT<Condition>::type;

template <typename T, typename U> struct IsSameT {
    enum {
        value = 0
    };
};

template <typename T> struct IsSameT<T, T> {
    enum {
        value = 1
    };
};

template <typename T, typename U> OMNI_API constexpr bool IsSame()
{
    return IsSameT<T, U>::value;
}

// Returns whether T matches either of U1 or U2
template <typename T, typename U1, typename U2> OMNI_API constexpr bool IsSameEither()
{
    return IsSameT<T, U1>::value || IsSameT<T, U2>::value;
}

template <bool Condition, typename Then, typename Else> struct IfT {
    using type = Then;
};

template <class Then, class Else> struct IfT<false, Then, Else> {
    using type = Else;
};

template <bool Condition, typename Then, typename Else> using If = typename IfT<Condition, Then, Else>::type;

template <typename T> struct IsConstT {
    enum {
        value = 0
    };
};

template <typename T> struct IsConstT<const T> {
    enum {
        value = 1
    };
};

template <typename T> OMNI_API constexpr bool IsConst()
{
    return IsConstT<T>::value;
}

template <class T> struct RemoveConstT {
    using type = T;
};
template <class T> struct RemoveConstT<const T> {
    using type = T;
};

template <class T> using RemoveConst = typename RemoveConstT<T>::type;

template <class T> struct RemoveVolatileT {
    using type = T;
};
template <class T> struct RemoveVolatileT<volatile T> {
    using type = T;
};

template <class T> using RemoveVolatile = typename RemoveVolatileT<T>::type;

template <class T> struct RemoveRefT {
    using type = T;
};
template <class T> struct RemoveRefT<T &> {
    using type = T;
};
template <class T> struct RemoveRefT<T &&> {
    using type = T;
};

template <class T> using RemoveRef = typename RemoveRefT<T>::type;

template <class T> using RemoveCvRef = RemoveConst<RemoveVolatile<RemoveRef<T>>>;

template <class T> struct RemovePtrT {
    using type = T;
};
template <class T> struct RemovePtrT<T *> {
    using type = T;
};
template <class T> struct RemovePtrT<const T *> {
    using type = T;
};
template <class T> struct RemovePtrT<volatile T *> {
    using type = T;
};
template <class T> struct RemovePtrT<const volatile T *> {
    using type = T;
};

template <class T> using RemovePtr = typename RemovePtrT<T>::type;

// Insert into template/function arguments to enable this overload only for
// vectors of exactly, at most (LE), or more than (GT) this many bytes.
//
// As an example, checking for a total size of 16 bytes will match both
// Simd<uint8_t, 16, 0> and Simd<uint8_t, 8, 1>.
#define OMNI_IF_V_SIZE(T, kN, bytes) simd::EnableIf<kN * sizeof(T) == bytes> * = nullptr
#define OMNI_IF_V_SIZE_LE(T, kN, bytes) simd::EnableIf<kN * sizeof(T) <= bytes> * = nullptr
#define OMNI_IF_V_SIZE_GT(T, kN, bytes) simd::EnableIf<(kN * sizeof(T) > bytes)> * = nullptr

#define OMNI_IF_LANES(kN, lanes) simd::EnableIf<(kN == lanes)> * = nullptr
#define OMNI_IF_LANES_LE(kN, lanes) simd::EnableIf<(kN <= lanes)> * = nullptr
#define OMNI_IF_LANES_GT(kN, lanes) simd::EnableIf<(kN > lanes)> * = nullptr

#define OMNI_IF_UNSIGNED(T) simd::EnableIf<!simd::IsSigned<T>()> * = nullptr
#define OMNI_IF_NOT_UNSIGNED(T) simd::EnableIf<simd::IsSigned<T>()> * = nullptr
#define OMNI_IF_SIGNED(T) \
    simd::EnableIf<simd::IsSigned<T>() && !simd::IsFloat<T>() && !simd::IsSpecialFloat<T>()> * = nullptr
#define OMNI_IF_FLOAT(T) simd::EnableIf<simd::IsFloat<T>()> * = nullptr
#define OMNI_IF_NOT_FLOAT(T) simd::EnableIf<!simd::IsFloat<T>()> * = nullptr
#define OMNI_IF_FLOAT3264(T) simd::EnableIf<simd::IsFloat3264<T>()> * = nullptr
#define OMNI_IF_NOT_FLOAT3264(T) simd::EnableIf<!simd::IsFloat3264<T>()> * = nullptr
#define OMNI_IF_SPECIAL_FLOAT(T) simd::EnableIf<simd::IsSpecialFloat<T>()> * = nullptr
#define OMNI_IF_NOT_SPECIAL_FLOAT(T) simd::EnableIf<!simd::IsSpecialFloat<T>()> * = nullptr
#define OMNI_IF_FLOAT_OR_SPECIAL(T) simd::EnableIf<simd::IsFloat<T>() || simd::IsSpecialFloat<T>()> * = nullptr
#define OMNI_IF_NOT_FLOAT_NOR_SPECIAL(T) simd::EnableIf<!simd::IsFloat<T>() && !simd::IsSpecialFloat<T>()> * = nullptr
#define OMNI_IF_INTEGER(T) simd::EnableIf<simd::IsInteger<T>()> * = nullptr

#define OMNI_IF_T_SIZE(T, bytes) simd::EnableIf<sizeof(T) == (bytes)> * = nullptr
#define OMNI_IF_NOT_T_SIZE(T, bytes) simd::EnableIf<sizeof(T) != (bytes)> * = nullptr
// bit_array = 0x102 means 1 or 8 bytes. There is no NONE_OF because it sounds
// too similar. If you want the opposite of this (2 or 4 bytes), ask for those
// bits explicitly (0x14) instead of attempting to 'negate' 0x102.
#define OMNI_IF_T_SIZE_ONE_OF(T, bit_array) simd::EnableIf<((size_t{ 1 } << sizeof(T)) & (bit_array)) != 0> * = nullptr
#define OMNI_IF_T_SIZE_LE(T, bytes) simd::EnableIf<(sizeof(T) <= (bytes))> * = nullptr
#define OMNI_IF_T_SIZE_GT(T, bytes) simd::EnableIf<(sizeof(T) > (bytes))> * = nullptr

#define OMNI_IF_SAME(T, expected) simd::EnableIf<simd::IsSame<simd::RemoveCvRef<T>, expected>()> * = nullptr
#define OMNI_IF_NOT_SAME(T, expected) simd::EnableIf<!simd::IsSame<simd::RemoveCvRef<T>, expected>()> * = nullptr

// One of two expected types
#define OMNI_IF_SAME2(T, expected1, expected2) \
    simd::EnableIf<simd::IsSameEither<simd::RemoveCvRef<T>, expected1, expected2>()> * = nullptr

#define OMNI_IF_U8(T) OMNI_IF_SAME(T, uint8_t)
#define OMNI_IF_U16(T) OMNI_IF_SAME(T, uint16_t)
#define OMNI_IF_U32(T) OMNI_IF_SAME(T, uint32_t)
#define OMNI_IF_U64(T) OMNI_IF_SAME(T, uint64_t)

#define OMNI_IF_I8(T) OMNI_IF_SAME(T, int8_t)
#define OMNI_IF_I16(T) OMNI_IF_SAME(T, int16_t)
#define OMNI_IF_I32(T) OMNI_IF_SAME(T, int32_t)
#define OMNI_IF_I64(T) OMNI_IF_SAME(T, int64_t)

#define OMNI_IF_BF16(T) OMNI_IF_SAME(T, simd::bfloat16_t)
#define OMNI_IF_NOT_BF16(T) OMNI_IF_NOT_SAME(T, simd::bfloat16_t)

#define OMNI_IF_F16(T) OMNI_IF_SAME(T, simd::float16_t)
#define OMNI_IF_NOT_F16(T) OMNI_IF_NOT_SAME(T, simd::float16_t)

#define OMNI_IF_F32(T) OMNI_IF_SAME(T, float)
#define OMNI_IF_F64(T) OMNI_IF_SAME(T, double)

// Use instead of OMNI_IF_T_SIZE to avoid ambiguity with float16_t/float/double
// overloads.
#define OMNI_IF_UI8(T) OMNI_IF_SAME2(T, uint8_t, int8_t)
#define OMNI_IF_UI16(T) OMNI_IF_SAME2(T, uint16_t, int16_t)
#define OMNI_IF_UI32(T) OMNI_IF_SAME2(T, uint32_t, int32_t)
#define OMNI_IF_UI64(T) OMNI_IF_SAME2(T, uint64_t, int64_t)

#define OMNI_IF_LANES_PER_BLOCK(T, N, LANES) \
    simd::EnableIf<OMNI_MIN(sizeof(T) * N, 16) / sizeof(T) == (LANES)> * = nullptr

// Empty struct used as a size tag type.
template <size_t N> struct SizeTag {};

template <class T> class DeclValT {
private:
    template <class U, class URef = U &&> static URef TryAddRValRef(int);

    template <class U, class Arg> static U TryAddRValRef(Arg);

public:
    using type = decltype(TryAddRValRef<T>(0));
    enum {
        kDisableDeclValEvaluation = 1
    };
};

// simd::DeclVal<T>() can only be used in unevaluated contexts such as within an
// expression of a decltype specifier.

// simd::DeclVal<T>() does not require that T have a public default constructor
template <class T> OMNI_API typename DeclValT<T>::type DeclVal() noexcept
{
    static_assert(!DeclValT<T>::kDisableDeclValEvaluation, "DeclVal() cannot be used in an evaluated context");
}

template <class T> struct IsArrayT {
    enum {
        value = 0
    };
};

template <class T> struct IsArrayT<T[]> {
    enum {
        value = 1
    };
};

template <class T, size_t N> struct IsArrayT<T[N]> {
    enum {
        value = 1
    };
};

template <class T> static constexpr bool IsArray()
{
    return IsArrayT<T>::value;
}

template <class From, class To> class IsConvertibleT {
private:
    template <class T> static simd::SizeTag<1> TestFuncWithToArg(T);

    template <class T, class U>
    static decltype(IsConvertibleT<T, U>::template TestFuncWithToArg<U>(DeclVal<T>())) TryConvTest(int);

    template <class T, class U, class Arg> static simd::SizeTag<0> TryConvTest(Arg);

public:
    enum {
        value =
            (IsSame<RemoveConst<RemoveVolatile<From>>, void>() && IsSame<RemoveConst<RemoveVolatile<To>>, void>()) ||
        (!IsArray<To>() &&
        (IsSame<To, decltype(DeclVal<To>())>() || !IsSame<const RemoveConst<To>, RemoveConst<To>>()) &&
        IsSame<decltype(TryConvTest<From, To>(0)), simd::SizeTag<1>>())
    };
};

template <class From, class To> OMNI_API constexpr bool IsConvertible()
{
    return IsConvertibleT<From, To>::value;
}

template <class From, class To> class IsStaticCastableT {
private:
    template <class T, class U, class = decltype(static_cast<U>(DeclVal<T>()))>
    static simd::SizeTag<1> TryStaticCastTest(int);

    template <class T, class U, class Arg> static simd::SizeTag<0> TryStaticCastTest(Arg);

public:
    enum {
        value = IsSame<decltype(TryStaticCastTest<From, To>(0)), simd::SizeTag<1>>()
    };
};

template <class From, class To> static constexpr bool IsStaticCastable()
{
    return IsStaticCastableT<From, To>::value;
}

#define OMNI_IF_CASTABLE(From, To) simd::EnableIf<IsStaticCastable<From, To>()> * = nullptr

#define OMNI_IF_OP_CASTABLE(op, T, Native) OMNI_IF_CASTABLE(decltype(DeclVal<Native>() op DeclVal<T>()), Native)

template <class T, class From> class IsAssignableT {
private:
    template <class T1, class T2, class = decltype(DeclVal<T1>() = DeclVal<T2>())>
    static simd::SizeTag<1> TryAssignTest(int);

    template <class T1, class T2, class Arg> static simd::SizeTag<0> TryAssignTest(Arg);

public:
    enum {
        value = IsSame<decltype(TryAssignTest<T, From>(0)), simd::SizeTag<1>>()
    };
};

template <class T, class From> static constexpr bool IsAssignable()
{
    return IsAssignableT<T, From>::value;
}

#define OMNI_IF_ASSIGNABLE(T, From) simd::EnableIf<IsAssignable<T, From>()> * = nullptr

// ----------------------------------------------------------------------------
// IsSpecialFloat

// These types are often special-cased and not supported in all ops.
template <typename T> OMNI_API constexpr bool IsSpecialFloat()
{
    return IsSameEither<RemoveCvRef<T>, simd::float16_t, simd::bfloat16_t>();
}

// -----------------------------------------------------------------------------
// IsIntegerLaneType and IsInteger

template <class T> OMNI_API constexpr bool IsIntegerLaneType()
{
    return false;
}

template <> OMNI_INLINE constexpr bool IsIntegerLaneType<int8_t>()
{
    return true;
}

template <> OMNI_INLINE constexpr bool IsIntegerLaneType<uint8_t>()
{
    return true;
}

template <> OMNI_INLINE constexpr bool IsIntegerLaneType<int16_t>()
{
    return true;
}

template <> OMNI_INLINE constexpr bool IsIntegerLaneType<uint16_t>()
{
    return true;
}

template <> OMNI_INLINE constexpr bool IsIntegerLaneType<int32_t>()
{
    return true;
}

template <> OMNI_INLINE constexpr bool IsIntegerLaneType<uint32_t>()
{
    return true;
}

template <> OMNI_INLINE constexpr bool IsIntegerLaneType<int64_t>()
{
    return true;
}

template <> OMNI_INLINE constexpr bool IsIntegerLaneType<uint64_t>()
{
    return true;
}

namespace detail {
template <class T> static OMNI_INLINE constexpr bool IsNonCvInteger()
{
    // NOTE: Do not add a IsNonCvInteger<wchar_t>() specialization below as it is
    // possible for IsSame<wchar_t, uint16_t>() to be true when compiled with MSVC
    // with the /Zc:wchar_t- option.
    return IsIntegerLaneType<T>() || IsSame<T, wchar_t>() || IsSameEither<T, size_t, ptrdiff_t>() ||
        IsSameEither<T, intptr_t, uintptr_t>();
}

template <> OMNI_INLINE constexpr bool IsNonCvInteger<bool>()
{
    return true;
}

template <> OMNI_INLINE constexpr bool IsNonCvInteger<char>()
{
    return true;
}

template <> OMNI_INLINE constexpr bool IsNonCvInteger<signed char>()
{
    return true;
}

template <> OMNI_INLINE constexpr bool IsNonCvInteger<unsigned char>()
{
    return true;
}

template <> OMNI_INLINE constexpr bool IsNonCvInteger<short>()
{ // NOLINT
    return true;
}

template <> OMNI_INLINE constexpr bool IsNonCvInteger<unsigned short>()
{ // NOLINT
    return true;
}

template <> OMNI_INLINE constexpr bool IsNonCvInteger<int>()
{
    return true;
}

template <> OMNI_INLINE constexpr bool IsNonCvInteger<unsigned>()
{
    return true;
}

template <> OMNI_INLINE constexpr bool IsNonCvInteger<long>()
{ // NOLINT
    return true;
}

template <> OMNI_INLINE constexpr bool IsNonCvInteger<unsigned long>()
{ // NOLINT
    return true;
}

template <> OMNI_INLINE constexpr bool IsNonCvInteger<long long>()
{ // NOLINT
    return true;
}

template <> OMNI_INLINE constexpr bool IsNonCvInteger<unsigned long long>()
{ // NOLINT
    return true;
}

#if defined(__cpp_char8_t) && __cpp_char8_t >= 201811L
template <> OMNI_INLINE constexpr bool IsNonCvInteger<char8_t>()
{
    return true;
}
#endif

template <> OMNI_INLINE constexpr bool IsNonCvInteger<char16_t>()
{
    return true;
}

template <> OMNI_INLINE constexpr bool IsNonCvInteger<char32_t>()
{
    return true;
}
} // namespace detail

template <class T> OMNI_API constexpr bool IsInteger()
{
    return detail::IsNonCvInteger<RemoveCvRef<T>>();
}

// -----------------------------------------------------------------------------
// BitCastScalar
template <class To, class From> OMNI_API constexpr To BitCastScalar(const From &val)
{
    To result;
    CopySameSize(&val, &result);
    return result;
}


// ------------------------------------------------------------------------------
// F16 lane type

#pragma pack(push, 1)

namespace detail {
template <class T, class TVal = RemoveCvRef<T>, bool = IsSpecialFloat<TVal>()>
struct SpecialFloatUnwrapArithOpOperandT {};

template <class T, class TVal> struct SpecialFloatUnwrapArithOpOperandT<T, TVal, false> {
    using type = T;
};

template <class T> using SpecialFloatUnwrapArithOpOperand = typename SpecialFloatUnwrapArithOpOperandT<T>::type;

template <class T, class TVal = RemoveCvRef<T>> struct NativeSpecialFloatToWrapperT {
    using type = T;
};

template <class T> using NativeSpecialFloatToWrapper = typename NativeSpecialFloatToWrapperT<T>::type;
} // namespace detail

struct alignas(2) bfloat16_t {
    union {
        uint16_t bits;
    };

    bfloat16_t() noexcept = default;

    constexpr bfloat16_t(bfloat16_t &&) noexcept = default;

    constexpr bfloat16_t(const bfloat16_t &) noexcept = default;

    bfloat16_t &operator = (bfloat16_t &&arg) noexcept = default;

    bfloat16_t &operator = (const bfloat16_t &arg) noexcept = default;

private:
    struct BF16FromU16BitsTag {};

    constexpr bfloat16_t(BF16FromU16BitsTag /* tag */, uint16_t u16_bits) : bits(u16_bits) {}

public:
    static constexpr bfloat16_t FromBits(uint16_t bits)
    {
        return bfloat16_t(BF16FromU16BitsTag(), bits);
    }
};

static_assert(sizeof(simd::bfloat16_t) == 2, "Wrong size of bfloat16_t");

#pragma pack(pop)

OMNI_API constexpr float F32FromBF16(bfloat16_t bf)
{
    return BitCastScalar<float>(static_cast<uint32_t>(static_cast<uint32_t>(BitCastScalar<uint16_t>(bf)) << 16));
}

namespace detail {
// Returns the increment to add to the bits of a finite F32 value to round a
// finite F32 to the nearest BF16 value
static OMNI_INLINE OMNI_MAYBE_UNUSED constexpr uint32_t F32BitsToBF16RoundIncr(const uint32_t f32_bits)
{
    return static_cast<uint32_t>(((f32_bits & 0x7FFFFFFFu) < 0x7F800000u) ? (0x7FFFu + ((f32_bits >> 16) & 1u)) : 0u);
}

// Converts f32_bits (which is the bits of a F32 value) to BF16 bits,
// rounded to the nearest F16 value
static OMNI_INLINE OMNI_MAYBE_UNUSED constexpr uint16_t F32BitsToBF16Bits(const uint32_t f32_bits)
{
    return static_cast<uint16_t>(((f32_bits + F32BitsToBF16RoundIncr(f32_bits)) >> 16) |
        (static_cast<uint32_t>((f32_bits & 0x7FFFFFFFu) > 0x7F800000u) << 6));
}
} // namespace detail

OMNI_API constexpr bfloat16_t BF16FromF32(float f)
{
    return bfloat16_t::FromBits(detail::F32BitsToBF16Bits(BitCastScalar<uint32_t>(f)));
}

OMNI_API constexpr bfloat16_t BF16FromF64(double f64)
{
    return BF16FromF32(static_cast<float>(
        BitCastScalar<double>(static_cast<uint64_t>((BitCastScalar<uint64_t>(f64) & 0xFFFFFFC000000000ULL) |
        ((BitCastScalar<uint64_t>(f64) + 0x0000003FFFFFFFFFULL) & 0x0000004000000000ULL)))));
}

// More convenient to define outside bfloat16_t because these may use
// F32FromBF16, which is defined after the struct.

constexpr inline bool operator == (bfloat16_t lhs, bfloat16_t rhs) noexcept
{
    return F32FromBF16(lhs) == F32FromBF16(rhs);
}

constexpr inline bool operator != (bfloat16_t lhs, bfloat16_t rhs) noexcept
{
    return F32FromBF16(lhs) != F32FromBF16(rhs);
}

constexpr inline bool operator < (bfloat16_t lhs, bfloat16_t rhs) noexcept
{
    return F32FromBF16(lhs) < F32FromBF16(rhs);
}

constexpr inline bool operator <= (bfloat16_t lhs, bfloat16_t rhs) noexcept
{
    return F32FromBF16(lhs) <= F32FromBF16(rhs);
}

constexpr inline bool operator > (bfloat16_t lhs, bfloat16_t rhs) noexcept
{
    return F32FromBF16(lhs) > F32FromBF16(rhs);
}

constexpr inline bool operator >= (bfloat16_t lhs, bfloat16_t rhs) noexcept
{
    return F32FromBF16(lhs) >= F32FromBF16(rhs);
}

// ------------------------------------------------------------------------------
// Type relations

namespace detail {
template <typename T> struct Relations;
template <> struct Relations<uint8_t> {
    using Unsigned = uint8_t;
    using Signed = int8_t;
    using Wide = uint16_t;
    enum {
        is_signed = 0,
        is_float = 0,
        is_bf16 = 0
    };
};
template <> struct Relations<int8_t> {
    using Unsigned = uint8_t;
    using Signed = int8_t;
    using Wide = int16_t;
    enum {
        is_signed = 1,
        is_float = 0,
        is_bf16 = 0
    };
};
template <> struct Relations<uint16_t> {
    using Unsigned = uint16_t;
    using Signed = int16_t;
    using Float = float16_t;
    using Wide = uint32_t;
    using Narrow = uint8_t;
    enum {
        is_signed = 0,
        is_float = 0,
        is_bf16 = 0
    };
};
template <> struct Relations<int16_t> {
    using Unsigned = uint16_t;
    using Signed = int16_t;
    using Float = float16_t;
    using Wide = int32_t;
    using Narrow = int8_t;
    enum {
        is_signed = 1,
        is_float = 0,
        is_bf16 = 0
    };
};
template <> struct Relations<uint32_t> {
    using Unsigned = uint32_t;
    using Signed = int32_t;
    using Float = float;
    using Wide = uint64_t;
    using Narrow = uint16_t;
    enum {
        is_signed = 0,
        is_float = 0,
        is_bf16 = 0
    };
};
template <> struct Relations<int32_t> {
    using Unsigned = uint32_t;
    using Signed = int32_t;
    using Float = float;
    using Wide = int64_t;
    using Narrow = int16_t;
    enum {
        is_signed = 1,
        is_float = 0,
        is_bf16 = 0
    };
};
template <> struct Relations<uint64_t> {
    using Unsigned = uint64_t;
    using Signed = int64_t;
    using Float = double;
    using Wide = uint128_t;
    using Narrow = uint32_t;
    enum {
        is_signed = 0,
        is_float = 0,
        is_bf16 = 0
    };
};
template <> struct Relations<int64_t> {
    using Unsigned = uint64_t;
    using Signed = int64_t;
    using Float = double;
    using Narrow = int32_t;
    enum {
        is_signed = 1,
        is_float = 0,
        is_bf16 = 0
    };
};
template <> struct Relations<uint128_t> {
    using Unsigned = uint128_t;
    using Narrow = uint64_t;
    enum {
        is_signed = 0,
        is_float = 0,
        is_bf16 = 0
    };
};
template <> struct Relations<float16_t> {
    using Unsigned = uint16_t;
    using Signed = int16_t;
    using Float = float16_t;
    using Wide = float;
    enum {
        is_signed = 1,
        is_float = 1,
        is_bf16 = 0
    };
};
template <> struct Relations<bfloat16_t> {
    using Unsigned = uint16_t;
    using Signed = int16_t;
    using Wide = float;
    enum {
        is_signed = 1,
        is_float = 1,
        is_bf16 = 1
    };
};
template <> struct Relations<float> {
    using Unsigned = uint32_t;
    using Signed = int32_t;
    using Float = float;
    using Wide = double;
    using Narrow = float16_t;
    enum {
        is_signed = 1,
        is_float = 1,
        is_bf16 = 0
    };
};
template <> struct Relations<double> {
    using Unsigned = uint64_t;
    using Signed = int64_t;
    using Float = double;
    using Narrow = float;
    enum {
        is_signed = 1,
        is_float = 1,
        is_bf16 = 0
    };
};

template <size_t N> struct TypeFromSize;
template <> struct TypeFromSize<1> {
    using Unsigned = uint8_t;
    using Signed = int8_t;
};
template <> struct TypeFromSize<2> {
    using Unsigned = uint16_t;
    using Signed = int16_t;
    using Float = float16_t;
};
template <> struct TypeFromSize<4> {
    using Unsigned = uint32_t;
    using Signed = int32_t;
    using Float = float;
};
template <> struct TypeFromSize<8> {
    using Unsigned = uint64_t;
    using Signed = int64_t;
    using Float = double;
};
template <> struct TypeFromSize<16> {
    using Unsigned = uint128_t;
};
} // namespace detail

// Aliases for types of a different category, but the same size.
template <typename T> using MakeUnsigned = typename detail::Relations<T>::Unsigned;
template <typename T> using MakeSigned = typename detail::Relations<T>::Signed;
template <typename T> using MakeFloat = typename detail::Relations<T>::Float;

// Aliases for types of the same category, but different size.
template <typename T> using MakeWide = typename detail::Relations<T>::Wide;
template <typename T> using MakeNarrow = typename detail::Relations<T>::Narrow;

// Obtain type from its size [bytes].
template <size_t N> using UnsignedFromSize = typename detail::TypeFromSize<N>::Unsigned;
template <size_t N> using SignedFromSize = typename detail::TypeFromSize<N>::Signed;
template <size_t N> using FloatFromSize = typename detail::TypeFromSize<N>::Float;

using UnsignedTag = SizeTag<0>;
using SignedTag = SizeTag<0x100>; // integer
using FloatTag = SizeTag<0x200>;
using SpecialTag = SizeTag<0x300>;

template <typename T, class R = detail::Relations<T>>
constexpr auto TypeTag() -> simd::SizeTag<((R::is_signed + R::is_float + R::is_bf16) << 8)>
{
    return simd::SizeTag<((R::is_signed + R::is_float + R::is_bf16) << 8)>();
}

// For when we only want to distinguish FloatTag from everything else.
using NonFloatTag = SizeTag<0x400>;

template <typename T, class R = detail::Relations<T>>
constexpr auto IsFloatTag() -> simd::SizeTag<(R::is_float ? 0x200 : 0x400)>
{
    return simd::SizeTag<(R::is_float ? 0x200 : 0x400)>();
}

// ------------------------------------------------------------------------------
// Type traits

template <typename T> OMNI_API constexpr bool IsFloat3264()
{
    return IsSameEither<RemoveCvRef<T>, float, double>();
}

template <typename T> OMNI_API constexpr bool IsFloat()
{
    // Cannot use T(1.25) != T(1) for float16_t, which can only be converted to or
    // from a float, not compared. Include float16_t in case OMNI_HAVE_FLOAT16=1.
    return IsSame<RemoveCvRef<T>, float16_t>() || IsFloat3264<T>();
}

template <typename T> OMNI_API constexpr bool IsSigned()
{
    return static_cast<T>(0) > static_cast<T>(-1);
}

template <> constexpr bool IsSigned<float16_t>()
{
    return true;
}

template <> constexpr bool IsSigned<bfloat16_t>()
{
    return true;
}

template <> constexpr bool IsSigned<simd::uint128_t>()
{
    return false;
}

template <> constexpr bool IsSigned<simd::K64V64>()
{
    return false;
}

template <> constexpr bool IsSigned<simd::K32V32>()
{
    return false;
}

template <typename T, bool = IsInteger<T>() && !IsIntegerLaneType<T>()> struct MakeLaneTypeIfIntegerT {
    using type = T;
};

template <typename T> struct MakeLaneTypeIfIntegerT<T, true> {
    using type = simd::If<IsSigned<T>(), SignedFromSize<sizeof(T)>, UnsignedFromSize<sizeof(T)>>;
};

template <typename T> using MakeLaneTypeIfInteger = typename MakeLaneTypeIfIntegerT<T>::type;

// Largest/smallest representable integer values.
template <typename T> OMNI_API constexpr T LimitsMax()
{
    static_assert(IsInteger<T>(), "Only for integer types");
    using TU = UnsignedFromSize<sizeof(T)>;
    return static_cast<T>(IsSigned<T>() ? (static_cast<TU>(~TU(0)) >> 1) : static_cast<TU>(~TU(0)));
}

template <typename T> OMNI_API constexpr T LimitsMin()
{
    static_assert(IsInteger<T>(), "Only for integer types");
    return IsSigned<T>() ? static_cast<T>(-1) - LimitsMax<T>() : static_cast<T>(0);
}

// Largest/smallest representable value (integer or float). This naming avoids
// confusion with numeric_limits<float>::min() (the smallest positive value).
// Cannot be constexpr because we use CopySameSize for [b]float16_t.
template <typename T> OMNI_API constexpr T LowestValue()
{
    return LimitsMin<T>();
}

template <> OMNI_INLINE constexpr bfloat16_t LowestValue<bfloat16_t>()
{
    return bfloat16_t::FromBits(uint16_t{ 0xFF7Fu }); // -1.1111111 x 2^127
}

template <> OMNI_INLINE constexpr float LowestValue<float>()
{
    return -3.402823466e+38F;
}

template <> OMNI_INLINE constexpr double LowestValue<double>()
{
    return -1.7976931348623158e+308;
}

template <typename T> OMNI_API constexpr T HighestValue()
{
    return LimitsMax<T>();
}

template <> OMNI_INLINE constexpr bfloat16_t HighestValue<bfloat16_t>()
{
    return bfloat16_t::FromBits(uint16_t{ 0x7F7Fu }); // 1.1111111 x 2^127
}

template <> OMNI_INLINE constexpr float HighestValue<float>()
{
    return 3.402823466e+38F;
}

template <> OMNI_INLINE constexpr double HighestValue<double>()
{
    return 1.7976931348623158e+308;
}

// Difference between 1.0 and the next representable value. Equal to
// 1 / (1ULL << MantissaBits<T>()), but hard-coding ensures precision.
template <typename T> OMNI_API constexpr T Epsilon()
{
    return 1;
}

template <> OMNI_INLINE constexpr bfloat16_t Epsilon<bfloat16_t>()
{
    return bfloat16_t::FromBits(uint16_t{ 0x3C00u }); // 0.0078125
}

template <> OMNI_INLINE constexpr float Epsilon<float>()
{
    return 1.192092896e-7f;
}

template <> OMNI_INLINE constexpr double Epsilon<double>()
{
    return 2.2204460492503131e-16;
}

// Returns width in bits of the mantissa field in IEEE binary16/32/64.
template <typename T> constexpr int MantissaBits()
{
    static_assert(sizeof(T) == 0, "Only instantiate the specializations");
    return 0;
}

template <> constexpr int MantissaBits<bfloat16_t>()
{
    return 7;
}

template <> constexpr int MantissaBits<float16_t>()
{
    return 10;
}

template <> constexpr int MantissaBits<float>()
{
    return 23;
}

template <> constexpr int MantissaBits<double>()
{
    return 52;
}

// Returns the (left-shifted by one bit) IEEE binary16/32/64 representation with
// the largest possible (biased) exponent field. Used by IsInf.
template <typename T> constexpr MakeSigned<T> MaxExponentTimes2()
{
    return -(MakeSigned<T>{ 1 } << (MantissaBits<T>() + 1));
}

// Returns bitmask of the sign bit in IEEE binary16/32/64.
template <typename T> constexpr MakeUnsigned<T> SignMask()
{
    return MakeUnsigned<T>{ 1 } << (sizeof(T) * 8 - 1);
}

// Returns bitmask of the exponent field in IEEE binary16/32/64.
template <typename T> constexpr MakeUnsigned<T> ExponentMask()
{
    return (~(MakeUnsigned<T>{ 1 } << MantissaBits<T>()) + 1) & static_cast<MakeUnsigned<T>>(~SignMask<T>());
}

// Returns bitmask of the mantissa field in IEEE binary16/32/64.
template <typename T> constexpr MakeUnsigned<T> MantissaMask()
{
    return (MakeUnsigned<T>{ 1 } << MantissaBits<T>()) - 1;
}

// Returns 1 << mantissa_bits as a floating-point number. All integers whose
// absolute value are less than this can be represented exactly.
template <typename T> OMNI_INLINE constexpr T MantissaEnd()
{
    static_assert(sizeof(T) == 0, "Only instantiate the specializations");
    return 0;
}

template <> OMNI_INLINE constexpr bfloat16_t MantissaEnd<bfloat16_t>()
{
    return bfloat16_t::FromBits(uint16_t{ 0x4300u }); // 1.0 x 2^7
}

template <> OMNI_INLINE constexpr float MantissaEnd<float>()
{
    return 8388608.0f; // 1 << 23
}

template <> OMNI_INLINE constexpr double MantissaEnd<double>()
{
    // floating point literal with p52 requires C++17.
    return 4503599627370496.0; // 1 << 52
}

// Returns width in bits of the exponent field in IEEE binary16/32/64.
template <typename T> constexpr int ExponentBits()
{
    // Exponent := remaining bits after deducting sign and mantissa.
    return 8 * sizeof(T) - 1 - MantissaBits<T>();
}

// Returns largest value of the biased exponent field in IEEE binary16/32/64,
// right-shifted so that the LSB is bit zero. Example: 0xFF for float.
// This is expressed as a signed integer for more efficient comparison.
template <typename T> constexpr MakeSigned<T> MaxExponentField()
{
    return (MakeSigned<T>{ 1 } << ExponentBits<T>()) - 1;
}

// ------------------------------------------------------------------------------
// Additional F16/BF16 operators
#define OMNI_RHS_SPECIAL_FLOAT_ARITH_OP(op, op_func, T2)                                                           \
    template <typename T1,                                                                                         \
        simd::EnableIf<simd::IsInteger<RemoveCvRef<T1>>() || simd::IsFloat3264<RemoveCvRef<T1>>()> * = nullptr,    \
        typename RawResultT = decltype(DeclVal<T1>() op DeclVal<T2::Native>()),                                    \
        typename ResultT = detail::NativeSpecialFloatToWrapper<RawResultT>, OMNI_IF_CASTABLE(RawResultT, ResultT)> \
    static OMNI_INLINE constexpr ResultT op_func(T1 a, T2 b) noexcept                                              \
    {                                                                                                              \
        return static_cast<ResultT>(a op b.native);                                                                \
    }

#define OMNI_SPECIAL_FLOAT_CMP_AGAINST_NON_SPECIAL_OP(op, op_func, T1)                                             \
    OMNI_RHS_SPECIAL_FLOAT_ARITH_OP(op, op_func, T1)                                                               \
    template <typename T2,                                                                                         \
        simd::EnableIf<simd::IsInteger<RemoveCvRef<T2>>() || simd::IsFloat3264<RemoveCvRef<T2>>()> * = nullptr,    \
        typename RawResultT = decltype(DeclVal<T1::Native>() op DeclVal<T2>()),                                    \
        typename ResultT = detail::NativeSpecialFloatToWrapper<RawResultT>, OMNI_IF_CASTABLE(RawResultT, ResultT)> \
    static OMNI_INLINE constexpr ResultT op_func(T1 a, T2 b) noexcept                                              \
    {                                                                                                              \
        return static_cast<ResultT>(a.native op b);                                                                \
    }

#undef OMNI_RHS_SPECIAL_FLOAT_ARITH_OP
#undef OMNI_SPECIAL_FLOAT_CMP_AGAINST_NON_SPECIAL_OP

// ------------------------------------------------------------------------------
// Type conversions (after IsSpecialFloat)

OMNI_API float F32FromBF16Mem(const void *ptr)
{
    bfloat16_t bf;
    CopyBytes<2>(OMNI_ASSUME_ALIGNED(ptr, 2), &bf);
    return F32FromBF16(bf);
}

// For casting from TFrom to TTo
template <typename TTo, typename TFrom, OMNI_IF_NOT_SPECIAL_FLOAT(TTo), OMNI_IF_NOT_SPECIAL_FLOAT(TFrom),
    OMNI_IF_NOT_SAME(TTo, TFrom)>
OMNI_API constexpr TTo ConvertScalarTo(const TFrom in)
{
    return static_cast<TTo>(in);
}

template <typename TTo, typename TFrom, OMNI_IF_BF16(TTo), OMNI_IF_NOT_SPECIAL_FLOAT(TFrom),
    OMNI_IF_NOT_SAME(TFrom, double)>
OMNI_API constexpr TTo ConvertScalarTo(const TFrom in)
{
    return BF16FromF32(static_cast<float>(in));
}

template <typename TTo, OMNI_IF_BF16(TTo)> OMNI_API constexpr TTo ConvertScalarTo(const double in)
{
    return BF16FromF64(in);
}

template <typename TTo, typename TFrom, OMNI_IF_F16(TFrom), OMNI_IF_NOT_SPECIAL_FLOAT(TTo)>
OMNI_API constexpr TTo ConvertScalarTo(const TFrom in)
{
    return static_cast<TTo>(F32FromF16(in));
}

template <typename TTo, typename TFrom, OMNI_IF_BF16(TFrom), OMNI_IF_NOT_SPECIAL_FLOAT(TTo)>
OMNI_API constexpr TTo ConvertScalarTo(TFrom in)
{
    return static_cast<TTo>(F32FromBF16(in));
}

// Same: return unchanged
template <typename TTo> OMNI_API constexpr TTo ConvertScalarTo(TTo in)
{
    return in;
}

// ------------------------------------------------------------------------------
// Helper functions

template <typename T1, typename T2> constexpr inline T1 DivCeil(T1 a, T2 b)
{
    return (a + b - 1) / b;
}

constexpr inline size_t RoundUpTo(size_t what, size_t align)
{
    return DivCeil(what, align) * align;
}

constexpr inline size_t RoundDownTo(size_t what, size_t align)
{
    return what - (what % align);
}

namespace detail {
template <class T> static OMNI_INLINE constexpr T ScalarShr(simd::UnsignedTag /* type_tag */, T val, int shift_amt)
{
    return static_cast<T>(val >> shift_amt);
}

// T is signed and (val >> shift_amt) is a non-arithmetic right shift
template <class T> static OMNI_INLINE constexpr T ScalarShr(simd::SignedTag /* type_tag */, T val, int shift_amt)
{
    using TU = MakeUnsigned<MakeLaneTypeIfInteger<T>>;
    return static_cast<T>((val < 0) ? static_cast<TU>(~(static_cast<TU>(~static_cast<TU>(val)) >> shift_amt)) :
                                      static_cast<TU>(static_cast<TU>(val) >> shift_amt));
}
} // namespace detail

// If T is an signed integer type, ScalarShr is guaranteed to perform an
// arithmetic right shift

// Otherwise, if T is an unsigned integer type, ScalarShr is guaranteed to
// perform a logical right shift
template <class T, OMNI_IF_INTEGER(RemoveCvRef<T>)> OMNI_API constexpr RemoveCvRef<T> ScalarShr(T val, int shift_amt)
{
    using NonCvRefT = RemoveCvRef<T>;
    return detail::ScalarShr(simd::SizeTag<(
        (IsSigned<NonCvRefT>() && (LimitsMin<NonCvRefT>() >> (sizeof(T) * 8 - 1)) != static_cast<NonCvRefT>(-1)) ?
        0x100 :
        0)>(),
        static_cast<NonCvRefT>(val), shift_amt);
}

// Undefined results for x == 0.
OMNI_API size_t Num0BitsBelowLS1Bit_Nonzero32(const uint32_t x)
{
    return static_cast<size_t>(__builtin_ctz(x));
}

OMNI_API size_t Num0BitsBelowLS1Bit_Nonzero64(const uint64_t x)
{
    return static_cast<size_t>(__builtin_ctzll(x));
}

// Undefined results for x == 0.
OMNI_API size_t Num0BitsAboveMS1Bit_Nonzero32(const uint32_t x)
{
    return static_cast<size_t>(__builtin_clz(x));
}

OMNI_API size_t Num0BitsAboveMS1Bit_Nonzero64(const uint64_t x)
{
    return static_cast<size_t>(__builtin_clzll(x));
}

template <class T, OMNI_IF_INTEGER(RemoveCvRef<T>),
    OMNI_IF_T_SIZE_ONE_OF(RemoveCvRef<T>, (1 << 1) | (1 << 2) | (1 << 4))>
OMNI_API size_t PopCount(T x)
{
    uint32_t u32_x = static_cast<uint32_t>(static_cast<UnsignedFromSize<sizeof(RemoveCvRef<T>)>>(x));
    return static_cast<size_t>(__builtin_popcountl(u32_x));
}

template <class T, OMNI_IF_INTEGER(RemoveCvRef<T>), OMNI_IF_T_SIZE(RemoveCvRef<T>, 8)> OMNI_API size_t PopCount(T x)
{
    uint64_t u64_x = static_cast<uint64_t>(static_cast<UnsignedFromSize<sizeof(RemoveCvRef<T>)>>(x));
    return static_cast<size_t>(__builtin_popcountll(u64_x));
}

// Skip OMNI_API due to GCC "function not considered for inlining". Previously
// such errors were caused by underlying type mismatches, but it's not clear
// what is still mismatched despite all the casts.
template <typename TI> constexpr size_t FloorLog2(TI x)
{
    return x == TI{ 1 } ? 0 : static_cast<size_t>(FloorLog2(static_cast<TI>(x >> 1)) + 1);
}

template <typename TI> constexpr size_t CeilLog2(TI x)
{
    return x == TI{ 1 } ? 0 : static_cast<size_t>(FloorLog2(static_cast<TI>(x - 1)) + 1);
}

template <typename T, typename T2, OMNI_IF_FLOAT(T), OMNI_IF_NOT_SPECIAL_FLOAT(T)>
OMNI_INLINE constexpr T AddWithWraparound(T t, T2 increment)
{
    return t + static_cast<T>(increment);
}

template <typename T, typename T2, OMNI_IF_SPECIAL_FLOAT(T)>
OMNI_INLINE constexpr T AddWithWraparound(T t, T2 increment)
{
    return ConvertScalarTo<T>(ConvertScalarTo<float>(t) + ConvertScalarTo<float>(increment));
}

template <typename T, typename T2, OMNI_IF_NOT_FLOAT(T)> OMNI_INLINE constexpr T AddWithWraparound(T t, T2 n)
{
    using TU = MakeUnsigned<T>;
    return static_cast<T>(static_cast<TU>(
        static_cast<unsigned long long>(static_cast<unsigned long long>(t) + static_cast<unsigned long long>(n)) &
        uint64_t{ simd::LimitsMax<TU>() }));
}

// 64 x 64 = 128 bit multiplication
OMNI_API uint64_t Mul128(uint64_t a, uint64_t b, uint64_t *OMNI_RESTRICT upper)
{
    __uint128_t product = (__uint128_t)a * (__uint128_t)b;
    *upper = (uint64_t)(product >> 64);
    return (uint64_t)(product & 0xFFFFFFFFFFFFFFFFULL);
}

OMNI_API int64_t Mul128(int64_t a, int64_t b, int64_t *OMNI_RESTRICT upper)
{
    __int128_t product = (__int128_t)a * (__int128_t)b;
    *upper = (int64_t)(product >> 64);
    return (int64_t)(product & 0xFFFFFFFFFFFFFFFFULL);
}


namespace detail {
template <typename T> static OMNI_INLINE constexpr T ScalarAbs(simd::FloatTag /* tag */, T val)
{
    using TU = MakeUnsigned<T>;
    return BitCastScalar<T>(static_cast<TU>(BitCastScalar<TU>(val) & (~SignMask<T>())));
}

template <typename T> static OMNI_INLINE constexpr T ScalarAbs(simd::SpecialTag /* tag */, T val)
{
    return ScalarAbs(simd::FloatTag(), val);
}

template <typename T> static OMNI_INLINE constexpr T ScalarAbs(simd::SignedTag /* tag */, T val)
{
    using TU = MakeUnsigned<T>;
    return (val < T{ 0 }) ? static_cast<T>(TU{ 0 } - static_cast<TU>(val)) : val;
}

template <typename T> static OMNI_INLINE constexpr T ScalarAbs(simd::UnsignedTag /* tag */, T val)
{
    return val;
}
} // namespace detail

template <typename T> OMNI_API constexpr RemoveCvRef<T> ScalarAbs(T val)
{
    using TVal = MakeLaneTypeIfInteger<detail::NativeSpecialFloatToWrapper<RemoveCvRef<T>>>;
    return detail::ScalarAbs(simd::TypeTag<TVal>(), static_cast<TVal>(val));
}

template <typename T> OMNI_API constexpr bool ScalarIsNaN(T val)
{
    using TF = detail::NativeSpecialFloatToWrapper<RemoveCvRef<T>>;
    using TU = MakeUnsigned<TF>;
    return (BitCastScalar<TU>(ScalarAbs(val)) > ExponentMask<TF>());
}

template <typename T> OMNI_API constexpr bool ScalarIsInf(T val)
{
    using TF = detail::NativeSpecialFloatToWrapper<RemoveCvRef<T>>;
    using TU = MakeUnsigned<TF>;
    return static_cast<TU>(BitCastScalar<TU>(static_cast<TF>(val)) << 1) == static_cast<TU>(MaxExponentTimes2<TF>());
}

namespace detail {
template <typename T> static OMNI_INLINE constexpr bool ScalarIsFinite(simd::FloatTag /* tag */, T val)
{
    using TU = MakeUnsigned<T>;
    return (BitCastScalar<TU>(simd::ScalarAbs(val)) < ExponentMask<T>());
}

template <typename T> static OMNI_INLINE constexpr bool ScalarIsFinite(simd::NonFloatTag /* tag */, T /* val */)
{
    // Integer values are always finite
    return true;
}
} // namespace detail

template <typename T> OMNI_API constexpr bool ScalarIsFinite(T val)
{
    using TVal = MakeLaneTypeIfInteger<detail::NativeSpecialFloatToWrapper<RemoveCvRef<T>>>;
    return detail::ScalarIsFinite(simd::IsFloatTag<TVal>(), static_cast<TVal>(val));
}

template <typename T> OMNI_API constexpr RemoveCvRef<T> ScalarCopySign(T magn, T sign)
{
    using TF = RemoveCvRef<detail::NativeSpecialFloatToWrapper<RemoveCvRef<T>>>;
    using TU = MakeUnsigned<TF>;
    return BitCastScalar<TF>(static_cast<TU>((BitCastScalar<TU>(static_cast<TF>(magn)) & (~SignMask<TF>())) |
        (BitCastScalar<TU>(static_cast<TF>(sign)) & SignMask<TF>())));
}

template <typename T> OMNI_API constexpr bool ScalarSignBit(T val)
{
    using TVal = MakeLaneTypeIfInteger<detail::NativeSpecialFloatToWrapper<RemoveCvRef<T>>>;
    using TU = MakeUnsigned<TVal>;
    return ((BitCastScalar<TU>(static_cast<TVal>(val)) & SignMask<TVal>()) != 0);
}

// Prevents the compiler from eliding the computations that led to "output".
template <class T> OMNI_API void PreventElision(T &&output)
{
    // Works by indicating to the compiler that "output" is being read and
    // modified. The +r constraint avoids unnecessary writes to memory, but only
    // works for built-in types (typically FuncOutput).
    asm volatile("" : "+r"(output) : : "memory");
}

#define OMNI_IS_LITTLE_ENDIAN 1
#define OMNI_ARCH_ARM_A64 1
#define OMNI_IF_CONSTEXPR if constexpr
} // namespace omni

#endif // OMNI_BASE_H_
