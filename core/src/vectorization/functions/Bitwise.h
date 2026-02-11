/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#pragma once
#include "util/compiler_util.h"
#include "vectorization/Status.h"
#include <cmath>

namespace omniruntime::vectorization {
template <typename T>
struct BitwiseAndFunction {
    template <typename TInput>
    ALWAYS_INLINE Status call(TInput &result, const TInput &a, const TInput &b)
    {
        result = a & b;
        return Status::OK();
    }
};

template <typename T>
struct BitwiseOrFunction {
    template <typename TInput>
    ALWAYS_INLINE Status call(TInput &result, const TInput &a, const TInput &b)
    {
        result = a | b;
        return Status::OK();
    }
};

template <typename T>
struct BitwiseXorFunction {
    template <typename TInput>
    ALWAYS_INLINE Status call(TInput &result, const TInput &a, const TInput &b)
    {
        result = a ^ b;
        return Status::OK();
    }
};

/// Bitwise NOT function
/// bitwise_not(a) -> ~a
/// Returns the bitwise NOT (complement) of the input integer.
/// Flips all bits: 0 becomes 1, and 1 becomes 0.
template <typename T>
struct BitwiseNotFunction {
    template <typename TInput>
    ALWAYS_INLINE Status call(TInput &result, const TInput &a)
    {
        result = ~a;
        return Status::OK();
    }
};

template <typename T>
struct ShiftLeftFunction {
    template <typename TInput1, typename TInput2>
    ALWAYS_INLINE Status call(TInput1 &result, const TInput1 &a, const TInput2 &b)
    {
        static_assert(std::is_integral_v<TInput1>, "ShiftLeft only supports integral types");

        TInput2 shift = b;
        if constexpr (std::is_same_v<TInput1, int32_t> || std::is_same_v<TInput1, uint32_t>) {
            if (shift < 0) {
                shift = shift % 32 + 32;
            }
            if (shift >= 32) {
                shift = shift % 32;
            }
        } else if constexpr (std::is_same_v<TInput1, int64_t> || std::is_same_v<TInput1, uint64_t>) {
            if (shift < 0) {
                shift = shift % 64 + 64;
            }
            if (shift >= 64) {
                shift = shift % 64;
            }
        }

        result = a << shift;
        return Status::OK();
    }
};

template <typename T>
struct ShiftRightFunction {
    template <typename TInput1, typename TInput2>
    ALWAYS_INLINE Status call(TInput1 &result, const TInput1 &a, const TInput2 &b)
    {
        static_assert(std::is_integral_v<TInput1>, "ShiftRight only supports integral types");

        TInput2 shift = b;
        if constexpr (std::is_same_v<TInput1, int32_t> || std::is_same_v<TInput1, uint32_t>) {
            if (shift < 0) {
                shift = shift % 32 + 32;
            }
            if (shift >= 32) {
                shift = shift % 32;
            }
        } else if constexpr (std::is_same_v<TInput1, int64_t> || std::is_same_v<TInput1, uint64_t>) {
            if (shift < 0) {
                shift = shift % 64 + 64;
            }
            if (shift >= 64) {
                shift = shift % 64;
            }
        }

        result = a >> shift;
        return Status::OK();
    }
};

/// Bit Get function
/// bit_get(num, pos) -> int8_t (0 or 1)
/// Returns the value of the bit at the specified position.
/// Position 0 is the least significant bit.
/// The result is 0 or 1.
template <typename T>
struct BitGetFunction {
    template <typename TInput>
    ALWAYS_INLINE Status call(int8_t &result, const TInput &num, const int32_t &pos)
    {
        static_assert(std::is_integral_v<TInput>, "BitGet only supports integral types");
        constexpr int kMaxBits = sizeof(TInput) * 8;
        
        // Validate position is within valid range
        if (pos < 0 || pos >= kMaxBits) {
            // For out-of-range positions, return 0 (following Spark behavior)
            // Alternatively, could throw an error
            result = 0;
            return Status::OK();
        }
        
        result = static_cast<int8_t>((num >> pos) & 1);
        return Status::OK();
    }
};
/// Bit Count function
 	 /// bit_count(x) -> int32_t
 	 /// Returns the number of bits that are set (1) in the binary representation of x.
 	 /// Supports: bool, byte, short, int, long
 	 /// Examples:
 	 ///   bit_count(0) = 0
 	 ///   bit_count(1) = 1
 	 ///   bit_count(255) = 8 (0xFF has 8 bits set)
 	 ///   bit_count(-1) = 8 for int8_t (0xFF), 32 for int32_t (0xFFFFFFFF)
 	 template <typename T>
 	 struct BitCountFunction {
 	     template <typename TInput>
 	     ALWAYS_INLINE Status call(int32_t &result, const TInput &num)
 	     {
 	         
 	         // Calculate the number of bits to count based on input type
 	         constexpr int kMaxBits = sizeof(TInput) * 8;
 	         
 	         // Convert to unsigned type for proper bit counting
 	         // This ensures negative numbers are counted correctly
 	         if constexpr (std::is_same_v<TInput, bool>) {
 	             // For boolean: true=1, false=0
 	             result = num ? 1 : 0;
 	         } else if constexpr (sizeof(TInput) <= 4) {
 	             // For 8-bit, 16-bit, and 32-bit integers
 	             // Mask to only count the relevant bits
 	             uint32_t value;
 	             if constexpr (std::is_same_v<TInput, int8_t>) {
 	                 value = static_cast<uint8_t>(num);
 	             } else if constexpr (std::is_same_v<TInput, int16_t>) {
 	                 value = static_cast<uint16_t>(num);
 	             } else {
 	                 value = static_cast<uint32_t>(num);
 	             }
 	             // Use GCC/Clang built-in popcount for 32-bit
 	             result = __builtin_popcount(value);
 	         } else {
 	             // For 64-bit integers
 	             uint64_t value = static_cast<uint64_t>(num);
 	             // Use GCC/Clang built-in popcountll for 64-bit
 	             result = __builtin_popcountll(value);
 	         }
 	         
 	         return Status::OK();
 	}
};
}