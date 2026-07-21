/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: BIN function for the vectorization framework.
 *              bin(integer) -> varchar, matching Flink BIN() semantics
 *              (java.lang.Long.toBinaryString: unsigned 64-bit, no leading zeros, 0 -> "0").
 */

#pragma once

#include "util/compiler_util.h"
#include "vectorization/Status.h"
#include <string>
#include <cstdint>
#include <limits>

namespace omniruntime::vectorization {

struct BinUtil {
    static constexpr int kU64Bits = std::numeric_limits<uint64_t>::digits;

    ALWAYS_INLINE static int countLeadingZeros(uint64_t value)
    {
        if (value == 0) {
            return kU64Bits;
        }
#if defined(__GNUC__) || defined(__clang__)
        constexpr int kULLBits = std::numeric_limits<unsigned long long>::digits;
        return __builtin_clzll(value) - (kULLBits - kU64Bits);
#else
        int count = 0;
        for (int i = kU64Bits - 1; i >= 0; --i) {
            if ((value >> i) & 1ULL) {
                break;
            }
            ++count;
        }
        return count;
#endif
    }

    // Produce Long.toBinaryString(input): the value is treated as an unsigned 64-bit
    // integer, emitted with no leading zeros ("0" for zero). Negative inputs therefore
    // yield their 64-bit two's-complement bit pattern (e.g. -1 -> 64 ones).
    ALWAYS_INLINE static void toBinary(int64_t input, std::string& result)
    {
        uint64_t value = static_cast<uint64_t>(input);
        if (value == 0) {
            result = "0";
            return;
        }

        int len = kU64Bits - countLeadingZeros(value);
        result.resize(len);
        for (int i = len - 1; i >= 0; --i) {
            result[i] = static_cast<char>('0' + static_cast<char>(value & 1ULL));
            value >>= 1;
        }
    }
};

/**
 * @brief bin(bigint) -> varchar
 * Binary string of a 64-bit integer (Long.toBinaryString semantics).
 *
 * Examples:
 *   bin(0)   = "0"
 *   bin(12)  = "1100"
 *   bin(-1)  = "1111111111111111111111111111111111111111111111111111111111111111"
 */
template <typename T>
struct BinBigintFunction {
    ALWAYS_INLINE bool call(std::string& result, const int64_t& input)
    {
        BinUtil::toBinary(input, result);
        return true;
    }

    ALWAYS_INLINE bool callNullable(std::string& result, const int64_t* input)
    {
        if (input == nullptr) {
            return false;
        }
        return call(result, *input);
    }
};

/**
 * @brief bin(int) -> varchar
 * Binary string of a 32-bit integer. The value is sign-extended to 64 bits before
 * conversion, matching Flink's int->long widening when calling Long.toBinaryString.
 *
 * Examples:
 *   bin(4)   = "100"
 *   bin(12)  = "1100"
 *   bin(-1)  = "1111111111111111111111111111111111111111111111111111111111111111"
 */
template <typename T>
struct BinIntFunction {
    ALWAYS_INLINE bool call(std::string& result, const int32_t& input)
    {
        BinUtil::toBinary(static_cast<int64_t>(input), result);
        return true;
    }

    ALWAYS_INLINE bool callNullable(std::string& result, const int32_t* input)
    {
        if (input == nullptr) {
            return false;
        }
        return call(result, *input);
    }
};

}  // namespace omniruntime::vectorization
