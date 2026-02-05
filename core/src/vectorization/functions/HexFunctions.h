/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: Hex functions for vectorization framework
 */

#pragma once

#include "util/compiler_util.h"
#include "vectorization/Status.h"
#include <string>
#include <string_view>
#include <cstdint>
#include <limits>

namespace omniruntime::vectorization {

struct HexUtil {
    // Lookup table for hex characters
    static constexpr const char* kHexChars = "0123456789ABCDEF";
    
    // Full lookup table for byte to hex string conversion (faster)
    static constexpr const char* kHexTable =
        "000102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F"
        "202122232425262728292A2B2C2D2E2F303132333435363738393A3B3C3D3E3F"
        "404142434445464748494A4B4C4D4E4F505152535455565758595A5B5C5D5E5F"
        "606162636465666768696A6B6C6D6E6F707172737475767778797A7B7C7D7E7F"
        "808182838485868788898A8B8C8D8E8F909192939495969798999A9B9C9D9E9F"
        "A0A1A2A3A4A5A6A7A8A9AAABACADAEAFB0B1B2B3B4B5B6B7B8B9BABBBCBDBEBF"
        "C0C1C2C3C4C5C6C7C8C9CACBCCCDCECFD0D1D2D3D4D5D6D7D8D9DADBDCDDDEDF"
        "E0E1E2E3E4E5E6E7E8E9EAEBECEDEEEFF0F1F2F3F4F5F6F7F8F9FAFBFCFDFEFF";
    
    static constexpr int kU64Bits = std::numeric_limits<uint64_t>::digits;
    
    ALWAYS_INLINE static int countLeadingZeros(uint64_t value) {
        if (value == 0) return kU64Bits;
#if defined(__GNUC__) || defined(__clang__)
        constexpr int kULLBits = std::numeric_limits<unsigned long long>::digits;
        return __builtin_clzll(value) - (kULLBits - kU64Bits);
#else
        int count = 0;
        for (int i = kU64Bits - 1; i >= 0; --i) {
            if ((value >> i) & 1) break;
            ++count;
        }
        return count;
#endif
    }
    
    ALWAYS_INLINE static void toHex(int64_t input, std::string& result) {
        uint64_t value = static_cast<uint64_t>(input);
        
        if (value == 0) {
            result = "0";
            return;
        }
        
        int resultSize = ((kU64Bits - countLeadingZeros(value)) + 3) / 4;
        result.resize(resultSize);
        
        int len = 0;
        do {
            len += 1;
            result[resultSize - len] = kHexChars[value & 0xF];
            value >>= 4;
        } while (value != 0);
    }
    
    ALWAYS_INLINE static void toHex(const std::string_view& input, std::string& result) {
        const size_t inputSize = input.size();
        if (inputSize == 0) {
            result.clear();
            return;
        }
        
        result.resize(inputSize * 2);
        const unsigned char* inputBuffer = reinterpret_cast<const unsigned char*>(input.data());
        
        for (size_t i = 0; i < inputSize; ++i) {
            result[i * 2] = kHexTable[inputBuffer[i] * 2];
            result[i * 2 + 1] = kHexTable[inputBuffer[i] * 2 + 1];
        }
    }
};

/**
 * @brief hex(bigint) -> varchar
 * Converts a 64-bit integer to its hexadecimal representation.
 * For negative numbers, returns the two's complement hex string.
 * 
 * Examples:
 *   hex(17) = "11"
 *   hex(-17) = "FFFFFFFFFFFFFFEF"
 *   hex(0) = "0"
 *   hex(-1) = "FFFFFFFFFFFFFFFF"
 */
template <typename T>
struct HexBigintFunction {
    ALWAYS_INLINE bool call(std::string &result, const int64_t &input)
    {
        HexUtil::toHex(input, result);
        return true;
    }
    
    ALWAYS_INLINE bool callNullable(std::string &result, const int64_t *input)
    {
        if (input == nullptr) {
            return false;
        }
        return call(result, *input);
    }
};

/**
 * @brief hex(varchar) -> varchar
 * Converts each character in the string to its 2-digit hex representation.
 * 
 * Examples:
 *   hex("") = ""
 *   hex("Spark SQL") = "537061726B2053514C"
 */
template <typename T>
struct HexVarcharFunction {
    ALWAYS_INLINE bool call(std::string &result, const std::string_view &input)
    {
        HexUtil::toHex(input, result);
        return true;
    }
    
    ALWAYS_INLINE bool callNullable(std::string &result, const std::string_view *input)
    {
        if (input == nullptr) {
            return false;
        }
        return call(result, *input);
    }
};

/**
 * @brief hex(varbinary) -> varchar
 * Converts each byte in the binary data to its 2-digit hex representation.
 * Same behavior as HexVarcharFunction.
 */
template <typename T>
struct HexVarbinaryFunction {
    ALWAYS_INLINE bool call(std::string &result, const std::string_view &input)
    {
        HexUtil::toHex(input, result);
        return true;
    }
    
    ALWAYS_INLINE bool callNullable(std::string &result, const std::string_view *input)
    {
        if (input == nullptr) {
            return false;
        }
        return call(result, *input);
    }
};

}  // namespace omniruntime::vectorization
