/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#pragma once
#include "util/compiler_util.h"
#include <algorithm>
#include <limits>
#include <stdexcept>
#include <string_view>
#include "vectorization/Status.h"
#include "vectorization/functions/Base64Util.h"
#include "type/string_Impl.h"

namespace omniruntime::vectorization {

/// Returns the Unicode code point of the first character in UTF-8 string (aligned with
/// Velox utf8proc_codepoint). Returns 0 if empty, -1 on invalid/incomplete UTF-8.
inline int32_t Utf8FirstCodepoint(const char* data, size_t size, int& byteLen) {
    if (size == 0) {
        byteLen = 0;
        return 0;
    }
    const unsigned char* u = reinterpret_cast<const unsigned char*>(data);
    unsigned char u0 = u[0];
    if (u0 <= 127) {
        byteLen = 1;
        return u0;
    }
    if (size < 2) {
        byteLen = 1;
        return -1;
    }
    unsigned char u1 = u[1];
    if (u0 >= 192 && u0 <= 223) {
        byteLen = 2;
        return (u0 - 192) * 64 + (u1 - 128);
    }
    if (u0 == 0xed && (u1 & 0xa0) == 0xa0) {
        byteLen = 1;
        return -1;  // surrogate U+D800..U+DFFF invalid in UTF-8
    }
    if (size < 3) {
        byteLen = 1;
        return -1;
    }
    unsigned char u2 = u[2];
    if (u0 >= 224 && u0 <= 239) {
        byteLen = 3;
        return (u0 - 224) * 4096 + (u1 - 128) * 64 + (u2 - 128);
    }
    if (size < 4) {
        byteLen = 1;
        return -1;
    }
    unsigned char u3 = u[3];
    if (u0 >= 240 && u0 <= 247) {
        byteLen = 4;
        return (u0 - 240) * 262144 + (u1 - 128) * 4096 + (u2 - 128) * 64 + (u3 - 128);
    }
    byteLen = 1;
    return -1;  // invalid lead byte
}
template <typename T>
struct StartsWithFunction {
    ALWAYS_INLINE Status call(bool &result, const std::string_view &str, const std::string_view &pattern)
    {
        if (pattern.length() > str.length()) {
            result = false;
            return Status::OK();
        }

        if (pattern.empty()) {
            result = true;
            return Status::OK();
        }

        result = std::equal(pattern.begin(), pattern.end(), str.begin());
        return Status::OK();
    }
};

/// contains function
/// contains(string, string) -> bool
/// Searches the second argument in the first one.
/// Returns true if it is found
template <typename T>
struct ContainsFunction {
    ALWAYS_INLINE bool call(bool &result, const std::string_view &str, const std::string_view &pattern)
    {
        result = std::string_view(str).find(std::string_view(pattern)) != std::string_view::npos;
        return true;
    }
};

/// trim function
/// trim(string) -> string
/// Removes leading and trailing whitespace characters from the input string.
/// Whitespace characters include space, tab, newline, carriage return, etc.
template <typename T>
struct TrimFunction {
    ALWAYS_INLINE bool callNullable(std::string &result, const std::string_view *str)
    {
        if (str == nullptr) {
            return false;
        }
        // Find the first non-whitespace character from the beginning
        auto start = str->find_first_not_of(" ");
        if (start == std::string_view::npos) {
            // All characters are whitespace
            result.clear();
            return true;
        }
        // Find the last non-whitespace character from the end
        auto end = str->find_last_not_of(" ");
        // Extract the trimmed substring
        result = std::string(str->substr(start, end - start + 1));
        return true;
    }
};

/// locate function
/// locate(substring, string, start) -> integer
/// Returns the position of the first occurrence of substring in string starting from position start.
/// Returns 1-based position (first character is at position 1).
/// Returns 0 if substring is not found, start < 1, or start > string length.
/// Returns 1 if substring is empty.
/// Note: Under SimpleFunction null propagation, any NULL argument (substring/string/start) yields
/// result NULL; Spark's "start NULL -> 0" semantics are not supported by the current framework.
template <typename T>
struct LocateFunction {
    // Non-nullable version for better performance when all arguments are non-null
    ALWAYS_INLINE bool call(int32_t &result, const std::string_view &subString,
        const std::string_view &string, const int32_t &start)
    {
        if (start < 1) {
            result = 0;
            return true;
        }
        if (subString.empty()) {
            result = 1;
            return true;
        }
        
        // Calculate string length in characters (Unicode-aware)
        int64_t stringLength = stringImpl::length<false /*isAscii*/>(string);
        if (start > static_cast<int32_t>(stringLength)) {
            result = 0;
            return true;
        }

        // Find the start byte index of the start character for Unicode strings
        int64_t startByteIndex = stringImpl::cappedByteLengthUnicode(
            string.data(), string.size(), start - 1);

        // Search from start position
        std::string_view searchString(string.data() + startByteIndex, string.size() - startByteIndex);
        auto position = stringImpl::StringPosition<false /*isAscii*/, true /*lpos*/>(
            searchString, subString, 1 /*instance*/);
        if (position > 0) {
            result = position + start - 1;
        } else {
            result = 0;
        }
        return true;
    }

    // Nullable version supporting both ASCII and Unicode
    ALWAYS_INLINE bool callNullable(int32_t &result, const std::string_view *subString,
        const std::string_view *string, const int32_t *start)
    {
        // Call the non-nullable version for better code reuse
        return call(result, *subString, *string, *start);
    }
};

/// substr(string, start) -> varchar
/// substr(string, start, length) -> varchar
/// Spark semantics (velox/functions/sparksql/String.h). Spark + Gluten: int32_t only.
/// start=0 -> first char; start<=0 after adjustment -> start=1; length<=0 -> empty.
/// Note: SimpleFunction passes args as (last_arg, ..., first_arg); call signatures match that order.
template <typename T>
struct SubstrFunction {
    /// 2-arg: framework passes (start, string)
    ALWAYS_INLINE bool call(std::string &result, const std::string_view &input, int32_t start)
    {
        return doCall(result, input, start, std::numeric_limits<int32_t>::max());
    }

    /// 3-arg: framework passes (length, start, string)
    ALWAYS_INLINE bool call(std::string &result, const std::string_view &input, int32_t start, int32_t length)
    {
        return doCall(result, input, start, length);
    }

private:
    ALWAYS_INLINE bool doCall(std::string &result, const std::string_view &input, int32_t start, int32_t length)
    {
        if (length <= 0) {
            result.clear();
            return true;
        }
        if (start == 0) {
            start = 1;
        }
        int32_t numCharacters = static_cast<int32_t>(std::min(
            stringImpl::length<false>(input), static_cast<int64_t>(std::numeric_limits<int32_t>::max())));
        if (start < 0) {
            start = numCharacters + start + 1;
        }
        int32_t last;
        if (numCharacters - start + 1 < length) {
            last = numCharacters;
        } else {
            last = start + length - 1;
        }
        if (start <= 0) {
            start = 1;
        }
        length = last - start + 1;
        if (length <= 0) {
            result.clear();
            return true;
        }
        size_t startByte = static_cast<size_t>(stringImpl::cappedByteLengthUnicode(
            input.data(), static_cast<int64_t>(input.size()), static_cast<int64_t>(start - 1)));
        size_t segmentByteLen = static_cast<size_t>(stringImpl::cappedByteLengthUnicode(
            input.data() + startByte, static_cast<int64_t>(input.size()) - static_cast<int64_t>(startByte),
            static_cast<int64_t>(length)));
        result.assign(input.data() + startByte, segmentByteLen);
        return true;
    }
};

/// position function
/// position(substring, string) -> integer
/// Returns the 1-based position of the first occurrence of substring in string.
/// Equivalent to locate(substring, string, 1). Returns 0 if not found, 1 if substring is empty.
/// Under SimpleFunction null propagation, any NULL argument yields result NULL.
template <typename T>
struct PositionFunction {
    ALWAYS_INLINE bool call(int32_t &result, const std::string_view &subString, const std::string_view &string)
    {
        auto pos = stringImpl::StringPosition<false /*isAscii*/, true /*lpos*/>(string, subString, 1 /*instance*/);
        result = static_cast<int32_t>(pos);
        return true;
    }

    ALWAYS_INLINE bool callNullable(int32_t &result, const std::string_view *subString, const std::string_view *string)
    {
        return call(result, *subString, *string);
    }
};

/// char_length / character_length / length function
/// length(string) -> integer
/// Returns the number of characters in the input string (Unicode code points).
/// Empty string returns 0. Supports CHAR and VARCHAR. NULL input yields NULL output.
template <typename T>
struct CharLengthFunction {
    ALWAYS_INLINE bool call(int32_t &result, const std::string_view &str)
    {
        result = static_cast<int32_t>(stringImpl::length<false /*isAscii*/>(str));
        return true;
    }

    ALWAYS_INLINE bool callNullable(int32_t &result, const std::string_view *str)
    {
        return call(result, *str);
    }
};

/// ascii(string) -> int32
/// Returns the ASCII/Unicode code point of the first character; 0 for empty string.
template <typename T>
struct AsciiFunction {
    ALWAYS_INLINE void call(int32_t& result, const std::string_view& s) {
        if (s.empty()) {
            result = 0;
            return;
        }
        int byteLen = 0;
        result = Utf8FirstCodepoint(s.data(), s.size(), byteLen);
    }
};

/// chr(n) / char(n) -> string
/// Returns the Unicode code point n as a single character string.
/// If n < 0, returns empty string. If n >= 256, equivalent to chr(n % 256).
/// Supports all integer types (byte/short/int/long).
template <typename T>
struct ChrFunction {
    ALWAYS_INLINE void call(std::string& result, const int64_t& n) {
        if (n < 0) {
            result.clear();
            return;
        }
        int64_t codePoint = n & 0xFF;
        result.resize(codePoint < 0x80 ? 1 : 2);
        char* p = &result[0];
        if (codePoint < 0x80) {
            p[0] = static_cast<char>(codePoint);
        } else {
            p[0] = static_cast<char>(0xC0 + (codePoint >> 6));
            p[1] = static_cast<char>(0x80 + (codePoint & 0x3F));
        }
    }
};

/// lower function
/// lower(string) -> string
/// Converts the input string to lowercase. Aligned with Velox lower semantics:
/// ASCII letters A-Z are converted to a-z; other bytes are unchanged (ASCII path).
/// Empty string returns empty string. NULL input yields NULL output.
template <typename T>
struct LowerFunction {
    ALWAYS_INLINE bool call(std::string& result, const std::string_view& input)
    {
        result.resize(input.size());
        for (size_t i = 0; i < input.size(); ++i) {
            unsigned char c = static_cast<unsigned char>(input[i]);
            result[i] = (c >= 'A' && c <= 'Z') ? static_cast<char>(c + 32) : input[i];
        }
        return true;
    }

    ALWAYS_INLINE bool callNullable(std::string& result, const std::string_view* input)
    {
        if (input == nullptr) {
            return false;
        }
        return call(result, *input);
    }
};

/// unbase64(string) -> varbinary (as string)
/// Decodes Base64-encoded string to binary. Returns Status on decode error (row becomes NULL).
template <typename T>
struct UnBase64Function {
    ALWAYS_INLINE Status call(std::string& result, const std::string_view& input) {
        size_t maxDecodedSize = (input.size() / 4) * 3 + 3;
        result.resize(maxDecodedSize);
        size_t actualSize = 0;
        Status st = Base64Decode(input.data(), input.size(), &result[0], maxDecodedSize, &actualSize);
        if (!st.ok()) {
            return st;
        }
        result.resize(actualSize);
        return Status::OK();
    }
};

/// Pad functions base template
/// pad(string, size, padString) -> varchar
/// Pads string to size characters with padString. If size is less than the length
/// of string, the result is truncated to size characters. size must not be negative
/// and padString must be non-empty.
/// Supports both ASCII and Unicode strings.
/// @tparam lpad If true, left pads the string (padding at beginning).
///              If false, right pads the string (padding at end).
/// @tparam T Type parameter for function registration.
template <bool lpad, typename T>
struct PadFunctionBase {
    static constexpr size_t kPadMaxSize = 1024 * 1024; // 1MB max size

    ALWAYS_INLINE bool call(std::string &result, const std::string_view &string,
        const int64_t &size, const std::string_view &padString)
    {
        // Validate size
        if (size < 0 || static_cast<size_t>(size) > kPadMaxSize) {
            result.clear();
            return true;
        }

        // Validate padString
        if (padString.empty()) {
            result.clear();
            return true;
        }

        int64_t padStringCharLength = stringImpl::length<false /*isAscii*/>(padString);
        if (padStringCharLength <= 0) {
            result.clear();
            return true;
        }

        int64_t stringCharLength = stringImpl::length<false /*isAscii*/>(string);

        // If string has at least size characters, truncate it if necessary
        if (stringCharLength >= size) {
            size_t prefixByteSize = static_cast<size_t>(
                stringImpl::cappedByteLengthUnicode(string.data(), string.size(), size));
            result.assign(string.data(), prefixByteSize);
            return true;
        }

        // Calculate padding needed
        int64_t fullPaddingCharLength = size - stringCharLength;
        int64_t fullPadCopies = fullPaddingCharLength / padStringCharLength;
        int64_t remainingPadChars = fullPaddingCharLength % padStringCharLength;

        // Calculate byte length of the partial pad prefix
        size_t padPrefixByteLength = static_cast<size_t>(
            stringImpl::cappedByteLengthUnicode(padString.data(), padString.size(), remainingPadChars));

        // Calculate total byte length
        int64_t fullPaddingByteLength = static_cast<int64_t>(padString.size()) * fullPadCopies +
            static_cast<int64_t>(padPrefixByteLength);
        int64_t outputByteLength = static_cast<int64_t>(string.size()) + fullPaddingByteLength;

        result.resize(static_cast<size_t>(outputByteLength));

        size_t paddingOffset;
        if constexpr (lpad) {
            // For lpad: padding comes first, then the string
            paddingOffset = 0;
            // Copy original string after the padding
            std::memcpy(result.data() + fullPaddingByteLength, string.data(), string.size());
        } else {
            // For rpad: string comes first, then the padding
            paddingOffset = string.size();
            // Copy original string at the beginning
            std::memcpy(result.data(), string.data(), string.size());
        }

        // Copy full pad strings
        for (int64_t i = 0; i < fullPadCopies; i++) {
            std::memcpy(result.data() + paddingOffset + i * padString.size(),
                padString.data(), padString.size());
        }

        // Copy partial pad prefix
        std::memcpy(result.data() + paddingOffset + fullPadCopies * padString.size(),
            padString.data(), padPrefixByteLength);

        return true;
    }

    ALWAYS_INLINE bool callNullable(std::string &result, const std::string_view *string,
        const int64_t *size, const std::string_view *padString)
    {
        if (string == nullptr || size == nullptr || padString == nullptr) {
            return false;
        }
        return call(result, *string, *size, *padString);
    }
};

/// lpad function
/// lpad(string, size, padString) -> varchar
/// Left pads string to size characters with padString.
template <typename T>
struct LPadFunction : public PadFunctionBase<true, T> {};

/// rpad function
/// rpad(string, size, padString) -> varchar
/// Right pads string to size characters with padString.
template <typename T>
struct RPadFunction : public PadFunctionBase<false, T> {};
}
