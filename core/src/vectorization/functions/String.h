/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#pragma once
#include "util/compiler_util.h"
#include <algorithm>
#include <stdexcept>
#include "vectorization/Status.h"
#include "type/string_Impl.h"

namespace omniruntime::vectorization {
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
}
