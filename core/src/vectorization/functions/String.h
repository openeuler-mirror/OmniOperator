/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#pragma once
#include "util/compiler_util.h"
#include <algorithm>
#include <stdexcept>
#include "vectorization/Status.h"

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
    ALWAYS_INLINE Status call(std::string &result, bool &notNull, const std::string_view &str)
    {
        notNull = true;
        if (str.empty()) {
            result.clear();
            return Status::OK();
        }

        // Find the first non-whitespace character from the beginning
        auto start = str.find_first_not_of(" \t\n\r\f\v");
        if (start == std::string_view::npos) {
            // All characters are whitespace
            result.clear();
            return Status::OK();
        }

        // Find the last non-whitespace character from the end
        auto end = str.find_last_not_of(" \t\n\r\f\v");
        
        // Extract the trimmed substring
        result = std::string(str.substr(start, end - start + 1));
        return Status::OK();
    }
};
}
