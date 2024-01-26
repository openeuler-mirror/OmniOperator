/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Description: decimal128 utils
 */

#ifndef OMNI_RUNTIME_DATA_OPERATIONS_H
#define OMNI_RUNTIME_DATA_OPERATIONS_H

#include <string>
#include <regex>
#include <limits>
#include "util/compiler_util.h"

namespace omniruntime {
namespace type {
static std::regex g_decimalRegex("[+-]?[[:digit:]]+([.][[:digit:]]+)?([eE][+-]?[[:digit:]]+)?");
static std::regex g_doubleRegex(
    "[[:blank:]]*([+-])?[[:digit:]]+([.][[:digit:]]+)?([eE][+-]?[[:digit:]]+)?[[:blank:]]*");
static std::regex g_dateRegex(R"(\d{4}-\d{2}-\d{2}$)");

enum Status {
    SUCCESS,
    OVERFLOW,
    IS_NOT_A_NUMBER
};

template<typename T, bool allowTruncate = true>
inline Status ConvertStringToInteger(T &result, const char *bytes, int length)
{
    static_assert(std::is_same_v<T, int8_t> || std::is_same_v<T, int16_t> || std::is_same_v<T, int32_t> ||
        std::is_same_v<T, int64_t>, "Only integer type is allowed");
    int offset = 0;
    while (offset < length && bytes[offset] == ' ') offset++;
    if (offset == length) return Status::IS_NOT_A_NUMBER;

    int end = length - 1;
    while (end > offset && bytes[end] == ' ') end--;

    char b = bytes[offset];
    bool negative = b == '-';
    if (negative || b == '+') {
        if (end - offset == 0) {
            return Status::IS_NOT_A_NUMBER;
        }
        offset++;
    }

    char separator = '.';
    constexpr int radix = 10;
    constexpr T minValue = std::is_same_v<T, int8_t> ? INT8_MIN : std::is_same_v<T, int16_t> ? INT16_MIN
        : std::is_same_v<T, int32_t> ? INT32_MIN : INT64_MIN;
    constexpr T stopValue = minValue / radix;

    result = 0;
    while (offset <= end) {
        b = bytes[offset];
        offset++;
        if constexpr (allowTruncate) {
            if (b == separator) {
                break;
            }
        }

        int digit;
        if (b >= '0' && b <= '9') {
            digit = b - '0';
        } else {
            return Status::IS_NOT_A_NUMBER;
        }

        if ((result < stopValue) || (result == stopValue && digit > 8)) {
            return Status::OVERFLOW;
        }

        result = result * radix - digit;
    }

    while (offset <= end) {
        char currentByte = bytes[offset];
        if (currentByte < '0' || currentByte > '9') {
            return Status::IS_NOT_A_NUMBER;
        }
        offset++;
    }

    if (!negative) {
        if (result == minValue) {
            return Status::OVERFLOW;
        }
        result = -result;
    }
    return Status::SUCCESS;
}

inline int StringToDouble(const std::string &s, double &result)
{
    if (!regex_match(s, g_doubleRegex)) {
        return -1;
    }
    int status = 0;
    try {
        result = stod(s);
    } catch (std::exception &e) {
        status = 1;
    }
    return status;
}
}
}
#endif // OMNI_RUNTIME_DATA_OPERATIONS_H
