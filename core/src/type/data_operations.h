/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Description: decimal128 utils
 */

#ifndef OMNI_RUNTIME_DATA_OPERATIONS_H
#define OMNI_RUNTIME_DATA_OPERATIONS_H

#include <string>
#include <regex>
#include "util/compiler_util.h"

namespace omniruntime {
namespace type {
static std::regex g_decimalRegex("[+-]?[[:digit:]]+([.][[:digit:]]+)?([eE][+-]?[[:digit:]]+)?");
static std::regex g_doubleRegex(
    "[[:blank:]]*([+-])?[[:digit:]]+([.][[:digit:]]+)?([eE][+-]?[[:digit:]]+)?[[:blank:]]*");
static std::regex g_dateRegex(R"(\d{4}-\d{2}-\d{2}$)");

// if the string is empty or all characters are spaces then return false, other cases return true
static ALWAYS_INLINE bool DoSkipSpaces(const char *str, int32_t strLen, int32_t &strStart, int32_t &strEnd)
{
    int32_t strIdx = 0;
    while (strIdx < strLen && str[strIdx] == ' ') {
        // skip leading space characters
        strIdx++;
    }
    if (strIdx == strLen) {
        // there is no character or all characters are spaces
        return false;
    }
    strStart = strIdx;

    strIdx = strLen - 1;
    while (strIdx > strStart && str[strIdx] == ' ') {
        // skip tail space characters
        strIdx--;
    }
    strEnd = strIdx;
    return true;
}

template <typename T>
static T ConvertStringToInteger(const char *str, int32_t strLen, bool &isInvalid, bool &isOverflow)
{
    T result = 0;
    int32_t strStart = 0;
    int32_t strEnd = 0;
    isInvalid = !DoSkipSpaces(str, strLen, strStart, strEnd);
    if (isInvalid) {
        return result;
    }

    int32_t strIdx = strStart;
    bool isNegative = false;
    auto firstChar = str[strIdx];
    if (firstChar == '-' || firstChar == '+') {
        isNegative = firstChar == '-';
        if (strIdx == strEnd) {
            // the string is + or -
            isInvalid = true;
            result = 0;
            return result;
        }
        strIdx++;
    }

    if (isNegative) {
        for (; strIdx <= strEnd; strIdx++) {
            auto c = str[strIdx];
            if (std::isdigit(c) == 0) {
                isInvalid = true;
                result = 0;
                break;
            }

            result = result * 10 - (c - '0');
            if (result > 0) {
                // overflow check
                isOverflow = true;
                result = 0;
                break;
            }
        }
    } else {
        for (; strIdx <= strEnd; strIdx++) {
            auto c = str[strIdx];
            if (std::isdigit(c) == 0) {
                isInvalid = true;
                result = 0;
                break;
            }

            result = result * 10 + (c - '0');
            if (result < 0) {
                // overflow check
                isOverflow = true;
                result = 0;
                break;
            }
        }
    }
    return result;
}

template <typename T>
static T ConvertStringToIntegerWithTruncate(const char *str, int32_t strLen, bool &isInvalid, bool &isOverflow)
{
    T result = 0;
    int32_t strStart = 0;
    int32_t strEnd = 0;
    isInvalid = !DoSkipSpaces(str, strLen, strStart, strEnd);
    if (isInvalid) {
        return result;
    }

    int32_t strIdx = strStart;
    bool isNegative = false;
    auto firstChar = str[strIdx];
    if (firstChar == '-' || firstChar == '+') {
        isNegative = firstChar == '-';
        if (strIdx == strEnd) {
            // the string is + or -
            isInvalid = true;
            result = 0;
            return result;
        }
        strIdx++;
    }

    bool hasDecimalPoint = false;
    if (isNegative) {
        for (; strIdx <= strEnd; strIdx++) {
            auto c = str[strIdx];
            if (!hasDecimalPoint && c == '.') {
                hasDecimalPoint = true;
                if (++strIdx > strEnd) {
                    break;
                }
                c = str[strIdx];
            }
            if (std::isdigit(c) == 0) {
                isInvalid = true;
                result = 0;
                break;
            }
            if (!hasDecimalPoint) {
                result = result * 10 - (c - '0');
            }

            if (result > 0) {
                // overflow check
                isOverflow = true;
                result = 0;
                break;
            }
        }
    } else {
        for (; strIdx <= strEnd; strIdx++) {
            auto c = str[strIdx];
            if (!hasDecimalPoint && c == '.') {
                hasDecimalPoint = true;
                if (++strIdx > strEnd) {
                    break;
                }
                c = str[strIdx];
            }
            if (std::isdigit(c) == 0) {
                isInvalid = true;
                result = 0;
                break;
            }
            if (!hasDecimalPoint) {
                result = result * 10 + (c - '0');
            }
            if (result < 0) {
                // overflow check
                isOverflow = true;
                result = 0;
                break;
            }
        }
    }
    return result;
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
