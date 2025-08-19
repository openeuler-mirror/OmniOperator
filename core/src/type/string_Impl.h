/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: Expression code generator
 */

#pragma once

#include <string>
#include "util/compiler_util.h"

namespace omniruntime::stringImpl {
#define utf_cont(ch) (((ch)&0xc0) == 0x80)
/**
 * Return the length in chars of a utf8 string stored in the input buffer
 * @param inputBuffer input buffer that hold the string
 * @param bufferLength size of input buffer
 * @return the number of characters represented by the input utf8 string
 */
ALWAYS_INLINE int64_t lengthUnicode(const char *inputBuffer, size_t bufferLength)
{
    // First address after the last byte in the buffer
    auto buffEndAddress = inputBuffer + bufferLength;
    auto currentChar = inputBuffer;
    int64_t size = 0;
    while (currentChar < buffEndAddress) {
        // This function detects bytes that come after the first byte in a
        // multi-byte UTF-8 character (provided that the string is valid UTF-8). We
        // increment size only for the first byte so that we treat all bytes as part
        // of a single character.
        if (!utf_cont(*currentChar)) {
            size++;
        }

        currentChar++;
    }
    return size;
}

/// Returns the start byte index of the Nth instance of subString in
/// string. Search starts from startPosition. Positions start with 0. If not
/// found, -1 is returned. To facilitate finding overlapping strings, the
/// nextStartPosition is incremented by 1
static inline int64_t FindNthInstanceByteIndexFromStart(const std::string_view &string,
    const std::string_view subString, const size_t instance = 1, const size_t startPosition = 0)
{
    if (startPosition >= string.size()) {
        return -1;
    }

    auto byteIndex = string.find(subString, startPosition);
    // Not found
    if (byteIndex == std::string_view::npos) {
        return -1;
    }

    // Search done
    if (instance == 1) {
        return byteIndex;
    }

    // Find next occurrence
    return FindNthInstanceByteIndexFromStart(string, subString, instance - 1, byteIndex + 1);
}

/// Returns the start byte index of the Nth instance of subString in
/// string from the end. Search starts from endPosition. Positions start with 0.
/// If not found, -1 is returned. To facilitate finding overlapping strings, the
/// nextStartPosition is incremented by 1
inline int64_t FindNthInstanceByteIndexFromEnd(const std::string_view string, const std::string_view subString,
    const size_t instance = 1)
{
    assert(instance > 0);

    if (subString.empty()) {
        return 0;
    }

    size_t foundCnt = 0;
    size_t index = string.size();
    do {
        if (index == 0) {
            return -1;
        }

        index = string.rfind(subString, index - 1);
        if (index == std::string_view::npos) {
            return -1;
        }
        ++foundCnt;
    } while (foundCnt < instance);
    return index;
}

/// Return length of the input string in chars
template <bool isAscii, typename T>
ALWAYS_INLINE int64_t length(const T &input)
{
    if constexpr (isAscii) {
        return input.size();
    } else {
        return lengthUnicode(input.data(), input.size());
    }
}

template <bool isAscii, bool lpos = true>
ALWAYS_INLINE int64_t StringPosition(std::string_view string, std::string_view subString, int64_t instance)
{
    if (subString.size() == 0) {
        return 1;
    }

    int64_t byteIndex = -1;
    if constexpr (lpos) {
        byteIndex = FindNthInstanceByteIndexFromStart(string, subString, instance);
    } else {
        byteIndex = FindNthInstanceByteIndexFromEnd(string, subString, instance);
    }

    if (byteIndex == -1) {
        return 0;
    }

    // Return the number of characters from the beginning of the string to
    // byteIndex.
    return length<isAscii>(std::string_view(string.data(), byteIndex)) + 1;
}
}
