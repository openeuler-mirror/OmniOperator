/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: Expression code generator
  * Copyright (c) Facebook, Inc. and its affiliates.
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  *     http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

#pragma once

#include <string>
#include <re2/re2.h>
#include "util/compiler_util.h"
#include "string_ref.h"
#include "util/bit_util.h"
#include "util/format.h"
#include "util/omni_exception.h"

namespace omniruntime::stringImpl {
inline int32_t utf8proc_codepoint(const char *u_input, const char *end, int &sz)
{
    auto u = reinterpret_cast<const unsigned char *>(u_input);
    unsigned char u0 = u[0];
    if (u0 <= 127) {
        sz = 1;
        return u0;
    }
    if (end - u_input < 2) {
        return -1;
    }
    unsigned char u1 = u[1];
    if (u0 >= 192 && u0 <= 223) {
        sz = 2;
        return (u0 - 192) * 64 + (u1 - 128);
    }
    if (u[0] == 0xed && (u[1] & 0xa0) == 0xa0) {
        return -1; // code points, 0xd800 to 0xdfff
    }
    if (end - u_input < 3) {
        return -1;
    }
    unsigned char u2 = u[2];
    if (u0 >= 224 && u0 <= 239) {
        sz = 3;
        return (u0 - 224) * 4096 + (u1 - 128) * 64 + (u2 - 128);
    }
    if (end - u_input < 4) {
        return -1;
    }
    unsigned char u3 = u[3];
    if (u0 >= 240 && u0 <= 247) {
        sz = 4;
        return (u0 - 240) * 262144 + (u1 - 128) * 4096 + (u2 - 128) * 64 + (u3 - 128);
    }
    return -1;
}

FOLLY_ALWAYS_INLINE bool isAsciiSpace(char ch)
{
    return ch == ' ';
}

FOLLY_ALWAYS_INLINE int64_t asciiWhitespaceCodes()
{
    int8_t codes[] = {9, 10, 11, 12, 13, 28, 29, 30, 31, 32};
    int64_t bitMask = 0;
    for (auto code : codes) {
        BitUtil::SetBit(&bitMask, code, true);
    }
    return bitMask;
}

FOLLY_ALWAYS_INLINE bool isAsciiWhiteSpace(char ch)
{
    static const auto kAsciiCodes = asciiWhitespaceCodes();

    const uint32_t code = ch;

    if (code <= 32) {
        return BitUtil::IsBitSet(&kAsciiCodes, code);
    }

    return false;
}

FOLLY_ALWAYS_INLINE std::array<int64_t, 2> unicodeWhitespaceCodes()
{
    int16_t codes[] = {8192, 8193, 8194, 8195, 8196, 8197, 8198, 8200, 8201, 8202, 8232, 8233, 8287};
    std::array<int64_t, 2> bitMask{0, 0};
    for (auto code : codes) {
        BitUtil::SetBit(&bitMask, code - 8192, true);
    }
    return bitMask;
}

FOLLY_ALWAYS_INLINE bool isUnicodeWhiteSpace(int32_t codePoint)
{
    static const auto kAsciiCodes = asciiWhitespaceCodes();
    static const auto kUnicodeCodes = unicodeWhitespaceCodes();
    if (codePoint < 0) {
        return false;
    }

    if (codePoint < 5'000) {
        if (codePoint > 32) {
            return false; // Most common path. Uses 2 comparisons.
        }

        return BitUtil::IsBitSet(&kAsciiCodes, codePoint);
    }

    if (codePoint >= 8192) {
        if (codePoint <= 8287) {
            return BitUtil::IsBitSet(kUnicodeCodes.data(), codePoint - 8192);
        }

        return codePoint == 12288;
    }

    return codePoint == 5760;
}

// Returns -1 if 'data' does not end with a white space, otherwise returns the
// size of the white space character at the end of 'data'. 'size' is the size of
// 'data' in bytes.
FOLLY_ALWAYS_INLINE int endsWithUnicodeWhiteSpace(const char *data, size_t size)
{
    if (size >= 1) {
        // Check ASCII whitespaces.
        auto &lastChar = data[size - 1];
        if (isAsciiWhiteSpace(lastChar)) {
            return 1;
        }
    }

    // All Unicode whitespaces are 3-byte characters.
    if (size >= 3) {
        int32_t codePointSize;
        auto codePoint = utf8proc_codepoint(data + size - 3, data + size, codePointSize);
        if (codePoint != -1 && codePointSize == 3 && isUnicodeWhiteSpace(codePoint)) {
            return 3;
        }
    }

    return -1;
}

#define utf_cont(ch) (((ch)&0xc0) == 0x80)
/**
 * Return the length in chars of a utf8 string stored in the input buffer
 * @param inputBuffer input buffer that hold the string
 * @param bufferLength size of input buffer
 * @return the number of characters represented by the input utf8 string
 */
inline int64_t lengthUnicode(const char *inputBuffer, size_t bufferLength)
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
    OMNI_CHECK(instance > 0, "instance must be greater than 0");

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

/// Return the size in bytes for the char pointed to by u_input.
/// This function is not part of the original utf8proc, it is a simplified
/// verion of utf8proc_codepoint. It assumes a valid utf8 input otherwise output
/// is undefined.
inline int utf8proc_char_length(const char *u_input)
{
    auto u = (const unsigned char *) u_input;
    unsigned char u0 = u[0];
    if (u0 <= 127) {
        return 1;
    }

    if (u0 >= 192 && u0 <= 223) {
        return 2;
    }

    if (u0 >= 224 && u0 <= 239) {
        return 3;
    }

    if (u0 >= 240 && u0 <= 247) {
        return 4;
    }
    return -1;
}

inline int64_t cappedByteLengthUnicode(const char *input, int64_t size, int64_t maxChars)
{
    int64_t utf8Position = 0;
    int64_t numCharacters = 0;
    while (utf8Position < size && numCharacters < maxChars) {
        auto charSize = utf8proc_char_length(input + utf8Position);
        utf8Position += UNLIKELY(charSize < 0) ? 1 : charSize;
        numCharacters++;
    }
    return utf8Position;
}

template <bool isAscii, typename TString>
int64_t cappedByteLength(const TString &input, size_t maxCharacters)
{
    if constexpr (isAscii) {
        return input.size() > maxCharacters ? maxCharacters : input.size();
    } else {
        return cappedByteLengthUnicode(input.data(), input.size(), maxCharacters);
    }
}

template <typename T>
re2::StringPiece toStringPiece(const T &s)
{
    return re2::StringPiece(s.data(), s.size());
}

inline bool performChecks(std::string &result, const std::string &stringInput, const std::string &pattern,
    const std::string &replace, const int32_t &position)
{
    if (position > stringInput.size()) {
        result = stringInput;
        return true;
    }

    if (stringInput.size() == 0 && pattern.size() == 0 && position == 1) {
        result = replace;
        return true;
    }
    return false;
}

/// This function preprocesses an input replacement string to follow RE2 syntax
/// for java.util.regex used by Presto and Spark. These are the replacements
/// that are required.
/// 1. RE2 replacement only supports group index capture, so we need to convert
/// group name captures to group index captures.
/// 2. Group index capture in java.util.regex replacement is '$N', while in RE2
/// replacement it is '\N'. We need to convert it.
/// 3. Replacement in RE2 only supports '\' followed by a digit or another '\',
/// while java.util.regex will ignore '\' in replacements, so we need to
/// unescape it.
inline std::string PrepareRegexpReplaceReplacement(const RE2 &re, const std::string &replacement)
{
    if (replacement.size() == 0) {
        return std::string{};
    }

    auto newReplacement = replacement;
    static const RE2 kExtractRegex(R"(\${([^}]*)})");

    // If newReplacement contains a reference to a
    // named capturing group ${name}, replace the name with its index.
    re2::StringPiece groupName[2];
    while (kExtractRegex.Match(newReplacement, 0, newReplacement.size(), RE2::UNANCHORED, groupName, 2)) {
        auto groupIter = re.NamedCapturingGroups().find(std::string(groupName[1]));
        if (groupIter == re.NamedCapturingGroups().end()) {}

        RE2::GlobalReplace(&newReplacement, Format(R"(\${{{}}})", std::string(groupName[1])),
            Format("${}", groupIter->second));
    }

    // Convert references to numbered capturing groups from $g to \g.
    static const RE2 kConvertRegex(R"(\$(\d+))");
    RE2::GlobalReplace(&newReplacement, kConvertRegex, R"(\\\1)");

    // Un-escape character except digit or '\\'
    static const RE2 kUnescapeRegex(R"(\\([^0-9\\]))");
    RE2::GlobalReplace(&newReplacement, kUnescapeRegex, R"(\1)");

    return newReplacement;
}

FOLLY_ALWAYS_INLINE int64_t asciiWhitespaces()
{
    int8_t codes[] = {9, 10, 11, 12, 13, 28, 29, 30, 31, 32};
    int64_t bitMask = 0;
    for (auto code : codes) {
        BitUtil::SetBit(&bitMask, code, true);
    }
    return bitMask;
}

template <bool leftTrim, bool rightTrim, typename TOutString, typename TInString>
ALWAYS_INLINE void TrimUnicodeWhiteSpace(TOutString &output, const TInString &input)
{
    auto emptyOutput = [&]() {
        if constexpr (std::is_same_v<TOutString, type::StringRef>) {
            output = type::StringRef("");
        } else {
            output = TOutString{};
        }
    };
    if (input.empty()) {
        emptyOutput();
        return;
    }

    auto curStartPos = 0;
    if constexpr (leftTrim) {
        int codePointSize = 0;
        while (curStartPos < input.size()) {
            auto codePoint = utf8proc_codepoint(input.data() + curStartPos, input.data() + input.size(), codePointSize);
            if (codePoint == -1 || !isUnicodeWhiteSpace(codePoint)) {
                break;
            }
            curStartPos += codePointSize;
        }

        if (curStartPos >= input.size()) {
            emptyOutput();
            return;
        }
    }

    const auto startIndex = curStartPos;
    const auto *stringStart = input.data() + startIndex;
    auto endIndex = input.size() - 1;

    if constexpr (rightTrim) {
        // Right trim traverses the string backwards.

        while (endIndex >= startIndex) {
            auto charSize = endsWithUnicodeWhiteSpace(stringStart, endIndex - startIndex + 1);
            if (charSize == -1) {
                break;
            }
            endIndex -= charSize;
        }

        if (endIndex < startIndex) {
            emptyOutput();
            return;
        }
    }

    auto view = std::string_view(stringStart, endIndex - startIndex + 1);
    output = view;
}
}
