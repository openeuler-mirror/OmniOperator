/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: string some common operators
 */

#ifndef OMNI_RUNTIME_STRING_UTIL_H
#define OMNI_RUNTIME_STRING_UTIL_H

#include <iostream>
#include <string>
#include <memory>
#include <locale>
#include <codecvt>
#include <huawei_secure_c/include/securec.h>
#include "util/utf8_util.h"

namespace omniruntime::codegen::function {
static std::string REPLACE_ERR_MSG = "Replace failed";
static std::string CONCAT_ERR_MSG = "Concat failed";
static constexpr uint8_t EMPTY[] = "";
static int32_t STEP = static_cast<int>('a') - static_cast<int>('A');
static uint8_t BytesOfCodePointInUTF8[] = {
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 0x00..0F
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 0x10..1F
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 0x20..2F
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 0x30..3F
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 0x40..4F
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 0x50..5F
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 0x60..6F
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 0x70..7F
    // Consecutive bytes cannot be used as the first byte
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 0x80..8F
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 0x90..9F
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 0xA0..AF
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 0xB0..BF
    0, 0, // 0xC0..C1 - disallowed in UTF-8
    2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, // 0xC2..CF
    2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, // 0xD0..DF
    3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, // 0xE0..EF
    4, 4, 4, 4, 4, // 0xF0..F4
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 // 0xF5..FF - disallowed in UTF-8
};

class StringUtil {
public:
    static inline std::wstring ToWideString(std::string &s)
    {
        std::wstring_convert<std::codecvt_utf8<wchar_t>> convert;
        return convert.from_bytes(s);
    }

    static inline const char *CastStrStr(bool *hasErr, const char *str, int32_t srcWidth, int32_t strLen,
        int32_t *outLen, int32_t dstWidth)
    {
        int32_t chCount = std::min(srcWidth, dstWidth);
        int32_t dstLen = 0;
        int32_t count = 0;
        while (dstLen < strLen && count < chCount) {
            int32_t charLen = omniruntime::Utf8Util::LengthOfCodePoint(str[dstLen]);
            if (charLen < 0) {
                *hasErr = true;
                *outLen = 0;
                return nullptr;
            }
            dstLen += charLen;
            count++;
        }
        *outLen = dstLen;
        return str;
    }

    static inline const char *ConcatCharDiffWidths(int64_t contextPtr, const char *ap, int32_t aWidth, int32_t apLen,
        const char *bp, int32_t bpLen, bool *hasErr, int32_t *outLen)
    {
        int32_t aPaddingCount = bpLen > 0 ? aWidth - omniruntime::Utf8Util::CountCodePoints(ap, apLen) : 0;
        *outLen = apLen + aPaddingCount + bpLen;
        if (*outLen <= 0) {
            *outLen = 0;
            return reinterpret_cast<const char *>(EMPTY);
        }

        // allocate one more byte is mainly for memcpy_s, when the copy source and destination are
        // both empty strings, the security function will not return an error.
        auto ret = ArenaAllocatorMalloc(contextPtr, *outLen + 1);
        errno_t res1 = memcpy_s(ret, *outLen + 1, ap, apLen);
        errno_t res2 = memset_s(ret + apLen, *outLen - apLen + 1, ' ', aPaddingCount);
        errno_t res3 = memcpy_s(ret + apLen + aPaddingCount, *outLen - (apLen + aPaddingCount) + 1, bp, bpLen);
        if (res1 != EOK || res2 != EOK || res3 != EOK) {
            *hasErr = true;
            *outLen = 0;
            return nullptr;
        }
        return ret;
    }

    static inline const char *ConcatStrDiffWidths(int64_t contextPtr, const char *ap, int32_t apLen, const char *bp,
        int32_t bpLen, bool *hasErr, int32_t *outLen)
    {
        *outLen = apLen + bpLen;
        if (*outLen <= 0) {
            *outLen = 0;
            return reinterpret_cast<const char *>(EMPTY);
        }
        // allocate one more byte is mainly for memcpy_s, when the copy source and destination are
        // both empty strings, the security function will not return an error.
        auto ret = ArenaAllocatorMalloc(contextPtr, *outLen + 1);
        errno_t res1 = memcpy_s(ret, *outLen + 1, ap, apLen);
        errno_t res2 = memcpy_s(ret + apLen, *outLen - apLen + 1, bp, bpLen);
        if (res1 != EOK || res2 != EOK) {
            *hasErr = true;
            *outLen = 0;
            return nullptr;
        }
        return ret;
    }

    static inline const char *ConcatWsStrDiffWidths(int64_t contextPtr, const char *separator, int32_t separatorLen,
        const char *ap, int32_t apLen, const char *bp, int32_t bpLen, bool *hasErr, int32_t *outLen)
    {
        *outLen = apLen + separatorLen + bpLen;
        if (*outLen <= 0) {
            *outLen = 0;
            return reinterpret_cast<const char *>(EMPTY);
        }
        auto ret = ArenaAllocatorMalloc(contextPtr, *outLen + 1);
        errno_t res1 = memcpy_s(ret, *outLen + 1, ap, apLen);
        errno_t res2 = memcpy_s(ret + apLen, *outLen + 1 - apLen, separator, separatorLen);
        errno_t res3 = memcpy_s(ret + apLen + separatorLen, *outLen + 1 - apLen - separatorLen, bp, bpLen);
        if (res1 != EOK || res2 != EOK || res3 != EOK) {
            *hasErr = true;
            *outLen = 0;
            return nullptr;
        }
        return ret;
    }

    static inline const char *ReplaceWithSearchNotEmpty(int64_t contextPtr, const char *str, int32_t strLen,
        const char *searchStr, int32_t searchLen, const char *replaceStr, int32_t replaceLen, bool *hasErr,
        int32_t *outLen)
    {
        if (strLen == 0) {
            *outLen = 0;
            return reinterpret_cast<const char *>(EMPTY);
        }
        std::string s = std::string(str, strLen);
        std::string search = std::string(searchStr, searchLen);
        std::string replace = std::string(replaceStr, replaceLen);
        std::string::size_type matchIndex = 0;
        if (replaceLen == 0) {
            while ((matchIndex = s.find(search, matchIndex)) != std::string::npos) {
                s = s.substr(0, matchIndex) + s.substr(matchIndex + searchLen);
            }
        } else {
            while ((matchIndex = s.find(search, matchIndex)) != std::string::npos) {
                s.replace(matchIndex, searchLen, replace);
                matchIndex += replaceLen;
            }
        }

        *outLen = static_cast<int32_t>(s.length());
        auto ret = ArenaAllocatorMalloc(contextPtr, *outLen + 1);
        error_t res = memcpy_s(ret, *outLen + 1, s.c_str(), s.length());
        if (res != EOK) {
            *hasErr = true;
            *outLen = 0;
            return nullptr;
        }
        return ret;
    }

    static inline const char *ReplaceWithSearchEmpty(int64_t contextPtr, const char *str, int32_t strLen,
        const char *replaceStr, int32_t replaceLen, bool *hasErr, int32_t *outLen)
    {
        int32_t strCodePoints = omniruntime::Utf8Util::CountCodePoints(str, strLen);
        *outLen = strLen + (strCodePoints + 1) * replaceLen;
        auto ret = ArenaAllocatorMalloc(contextPtr, *outLen + 1);
        int32_t indexBuffer = 0;
        errno_t res;
        for (int32_t index = 0; index < strLen;) {
            res = memcpy_s(ret + indexBuffer, *outLen - indexBuffer + 1, replaceStr, replaceLen);
            if (res != EOK) {
                *hasErr = true;
                *outLen = 0;
                return nullptr;
            }
            indexBuffer += replaceLen;
            int32_t codePointLength = omniruntime::Utf8Util::LengthOfCodePoint(*(str + index));
            res = memcpy_s(ret + indexBuffer, *outLen - indexBuffer + 1, str + index, codePointLength);
            if (res != EOK) {
                *hasErr = true;
                *outLen = 0;
                return nullptr;
            }
            indexBuffer += codePointLength;
            index += codePointLength;
        }
        res = memcpy_s(ret + indexBuffer, *outLen - indexBuffer + 1, replaceStr, replaceLen);
        if (res != EOK) {
            *hasErr = true;
            *outLen = 0;
            return nullptr;
        }
        return ret;
    }

    static inline void TrimString(std::string &str)
    {
        str.erase(0, str.find_first_not_of(' '));
        str.erase(str.find_last_not_of(' ') + 1);
    }

    static inline bool StrContainsStr(const char *srcStr, int32_t srcLen, const char *matchStr, int32_t matchLen)
    {
        int next[matchLen];
        next[0] = -1;
        int i = 0;
        int j = -1;
        while (i < matchLen - 1) {
            if (j == -1 || matchStr[i] == matchStr[j]) {
                i++;
                j++;
                next[i] = j;
            } else {
                j = next[j];
            }
        }

        i = 0;
        j = 0;
        while (i < srcLen && j < matchLen) {
            if (j == -1 || srcStr[i] == matchStr[j]) {
                i++;
                j++;
            } else {
                j = next[j];
            }
        }

        return j == matchLen;
    }

    static inline int32_t NumChars(const char *str, int32_t strLen)
    {
        int32_t len = 0;
        int32_t i = 0;

        while (i < strLen) {
            len += 1;
            int32_t offset = str[i] & 0xFF;
            uint8_t numBytes = BytesOfCodePointInUTF8[offset];
            i += numBytes == 0 ? 1 : numBytes;
        }
        return len;
    }

    static inline void CopyChars(const char *srcStr, int32_t srcStrLen, int32_t startIdx, int32_t copyLen,
        char *outStr, int32_t outStrLen, bool *hasErr)
    {
        int32_t maxCopy = std::min({srcStrLen - startIdx, copyLen, outStrLen});
        error_t res = memcpy_s(outStr, outStrLen, srcStr + startIdx, maxCopy);
        if (res != EOK) {
            *hasErr = true;
        }
    }

    static inline int32_t FindChar(const char *str, int32_t strLen, const char *searchChar, int32_t searchCharLen,
        int32_t start)
    {
        while (start <= strLen - searchCharLen) {
            if (std::memcmp(str + start, searchChar, searchCharLen) == 0) {
                return start;
            }
            start += 1;
        }
        return -1;
    }

    static inline std::string StringTrimLeft(const std::string &srcStr, const std::string &trimStr, bool *hasErr)
    {
        int32_t searchIdx = 0;
        int32_t trimIdx = 0;
        while (searchIdx < srcStr.size()) {
            int32_t searchCharBytes = NumBytesForFirstByte(srcStr[searchIdx]);
            char searchChar[searchCharBytes] = {0};
            CopyChars(srcStr.c_str(), srcStr.size(), searchIdx, searchCharBytes, searchChar, searchCharBytes, hasErr);
            if (FindChar(trimStr.c_str(), trimStr.size(), searchChar, searchCharBytes, 0) >= 0) {
                trimIdx += searchCharBytes;
            } else {
                break;
            }
            searchIdx += searchCharBytes;
        }
        if (searchIdx == 0) {
            return srcStr;
        }
        if (trimIdx >= srcStr.size()) {
            return "";
        }
        return srcStr.substr(trimIdx);
    }

    static inline std::string StringTrimRight(const std::string &srcStr, const std::string &trimStr, bool *hasErr)
    {
        int32_t charIdx = 0;
        int32_t numChars = 0;
        int32_t stringCharLen[srcStr.size()] = {};
        int32_t stringCharPos[srcStr.size()] = {};

        while (charIdx < srcStr.size()) {
            stringCharPos[numChars] = charIdx;
            stringCharLen[numChars] = NumBytesForFirstByte(srcStr[charIdx]);
            charIdx += stringCharLen[numChars];
            numChars++;
        }

        int32_t trimEnd = srcStr.size() - 1;
        while (numChars > 0) {
            int32_t searchIdx = stringCharPos[numChars - 1];
            int32_t searchCharBytes = stringCharLen[numChars - 1];
            char searchChar[searchCharBytes] = {0};
            CopyChars(srcStr.c_str(), srcStr.size(), searchIdx, searchCharBytes, searchChar, searchCharBytes, hasErr);
            if (FindChar(trimStr.c_str(), trimStr.size(), searchChar, searchCharBytes, 0) >= 0) {
                trimEnd -= searchCharBytes;
            } else {
                break;
            }
            numChars --;
        }
        if (trimEnd == srcStr.size() - 1) {
            return srcStr;
        }
        if (trimEnd < 0) {
            return "";
        }
        return srcStr.substr(0, trimEnd + 1);
    }

    static inline const char *Trim(int64_t contextPtr, const char *srcStr, int32_t srcStrLen, const char *trimStr,
        int32_t trimStrLen, bool *hasErr, int32_t *outLen)
    {
        std::string src = std::string(srcStr, srcStrLen);
        std::string trim = std::string(trimStr, trimStrLen);
        std::string result = StringTrimRight(StringTrimLeft(src, trim, hasErr), trim, hasErr);
        if (*hasErr) {
        *outLen = 0;
        return nullptr;
        }
        *outLen = static_cast<int32_t>(result.length());
        auto ret = ArenaAllocatorMalloc(contextPtr, *outLen + 1);
        if (memcpy_s(ret, *outLen + 1, result.c_str(), result.length()) != EOK) {
            *hasErr = true;
            *outLen = 0;
            return nullptr;
        }
        return ret;
    }

    static inline const char *LTrim(int64_t contextPtr, const char *srcStr, int32_t srcStrLen, const char *trimStr,
        int32_t trimStrLen, bool *hasErr, int32_t *outLen)
    {
        std::string src = std::string(srcStr, srcStrLen);
        std::string trim = std::string(trimStr, trimStrLen);
        std::string result = StringTrimLeft(src, trim, hasErr);
        if (*hasErr) {
        *outLen = 0;
        return nullptr;
        }
        *outLen = static_cast<int32_t>(result.length());
        auto ret = ArenaAllocatorMalloc(contextPtr, *outLen + 1);
        if (memcpy_s(ret, *outLen + 1, result.c_str(), result.length()) != EOK) {
            *hasErr = true;
            *outLen = 0;
            return nullptr;
        }
        return ret;
    }

    static inline const char *RTrim(int64_t contextPtr, const char *srcStr, int32_t srcStrLen, const char *trimStr,
        int32_t trimStrLen, bool *hasErr, int32_t *outLen)
    {
        std::string src = std::string(srcStr, srcStrLen);
        std::string trim = std::string(trimStr, trimStrLen);
        std::string result = StringTrimRight(src, trim, hasErr);
        if (*hasErr) {
        *outLen = 0;
        return nullptr;
        }
        *outLen = static_cast<int32_t>(result.length());
        auto ret = ArenaAllocatorMalloc(contextPtr, *outLen + 1);
        if (memcpy_s(ret, *outLen + 1, result.c_str(), result.length()) != EOK) {
            *hasErr = true;
            *outLen = 0;
            return nullptr;
        }
        return ret;
    }

    static inline int32_t NumBytesForFirstByte(const char c)
    {
        uint8_t offset = c & 0xFF;
        uint8_t numBytes = BytesOfCodePointInUTF8[offset];
        return (numBytes == 0) ? 1 : numBytes;
    }
}; // class stringUtils
} // namespace codegen function

#endif // OMNI_RUNTIME_STRING_UTIL_H
