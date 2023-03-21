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

namespace omniruntime {
namespace codegen {
static std::string REPLACE_ERR_MSG = "Replace failed";
static std::string CONCAT_ERR_MSG = "Concat failed";
static constexpr uint8_t EMPTY[] = "";
static int32_t STEP = static_cast<int>('a') - static_cast<int>('A');

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
        auto ret = omniruntime::codegen::ArenaAllocatorMalloc(contextPtr, *outLen + 1);
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
        auto ret = omniruntime::codegen::ArenaAllocatorMalloc(contextPtr, *outLen + 1);
        errno_t res1 = memcpy_s(ret, *outLen + 1, ap, apLen);
        errno_t res2 = memcpy_s(ret + apLen, *outLen - apLen + 1, bp, bpLen);
        if (res1 != EOK || res2 != EOK) {
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
        auto ret = omniruntime::codegen::ArenaAllocatorMalloc(contextPtr, *outLen + 1);
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
        auto ret = omniruntime::codegen::ArenaAllocatorMalloc(contextPtr, *outLen + 1);
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
}; // class stringUtils
} // namespace codegen
} // namespace runtime

#endif // OMNI_RUNTIME_STRING_UTIL_H
