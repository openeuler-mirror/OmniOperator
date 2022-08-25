/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry  function  implementation
 */
#include <string>
#include <cstring>
#include <regex>
#include <huawei_secure_c/include/securec.h>
#include "context_helper.h"
#include "util/engine.h"
#include "stringfunctions.h"

#ifdef _WIN32
#else
#define DLLEXPORT
#endif

using namespace std;
using namespace omniruntime::codegen;

namespace {
const int THOUSANDS = 1000;
const int HUNDREDS = 100;
const int TENS = 10;
const double SECOND_OF_DAY = 86400.0;
const int BASE_YEAR = 1900;

const int THOU = 0;
const int HUN = 1;
const int TEN = 2;
const int ONE = 3;
}

extern DLLEXPORT int32_t StrCompare(const char *ap, int32_t apLen, const char *bp, int32_t bpLen)
{
    int min = bpLen;
    if (apLen < min) {
        min = apLen;
    }

    int32_t result = memcmp(ap, bp, min);
    if (result != 0) {
        return result;
    } else {
        return apLen - bpLen;
    }
}

extern DLLEXPORT bool LikeStr(const char *str, int32_t strLen, const char *regexToMatch, int32_t regexLen)
{
    string s = string(str, strLen);
    string r = string(regexToMatch, regexLen);

    regex re = regex(r);
    return regex_match(s, re);
}

extern DLLEXPORT bool LikeChar(const char *str, int32_t strWidth, int32_t strLen, const char *regexToMatch,
    int32_t regexLen)
{
    string s = string(str, strWidth);
    string r = string(regexToMatch, regexLen);

    regex re = regex(r);
    return regex_match(s, re);
}

extern DLLEXPORT const char *ConcatStrStr(int64_t contextPtr, const char *ap, int32_t apLen, const char *bp,
    int32_t bpLen, int32_t *outLen)
{
    *outLen = apLen + bpLen;
    if (*outLen <= 0) {
        *outLen = 0;
        return "";
    }

    auto ret = ArenaAllocatorMalloc(contextPtr, *outLen);
    errno_t res1 = memcpy_s(ret, *outLen, ap, apLen);
    errno_t res2 = memcpy_s(ret + apLen, *outLen, bp, bpLen);
    if (res1 != EOK || res2 != EOK) {
        char message[] = "Concat failed";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return nullptr;
    }
    return ret;
}

extern DLLEXPORT const char *ConcatCharChar(int64_t contextPtr, const char *ap, int32_t aWidth, int32_t apLen,
    const char *bp, int32_t bWidth, int32_t bpLen, int32_t *outLen)
{
    *outLen = aWidth + bWidth;
    if (*outLen <= 0) {
        *outLen = 0;
        return "";
    }

    auto ret = ArenaAllocatorMalloc(contextPtr, *outLen);
    errno_t res1 = memcpy_s(ret, *outLen, ap, apLen);
    errno_t res2 = memset_s(ret + apLen, *outLen, ' ', aWidth - apLen);
    errno_t res3 = memcpy_s(ret + aWidth, *outLen, bp, bpLen);
    errno_t res4 = memset_s(ret + aWidth + bpLen, *outLen, ' ', bWidth - bpLen);
    if (res1 != EOK || res2 != EOK || res3 != EOK || res4 != EOK) {
        char message[] = "Concat failed";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return nullptr;
    }
    return ret;
}

extern DLLEXPORT const char *ConcatCharStr(int64_t contextPtr, const char *ap, int32_t aWidth, int32_t apLen,
    const char *bp, int32_t bpLen, int32_t *outLen)
{
    *outLen = aWidth + bpLen;
    if (*outLen <= 0) {
        *outLen = 0;
        return "";
    }

    auto ret = ArenaAllocatorMalloc(contextPtr, *outLen);
    errno_t res1 = memcpy_s(ret, *outLen, ap, apLen);
    errno_t res2 = memset_s(ret + apLen, *outLen, ' ', aWidth - apLen);
    errno_t res3 = memcpy_s(ret + aWidth, *outLen, bp, bpLen);
    if (res1 != EOK || res2 != EOK || res3 != EOK) {
        char message[] = "Concat failed";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return nullptr;
    }
    return ret;
}

extern DLLEXPORT const char *ConcatStrChar(int64_t contextPtr, const char *ap, int32_t apLen, const char *bp,
    int32_t bWidth, int32_t bpLen, int32_t *outLen)
{
    *outLen = apLen + bWidth;
    if (*outLen <= 0) {
        *outLen = 0;
        return "";
    }

    auto ret = ArenaAllocatorMalloc(contextPtr, *outLen);
    errno_t res1 = memcpy_s(ret, *outLen, ap, apLen);
    errno_t res2 = memcpy_s(ret + apLen, *outLen, bp, bpLen);
    errno_t res3 = memset_s(ret + apLen + bpLen, *outLen, ' ', bWidth - bpLen);
    if (res1 != EOK || res2 != EOK || res3 != EOK) {
        char message[] = "Concat failed";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return nullptr;
    }
    return ret;
}

extern DLLEXPORT int32_t CastString(int64_t contextPtr, const char *str, int32_t strLen)
{
    // Date is in the format 1996-02-28
    // Doesn't account for leap seconds or daylight savings
    // Should be ok just for dates
    string regexToMatch = "\\d{4}-\\d{2}-\\d{2}$";
    regex re = regex(regexToMatch);
    string s = string(str, strLen);
    if (!regex_match(s, re)) {
        char message[] = "Only support cast date\'YYYY-MM-DD\' to integer";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return -1;
    }

    int32_t i1 = 5;
    int32_t i2 = 8;
    int base = static_cast<int32_t>('0');
    int yr =
        THOUSANDS * (str[THOU] - base) + HUNDREDS * (str[HUN] - base) + TENS * (str[TEN] - base) + (str[ONE] - base);
    int mnth = TENS * (str[i1] - base) + (str[i1 + 1] - base); // compute month
    int day = TENS * (str[i2] - base) + (str[i2 + 1] - base);  // compute day

    struct std::tm epoch = { 0, 0, 0, 1, 1, 70 };
    struct std::tm t = { 0, 0, 0, day, mnth, yr - BASE_YEAR };
    std::time_t epochTime = std::mktime(&epoch);
    std::time_t desiredTime = std::mktime(&t);
    return static_cast<int32_t>(std::difftime(desiredTime, epochTime) / SECOND_OF_DAY);
}

extern DLLEXPORT const char *ToUpperStr(int64_t contextPtr, const char *str, int32_t strLen, int32_t *outLen)
{
    auto ret = ArenaAllocatorMalloc(contextPtr, strLen);
    for (int32_t i = 0; i < strLen; i++) {
        auto currItem = *(str + i);
        if (currItem >= static_cast<int>('a') && currItem <= static_cast<int>('z')) {
            *(ret + i) = static_cast<char>(currItem - STEP);
        } else {
            *(ret + i) = currItem;
        }
    }
    *outLen = strLen;
    return ret;
}

extern DLLEXPORT const char *ToUpperChar(int64_t contextPtr, const char *str, int32_t width, int32_t strLen,
    int32_t *outLen)
{
    return ToUpperStr(contextPtr, str, strLen, outLen);
}

extern DLLEXPORT const char *ToLowerStr(int64_t contextPtr, const char *str, int32_t strLen, int32_t *outLen)
{
    auto ret = ArenaAllocatorMalloc(contextPtr, strLen);
    for (int32_t i = 0; i < strLen; i++) {
        auto currItem = *(str + i);
        if (currItem >= static_cast<int>('A') && currItem <= static_cast<int>('Z')) {
            *(ret + i) = static_cast<char>(currItem + STEP);
        } else {
            *(ret + i) = currItem;
        }
    }
    *outLen = strLen;
    return ret;
}

extern DLLEXPORT const char *ToLowerChar(int64_t contextPtr, const char *str, int32_t width, int32_t strLen,
    int32_t *outLen)
{
    return ToLowerStr(contextPtr, str, strLen, outLen);
}

extern DLLEXPORT int64_t LengthChar(const char *str, int32_t width, int32_t strLen)
{
    return width;
}

extern DLLEXPORT int64_t LengthStr(const char *str, int32_t strLen)
{
    return omniruntime::Utf8Util::CountCodePoints(str, strLen);
}

extern DLLEXPORT const char *ReplaceStrStrStrWithRep(int64_t contextPtr, const char *str, int32_t strLen,
    const char *searchStr, int32_t searchLen, const char *replaceStr, int32_t replaceLen, int32_t *outLen)
{
    EngineType engineType = EngineUtil::GetInstance().GetEngineType();
    if (searchLen == 0 && engineType == EngineType::Spark) {
        *outLen = strLen;
        auto ret = ArenaAllocatorMalloc(contextPtr, *outLen);
        errno_t res = memcpy_s(ret, *outLen + 1, str, strLen);
        if (res != EOK) {
            char message[] = "Replace failed";
            SetError(contextPtr, message, sizeof(message) / sizeof(char));
            return nullptr;
        }
        return ret;
    } else if (searchLen == 0) {
        *outLen = strLen + (strLen + 1) * replaceLen;
        auto ret = ArenaAllocatorMalloc(contextPtr, *outLen);
        for (int32_t index = 0; index < strLen; index++) {
            errno_t res1 = memcpy_s(ret + (replaceLen + 1) * index, *outLen + 1, replaceStr, replaceLen);
            errno_t res2 = memcpy_s(ret + (replaceLen + 1) * index + replaceLen, *outLen + 1, str + index, 1);
            if (res1 != EOK || res2 != EOK) {
                char message[] = "Replace failed";
                SetError(contextPtr, message, sizeof(message) / sizeof(char));
                return nullptr;
            }
        }
        errno_t res = memcpy_s(ret + (replaceLen + 1) * strLen, *outLen + 1, replaceStr, replaceLen);
        if (res != EOK) {
            char message[] = "Replace failed";
            SetError(contextPtr, message, sizeof(message) / sizeof(char));
            return nullptr;
        }
        return ret;
    }

    if (strLen == 0) {
        *outLen = 0;
        return "";
    }

    string s = string(str, strLen);
    string search = string(searchStr, searchLen);
    string replace = string(replaceStr, replaceLen);
    string::size_type matchIndex = 0;
    if (replaceLen == 0) {
        while ((matchIndex = s.find(search, matchIndex)) != string::npos) {
            s = s.substr(0, matchIndex) + s.substr(matchIndex + searchLen);
        }
    } else {
        while ((matchIndex = s.find(search, matchIndex)) != string::npos) {
            s.replace(matchIndex, searchLen, replace);
            matchIndex += replaceLen;
        }
    }

    *outLen = s.length();
    auto ret = ArenaAllocatorMalloc(contextPtr, *outLen);
    error_t res = memcpy_s(ret, *outLen + 1, s.c_str(), s.length());
    if (res != EOK) {
        char message[] = "Replace failed";
        SetError(contextPtr, message, sizeof(message) / sizeof(char));
        return nullptr;
    }
    return ret;
}

extern DLLEXPORT const char *ReplaceStrStrWithoutRep(int64_t contextPtr, const char *str, int32_t strLen,
    const char *searchStr, int32_t searchLen, int32_t *outLen)
{
    return ReplaceStrStrStrWithRep(contextPtr, str, strLen, searchStr, searchLen, "", 0, outLen);
}