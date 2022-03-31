/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry  function  implementation
 */
#include <iostream>
#include <string>
#include <cstring>
#include <regex>
#include <huawei_secure_c/include/securec.h>
#include "common.h"
#include "context_helper.h"

using namespace std;

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
extern "C" {
INLINE int32_t StrCompare(const char *ap, int32_t apLen, const char *bp, int32_t bpLen)
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

INLINE bool Like(const char *str, int32_t strLen, const char *regexToMatch, int32_t regexLen)
{
    string s = string(str, strLen);
    string r = string(regexToMatch, regexLen);

    regex re = regex(r);
    return regex_match(s, re);
}

INLINE const char *ConcatStrStr(int64_t contextPtr, const char *ap, int32_t apLen, const char *bp,
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
        std::cerr << "Concat failed" << std::endl;
    }

    return ret;
}

INLINE const char *ConcatCharStr(int64_t contextPtr, const char *ap, int32_t aWidth, int32_t apLen,
                                 const char *bp, int32_t bpLen, int32_t *outLen)
{
    if (bpLen == 0) {
        *outLen = apLen;
        return ap;
    }
    int32_t apPaddedLen = 0;
    if (apLen <= aWidth) {
        if (apPaddedLen == apLen) {
            apPaddedLen = apLen;
        } else {
            apPaddedLen = aWidth;
        }
    }
    *outLen = apPaddedLen + bpLen;
    if (*outLen <= 0) {
        *outLen = 0;
        return "";
    }
    auto ret = ArenaAllocatorMalloc(contextPtr, *outLen);
    errno_t res1 = memcpy_s(ret, *outLen, ap, apLen);
    errno_t res2 = memset_s(ret + apLen, *outLen, ' ', apPaddedLen - apLen);
    errno_t res3 = memcpy_s(ret + apPaddedLen, *outLen, bp, bpLen);
    if (res1 != EOK || res2 != EOK || res3 != EOK) {
        std::cerr << "Concat failed" << std::endl;
    }

    return ret;
}

INLINE const char *ConcatCharChar(int64_t contextPtr, const char *ap, int32_t aWidth, int32_t apLen,
                                            const char *bp, int32_t bWidth, int32_t bpLen, int32_t *outLen)
{
    return ConcatCharStr(contextPtr, ap, aWidth, apLen, bp, bpLen, outLen);
}

INLINE const char *ConcatStrChar(int64_t contextPtr, const char *ap, int32_t apLen, const char *bp,
                                           int32_t bWidth, int32_t bpLen, int32_t *outLen)
{
    if (bpLen == 0) {
        *outLen = apLen;
        return ap;
    }
    int32_t bpPaddedLen = 0;
    if (bpLen <= bWidth) {
        bpPaddedLen = bWidth;
    }

    *outLen = apLen + bpPaddedLen;
    if (*outLen <= 0) {
        *outLen = 0;
        return "";
    }
    auto ret = ArenaAllocatorMalloc(contextPtr, *outLen);
    errno_t res1 = memcpy_s(ret, *outLen, ap, apLen);
    errno_t res2 = memcpy_s(ret + apLen, *outLen, bp, bpLen);
    errno_t res3 = memset_s(ret + apLen + bpLen, *outLen, ' ', bpPaddedLen - bpLen);
    if (res1 != EOK || res2 != EOK || res3 != EOK) {
        std::cerr << "Concat failed" << std::endl;
    }

    return ret;
}

INLINE int32_t CastString(const char *str, int32_t strLen)
{
    // Date is in the format 1996-02-28
    // Doesn't account for leap seconds or daylight savings
    // Should be ok just for dates
    int32_t i1 = 5;
    int32_t i2 = 8;
    int yr = THOUSANDS * (str[THOU] - '0') + HUNDREDS * (str[HUN] - '0') + TENS * (str[TEN] - '0') + (str[ONE] - '0');
    int mnth = TENS * (str[i1] - '0') + (str[i1 + 1] - '0'); // compute mnth
    int day = TENS * (str[i2] - '0') + (str[i2 + 1] - '0');  // compute day

    struct std::tm epoch = { 0, 0, 0, 1, 1, 70 };
    struct std::tm t = { 0, 0, 0, day, mnth, yr - BASE_YEAR };
    std::time_t epochTime = std::mktime(&epoch);
    std::time_t desiredTime = std::mktime(&t);
    return static_cast<int32_t>(std::difftime(desiredTime, epochTime) / SECOND_OF_DAY);
}

INLINE const char *ToUpper(int64_t contextPtr, const char *str, int32_t strLen, int32_t *outLen)
{
    auto ret = ArenaAllocatorMalloc(contextPtr, strLen);
    for (int i = 0; i < strLen; i++) {
        if (*(str + i) > 96 && *(str + i) < 123) {
            *(ret + i) = *(str + i) - 32;
        } else {
            *(ret + i) = *(str + i);
        }
    }
    *outLen = strLen;
    return ret;
}

INLINE const char *ToUpperChar(int64_t contextPtr, const char *str, int32_t width, int32_t strLen, int32_t *outLen)
{
    return ToUpper(contextPtr, str, strLen, outLen);
}

#define SUBSTR(TYPE)                                                                                              \
    INLINE const char *Substr_##TYPE(int64_t contextPtr, const char *str, int32_t strLen,                         \
        TYPE startIdx, TYPE length, int32_t *outLen)                                                              \
    {                                                                                                             \
        if (startIdx == 0 || (length <= 0) || (strLen == 0) || startIdx + strLen < 0 || startIdx > strLen) {      \
            *outLen = 0;                                                                                          \
            return "";                                                                                            \
        }                                                                                                         \
        int endIdx;                                                                                               \
        if (startIdx > 0) {                                                                                       \
            startIdx = startIdx - 1;                                                                              \
            if (strLen - startIdx <= length) {                                                                    \
                endIdx = strLen;                                                                                  \
            } else if (length == 0) {                                                                             \
                endIdx = startIdx;                                                                                \
            } else {                                                                                              \
                endIdx = startIdx + length;                                                                       \
            }                                                                                                     \
        } else {                                                                                                  \
            startIdx += strLen;                                                                                   \
            if (startIdx + length < strLen) {                                                                     \
                endIdx = startIdx + length;                                                                       \
            } else {                                                                                              \
                endIdx = strLen;                                                                                  \
            }                                                                                                     \
        }                                                                                                         \
        *outLen = endIdx - startIdx;                                                                              \
        auto ret = ArenaAllocatorMalloc(contextPtr, *outLen);                                                     \
        errno_t res = memcpy_s(ret, *outLen, str + startIdx, *outLen);                                            \
        if (res != EOK) {                                                                                         \
            std::cerr << "Substring failed" << std::endl;                                                         \
        }                                                                                                         \
        return ret;                                                                                               \
    }

SUBSTR(int32)
SUBSTR(int64)
#undef SUBSTR

#define SUBSTR_CHAR(TYPE)                                                                                            \
    INLINE const char *Substr_char_##TYPE(int64_t contextPtr, const char *str, int32_t width,                        \
        int32_t strLen, TYPE startIdx, TYPE length, int32_t *outLen)                                                 \
    {                                                                                                                \
        return Substr_##TYPE(contextPtr, str, strLen, startIdx, length, outLen);                                     \
    }

SUBSTR_CHAR(int32)
SUBSTR_CHAR(int64)
#undef SUBSTR_CHAR


#define SUBSTR_WITH_START(TYPE)                                                                            \
    INLINE const char *SubstrWithStart_##TYPE(int64_t contextPtr, const char *str,                         \
        int32_t strLen, TYPE startIdx, int32_t *outLen)                                                    \
    {                                                                                                      \
        if (startIdx == 0 || strLen == 0 || startIdx + strLen < 0 || startIdx > strLen) {                  \
            *outLen = 0;                                                                                   \
            return "";                                                                                     \
        }                                                                                                  \
                                                                                                           \
        if (startIdx > 0) {                                                                                \
            startIdx -= 1;                                                                                 \
        } else {                                                                                           \
            startIdx += strLen;                                                                            \
        }                                                                                                  \
                                                                                                           \
        *outLen = strLen - startIdx;                                                                       \
                                                                                                           \
        auto ret = ArenaAllocatorMalloc(contextPtr, *outLen);                                              \
        errno_t res = memcpy_s(ret, *outLen, str + startIdx, *outLen);                                     \
        if (res != EOK) {                                                                                  \
            std::cerr << "Substring failed" << std::endl;                                                  \
        }                                                                                                  \
        return ret;                                                                                        \
    }

SUBSTR_WITH_START(int32)
SUBSTR_WITH_START(int64)

#undef SUBSTR_WITH_START

#define SUBSTR_CHAR_WITH_START(TYPE)                                                                           \
    INLINE const char *SubstrWithStart_char_##TYPE(int64_t contextPtr, const char *str,                        \
        int32_t width, int32_t strLen, TYPE startIdx, int32_t *outLen)                                         \
    {                                                                                                          \
        return SubstrWithStart_##TYPE(contextPtr, str, strLen, startIdx, outLen);                              \
    }
SUBSTR_CHAR_WITH_START(int32)
SUBSTR_CHAR_WITH_START(int64)
#undef SUBSTR_CHAR_WITH_START
}