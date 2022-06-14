/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry  function  implementation
 */
#include <iostream>
#include <string>
#include <cstring>
#include <regex>
#include <huawei_secure_c/include/securec.h>
#include "context_helper.h"

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
        std::cerr << "Concat failed" << std::endl;
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
        std::cerr << "Concat failed" << std::endl;
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
        std::cerr << "Concat failed" << std::endl;
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
        std::cerr << "Concat failed" << std::endl;
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

extern DLLEXPORT const char *ToUpper(int64_t contextPtr, const char *str, int32_t strLen, int32_t *outLen)
{
    auto ret = ArenaAllocatorMalloc(contextPtr, strLen);
    int step = static_cast<int>('a') - static_cast<int>('A');
    for (int i = 0; i < strLen; i++) {
        if (*(str + i) >= static_cast<int>('a') && *(str + i) <= static_cast<int>('z')) {
            *(ret + i) = *(str + i) - step;
        } else {
            *(ret + i) = *(str + i);
        }
    }
    *outLen = strLen;
    return ret;
}

extern DLLEXPORT const char *ToUpperChar(int64_t contextPtr, const char *str, int32_t width, int32_t strLen,
    int32_t *outLen)
{
    return ToUpper(contextPtr, str, strLen, outLen);
}