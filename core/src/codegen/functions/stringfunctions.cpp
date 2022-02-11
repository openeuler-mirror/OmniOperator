/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry  function  implementation
 */
#include <iostream>
#include <string>
#include <cstring>
#include <regex>
#include "../../../thirdparty/huawei_secure_c/include/securec.h"
#include "context_helper.h"

#ifdef _WIN32
#else
#define DLLEXPORT
#endif

using namespace std;

namespace {
    const int THOUSANDS  = 1000;
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

extern DLLEXPORT bool Like(const char *str, int32_t strLen, const char *regexToMatch, int32_t regexLen)
{
    string s = string(str, strLen);
    string r = string(regexToMatch, regexLen);
    // Using re2 library
    // return RE2::FullMatch(S, R);

    // Using std regex library
    regex re = regex(r);
    return regex_match(s, re);
}

extern DLLEXPORT const char *ConcatStr(int64_t contextPtr, const char *ap, int32_t apLen, const char *bp, int32_t bpLen,
                                       int32_t *outLen)
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

extern DLLEXPORT const char *ConcatChar(int64_t contextPtr, const char *ap, int32_t aWidth, int32_t apLen, const char *bp,
                                        int32_t bWidth, int32_t bpLen, int32_t *outLen)
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

extern DLLEXPORT int32_t CastString(const char *str, int32_t strLen)
{
    // Date is in the format 1996-02-28
    // Doesn't account for leap seconds or daylight savings
    // Should be ok just for dates
    int32_t i1 = 5;
    int32_t i2 = 8;
    int yr = THOUSANDS * (str[THOU] - '0') + HUNDREDS * (str[HUN] - '0') + TENS * (str[TEN] - '0') + (str[ONE] - '0');
    int mnth = TENS * (str[i1] - '0') + (str[i1 + 1] - '0'); // compute mnth
    int day = TENS * (str[i2] - '0') + (str[i2 + 1] - '0'); // compute day

    struct std::tm epoch = {0, 0, 0, 1, 1, 70};
    struct std::tm t = {0, 0, 0, day, mnth, yr - BASE_YEAR};
    std::time_t epochTime = std::mktime(&epoch);
    std::time_t desiredTime = std::mktime(&t);
    return static_cast<int32_t>(std::difftime(desiredTime, epochTime) / SECOND_OF_DAY);
}