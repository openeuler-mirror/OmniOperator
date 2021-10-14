/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry  function  implementation
 */
#include <iostream>
#include <string>
#include <cstring>
#include <memory>
#include <vector>
#include <cassert>
#include <algorithm>
#include <regex>



#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
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

    int g_currStrNum = 0;
}

vector<char *> stringsToFree;


extern "C" DLLEXPORT int32_t StrCompareExt(int64_t ap, int64_t apLen, int64_t bp, int64_t bpLen)
{
    char *a = reinterpret_cast<char *>((uintptr_t)ap);
    char *b = reinterpret_cast<char *>((uintptr_t)bp);
    int min = bpLen;
    if (apLen < min) {
        min = apLen;
    }

    int32_t result = memcmp(a, b, min);
    if (result != 0) {
        return result;
    } else {
        return apLen - bpLen;
    }
}


extern "C" DLLEXPORT bool LikeExt(int64_t str, int64_t strLen, int64_t regexToMatch, int64_t regexLen)
{
    string S = string(reinterpret_cast<char *>(str), strLen);
    string R = string(reinterpret_cast<char *>(regexToMatch), regexLen);
    // Using re2 library
    // return RE2::FullMatch(S, R);

    // Using std regex library
    regex Re = regex(R);
    return regex_match(S, Re);
}


extern "C" DLLEXPORT int64_t SubstrWithStartExt(int64_t str, int64_t strLen, int32_t startIdx, int64_t *outLen)
{
    char *s = reinterpret_cast<char*>(static_cast<uintptr_t>(str));

    if (startIdx == 0 || strLen == 0 || startIdx + strLen < 0 || startIdx > strLen) {
        *outLen = 0;
        const char *ret = "";
        return (int64_t)(ret);
    }

    if (startIdx > 0) {
        startIdx -= 1;
    } else {
        // negative start is relative to end of string
        startIdx += strLen;
    }

    *outLen = strLen - startIdx;
    auto ret = std::make_unique<char[]>(*outLen).release();

    memcpy(ret, s + startIdx, *outLen);

    return (int64_t)(ret);
}


extern "C" DLLEXPORT int64_t SubstrExt(int64_t str, int64_t strLen, int32_t startIdx, int32_t length, int64_t *outLen)
{
    char *s = reinterpret_cast<char*>(static_cast<uintptr_t>(str));

    if (startIdx == 0 || (length <= 0) || (strLen == 0) || startIdx + strLen < 0 || startIdx > strLen) {
        *outLen = 0;
        const char *ret = "";
        return (int64_t)(ret);    }
    int endIdx;
    if (startIdx > 0) {
        startIdx = startIdx - 1;
        // Quick exit if we are sure that the position is after the end
        if (strLen - startIdx <= length) {
            endIdx = strLen;
        } else if (length == 0) {
            endIdx = startIdx;
        } else {
            endIdx = startIdx + length;
        }
    } else {
        // negative start is relative to end of string
        startIdx += strLen;
        if (startIdx + length < strLen) {
            endIdx = startIdx + length;
        } else {
            endIdx = strLen;
        }
    }

    *outLen = endIdx - startIdx;
    auto ret = std::make_unique<char[]>(*outLen).release();
    memcpy(ret, s + startIdx, *outLen);

    return (int64_t)(ret);
}


extern "C" DLLEXPORT int64_t ConcatStrExt(int64_t ap, int64_t apLen, int64_t bp, int64_t bpLen, int64_t *outLen)
{
    char *a = reinterpret_cast<char *>((uintptr_t)ap);
    char *b = reinterpret_cast<char *>((uintptr_t)bp);
    *outLen = apLen + bpLen;
    if (*outLen <= 0) {
        *outLen = 0;
        const char *ret = "";
        return (int64_t)(ret);

    }

    auto ret = std::make_unique<char[]>(*outLen).release();
    memcpy(ret, a, apLen);
    memcpy(ret + apLen, b, bpLen);

    return (int64_t)(ret);
}

extern "C" DLLEXPORT int32_t CastString(int64_t str, int64_t strLen)
{
    // Date is in the format 1996-02-28
    // Doesn't account for leap seconds or daylight savings
    // Should be ok just for dates
    char *s = (char *) (uintptr_t)str;
    int yr = THOUSANDS * (s[THOU] - '0') + HUNDREDS * (s[HUN] - '0') + TENS * (s[TEN] - '0') + (s[ONE] - '0');
    int mnth = TENS * (s[5] - '0') + (s[6] - '0'); // compute mnth
    int day = TENS * (s[8] - '0') + (s[9] - '0'); // compute day

    struct std::tm epoch = {0, 0, 0, 1, 1, 70};
    struct std::tm t = {0, 0, 0, day, mnth, yr - BASE_YEAR};
    std::time_t epochTime = std::mktime(&epoch);
    std::time_t desiredTime = std::mktime(&t);
    return (int32_t)(std::difftime(desiredTime, epochTime) / SECOND_OF_DAY);
}


void FreeStrings()
{
    for (int i = g_currStrNum; i < stringsToFree.size(); i++) {
        delete[] stringsToFree[i];
        stringsToFree[i] = nullptr;
        g_currStrNum++;
    }
}