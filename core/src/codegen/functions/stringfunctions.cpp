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


extern "C" DLLEXPORT int32_t StrCompareExt(int64_t ap, int64_t bp)
{
    char *a = reinterpret_cast<char *>(ap);
    char *b = reinterpret_cast<char *>(bp);
    string As = string(a);
    string Bs = string(b);
    // return sign(Bs - As), more or less
    if (As < Bs) return -1;
    if (As > Bs) return 1;
    return 0;
}


extern "C" DLLEXPORT bool LikeExt(int64_t str, int64_t regexToMatch)
{
    string S = string(reinterpret_cast<char *>(str));
    string R = string(reinterpret_cast<char *>(regexToMatch));
    // Using re2 library
    // return RE2::FullMatch(S, R);

    // Using std regex library
    regex Re = regex(R);
    return regex_match(S, Re);
}


extern "C" DLLEXPORT int64_t SubstrWithStartExt(int64_t str, int32_t startIdx)
{
    char *s = reinterpret_cast<char*>(static_cast<uintptr_t>(str));
    int32_t length = 0;
    // calculate length of original string
    while (s[length] != '\0') {
        length++;
    }

    if (startIdx == 0 || length == 0 || startIdx + length < 0 || startIdx > length) {
        auto ret = std::make_unique<char[]>(1).release();
        ret[0] = '\0';
        return (int64_t)(ret);
    }

    if (startIdx > 0) {
        startIdx -= 1;
    } else {
        // negative start is relative to end of string
        startIdx += length;
    }

    auto ret = std::make_unique<char[]>((length - startIdx) + 1).release();
    for (int indexStart = startIdx, i = 0; indexStart < length; indexStart++, i++) {
        ret[i] = s[indexStart];
    }
    ret[(length - startIdx) + 1] = '\0';
    return (int64_t)(ret);
}


extern "C" DLLEXPORT int64_t SubstrExt(int64_t str, int32_t startIdx, int32_t length)
{
    char *s = reinterpret_cast<char*>(static_cast<uintptr_t>(str));
    int32_t totalLength = 0;
    // calculate length of original string
    while (s[totalLength] != '\0') {
        totalLength++;
    }
    if (startIdx == 0 || (length <= 0) || (totalLength == 0) || startIdx + totalLength < 0 || startIdx > totalLength) {
        auto ret = std::make_unique<char[]>(1).release();
        ret[0] = '\0';
        return (int64_t)(ret);
    }
    int endIdx;
    if (startIdx > 0) {
        startIdx = startIdx - 1;
        // Quick exit if we are sure that the position is after the end
        if (totalLength - startIdx <= length) {
            endIdx = totalLength;
        } else if (length == 0) {
            endIdx = startIdx;
        } else {
            endIdx = startIdx + length;
        }
    } else {
        // negative start is relative to end of string
        startIdx += totalLength;
        if (startIdx + length < totalLength) {
            endIdx = startIdx + length;
        } else {
            endIdx = totalLength;
        }
    }
    auto ret = std::make_unique<char[]>((endIdx - startIdx) + 1).release();
    for (int indexStart = startIdx, i = 0; indexStart < endIdx; indexStart++, i++) {
        ret[i] = s[indexStart];
    }
    ret[(endIdx - startIdx) + 1] = '\0';
    return (int64_t)(ret);
}


extern "C" DLLEXPORT int64_t ConcatStrExt(int64_t ap, int64_t bp)
{
    char *a = reinterpret_cast<char *>((uintptr_t)ap);
    char *b = reinterpret_cast<char *>((uintptr_t)bp);
    string As = string(a);
    string Bs = string(b);
    auto ret = std::make_unique<char[]>(As.size() + Bs.size() + 1).release();
    for (int i = 0; i < As.size(); i++) {
        ret[i] = As[i];
    }
    for (int j = 0; j < Bs.size(); j++) {
        ret[j + As.size()] = Bs[j];
    }
    ret[As.size() + Bs.size()] = '\0';
    return (int64_t)(ret);
}

extern "C" DLLEXPORT int32_t CastString(int64_t str)
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