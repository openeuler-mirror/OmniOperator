/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: batch string functions implementation
 */
#include "batch_stringfunctions.h"
#include <iostream>
#include <regex>

#ifdef _WIN32
#else
#define DLLEXPORT
#endif

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
const int32_t STEP = static_cast<int>('a') - static_cast<int>('A');
}

extern "C" DLLEXPORT void BatchStrCompare(uint8_t **ap, int32_t *apLen, uint8_t **bp, int32_t *bpLen, int32_t *res,
    int32_t rowCnt)
{
    int min = 0, result = 0;
    for (int i = 0; i < rowCnt; ++i) {
        min = bpLen[i];
        if (apLen[i] < min) {
            min = apLen[i];
        }

        result = memcmp(ap[i], bp[i], min);
        if (result != 0) {
            res[i] = result;
        } else {
            res[i] = apLen[i] - bpLen[i];
        }
    }
}

extern "C" DLLEXPORT void BatchLessThanStr(uint8_t **ap, int32_t *apLen, uint8_t **bp, int32_t *bpLen, bool *res,
    int32_t rowCnt)
{
    int32_t tmp[rowCnt];
    BatchStrCompare(ap, apLen, bp, bpLen, tmp, rowCnt);
    for (int i = 0; i < rowCnt; i++) {
        res[i] = (tmp[i] < 0);
    }
}

extern "C" DLLEXPORT void BatchLessThanEqualStr(uint8_t **ap, int32_t *apLen, uint8_t **bp, int32_t *bpLen, bool *res,
    int32_t rowCnt)
{
    int32_t tmp[rowCnt];
    BatchStrCompare(ap, apLen, bp, bpLen, tmp, rowCnt);
    for (int i = 0; i < rowCnt; i++) {
        res[i] = (tmp[i] <= 0);
    }
}

extern "C" DLLEXPORT void BatchGreaterThanStr(uint8_t **ap, int32_t *apLen, uint8_t **bp, int32_t *bpLen, bool *res,
    int32_t rowCnt)
{
    int32_t tmp[rowCnt];
    BatchStrCompare(ap, apLen, bp, bpLen, tmp, rowCnt);
    for (int i = 0; i < rowCnt; i++) {
        res[i] = (tmp[i] > 0);
    }
}

extern "C" DLLEXPORT void BatchGreaterThanEqualStr(uint8_t **ap, int32_t *apLen, uint8_t **bp, int32_t *bpLen,
    bool *res, int32_t rowCnt)
{
    int32_t tmp[rowCnt];
    BatchStrCompare(ap, apLen, bp, bpLen, tmp, rowCnt);
    for (int i = 0; i < rowCnt; i++) {
        res[i] = (tmp[i] >= 0);
    }
}

extern "C" DLLEXPORT void BatchEqualStr(uint8_t **ap, int32_t *apLen, uint8_t **bp, int32_t *bpLen, bool *res,
    int32_t rowCnt)
{
    int32_t tmp[rowCnt];
    BatchStrCompare(ap, apLen, bp, bpLen, tmp, rowCnt);
    for (int i = 0; i < rowCnt; i++) {
        res[i] = (tmp[i] == 0);
    }
}

extern "C" DLLEXPORT void BatchNotEqualStr(uint8_t **ap, int32_t *apLen, uint8_t **bp, int32_t *bpLen, bool *res,
    int32_t rowCnt)
{
    int32_t tmp[rowCnt];
    BatchStrCompare(ap, apLen, bp, bpLen, tmp, rowCnt);
    for (int i = 0; i < rowCnt; i++) {
        res[i] = (tmp[i] != 0);
    }
}

extern DLLEXPORT void BatchCastString(int64_t contextPtr, uint8_t **str, int32_t *strLen, int32_t *output,
    int32_t rowCnt)
{
    // Date is in the format 1996-02-28
    // Doesn't account for leap seconds or daylight savings
    // Should be ok just for dates
    std::string regexToMatch = "\\d{4}-\\d{2}-\\d{2}$";
    std::regex re = std::regex(regexToMatch);
    std::string s;
    int32_t i1 = 5;
    int32_t i2 = 8;
    int yr, mnth, day;
    int base = static_cast<int32_t>('0');
    struct std::tm epoch;
    struct std::tm t;
    std::time_t epochTime;
    std::time_t desiredTime;

    for (int i = 0; i < rowCnt; ++i) {
        s = std::string(reinterpret_cast<char *>(str[i]), strLen[i]);
        if (!regex_match(s, re)) {
            char message[] = "Only support cast date\'YYYY-MM-DD\' to integer";
            SetError(contextPtr, message, sizeof(message) / sizeof(char));
            output[i] = -1;
            continue;
        }

        yr = THOUSANDS * (str[i][THOU] - base) + HUNDREDS * (str[i][HUN] - base) + TENS * (str[i][TEN] - base) +
            (str[i][ONE] - base);
        mnth = TENS * (str[i][i1] - base) + (str[i][i1 + 1] - base); // compute month
        day = TENS * (str[i][i2] - base) + (str[i][i2 + 1] - base);  // compute day

        epoch = { 0, 0, 0, 1, 1, 70 };
        t = { 0, 0, 0, day, mnth, yr - BASE_YEAR };
        epochTime = std::mktime(&epoch);
        desiredTime = std::mktime(&t);
        output[i] = static_cast<int32_t>(std::difftime(desiredTime, epochTime) / SECOND_OF_DAY);
    }
}

extern "C" DLLEXPORT void BatchLikeStr(uint8_t **str, int32_t *strLen, uint8_t **regexToMatch, int32_t *regexLen,
    bool *isAnyNull, bool *output, int32_t rowCnt)
{
    std::string s;
    for (int32_t i = 0; i < rowCnt; i++) {
        if (isAnyNull[i]) {
            output[i] = false;
            continue;
        }
        s = std::string(reinterpret_cast<char *>(str[i]), strLen[i]);
        std::string r = std::string(reinterpret_cast<char *>(regexToMatch[i]), regexLen[i]);
        std::wregex re(StringUtil::ToWideString(r));
        output[i] = regex_match(StringUtil::ToWideString(s), re);
    }
}

extern "C" DLLEXPORT void BatchLikeChar(uint8_t **str, int32_t strWidth, int32_t *strLen, uint8_t **regexToMatch,
    int32_t *regexLen, bool *isAnyNull, bool *output, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        if (isAnyNull[i]) {
            output[i] = false;
            continue;
        }
        int32_t paddingCount =
            strWidth - omniruntime::Utf8Util::CountCodePoints(reinterpret_cast<const char *>(str[i]), strLen[i]);
        std::string originalStr;
        originalStr.reserve(strLen[i] + paddingCount);
        originalStr.append(reinterpret_cast<char *>(str[i]), strLen[i]);
        originalStr.append(paddingCount, ' ');
        std::string r = std::string(reinterpret_cast<char *>(regexToMatch[i]), regexLen[i]);
        std::wregex re(StringUtil::ToWideString(r));
        output[i] = regex_match(StringUtil::ToWideString(originalStr), re);
    }
}

extern "C" DLLEXPORT void BatchConcatStrStr(int64_t contextPtr, uint8_t **ap, int32_t *apLen, uint8_t **bp,
    int32_t *bpLen, bool *isAnyNull, uint8_t **output, int32_t *outLen, int32_t rowCnt)
{
    bool hasErr;
    for (int i = 0; i < rowCnt; i++) {
        if (isAnyNull[i]) {
            outLen[i] = 0;
            output[i] = nullptr;
            continue;
        }
        hasErr = false;
        auto ret = StringUtil::ConcatStrDiffWidths(contextPtr, reinterpret_cast<const char *>(ap[i]), apLen[i],
            reinterpret_cast<const char *>(bp[i]), bpLen[i], &hasErr, outLen + i);
        if (hasErr) {
            SetError(contextPtr, CONCAT_ERR_MSG.c_str(), CONCAT_ERR_MSG.length());
        }
        output[i] = reinterpret_cast<uint8_t *>(const_cast<char *>(ret));
    }
}

extern "C" DLLEXPORT void BatchConcatStrStrRetNull(bool *isNull, int64_t contextPtr, uint8_t **ap, int32_t *apLen,
    uint8_t **bp, int32_t *bpLen, uint8_t **output, int32_t *outLen, int32_t rowCnt)
{
    for (int32_t i = 0; i < rowCnt; i++) {
        auto ret = StringUtil::ConcatStrDiffWidths(contextPtr, reinterpret_cast<const char *>(ap[i]), apLen[i],
            reinterpret_cast<const char *>(bp[i]), bpLen[i], isNull + i, outLen + i);
        output[i] = reinterpret_cast<uint8_t *>(const_cast<char *>(ret));
    }
}

extern "C" DLLEXPORT void BatchConcatCharChar(int64_t contextPtr, uint8_t **ap, int32_t aWidth, int32_t *apLen,
    uint8_t **bp, int32_t bWidth, int32_t *bpLen, bool *isAnyNull, uint8_t **output, int32_t *outLen, int32_t rowCnt)
{
    bool hasErr;
    for (int32_t i = 0; i < rowCnt; i++) {
        if (isAnyNull[i]) {
            outLen[i] = 0;
            output[i] = nullptr;
            continue;
        }

        hasErr = false;
        auto ret = StringUtil::ConcatCharDiffWidths(contextPtr, reinterpret_cast<const char *>(ap[i]), aWidth, apLen[i],
            reinterpret_cast<const char *>(bp[i]), bpLen[i], &hasErr, outLen + i);
        if (hasErr) {
            SetError(contextPtr, CONCAT_ERR_MSG.c_str(), CONCAT_ERR_MSG.length());
        }
        output[i] = reinterpret_cast<uint8_t *>(const_cast<char *>(ret));
    }
}

extern "C" DLLEXPORT void BatchConcatCharCharRetNull(bool *isNull, int64_t contextPtr, uint8_t **ap, int32_t aWidth,
    int32_t *apLen, uint8_t **bp, int32_t bWidth, int32_t *bpLen, uint8_t **output, int32_t *outLen, int32_t rowCnt)
{
    for (int32_t i = 0; i < rowCnt; i++) {
        auto ret = StringUtil::ConcatCharDiffWidths(contextPtr, reinterpret_cast<const char *>(ap[i]), aWidth, apLen[i],
            reinterpret_cast<const char *>(bp[i]), bpLen[i], isNull + i, outLen + i);
        output[i] = reinterpret_cast<uint8_t *>(const_cast<char *>(ret));
    }
}

extern "C" DLLEXPORT void BatchConcatCharStr(int64_t contextPtr, uint8_t **ap, int32_t aWidth, int32_t *apLen,
    uint8_t **bp, int32_t *bpLen, bool *isAnyNull, uint8_t **output, int32_t *outLen, int32_t rowCnt)
{
    bool hasErr;
    for (int i = 0; i < rowCnt; i++) {
        if (isAnyNull[i]) {
            outLen[i] = 0;
            output[i] = nullptr;
            continue;
        }
        hasErr = false;
        auto ret = StringUtil::ConcatCharDiffWidths(contextPtr, reinterpret_cast<const char *>(ap[i]), aWidth, apLen[i],
            reinterpret_cast<const char *>(bp[i]), bpLen[i], &hasErr, outLen + i);
        if (hasErr) {
            SetError(contextPtr, CONCAT_ERR_MSG.c_str(), CONCAT_ERR_MSG.length());
        }
        output[i] = reinterpret_cast<uint8_t *>(const_cast<char *>(ret));
    }
}

extern "C" DLLEXPORT void BatchConcatCharStrRetNull(bool *isNull, int64_t contextPtr, uint8_t **ap, int32_t aWidth,
    int32_t *apLen, uint8_t **bp, int32_t *bpLen, uint8_t **output, int32_t *outLen, int32_t rowCnt)
{
    for (int32_t i = 0; i < rowCnt; i++) {
        auto ret = StringUtil::ConcatCharDiffWidths(contextPtr, reinterpret_cast<const char *>(ap[i]), aWidth, apLen[i],
            reinterpret_cast<const char *>(bp[i]), bpLen[i], isNull + i, outLen + i);

        output[i] = reinterpret_cast<uint8_t *>(const_cast<char *>(ret));
    }
}

extern "C" DLLEXPORT void BatchConcatStrChar(int64_t contextPtr, uint8_t **ap, int32_t *apLen, uint8_t **bp,
    int32_t bWidth, int32_t *bpLen, bool *isAnyNull, uint8_t **output, int32_t *outLen, int32_t rowCnt)
{
    bool hasErr;
    for (int32_t i = 0; i < rowCnt; i++) {
        if (isAnyNull[i]) {
            outLen[i] = 0;
            output[i] = nullptr;
            continue;
        }
        hasErr = false;
        auto ret = StringUtil::ConcatStrDiffWidths(contextPtr, reinterpret_cast<const char *>(ap[i]), apLen[i],
            reinterpret_cast<const char *>(bp[i]), bpLen[i], &hasErr, outLen + i);
        if (hasErr) {
            SetError(contextPtr, CONCAT_ERR_MSG.c_str(), CONCAT_ERR_MSG.length());
        }
        output[i] = reinterpret_cast<uint8_t *>(const_cast<char *>(ret));
    }
}

extern "C" DLLEXPORT void BatchConcatStrCharRetNull(bool *isNull, int64_t contextPtr, uint8_t **ap, int32_t *apLen,
    uint8_t **bp, int32_t bWidth, int32_t *bpLen, uint8_t **output, int32_t *outLen, int32_t rowCnt)
{
    for (int32_t i = 0; i < rowCnt; i++) {
        auto ret = StringUtil::ConcatStrDiffWidths(contextPtr, reinterpret_cast<const char *>(ap[i]), apLen[i],
            reinterpret_cast<const char *>(bp[i]), bpLen[i], isNull + i, outLen + i);
        output[i] = reinterpret_cast<uint8_t *>(const_cast<char *>(ret));
    }
}

extern "C" DLLEXPORT void BatchCastStrWithDiffWidths(int64_t contextPtr, uint8_t **str, int32_t srcWidth,
    int32_t *strLen, bool *isAnyNull, uint8_t **output, int32_t *outLen, int32_t dstWidth, int32_t rowCnt)
{
    for (int32_t i = 0; i < rowCnt; i++) {
        if (isAnyNull[i]) {
            outLen[i] = 0;
            output[i] = nullptr;
            continue;
        }
        bool hasErr = false;
        const char *ret = StringUtil::CastStrStr(&hasErr, reinterpret_cast<const char *>(str[i]), srcWidth, strLen[i],
            outLen + i, dstWidth);
        if (hasErr) {
            std::ostringstream errMsg;
            errMsg << "cast varchar[" << srcWidth << "] to varchar[" << dstWidth << "] failed.";
            SetError(contextPtr, errMsg.str().c_str(), errMsg.str().length());
        }
        output[i] = reinterpret_cast<uint8_t *>(const_cast<char *>(ret));
    }
}

extern "C" DLLEXPORT void BatchCastStrWithDiffWidthsRetNull(bool *isNull, int64_t contextPtr, uint8_t **srcStr,
    int32_t srcWidth, int32_t *strLen, uint8_t **output, int32_t *outLen, int32_t dstWidth, int32_t rowCnt)
{
    for (int32_t i = 0; i < rowCnt; ++i) {
        auto ret = StringUtil::CastStrStr(isNull + i, reinterpret_cast<const char *>(srcStr[i]), srcWidth, strLen[i],
            outLen + i, dstWidth);
        output[i] = reinterpret_cast<uint8_t *>(const_cast<char *>(ret));
    }
}

extern "C" DLLEXPORT void BatchLengthChar(uint8_t **str, const int32_t width, int32_t *strLen, bool *isAnyNull,
    int64_t *output, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; ++i) {
        output[i] = width;
    }
}

extern "C" DLLEXPORT void BatchLengthCharReturnInt32(uint8_t **str, const int32_t width, int32_t *strLen,
    bool *isAnyNull, int32_t *output, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; ++i) {
        output[i] = width;
    }
}

extern "C" DLLEXPORT void BatchLengthStr(uint8_t **str, int32_t *strLen, bool *isAnyNull, int64_t *output,
    int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; ++i) {
        if (isAnyNull[i]) {
            output[i] = 0;
            continue;
        }
        output[i] = omniruntime::Utf8Util::CountCodePoints(reinterpret_cast<const char *>(str[i]), strLen[i]);
    }
}

extern "C" DLLEXPORT void BatchLengthStrReturnInt32(uint8_t **str, int32_t *strLen, bool *isAnyNull, int32_t *output,
    int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; ++i) {
        if (isAnyNull[i]) {
            output[i] = 0;
            continue;
        }
        output[i] = omniruntime::Utf8Util::CountCodePoints(reinterpret_cast<const char *>(str[i]), strLen[i]);
    }
}

extern "C" DLLEXPORT void BatchReplaceStrStrStrWithRep(int64_t contextPtr, uint8_t **str, int32_t *strLen,
    uint8_t **searchStr, int32_t *searchLen, uint8_t **replaceStr, int32_t *replaceLen, bool *isAnyNull,
    uint8_t **output, int32_t *outLen, int32_t rowCnt)
{
    EngineType engineType = EngineUtil::GetInstance().GetEngineType();
    if (engineType == EngineType::Spark) {
        // lambda(&hasErr, i)
        ReplaceWithReplaceNotEmpty(contextPtr, str, strLen, searchStr, searchLen, replaceStr, replaceLen, isAnyNull,
            output, outLen, rowCnt, [str, strLen, outLen](bool *hasErr, int32_t i) -> uint8_t* {
                outLen[i] = strLen[i];
                return str[i];
            });
    } else {
        ReplaceWithReplaceNotEmpty(contextPtr, str, strLen, searchStr, searchLen, replaceStr, replaceLen, isAnyNull,
            output, outLen, rowCnt,
            [contextPtr, str, strLen, replaceStr, replaceLen, outLen](bool *hasErr, int32_t index) -> uint8_t* {
                auto result = StringUtil::ReplaceWithSearchEmpty(contextPtr, reinterpret_cast<const char *>(str[index]),
                    strLen[index], reinterpret_cast<const char *>(replaceStr[index]), replaceLen[index], hasErr,
                    outLen + index);
                return reinterpret_cast<uint8_t *>(const_cast<char *>(result));
            });
    }
}

extern "C" DLLEXPORT void BatchReplaceStrStrWithoutRep(int64_t contextPtr, uint8_t **str, int32_t *strLen,
    uint8_t **searchStr, int32_t *searchLen, bool *isAnyNull, uint8_t **output, int32_t *outLen, int32_t rowCnt)
{
    EngineType engineType = EngineUtil::GetInstance().GetEngineType();
    if (engineType == EngineType::Spark) {
        // lambda(&hasErr, i)
        ReplaceWithReplaceEmpty(contextPtr, str, strLen, searchStr, searchLen, isAnyNull, output, outLen, rowCnt,
            [str, strLen, outLen](bool *hasErr, int32_t index) -> uint8_t* {
                outLen[index] = strLen[index];
                return str[index];
            });
    } else {
        ReplaceWithReplaceEmpty(contextPtr, str, strLen, searchStr, searchLen, isAnyNull, output, outLen, rowCnt,
            [contextPtr, str, strLen, outLen](bool *hasErr, int32_t index) -> uint8_t* {
                auto result = StringUtil::ReplaceWithSearchEmpty(contextPtr, reinterpret_cast<const char *>(str[index]),
                    strLen[index], reinterpret_cast<const char *>(EMPTY), 0, hasErr, outLen + index);
                return reinterpret_cast<uint8_t *>(const_cast<char *>(result));
            });
    }
}