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