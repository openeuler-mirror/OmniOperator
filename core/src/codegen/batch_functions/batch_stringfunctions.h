/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: batch string functions implementation
 */
#ifndef OMNI_RUNTIME_BATCH_STRINGFUNCTIONS_H
#define OMNI_RUNTIME_BATCH_STRINGFUNCTIONS_H

#include <iostream>
#include <string>
#include <memory>
#include <locale>
#include <codecvt>
#include <huawei_secure_c/include/securec.h>
#include "util/utf8_util.h"
#include "util/engine.h"
#include "codegen/functions/context_helper.h"

#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif

namespace omniruntime {
namespace codegen {
// Defining constant of the gap for case conversions
const std::string REPLACE_ERR_MSG = "Replace failed";
const std::string CONCAT_ERR_MSG = "Concat failed";
static constexpr uint8_t EMPTY[] = "";
}
}

extern "C" DLLEXPORT void BatchLikeStr(uint8_t **str, int32_t *strLen, uint8_t **regexToMatch, int32_t *regexLen,
    bool *isAnyNull, bool *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchLikeChar(uint8_t **str, int32_t strWidth, int32_t *strLen, uint8_t **regexToMatch,
    int32_t *regexLen, bool *isAnyNull, bool *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchConcatStrStr(int64_t contextPtr, uint8_t **ap, int32_t *apLen, uint8_t **bp,
    int32_t *bpLen, bool *isAnyNull, uint8_t **output, int32_t *outLen, int32_t rowCnt);

extern "C" DLLEXPORT void BatchConcatStrStrRetNull(bool *isNull, int64_t contextPtr, uint8_t **ap, int32_t *apLen,
    uint8_t **bp, int32_t *bpLen, uint8_t **output, int32_t *outLen, int32_t rowCnt);

extern "C" DLLEXPORT void BatchConcatCharChar(int64_t contextPtr, uint8_t **ap, int32_t aWidth, int32_t *apLen,
    uint8_t **bp, int32_t bWidth, int32_t *bpLen, bool *isAnyNull, uint8_t **output, int32_t *outLen, int32_t rowCnt);

extern "C" DLLEXPORT void BatchConcatCharCharRetNull(bool *isNull, int64_t contextPtr, uint8_t **ap, int32_t aWidth,
    int32_t *apLen, uint8_t **bp, int32_t bWidth, int32_t *bpLen, uint8_t **output, int32_t *outLen, int32_t rowCnt);

extern "C" DLLEXPORT void BatchConcatCharStr(int64_t contextPtr, uint8_t **ap, int32_t aWidth, int32_t *apLen,
    uint8_t **bp, int32_t *bpLen, bool *isAnyNull, uint8_t **output, int32_t *outLen, int32_t rowCnt);

extern "C" DLLEXPORT void BatchConcatCharStrRetNull(bool *isNull, int64_t contextPtr, uint8_t **ap, int32_t aWidth,
    int32_t *apLen, uint8_t **bp, int32_t *bpLen, uint8_t **output, int32_t *outLen, int32_t rowCnt);

extern "C" DLLEXPORT void BatchConcatStrChar(int64_t contextPtr, uint8_t **ap, int32_t *apLen, uint8_t **bp,
    int32_t bWidth, int32_t *bpLen, bool *isAnyNull, uint8_t **output, int32_t *outLen, int32_t rowCnt);

extern "C" DLLEXPORT void BatchConcatStrCharRetNull(bool *isNull, int64_t contextPtr, uint8_t **ap, int32_t *apLen,
    uint8_t **bp, int32_t bWidth, int32_t *bpLen, uint8_t **output, int32_t *outLen, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastStrWithDiffWidths(int64_t contextPtr, uint8_t **str, int32_t srcWidth,
    int32_t *strLen, bool *isAnyNull, uint8_t **output, int32_t *outLen, int32_t dstWidth, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastStrWithDiffWidthsRetNull(bool *isNull, int64_t contextPtr, uint8_t **str,
    int32_t srcWidth, int32_t *strLen, uint8_t **output, int32_t *outLen, int32_t dstWidth, int32_t rowCnt);

template <typename T>
extern DLLEXPORT void BatchSubstr(int64_t contextPtr, uint8_t **str, int32_t *strLen, T *startIdx, T *length,
    bool *isAnyNull, uint8_t **output, int32_t *outLen, int32_t rowCnt)
{
    for (int32_t i = 0; i < rowCnt; i++) {
        if (isAnyNull[i]) {
            outLen[i] = 0;
            output[i] = nullptr;
            continue;
        }

        int64_t endIdx;
        int64_t startIndex;
        int64_t startCodePoint = startIdx[i];
        int64_t lengthCodePoint = length[i];
        int32_t len = strLen[i];
        output[i] = const_cast<uint8_t *>(omniruntime::codegen::EMPTY);
        if (startCodePoint == 0 || (lengthCodePoint <= 0) || (len == 0) || startCodePoint + len < 0 ||
            startCodePoint > len) {
            outLen[i] = 0;
            continue;
        }
        const char *charStr = reinterpret_cast<const char *>(str[i]);
        if (startCodePoint > 0) {
            startIndex = omniruntime::Utf8Util::OffsetOfCodePoint(charStr, len, startCodePoint - 1);
            if (startIndex < 0) {
                // before beginning of string
                outLen[i] = 0;
                continue;
            }
            endIdx = omniruntime::Utf8Util::OffsetOfCodePoint(charStr, len, startIndex, lengthCodePoint);
            if (endIdx < 0) {
                // after end of string
                endIdx = len;
            }
        } else {
            // negative start is relative to end of string
            int32_t codePoints = omniruntime::Utf8Util::CountCodePoints(charStr, len);
            startCodePoint += codePoints;
            // before beginning of string
            if (startCodePoint < 0) {
                EngineType engineType = EngineUtil::GetInstance().GetEngineType();
                if (engineType != EngineType::Spark || startCodePoint + lengthCodePoint <= 0) {
                    outLen[i] = 0;
                    continue;
                }
                lengthCodePoint += startCodePoint;
                startCodePoint = 0;
            }
            startIndex = omniruntime::Utf8Util::OffsetOfCodePoint(charStr, len, startCodePoint);
            endIdx = startCodePoint + lengthCodePoint < codePoints ?
                omniruntime::Utf8Util::OffsetOfCodePoint(charStr, len, startIndex, lengthCodePoint) :
                len;
        }

        outLen[i] = endIdx - startIndex;
        output[i] = str[i] + startIndex;
    }
}

template <typename T>
extern DLLEXPORT void BatchSubstrChar(int64_t contextPtr, uint8_t **str, int32_t width, int32_t *strLen, T *startIdx,
    T *length, bool *isAnyNull, uint8_t **output, int32_t *outLen, int32_t rowCnt)
{
    BatchSubstr<T>(contextPtr, str, strLen, startIdx, length, isAnyNull, output, outLen, rowCnt);
}

template <typename T>
extern DLLEXPORT void BatchSubstrWithStart(int64_t contextPtr, uint8_t **str, int32_t *strLen, T *startIdx,
    bool *isAnyNull, uint8_t **output, int32_t *outLen, int32_t rowCnt)
{
    int64_t startIndex;
    for (int32_t i = 0; i < rowCnt; i++) {
        if (isAnyNull[i]) {
            outLen[i] = 0;
            output[i] = nullptr;
            continue;
        }
        int64_t startCodePoint = startIdx[i];
        int32_t len = strLen[i];
        output[i] = const_cast<uint8_t *>(omniruntime::codegen::EMPTY);
        if (startCodePoint == 0 || len == 0 || startCodePoint + len < 0 || startCodePoint > len) {
            outLen[i] = 0;
            continue;
        }

        const char *charStr = reinterpret_cast<const char *>(str[i]);
        if (startCodePoint > 0) {
            startIndex = omniruntime::Utf8Util::OffsetOfCodePoint(charStr, len, startCodePoint - 1);
            if (startIndex < 0) {
                outLen[i] = 0;
                continue;
            }
        } else {
            // negative start is relative to end of string
            int32_t codePoints = omniruntime::Utf8Util::CountCodePoints(charStr, len);
            startCodePoint += codePoints;
            if (startCodePoint < 0) {
                EngineType engineType = EngineUtil::GetInstance().GetEngineType();
                if (engineType != EngineType::Spark) {
                    outLen[i] = 0;
                    continue;
                }
                startCodePoint = 0;
            }

            startIndex = omniruntime::Utf8Util::OffsetOfCodePoint(charStr, len, startCodePoint);
        }

        outLen[i] = len - startIndex;
        output[i] = str[i] + startIndex;
    }
}

template <typename T>
extern DLLEXPORT void BatchSubstrCharWithStart(int64_t contextPtr, uint8_t **str, int32_t width, int32_t *strLen,
    T *startIdx, bool *isAnyNull, uint8_t **output, int32_t *outLen, int32_t rowCnt)
{
    BatchSubstrWithStart(contextPtr, str, strLen, startIdx, isAnyNull, output, outLen, rowCnt);
}

extern "C" DLLEXPORT void BatchLengthChar(uint8_t **str, const int32_t width, int32_t *strLen, bool *isAnyNull,
    int64_t *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchLengthCharReturnInt32(uint8_t **str, const int32_t width, int32_t *strLen,
    bool *isAnyNull, int32_t *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchLengthStr(uint8_t **str, int32_t *strLen, bool *isAnyNull, int64_t *output,
    int32_t rowCnt);

extern "C" DLLEXPORT void BatchLengthStrReturnInt32(uint8_t **str, int32_t *strLen, bool *isAnyNull, int32_t *output,
    int32_t rowCnt);

extern "C" DLLEXPORT void BatchReplaceStrStrStrWithRep(int64_t contextPtr, uint8_t **str, int32_t *strLen,
    uint8_t **searchStr, int32_t *searchLen, uint8_t **replaceStr, int32_t *replaceLen, bool *isAnyNull,
    uint8_t **output, int32_t *outLen, int32_t rowCnt);

extern "C" DLLEXPORT void BatchReplaceStrStrWithoutRep(int64_t contextPtr, uint8_t **str, int32_t *strLen,
    uint8_t **searchStr, int32_t *searchLen, bool *isAnyNull, uint8_t **output, int32_t *outLen, int32_t rowCnt);

static inline std::wstring ToWideString(std::string &s)
{
    std::wstring_convert<std::codecvt_utf8<wchar_t>> convert;
    return convert.from_bytes(s);
}

static inline const uint8_t *CastStrStr(bool *hasErr, const char *str, int32_t srcWidth, int32_t strLen,
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
    return reinterpret_cast<const uint8_t *>(str);
}

static inline const char *ConcatCharDiffWidths(int64_t contextPtr, const char *ap, int32_t aWidth, int32_t apLen,
    const char *bp, int32_t bpLen, bool *hasErr, int32_t *outLen)
{
    int32_t aPaddingCount = bpLen > 0 ? aWidth - omniruntime::Utf8Util::CountCodePoints(ap, apLen) : 0;
    *outLen = apLen + aPaddingCount + bpLen;
    if (*outLen <= 0) {
        *outLen = 0;
        return reinterpret_cast<const char *>(omniruntime::codegen::EMPTY);
    }

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
        return reinterpret_cast<const char *>(omniruntime::codegen::EMPTY);
    }
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
    const char *searchStr, int32_t searchLen, const char *replaceStr, int32_t replaceLen, bool *hasErr, int32_t *outLen)
{
    if (strLen == 0) {
        *outLen = 0;
        return reinterpret_cast<const char *>(omniruntime::codegen::EMPTY);
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

    *outLen = s.length();
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

#endif // OMNI_RUNTIME_BATCH_STRINGFUNCTIONS_H
