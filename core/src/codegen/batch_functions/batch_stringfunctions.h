/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: batch string functions implementation
 */


#ifndef OMNI_RUNTIME_BATCH_STRINGFUNCTIONS_H
#define OMNI_RUNTIME_BATCH_STRINGFUNCTIONS_H

#include <iostream>
#include <string>
#include <memory>
#include <algorithm>
#include <huawei_secure_c/include/securec.h>
#include <locale>
#include <codecvt>
#include "util/utf8_util.h"
#include "util/engine.h"
#include "type/decimal128.h"
#include "../functions/context_helper.h"

#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif

using namespace omniruntime::type;

namespace omniruntime {
namespace codegen {
// Defining constant of the gap for case conversions
const int32_t STEP = static_cast<int>('a') - static_cast<int>('A');
const std::string SUBSTR_ERR_MSG = "Substring failed";
const std::string REPLACE_ERR_MSG = "Replace failed";
const std::string CONCAT_ERR_MSG = "Concat failed";
constexpr uint8_t EMPTY[] = "";
}
}

extern "C" DLLEXPORT void BatchLikeStr(uint8_t **str, int32_t *strLen, uint8_t **regexToMatch, int32_t *regexLen,
    bool *isAnyNull, bool *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchLikeChar(uint8_t **str, int32_t strWidth, int32_t *strLen, uint8_t **regexToMatch,
    int32_t *regexLen, bool *isAnyNull, bool *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchConcatStrStr(int64_t contextPtr, uint8_t **ap, int32_t *apLen, uint8_t **bp,
    int32_t *bpLen, bool *isAnyNull, uint8_t **output, int32_t *outLen, int32_t rowCnt);

extern "C" DLLEXPORT void BatchConcatCharChar(int64_t contextPtr, uint8_t **ap, int32_t aWidth, int32_t *apLen,
    uint8_t **bp, int32_t bWidth, int32_t *bpLen, bool *isAnyNull, uint8_t **output, int32_t *outLen, int32_t rowCnt);

extern "C" DLLEXPORT void BatchConcatCharStr(int64_t contextPtr, uint8_t **ap, int32_t aWidth, int32_t *apLen,
    uint8_t **bp, int32_t *bpLen, bool *isAnyNull, uint8_t **output, int32_t *outLen, int32_t rowCnt);

extern "C" DLLEXPORT void BatchConcatStrChar(int64_t contextPtr, uint8_t **ap, int32_t *apLen, uint8_t **bp,
    int32_t bWidth, int32_t *bpLen, bool *isAnyNull, uint8_t **output, int32_t *outLen, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastStrWithDiffWidths(int64_t contextPtr, uint8_t **str, int32_t srcWidth,
    int32_t *strLen, bool *isAnyNull, uint8_t **output, int32_t *outLen, int32_t dstWidth, int32_t rowCnt);

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
        if (startIdx[i] == 0 || (length[i] <= 0) || (strLen[i] == 0) || startIdx[i] + strLen[i] < 0 ||
            startIdx[i] > strLen[i]) {
            outLen[i] = 0;
            output[i] = const_cast<uint8_t *>(omniruntime::codegen::EMPTY);
            continue;
        }

        int64_t endIdx;
        int64_t startIndex;
        int64_t startCodePoint = startIdx[i];
        int64_t lengthCodePoint = length[i];
        int32_t len = strLen[i];
        if (startCodePoint == 0 || (lengthCodePoint <= 0) || (len == 0) || startCodePoint + len < 0 ||
            startCodePoint > len) {
            outLen[i] = 0;
            output[i] = const_cast<uint8_t *>(omniruntime::codegen::EMPTY);
            continue;
        }
        const char *charStr = reinterpret_cast<const char *>(str[i]);
        if (startCodePoint > 0) {
            startIndex = omniruntime::Utf8Util::OffsetOfCodePoint(charStr, len, startCodePoint - 1);
            if (startIndex < 0) {
                // before beginning of string
                outLen[i] = 0;
                output[i] = const_cast<uint8_t *>(omniruntime::codegen::EMPTY);
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
                    output[i] = const_cast<uint8_t *>(omniruntime::codegen::EMPTY);
                    continue;
                }
                lengthCodePoint += startCodePoint;
                startCodePoint = 0;
            }
            startIndex = omniruntime::Utf8Util::OffsetOfCodePoint(charStr, len, startCodePoint);
            if (startCodePoint + lengthCodePoint < codePoints) {
                endIdx = omniruntime::Utf8Util::OffsetOfCodePoint(charStr, len, startIndex, lengthCodePoint);
            } else {
                endIdx = len;
            }
        }

        outLen[i] = endIdx - startIndex;
        char *ret = omniruntime::codegen::ArenaAllocatorMalloc(contextPtr, outLen[i]);
        errno_t res = memcpy_s(ret, outLen[i], str[i] + startIndex, outLen[i]);
        if (res != EOK) {
            outLen[i] = 0;
            omniruntime::codegen::SetError(contextPtr, omniruntime::codegen::SUBSTR_ERR_MSG.c_str(),
                omniruntime::codegen::SUBSTR_ERR_MSG.length());
            output[i] = nullptr;
            continue;
        }
        output[i] = reinterpret_cast<uint8_t *>(ret);
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
    char *ret;
    errno_t res;
    int64_t startIndex;
    for (int32_t i = 0; i < rowCnt; i++) {
        if (isAnyNull[i]) {
            outLen[i] = 0;
            output[i] = nullptr;
            continue;
        }
        int64_t startCodePoint = startIdx[i];
        int32_t len = strLen[i];
        if (startCodePoint == 0 || len == 0 || startCodePoint + len < 0 || startCodePoint > len) {
            outLen[i] = 0;
            output[i] = const_cast<uint8_t *>(omniruntime::codegen::EMPTY);
            continue;
        }

        const char *charStr = reinterpret_cast<const char *>(str[i]);
        if (startCodePoint > 0) {
            startIndex = omniruntime::Utf8Util::OffsetOfCodePoint(charStr, len, startCodePoint - 1);
            if (startIndex < 0) {
                outLen[i] = 0;
                output[i] = const_cast<uint8_t *>(omniruntime::codegen::EMPTY);
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
                    output[i] = const_cast<uint8_t *>(omniruntime::codegen::EMPTY);
                    continue;
                }
                startCodePoint = 0;
            }

            startIndex = omniruntime::Utf8Util::OffsetOfCodePoint(charStr, len, startCodePoint);
        }

        outLen[i] = len - startIndex;

        ret = omniruntime::codegen::ArenaAllocatorMalloc(contextPtr, outLen[i]);
        res = memcpy_s(ret, outLen[i], str[i] + startIndex, outLen[i]);
        if (res != EOK) {
            omniruntime::codegen::SetError(contextPtr, omniruntime::codegen::SUBSTR_ERR_MSG.c_str(),
                omniruntime::codegen::SUBSTR_ERR_MSG.length());
            outLen[i] = 0;
            output[i] = nullptr;
            continue;
        }
        output[i] = reinterpret_cast<uint8_t *>(ret);
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

extern "C" DLLEXPORT void BatchLengthStrReturnInt32(uint8_t **str, int32_t *strLen, bool *isAnyNull, int32_t *output,
    int32_t rowCnt);

extern "C" DLLEXPORT void BatchLengthStr(uint8_t **str, int32_t *strLen, bool *isAnyNull, int64_t *output,
    int32_t rowCnt);

extern "C" DLLEXPORT void BatchReplaceStrStrStrWithRep(int64_t contextPtr, uint8_t **str, int32_t *strLen,
    uint8_t **searchStr, int32_t *searchLen, uint8_t **replaceStr, int32_t *replaceLen, bool *isAnyNull,
    uint8_t **output, int32_t *outLen, int32_t rowCnt);

extern "C" DLLEXPORT void BatchReplaceStrStrWithoutRep(int64_t contextPtr, uint8_t **str, int32_t *strLen,
    uint8_t **searchStr, int32_t *searchLen, bool *isAnyNull, uint8_t **output, int32_t *outLen, int32_t rowCnt);

extern "C" DLLEXPORT void BatchConcatStrStrRetNull(bool *isNull, int64_t contextPtr, uint8_t **ap, int32_t *apLen,
    uint8_t **bp, int32_t *bpLen, uint8_t **output, int32_t *outLen, int32_t rowCnt);

extern "C" DLLEXPORT void BatchConcatCharCharRetNull(bool *isNull, int64_t contextPtr, uint8_t **ap, int32_t aWidth,
    int32_t *apLen, uint8_t **bp, int32_t bWidth, int32_t *bpLen, uint8_t **output, int32_t *outLen, int32_t rowCnt);

extern "C" DLLEXPORT void BatchConcatCharStrRetNull(bool *isNull, int64_t contextPtr, uint8_t **ap, int32_t aWidth,
    int32_t *apLen, uint8_t **bp, int32_t *bpLen, uint8_t **output, int32_t *outLen, int32_t rowCnt);

extern "C" DLLEXPORT void BatchConcatStrCharRetNull(bool *isNull, int64_t contextPtr, uint8_t **ap, int32_t *apLen,
    uint8_t **bp, int32_t bWidth, int32_t *bpLen, uint8_t **output, int32_t *outLen, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastStrWithDiffWidthsRetNull(bool *isNull, int64_t contextPtr, uint8_t **str,
    int32_t srcWidth, int32_t *strLen, uint8_t **output, int32_t *outLen, int32_t dstWidth, int32_t rowCnt);

static inline std::wstring ToWideString(std::string &s)
{
    std::wstring_convert<std::codecvt_utf8<wchar_t>> convert;
    return convert.from_bytes(s);
}

static inline const uint8_t *CastStrStr(bool *isNull, const char *str, int32_t srcWidth, int32_t strLen,
                                     int32_t *outLen, int32_t dstWidth)
{
    int32_t chCount = std::min(srcWidth, dstWidth);
    int32_t dstLen = 0;
    int32_t count = 0;
    while (dstLen < strLen && count < chCount) {
        int32_t charLen = omniruntime::Utf8Util::LengthOfCodePoint(str[dstLen]);
        if (charLen < 0) {
                *isNull = true;
                *outLen = 0;
                return nullptr;
        }
        dstLen += charLen;
        count++;
    }
    *outLen = dstLen;
    return reinterpret_cast<const uint8_t *>(str);
}

#endif // OMNI_RUNTIME_BATCH_STRINGFUNCTIONS_H
