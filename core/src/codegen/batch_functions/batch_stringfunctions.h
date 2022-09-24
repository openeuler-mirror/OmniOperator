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
#include "codegen/string_util.h"

#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif

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

#endif // OMNI_RUNTIME_BATCH_STRINGFUNCTIONS_H
