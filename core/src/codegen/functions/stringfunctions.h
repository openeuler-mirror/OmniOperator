/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry  function
 */
#ifndef __STRINGFUNCTIONS_H__
#define __STRINGFUNCTIONS_H__

#include <iostream>
#include <string>
#include <memory>
#include <algorithm>
#include <huawei_secure_c/include/securec.h>
#include <locale>
#include <codecvt>
#include "context_helper.h"
#include "util/utf8_util.h"
#include "util/engine.h"

// All extern functions go here temporarily
#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif

namespace omniruntime {
namespace codegen {
// Defining constant of the gap for case conversions
const int32_t STEP = static_cast<int>('a') - static_cast<int>('A');
const std::string SUBSTR_ERR_MSG = "Substring failed";
const std::string REPLACE_ERR_MSG = "Replace failed";
const std::string CONCAT_ERR_MSG = "Concat failed";
}
}

extern DLLEXPORT int32_t StrCompare(const char *ap, int32_t apLen, const char *bp, int32_t bpLen);
extern DLLEXPORT bool LikeStr(const char *str, int32_t strLen, const char *regexToMatch, int32_t regexLen);
extern DLLEXPORT bool LikeChar(const char *str, int32_t strWidth, int32_t strLen, const char *regexToMatch,
    int32_t regexLen);
extern DLLEXPORT const char *ConcatStrStr(int64_t contextPtr, const char *ap, int32_t apLen, const char *bp,
    int32_t bpLen, int32_t *outLen);
extern DLLEXPORT const char *ConcatCharChar(int64_t contextPtr, const char *ap, int32_t aWidth, int32_t apLen,
    const char *bp, int32_t bWidth, int32_t bpLen, int32_t *outLen);
extern DLLEXPORT const char *ConcatCharStr(int64_t contextPtr, const char *ap, int32_t aWidth, int32_t apLen,
    const char *bp, int32_t bpLen, int32_t *outLen);
extern DLLEXPORT const char *ConcatStrChar(int64_t contextPtr, const char *ap, int32_t apLen, const char *bp,
    int32_t bWidth, int32_t bpLen, int32_t *outLen);
extern DLLEXPORT int32_t CastString(int64_t contextPtr, const char *str, int32_t strLen);

template <typename T>
extern DLLEXPORT const char *Substr(int64_t contextPtr, const char *str, int32_t strLen, T startIdx, T length,
    int32_t *outLen)
{
    if (startIdx == 0 || (length <= 0) || (strLen == 0) || startIdx + strLen < 0 || startIdx > strLen) {
        *outLen = 0;
        return "";
    }

    int64_t endIdx;
    int64_t startIndex;
    int64_t startCodePoint = startIdx;
    int64_t lengthCodePoint = length;
    if (startCodePoint > 0) {
        startIndex = omniruntime::Utf8Util::OffsetOfCodePoint(str, strLen, startCodePoint - 1);
        if (startIndex < 0) {
            // before beginning of string
            *outLen = 0;
            return "";
        }
        endIdx = omniruntime::Utf8Util::OffsetOfCodePoint(str, strLen, startIndex, lengthCodePoint);
        if (endIdx < 0) {
            // after end of string
            endIdx = strLen;
        }
    } else {
        // negative start is relative to end of string
        int32_t codePoints = omniruntime::Utf8Util::CountCodePoints(str, strLen);
        startCodePoint += codePoints;
        // before beginning of string
        if (startCodePoint < 0) {
            EngineType engineType = EngineUtil::GetInstance().GetEngineType();
            if (engineType != EngineType::Spark || startCodePoint + lengthCodePoint <= 0) {
                *outLen = 0;
                return "";
            }
            lengthCodePoint += startCodePoint;
            startCodePoint = 0;
        }
        startIndex = omniruntime::Utf8Util::OffsetOfCodePoint(str, strLen, startCodePoint);
        if (startCodePoint + lengthCodePoint < codePoints) {
            endIdx = omniruntime::Utf8Util::OffsetOfCodePoint(str, strLen, startIndex, lengthCodePoint);
        } else {
            endIdx = strLen;
        }
    }

    *outLen = endIdx - startIndex;
    auto ret = omniruntime::codegen::ArenaAllocatorMalloc(contextPtr, *outLen);
    errno_t res = memcpy_s(ret, *outLen, str + startIndex, *outLen);
    if (res != EOK) {
        omniruntime::codegen::SetError(contextPtr, omniruntime::codegen::SUBSTR_ERR_MSG.c_str(),
                                       omniruntime::codegen::SUBSTR_ERR_MSG.length());
        return nullptr;
    }
    return ret;
}

template <typename T>
extern DLLEXPORT const char *SubstrChar(int64_t contextPtr, const char *str, int32_t width, int32_t strLen, T startIdx,
    T length, int32_t *outLen)
{
    return Substr<T>(contextPtr, str, strLen, startIdx, length, outLen);
}

template <typename T>
extern DLLEXPORT const char *SubstrWithStart(int64_t contextPtr, const char *str, int32_t strLen, T startIdx,
    int32_t *outLen)
{
    if (startIdx == 0 || strLen == 0 || startIdx + strLen < 0 || startIdx > strLen) {
        *outLen = 0;
        return "";
    }

    int64_t startCodePoint = startIdx;
    int64_t startIndex;
    if (startCodePoint > 0) {
        startIndex = omniruntime::Utf8Util::OffsetOfCodePoint(str, strLen, startCodePoint - 1);
        if (startIndex < 0) {
            *outLen = 0;
            return "";
        }
    } else {
        // negative start is relative to end of string
        int32_t codePoints = omniruntime::Utf8Util::CountCodePoints(str, strLen);
        startCodePoint += codePoints;
        if (startCodePoint < 0) {
            EngineType engineType = EngineUtil::GetInstance().GetEngineType();
            if (engineType != EngineType::Spark) {
                *outLen = 0;
                return "";
            }
            startCodePoint = 0;
        }

        startIndex = omniruntime::Utf8Util::OffsetOfCodePoint(str, strLen, startCodePoint);
    }

    *outLen = strLen - startIndex;

    auto ret = omniruntime::codegen::ArenaAllocatorMalloc(contextPtr, *outLen);
    errno_t res = memcpy_s(ret, *outLen, str + startIndex, *outLen);
    if (res != EOK) {
        omniruntime::codegen::SetError(contextPtr, omniruntime::codegen::SUBSTR_ERR_MSG.c_str(),
                                       omniruntime::codegen::SUBSTR_ERR_MSG.length());
        return nullptr;
    }
    return ret;
}

template <typename T>
extern DLLEXPORT const char *SubstrCharWithStart(int64_t contextPtr, const char *str, int32_t width, int32_t strLen,
    T startIdx, int32_t *outLen)
{
    return SubstrWithStart(contextPtr, str, strLen, startIdx, outLen);
}

extern DLLEXPORT const char *ToUpperStr(int64_t contextPtr, const char *str, int32_t strLen, int32_t *outLen);

extern DLLEXPORT const char *ToUpperChar(int64_t contextPtr, const char *str, int32_t width, int32_t strLen,
    int32_t *outLen);

extern DLLEXPORT const char *ToLowerStr(int64_t contextPtr, const char *str, int32_t strLen, int32_t *outLen);

extern DLLEXPORT const char *ToLowerChar(int64_t contextPtr, const char *str, int32_t width, int32_t strLen,
    int32_t *outLen);

extern DLLEXPORT int64_t LengthChar(const char *str, int32_t width, int32_t strLen);

extern DLLEXPORT int32_t LengthCharReturnInt32(const char *str, int32_t width, int32_t strLen);

extern DLLEXPORT int64_t LengthStr(const char *str, int32_t strLen);

extern DLLEXPORT int32_t LengthStrReturnInt32(const char *str, int32_t strLen);

extern DLLEXPORT const char *ReplaceStrStrStrWithRep(int64_t contextPtr, const char *str, int32_t strLen,
    const char *searchStr, int32_t searchLen, const char *replaceStr, int32_t replaceLen, int32_t *outLen);

extern DLLEXPORT const char *ReplaceStrStrWithoutRep(int64_t contextPtr, const char *str, int32_t strLen,
    const char *searchStr, int32_t searchLen, int32_t *outLen);

static inline std::wstring ToWideString(std::string &s)
{
    std::wstring_convert<std::codecvt_utf8<wchar_t>> convert;
    return convert.from_bytes(s);
}
#endif