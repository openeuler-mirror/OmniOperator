/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry  function
 */
#ifndef __STRINGFUNCTIONS_H__
#define __STRINGFUNCTIONS_H__

#include <iostream>
#include <string>
#include <cstring>
#include <memory>
#include <vector>
#include <algorithm>
#include "context_helper.h"
#include "../../../thirdparty/huawei_secure_c/include/securec.h"


// All extern functions go here temporarily
#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif

extern DLLEXPORT int32_t StrCompare(const char *ap, int32_t apLen, const char *bp, int32_t bpLen);
extern DLLEXPORT bool Like(const char *str, int32_t strLen, const char *regexToMatch, int32_t regexLen);
extern DLLEXPORT const char *ConcatStr(int64_t contextPtr, const char *ap, int32_t apLen, const char *bp, int32_t bpLen,
                                       int32_t *outLen);
extern DLLEXPORT const char *ConcatChar(int64_t contextPtr, const char *ap, int32_t aWidth, int32_t apLen, const char *bp,
                                        int32_t bWidth, int32_t bpLen, int32_t *outLen);
extern DLLEXPORT const char *CastString(const char *str, int32_t strLen);

template<typename T>
extern DLLEXPORT const char *Substr(int64_t contextPtr, const char *str, int32_t strLen, T startIdx, T length,
                                        int32_t *outLen)
{
    if (startIdx == 0 || (length <= 0) || (strLen == 0) || startIdx + strLen < 0 || startIdx > strLen) {
        *outLen = 0;
        return "";
    }
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
    auto ret = ArenaAllocatorMalloc(contextPtr, *outLen);
    errno_t res = memcpy_s(ret, *outLen, str + startIdx, *outLen);
    if (res != EOK) {
        std::cerr << "Substring failed" << std::endl;
    }
    return ret;
}

template<typename T>
extern DLLEXPORT const char *SubstrChar(int64_t contextPtr, const char *str, int32_t width, int32_t strLen, T startIdx,
                                        T length, int32_t *outLen)
{
    return Substr<T>(contextPtr, str, strLen, startIdx, length, outLen);
}

template<typename T>
extern DLLEXPORT const char *SubstrWithStart(int64_t contextPtr, const char *str, int32_t strLen, T startIdx,
                                             int32_t *outLen)
{
    if (startIdx == 0 || strLen == 0 || startIdx + strLen < 0 || startIdx > strLen) {
        *outLen = 0;
        return "";
    }

    if (startIdx > 0) {
        startIdx -= 1;
    } else {
        // negative start is relative to end of string
        startIdx += strLen;
    }

    *outLen = strLen - startIdx;

    auto ret = ArenaAllocatorMalloc(contextPtr, *outLen);
    errno_t res = memcpy_s(ret, *outLen, str + startIdx, *outLen);
    if (res != EOK) {
        std::cerr << "Substring failed" << std::endl;
    }
    return ret;
}

template<typename T>
extern DLLEXPORT const char *SubstrCharWithStart(int64_t contextPtr, const char *str, int32_t width, int32_t strLen,
                                                T startIdx, int32_t *outLen)
{
    return SubstrWithStart(contextPtr, str, strLen, startIdx, outLen);
}

extern DLLEXPORT const char *ToUpper(int64_t contextPtr, const char *str, int32_t strLen, int32_t *outLen);

extern DLLEXPORT const char *ToUpperChar(int64_t contextPtr, const char *str, int32_t width, int32_t strLen,
                                         int32_t *outLen);

#endif