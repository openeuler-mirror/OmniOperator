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


// All extern functions go here temporarily
#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif

extern "C" DLLEXPORT bool StrCompareExt(const char *ap, int32_t apLen, const char *bp, int32_t bpLen);
extern "C" DLLEXPORT bool LikeExt(const char *str, int32_t strLen, const char *regexToMatch, int32_t regexLen);
extern "C" DLLEXPORT const char *SubstrExt(const char *str, int32_t strLen, int32_t startIdx, int32_t length,
                                           int32_t *outLen, int64_t contextPtr);
extern "C" DLLEXPORT const char *SubstrWithStartExt(const char *str, int32_t strLen, int32_t startIdx,
                                                    int32_t *outLen, int64_t contextPtr);
extern "C" DLLEXPORT const char *ConcatStrExt(const char *ap, int32_t apLen, const char *bp, int32_t bpLen,
                                              int32_t *outLen, int64_t contextPtr);
extern "C" DLLEXPORT const char *ConcatCharExt(const char *ap, int32_t width, int32_t apLen, const char *bp,
                                               int32_t bpLen, int32_t *outLen, int64_t contextPtr);
extern "C" DLLEXPORT const char *CastString(const char *str, int32_t strLen);

#endif