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
#include <cassert>
#include <algorithm>
#include <regex>


// All extern functions go here temporarily
#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif

extern "C" DLLEXPORT bool StrCompareExt(int64_t ap, int32_t apLen, int64_t bp, int32_t bpLen);
extern "C" DLLEXPORT bool LikeExt(int64_t str, int32_t strLen, int64_t regexToMatch, int32_t regexLen);
extern "C" DLLEXPORT int64_t SubstrExt(int64_t str, int32_t strLen, int32_t startIdx, int32_t length, int32_t *outLen);
extern "C" DLLEXPORT int64_t SubstrWithStartExt(int64_t str, int32_t strLen, int32_t startIdx, int32_t *outLen);
extern "C" DLLEXPORT int64_t ConcatStrExt(int64_t ap, int32_t apLen, int64_t bp, int32_t bpLen, int32_t *outLen);
extern "C" DLLEXPORT int32_t CastString(int64_t str, int32_t strLen);


void FreeStrings();


#endif