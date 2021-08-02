/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2027. All rights reserved.
 * Description: registry math function name
 */
#ifndef __MATHFUNCTIONS_H__
#define __MATHFUNCTIONS_H__

#include <iostream>


// All extern functions go here temporarily
#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif

extern "C" DLLEXPORT int32_t AbsInt32(int32_t x);

extern "C" DLLEXPORT int64_t AbsInt64(int64_t x);

extern "C" DLLEXPORT double AbsDouble(double x);

extern "C" DLLEXPORT double CastInt32(int32_t x);

extern "C" DLLEXPORT double CastInt64(int64_t x);

extern "C" DLLEXPORT int64_t CombineHash(int64_t prevHashVal, int64_t val);

#endif