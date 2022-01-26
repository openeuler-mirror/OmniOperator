/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
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

// Absolute value
template<typename T>
extern DLLEXPORT T Abs(T x)
{
    return std::abs(x);
}

extern "C" DLLEXPORT double CastInt32ToDouble(int32_t x);

extern "C" DLLEXPORT double CastInt64ToDouble(int64_t x);

extern "C" DLLEXPORT long CastInt32ToInt64(int32_t x);

extern "C" DLLEXPORT int CastInt64ToInt32(int64_t x);

extern "C" DLLEXPORT int64_t CombineHash(int64_t prevHashVal, int64_t val);

extern "C" DLLEXPORT int32_t Pmod(int32_t x, int32_t y);

#endif