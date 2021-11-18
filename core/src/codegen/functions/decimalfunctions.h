/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry math function name
 */
#ifndef OMNI_RUNTIME_DECIMALFUNCTIONS_H
#define OMNI_RUNTIME_DECIMALFUNCTIONS_H


#include <iostream>
#include "../../vector/decimal128.h"
#include <vector>

// All extern functions go here temporarily
#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif

extern "C" DLLEXPORT int32_t Decimal128CompareExt(int64_t x, int64_t y);

extern "C" DLLEXPORT int64_t AddDec128(int64_t x, int64_t y, int64_t contextPtr);

extern "C" DLLEXPORT int64_t SubDec128(int64_t x, int64_t y, int64_t contextPtr);

extern "C" DLLEXPORT int64_t DivDec128(int64_t x, int64_t y, int64_t contextPtr);

extern "C" DLLEXPORT int64_t MulDec128(int64_t x, int64_t y, int64_t contextPtr);

extern "C" DLLEXPORT int64_t AbsDecimal128(int64_t x, int64_t contextPtr);

#endif // OMNI_RUNTIME_DECIMALFUNCTIONS_H
