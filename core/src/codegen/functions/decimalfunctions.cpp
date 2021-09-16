/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry math function name
 */


#include "decimalfunctions.h"
#include <vector>
#include <memory>

#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif
using namespace omniruntime::vec;
using namespace std;

static vector<unique_ptr<int64_t[]>> g_decimalArraysToFree;

extern "C" DLLEXPORT int32_t Decimal128CompareExt(int64_t x, int64_t y)
{
    auto *l = reinterpret_cast<int64_t*>(x);
    auto *r = reinterpret_cast<int64_t*>(y);
    Decimal128 left (*(l + 1), *l);
    Decimal128 right (*(r + 1), *r);

    if (left < right) {
        return -1;
    }
    if (left > right) {
        return 1;
    }
    return 0;
}

extern "C" DLLEXPORT int64_t AddDec128(int64_t x, int64_t y)
{
    int32_t length = 2;
    auto *left = reinterpret_cast<int64_t*>(x);
    auto *right = reinterpret_cast<int64_t*>(y);
    Decimal128 lValue(*(left + 1), *left);
    Decimal128 rValue(*(right + 1), *right);

    lValue += rValue;

    auto result = std::make_unique<int64_t[]>(length).release();

    result[0] = lValue.LowBits();
    result[1] = lValue.HighBits();

    g_decimalArraysToFree.push_back(static_cast<unique_ptr<int64_t[]>>(result));
    return reinterpret_cast<int64_t>(result);
}

extern "C" DLLEXPORT int64_t SubDec128(int64_t x, int64_t y)
{
    int32_t length = 2;
    auto *left = reinterpret_cast<int64_t*>(x);
    auto *right = reinterpret_cast<int64_t*>(y);
    Decimal128 lValue(*(left + 1), *left);
    Decimal128 rValue(*(right + 1), *right);

    lValue -= rValue;

    auto result = std::make_unique<int64_t[]>(length).release();

    result[0] = lValue.LowBits();
    result[1] = lValue.HighBits();

    g_decimalArraysToFree.push_back(static_cast<unique_ptr<int64_t[]>>(result));
    return reinterpret_cast<int64_t>(result);
}

extern "C" DLLEXPORT int64_t DivDec128(int64_t x, int64_t y)
{
    int32_t length = 2;
    auto *left = reinterpret_cast<int64_t*>(x);
    auto *right = reinterpret_cast<int64_t*>(y);
    Decimal128 lValue(*(left + 1), *left);
    Decimal128 rValue(*(right + 1), *right);

    lValue /= rValue;

    auto result = std::make_unique<int64_t[]>(length).release();

    result[0] = lValue.LowBits();
    result[1] = lValue.HighBits();

    g_decimalArraysToFree.push_back(static_cast<unique_ptr<int64_t[]>>(result));
    return reinterpret_cast<int64_t>(result);
}

extern "C" DLLEXPORT int64_t MulDec128(int64_t x, int64_t y)
{
    int32_t length = 2;
    auto *left = reinterpret_cast<int64_t*>(x);
    auto *right = reinterpret_cast<int64_t*>(y);
    Decimal128 lValue(*(left + 1), *left);
    Decimal128 rValue(*(right + 1), *right);

    lValue *= rValue;

    auto result = std::make_unique<int64_t[]>(length).release();

    result[0] = lValue.LowBits();
    result[1] = lValue.HighBits();

    g_decimalArraysToFree.push_back(static_cast<unique_ptr<int64_t[]>>(result));
    return reinterpret_cast<int64_t>(result);
}

extern "C" DLLEXPORT int64_t AbsDecimal128(int64_t x)
{
    int32_t length = 2;
    auto *valueAdd = reinterpret_cast<int64_t*>(x);
    Decimal128 value(*(valueAdd + 1), *valueAdd);

    value.Abs();

    auto result = std::make_unique<int64_t[]>(length).release();

    result[0] = value.LowBits();
    result[1] = value.HighBits();

    g_decimalArraysToFree.push_back(static_cast<unique_ptr<int64_t[]>>(result));
    return reinterpret_cast<int64_t>(result);
}


void FreeDecimalArrays()
{
    for (int i = 0; i < g_decimalArraysToFree.size(); i++) {
        g_decimalArraysToFree[i].reset();
        g_decimalArraysToFree[i] = nullptr;
        i++;
    }
    g_decimalArraysToFree.clear();
}