/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#include "externalfunctions.h"


#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif

using namespace std;


// Example functions
extern "C" DLLEXPORT int32_t IdInt32(int32_t x)
{
    return x;
}


extern "C" DLLEXPORT int32_t Add1Int32(int32_t x)
{
    return x + 1;
}


extern "C" DLLEXPORT double DistanceDouble(double x1, double y1, double x2, double y2)
{
    double tmpx = x1 - x2;
    double tmpy = y1 - y2;
    return tmpx * tmpx + tmpy * tmpy;
}


// Add your functions below, following the format above
// Add any includes to necessary standard libraries in externalfunctions.h