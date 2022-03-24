/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry function name
 */
#ifndef __EXTERNALFUNCTIONS_H__
#define __EXTERNALFUNCTIONS_H__

#include <iostream>
#include <string>


#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif


// Put all declarations for external functions here
// If function types need to be separated, create new .h and .cpp files with the declarations and C++ implementations
// Make sure that they are included in external_func_registry.h

// Example functions
extern "C" DLLEXPORT int32_t IdInt32(int32_t x);
extern "C" DLLEXPORT int32_t Add1Int32(int32_t x);
extern "C" DLLEXPORT double DistanceDouble(double x1, double y1, double x2, double y2);


#endif