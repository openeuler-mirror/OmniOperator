/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

// After compiling to externalfunctions.so, place the .so file in the /opt/lib folder.

#ifndef __EXTSTRINGFUNCTIONS_H__
#define __EXTSTRINGFUNCTIONS_H__

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
extern "C" DLLEXPORT int32_t LengthStr(int64_t s);
extern "C" DLLEXPORT int32_t FirstCharStr(int64_t s);

#endif