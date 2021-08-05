/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry function name
 */
#ifndef __EXTERNALFUNCTIONS_H__
#define __EXTERNALFUNCTIONS_H__

#include <iostream>
#include <string>
#include <cstring>


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




// List of functions
// Add the string representation of external functions here
const std::string add1_int32_str = "Add1Int32";
const std::string id_int32_str = "IdInt32";

#endif