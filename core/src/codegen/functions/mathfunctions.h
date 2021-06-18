#ifndef __MATHFUNCTIONS_H__
#define __MATHFUNCTIONS_H__

#include <iostream>


// All extern functions go here temporarily
#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif

extern "C" DLLEXPORT int32_t abs_int32(int32_t x);

extern "C" DLLEXPORT int64_t abs_int64(int64_t x);

extern "C" DLLEXPORT double abs_double(double x);

extern "C" DLLEXPORT double cast_int32(int32_t x);

extern "C" DLLEXPORT double cast_int64(int64_t x);

#endif