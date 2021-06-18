#include "mathfunctions.h"
#include <iostream>
<<<<<<< HEAD
=======

>>>>>>> Clean function codegen and fix memory leaks involving strings

#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif


// Absolute value
extern "C" DLLEXPORT int32_t abs_int32(int32_t x) {
    return std::abs(x);
}

extern "C" DLLEXPORT int64_t abs_int64(int64_t x) {
    return std::abs(x);
}

extern "C" DLLEXPORT double abs_double(double x) {
    return std::abs(x);
}

extern "C" DLLEXPORT double cast_int32(int32_t x) {
    return (double)(x);
}

extern "C" DLLEXPORT double cast_int64(int64_t x) {
    return (double)(x);
}