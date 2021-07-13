#include "mathfunctions.h"
#include <iostream>


#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif


// Example functions
extern "C" DLLEXPORT int32_t id_int32(int32_t x) {
    return x;
}
extern "C" DLLEXPORT int32_t add1_int32(int32_t x) {
    return x + 1;
}

// Add your functions below, following the format above
// Add any includes to necessary standard libraries in externalfunctions.h