/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry function name
 */
#include "externalfunctions.h"

namespace omniruntime {
namespace codegen {
#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif


// Example functions
extern DLLEXPORT int32_t StringLength(char *str, int32_t length)
{
    return length;
}

// Add your functions below, following the format above
// Add any includes or templated functions to necessary standard libraries in externalfunctions.h
}
}