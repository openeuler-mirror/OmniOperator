/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry dictionary functions
 */

#ifndef OMNI_RUNTIME_EXTERNALFUNCTIONS_H
#define OMNI_RUNTIME_EXTERNALFUNCTIONS_H
#include <iostream>

#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif

// Example functions
extern DLLEXPORT int32_t StringLength(char *str, int32_t length);

template <typename T> extern DLLEXPORT T Increment(T x)
{
    return x + 1;
}

#endif // OMNI_RUNTIME_EXTERNALFUNCTIONS_H
