/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry varcharVector functions
 */

#ifndef OMNI_RUNTIME_VARCHARVECTORFUNCTIONS_H
#define OMNI_RUNTIME_VARCHARVECTORFUNCTIONS_H

#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif

#include <cstdint>

extern "C" DLLEXPORT void WrapVarcharVector(int8_t *vectorPtr, int32_t index, int8_t *data, int32_t dataLen);

#endif //OMNI_RUNTIME_VARCHARVECTORFUNCTIONS_H
