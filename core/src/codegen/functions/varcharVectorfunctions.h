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

extern DLLEXPORT int32_t WrapVarcharVector(int64_t vectorAddr, int32_t index, uint8_t *data, int32_t dataLen);

#endif //OMNI_RUNTIME_VARCHARVECTORFUNCTIONS_H
