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

namespace omniruntime::codegen::function {
extern DLLEXPORT int32_t WrapVarcharVector(int64_t vectorAddr, int32_t index, uint8_t *data, int32_t dataLen);

extern DLLEXPORT void WrapSetBitNull(int32_t *bits, int32_t index, bool isNull);

extern DLLEXPORT bool WrapIsBitNull(int32_t *bits, int32_t index);
}
#endif // OMNI_RUNTIME_VARCHARVECTORFUNCTIONS_H
