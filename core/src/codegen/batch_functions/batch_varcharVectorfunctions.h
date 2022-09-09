/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: batch varcharVector functions implementation
 */

#ifndef OMNI_RUNTIME_BATCH_VARCHARVECTORFUNCTIONS_H
#define OMNI_RUNTIME_BATCH_VARCHARVECTORFUNCTIONS_H

#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif

#include <cstdint>

extern DLLEXPORT int32_t BatchWrapVarcharVector(int64_t vectorAddr, uint8_t **data, int32_t *dataLen, int32_t rowCnt);


#endif //OMNI_RUNTIME_BATCH_VARCHARVECTORFUNCTIONS_H
