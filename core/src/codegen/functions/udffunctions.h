/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: udf functions.
 */
#ifndef OMNI_RUNTIME_UDFFUNCTIONS_H
#define OMNI_RUNTIME_UDFFUNCTIONS_H

#include <cstdint>
#include "util/error_code.h"

#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif

namespace omniruntime::codegen::function {
extern DLLEXPORT void EvaluateHiveUdfSingle(int64_t contextPtr, const char *udfClass, int32_t *inputTypes,
    int32_t retType, int32_t vecCount, int64_t inputValue, int64_t inputNull, int64_t inputLength, int64_t outputValue,
    int64_t outputNull, int64_t outputLength);

extern DLLEXPORT void EvaluateHiveUdfBatch(int64_t contextPtr, const char *udfClass, int32_t *inputTypes,
    int32_t retType, int32_t vecCount, int32_t rowCount, int64_t *inputValues, int64_t *inputNulls,
    int64_t *inputLengths, int64_t outputValue, int64_t outputNull, int64_t outputLength);
}
#endif // OMNI_RUNTIME_UDFFUNCTIONS_H
