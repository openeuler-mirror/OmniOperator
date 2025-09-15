/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: java udf functions.
 */
#ifndef OMNI_RUNTIME_JAVA_UDF_FUNCTIONS_H
#define OMNI_RUNTIME_JAVA_UDF_FUNCTIONS_H

#include <cstdint>
#include "type/data_type.h"
#include "util/error_code.h"

namespace omniruntime {
namespace udf {
omniruntime::op::ErrorCode InitUdf();

void ExecuteHiveUdfSingle(int64_t contextPtr, const char *udfClass, int32_t *inputTypes, int32_t retType,
    int32_t vecCount, int64_t inputValueAddr, int64_t inputNullAddr, int64_t inputLengthAddr, int64_t outputValueAddr,
    int64_t outputNullAddr, int64_t outputLengthAddr);

void ExecuteHiveUdfBatch(int64_t contextPtr, const char *udfClass, int32_t *inputTypes, int32_t retType,
    int32_t vecCount, int32_t rowCount, int64_t *inputValues, int64_t *inputNulls, int64_t *inputLengths,
    int64_t outputValueAddr, int64_t outputNullAddr, int64_t outputLengthAddr);
}
}
#endif // OMNI_RUNTIME_JAVA_UDF_FUNCTIONS_H
