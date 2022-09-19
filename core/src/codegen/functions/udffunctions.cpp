/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: udf functions.
 */
#include <mutex>
#include "udf/cplusplus/java_udf_functions.h"
#include "udffunctions.h"

namespace {
    std::once_flag init_udf_flag;
}

static void InitHiveUdf()
{
    auto ret = omniruntime::udf::InitUdf();
    if (ret != omniruntime::op::ErrorCode::SUCCESS) {
        LogError("Init UDF failed since %s.", GetErrorMessage(ret).c_str());
    }
}

extern DLLEXPORT void EvaluateHiveUdfSingle(int64_t contextPtr, const char *udfClass, int32_t *inputTypes,
    int32_t retType, int32_t vecCount, int64_t inputValue, int64_t inputNull, int64_t inputLength, int64_t outputValue,
    int64_t outputNull, int64_t outputLength)
{
    std::call_once(init_udf_flag, InitHiveUdf);
    omniruntime::udf::ExecuteHiveUdfSingle(contextPtr, udfClass, inputTypes, retType, vecCount, inputValue, inputNull,
        inputLength, outputValue, outputNull, outputLength);
}

extern DLLEXPORT void EvaluateHiveUdfBatch(int64_t contextPtr, const char *udfClass, int32_t *inputTypes,
    int32_t retType, int32_t vecCount, int32_t rowCount, int64_t *inputValues, int64_t *inputNulls,
    int64_t *inputLengths, int64_t outputValue, int64_t outputNull, int64_t outputLength)
{
    std::call_once(init_udf_flag, InitHiveUdf);
    omniruntime::udf::ExecuteHiveUdfBatch(contextPtr, udfClass, inputTypes, retType, vecCount, rowCount, inputValues,
        inputNulls, inputLengths, outputValue, outputNull, outputLength);
}
