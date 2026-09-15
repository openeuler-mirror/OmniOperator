/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 * Description: batch date time functions implementation
 */

#ifndef OMNI_RUNTIME_BATCH_DATETIME_FUNCTIONS_H
#define OMNI_RUNTIME_BATCH_DATETIME_FUNCTIONS_H
#include <cstdint>

// All extern functions go here temporarily
#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif

namespace omniruntime::codegen::function {
extern "C" DLLEXPORT void BatchUnixTimestampFromStr(const char **timeStrs, int32_t *timeLens, bool *isNullTimeStr,
    const char **fmtStrs, int32_t *fmtLens, bool *isNullFmtStr, const char **tzStrs, int32_t *tzLens, bool *isNullTzStr,
    const char **policyStrs, int32_t *policyLens, bool *isNullPolStr, bool *retIsNull, int64_t *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchUnixTimestampFromDate(int32_t *dates, const char **fmtStrs, int32_t *fmtLens,
    const char **tzStrs, int32_t *tzLens, const char **policyStrs, int32_t *policyLens,
    bool *isAnyNull, int64_t *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchFromUnixTime(bool *outputNull, int64_t contextPtr, int64_t *timestamps,
    const char **fmtStrs, int32_t *fmtLens, const char **tzStrs, int32_t *tzLens,
    char **output, int32_t *outLens, int32_t rowCnt);

extern "C" DLLEXPORT void BatchFromUnixTimeRetNull(bool *outputNull, int64_t contextPtr, int64_t *timestamps,
    const char **fmtStrs, int32_t *fmtLens, const char **tzStrs, int32_t *tzLens,
    char **output, int32_t *outLens, int32_t rowCnt);

extern "C" DLLEXPORT void BatchDateTimePlusYearMonthDate(int32_t *dates, bool *isNullDate,
    int32_t *months, bool *isNullMonths, bool *retIsNull, int32_t *output, int32_t rowCnt);
extern "C" DLLEXPORT void BatchDateTimePlusYearMonthTimestamp(int64_t *timestamps, bool *isNullTimestamp,
    int32_t *months, bool *isNullMonths, bool *retIsNull, int64_t *output, int32_t rowCnt);
extern "C" DLLEXPORT void BatchTimePlusYearMonth(int64_t *times, bool *isNullTime,
    int32_t *months, bool *isNullMonths, bool *retIsNull, int64_t *output, int32_t rowCnt);
extern "C" DLLEXPORT void BatchDateTimePlusDayTimeDate(int32_t *dates, bool *isNullDate,
    int64_t *intervalMillis, bool *isNullInterval, bool *retIsNull, int32_t *output, int32_t rowCnt);
extern "C" DLLEXPORT void BatchDateTimePlusDayTimeDateTimestamp(int32_t *dates, bool *isNullDate,
    int64_t *intervalMillis, bool *isNullInterval, bool *retIsNull, int64_t *output, int32_t rowCnt);
extern "C" DLLEXPORT void BatchDateTimePlusDayTimeTimestamp(int64_t *timestamps, bool *isNullTimestamp,
    int64_t *intervalMillis, bool *isNullInterval, bool *retIsNull, int64_t *output, int32_t rowCnt);
}
#endif // OMNI_RUNTIME_BATCH_DATETIME_FUNCTIONS_H
