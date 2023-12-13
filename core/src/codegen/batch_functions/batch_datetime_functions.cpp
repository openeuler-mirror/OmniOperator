/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 * Description: batch date time functions implementation
 */
#include "batch_datetime_functions.h"
#include <ctime>
#include "codegen/context_helper.h"
#include "type/date32.h"

namespace omniruntime::codegen::function {
extern "C" DLLEXPORT void BatchUnixTimestampFromStr(const char **timeStrs, int32_t *timeLens, const char **fmtStrs,
    int32_t *fmtLens, bool *isAnyNull, int64_t *output, int32_t rowCnt)
{
    for (int32_t i = 0; i < rowCnt; i++) {
        if (isAnyNull[i] || fmtLens[i] == 0 || timeLens[i] == 0) {
            output[i] = 0;
            continue;
        }

        struct tm timeInfo = { 0 };
        strptime(timeStrs[i], fmtStrs[i], &timeInfo);
        time_t timeStamp = mktime(&timeInfo);
        timeStamp -= timeInfo.tm_isdst ? 3600 : 0;
        output[i] = timeStamp;
    }
}

extern "C" DLLEXPORT void BatchUnixTimestampFromDate(int32_t *dates, const char **fmtStrs, int32_t *fmtLens,
    bool *isAnyNull, int64_t *output, int32_t rowCnt)
{
    for (int32_t i = 0; i < rowCnt; i++) {
        if (isAnyNull[i]) {
            output[i] = 0;
            continue;
        }
        time_t desiredTime = dates[i] * type::Date32::SECOND_OF_DAY;
        struct tm ltm;
        ltm = *localtime(&desiredTime);
        time_t result = desiredTime - ltm.tm_gmtoff;
        output[i] = static_cast<int64_t>(result);
    }
}

extern "C" DLLEXPORT void BatchFromUnixTime(bool *outputNull, int64_t contextPtr, int64_t *timestamps,
    const char **fmtStrs, int32_t *fmtLens, char **output, int32_t *outLens, int32_t rowCnt)
{
    for (int32_t i = 0; i < rowCnt; i++) {
        time_t timeStampVal = timestamps[i];
        struct tm *ltm = localtime(&timeStampVal);
        int32_t resultLen = fmtLens[i] + 3;
        auto result = ArenaAllocatorMalloc(contextPtr, resultLen);
        auto ret = strftime(result, resultLen, fmtStrs[i], ltm);
        outputNull[i] = (ret == 0);
        output[i] = result;
        outLens[i] = ret;
    }
}

extern "C" DLLEXPORT void BatchFromUnixTimeRetNull(bool *outputNull, int64_t contextPtr, int64_t *timestamps,
    const char **fmtStrs, int32_t *fmtLens, char **output, int32_t *outLens, int32_t rowCnt)
{
    return BatchFromUnixTime(outputNull, contextPtr, timestamps, fmtStrs, fmtLens, output, outLens, rowCnt);
}
}