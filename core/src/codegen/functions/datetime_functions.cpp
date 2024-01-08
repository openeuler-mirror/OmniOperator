/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 * Description: date time functions implementation
 */

#include "datetime_functions.h"
#include "codegen/context_helper.h"
#include "type/date32.h"

namespace omniruntime::codegen::function {
extern "C" DLLEXPORT int64_t UnixTimestampFromStr(const char *timeStr, int32_t timeLen, const char *fmtStr,
    int32_t fmtLen, bool isNull)
{
    if (isNull || fmtLen == 0 || timeLen == 0) {
        return 0;
    }

    struct tm timeInfo = { 0 };
    strptime(timeStr, fmtStr, &timeInfo);
    time_t timeStamp = mktime(&timeInfo);
    timeStamp -= timeInfo.tm_isdst ? 3600 : 0;
    return timeStamp;
}

extern "C" DLLEXPORT int64_t UnixTimestampFromDate(int32_t date, const char *fmtStr, int32_t fmtLen, bool isNull)
{
    if (isNull) {
        return 0;
    }
    time_t desiredTime = date * type::Date32::SECOND_OF_DAY;
    struct tm ltm;
    ltm = *localtime(&desiredTime);
    time_t result = desiredTime - ltm.tm_gmtoff;
    return static_cast<int64_t>(result);
}

extern "C" DLLEXPORT char *FromUnixTime(int64_t contextPtr, bool *isNull, int64_t timestamp, const char *fmtStr,
    int32_t fmtLen, int32_t *outLen)
{
    time_t timeStampVal = timestamp;
    struct tm *ltm = localtime(&timeStampVal);
    int32_t resultLen = fmtLen + 3;
    auto result = ArenaAllocatorMalloc(contextPtr, resultLen);
    auto ret = strftime(result, resultLen, fmtStr, ltm);
    *isNull = static_cast<int32_t>(ret) == 0;
    *outLen = ret;
    return result;
}

extern "C" DLLEXPORT char *FromUnixTimeRetNull(int64_t contextPtr, bool *isNull, int64_t timestamp, const char *fmtStr,
    int32_t fmtLen, int32_t *outLen)
{
    return FromUnixTime(contextPtr, isNull, timestamp, fmtStr, fmtLen, outLen);
}
}
