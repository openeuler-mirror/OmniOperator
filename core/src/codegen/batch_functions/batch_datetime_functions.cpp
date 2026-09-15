/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2024. All rights reserved.
 * Description: batch date time functions implementation
 */
#include "batch_datetime_functions.h"
#include "codegen/functions/datetime_functions.h"
#include <ctime>
#include "codegen/context_helper.h"
#include "type/date32.h"
#include "codegen/time_util.h"

namespace omniruntime::codegen::function {
extern "C" DLLEXPORT void BatchUnixTimestampFromStr(const char **timeStrs, int32_t *timeLens, bool *isNullTimeStr,
    const char **fmtStrs, int32_t *fmtLens, bool *isNullFmtStr, const char **tzStrs, int32_t *tzLens,
    bool *isNullTzStr, const char **policyStrs, int32_t *policyLens, bool *isNullPolStr,
    bool *retIsNull, int64_t *output, int32_t rowCnt)
{
    std::string tzStr(tzStrs[0], tzLens[0]);
    setenv("TZ", TimeZoneUtil::GetTZ(tzStr.c_str()), 1);
    tzset();
    for (int32_t i = 0; i < rowCnt; i++) {
        if (isNullTimeStr[i] || isNullFmtStr[i] || fmtLens[i] == 0 || timeLens[i] == 0) {
            retIsNull[i] = true;
            output[i] = 0;
            continue;
        }
        if (!TimeUtil::IsTimeValid(timeStrs[i], timeLens[i], fmtStrs[i], fmtLens[i], policyStrs[i])) {
            retIsNull[i] = true;
            output[i] = 0;
            continue;
        }
        struct tm timeInfo = { 0 };
        std::string timeStr(timeStrs[i], timeLens[i]);
        std::string fmtStr(fmtStrs[i], fmtLens[i]);
        strptime(timeStr.c_str(), fmtStr.c_str(), &timeInfo);
        time_t timeStamp = mktime(&timeInfo);
        if (TimeZoneUtil::JudgeDSTByUnixTimestampFromStr(tzStrs[i], tzLens[i], &timeInfo,
                                                        timeStrs[i], timeLens[i], fmtStrs[i], fmtLens[i])) {
            timeStamp -= type::SECOND_OF_HOUR;
        }
        output[i] = timeStamp;
    }
}

extern "C" DLLEXPORT void BatchUnixTimestampFromDate(int32_t *dates, const char **fmtStrs, int32_t *fmtLens,
    const char **tzStrs, int32_t *tzLens, const char **policyStrs, int32_t *policyLens,
    bool *isAnyNull, int64_t *output, int32_t rowCnt)
{
    std::string tzStr(tzStrs[0], tzLens[0]);
    setenv("TZ", TimeZoneUtil::GetTZ(tzStr.c_str()), 1);
    tzset();
    for (int32_t i = 0; i < rowCnt; i++) {
        if (isAnyNull[i]) {
            output[i] = 0;
            continue;
        }
        time_t desiredTime = type::SECOND_OF_DAY * dates[i];
        struct tm ltm;
        localtime_r(&desiredTime, &ltm);
        time_t result = desiredTime - ltm.tm_gmtoff;
        result += TimeZoneUtil::AdjustDSTByUnixTimestampFromDate(tzStrs[i], tzLens[i], &ltm, desiredTime) * 3600;
        output[i] = static_cast<int64_t>(result);
    }
}

extern "C" DLLEXPORT void BatchFromUnixTime(bool *outputNull, int64_t contextPtr, int64_t *timestamps,
    const char **fmtStrs, int32_t *fmtLens, const char **tzStrs, int32_t *tzLens,
    char **output, int32_t *outLens, int32_t rowCnt)
{
    std::string tzStr(tzStrs[0], tzLens[0]);
    setenv("TZ", TimeZoneUtil::GetTZ(tzStr.c_str()), 1);
    tzset();
    for (int32_t i = 0; i < rowCnt; i++) {
        time_t timeStampVal = timestamps[i];
        struct tm ltm;
        localtime_r(&timeStampVal, &ltm);
        if (!TimeZoneUtil::JudgeDSTByFromUnixTime(tzStrs[i], tzLens[i], &ltm)) {
            timeStampVal -= type::SECOND_OF_HOUR;
            localtime_r(&timeStampVal, &ltm);
        }
        int32_t resultLen = fmtLens[i] + 3;
        auto result = ArenaAllocatorMalloc(contextPtr, resultLen);
        std::string fmtStr(fmtStrs[i], fmtLens[i]);
        auto ret = strftime(result, resultLen, fmtStr.c_str(), &ltm);
        outputNull[i] = (ret == 0);
        output[i] = result;
        outLens[i] = ret;
    }
}

extern "C" DLLEXPORT void BatchFromUnixTimeRetNull(bool *outputNull, int64_t contextPtr, int64_t *timestamps,
    const char **fmtStrs, int32_t *fmtLens, const char **tzStrs, int32_t *tzLen,
    char **output, int32_t *outLens, int32_t rowCnt)
{
    BatchFromUnixTime(outputNull, contextPtr, timestamps, fmtStrs, fmtLens, tzStrs, tzLen, output, outLens, rowCnt);
}

extern "C" DLLEXPORT void BatchFromUnixTimeWithoutTz(int64_t contextPtr, int64_t *timestamps,
    const char **fmtStrs, int32_t *fmtLens, bool *resIsNull, char **output, int32_t *outLens, int32_t rowCnt)
{
    for (int32_t i = 0; i < rowCnt; ++i) {
        output[i] = FromUnixTimeWithoutTz(contextPtr, timestamps[i], fmtStrs[i], fmtLens[i],
            resIsNull[i], &outLens[i]);
    }
}

extern "C" DLLEXPORT void BatchDateTimePlusYearMonthDate(int32_t *dates, bool *isNullDate,
    int32_t *months, bool *isNullMonths, bool *retIsNull, int32_t *output, int32_t rowCnt)
{
    for (int32_t i = 0; i < rowCnt; ++i) {
        output[i] = DateTimePlusYearMonthDate(
            dates[i], isNullDate[i], months[i], isNullMonths[i], &retIsNull[i]);
    }
}

extern "C" DLLEXPORT void BatchDateTimePlusYearMonthTimestamp(int64_t *timestamps, bool *isNullTimestamp,
    int32_t *months, bool *isNullMonths, bool *retIsNull, int64_t *output, int32_t rowCnt)
{
    for (int32_t i = 0; i < rowCnt; ++i) {
        output[i] = DateTimePlusYearMonthTimestamp(
            timestamps[i], isNullTimestamp[i], months[i], isNullMonths[i], &retIsNull[i]);
    }
}

extern "C" DLLEXPORT void BatchTimePlusYearMonth(int64_t *times, bool *isNullTime,
    int32_t *months, bool *isNullMonths, bool *retIsNull, int64_t *output, int32_t rowCnt)
{
    for (int32_t i = 0; i < rowCnt; ++i) {
        output[i] = TimePlusYearMonth(
            times[i], isNullTime[i], months[i], isNullMonths[i], &retIsNull[i]);
    }
}

extern "C" DLLEXPORT void BatchDateTimePlusDayTimeDate(int32_t *dates, bool *isNullDate,
    int64_t *intervalMillis, bool *isNullInterval, bool *retIsNull, int32_t *output, int32_t rowCnt)
{
    for (int32_t i = 0; i < rowCnt; ++i) {
        output[i] = DateTimePlusDayTimeDate(
            dates[i], isNullDate[i], intervalMillis[i], isNullInterval[i], &retIsNull[i]);
    }
}

extern "C" DLLEXPORT void BatchDateTimePlusDayTimeDateTimestamp(int32_t *dates, bool *isNullDate,
    int64_t *intervalMillis, bool *isNullInterval, bool *retIsNull, int64_t *output, int32_t rowCnt)
{
    for (int32_t i = 0; i < rowCnt; ++i) {
        output[i] = DateTimePlusDayTimeDateTimestamp(
            dates[i], isNullDate[i], intervalMillis[i], isNullInterval[i], &retIsNull[i]);
    }
}

extern "C" DLLEXPORT void BatchDateTimePlusDayTimeTimestamp(int64_t *timestamps, bool *isNullTimestamp,
    int64_t *intervalMillis, bool *isNullInterval, bool *retIsNull, int64_t *output, int32_t rowCnt)
{
    for (int32_t i = 0; i < rowCnt; ++i) {
        output[i] = DateTimePlusDayTimeTimestamp(
            timestamps[i], isNullTimestamp[i], intervalMillis[i], isNullInterval[i], &retIsNull[i]);
    }
}
}
