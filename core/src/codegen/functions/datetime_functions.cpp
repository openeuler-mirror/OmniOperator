/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2024. All rights reserved.
 * Description: date time functions implementation
 */

#include "datetime_functions.h"
#include "codegen/context_helper.h"
#include "type/date32.h"
#include "codegen/time_util.h"
#include <algorithm>
#include <string>
#include <sstream>
#include <iostream>
#include "absl/time/time.h"
#include "absl/time/civil_time.h"
#include <ctime>
#include <cstdint>
#include <optional>

namespace omniruntime::codegen::function {
inline bool calPosDateValue(const char *timeStrPos, int len, int &out)
{
   int value = 0;
   for (int i = 0; i < len; ++i) {
       char timeChar = timeStrPos[i];
       if (timeChar < '0' || timeChar > '9') return false;
       value = value * 10 + (timeChar - '0');
   }
   out = value;
   return true;
}

inline int64_t toTimestamp(int year, int month, int day, int hour, int min, int sec, const absl::TimeZone &timeZone)
{
    absl::CivilSecond civilSecond(year, month, day, hour, min, sec);
    absl::Time time = absl::FromCivil(civilSecond, timeZone);
    return absl::ToUnixSeconds(time);
}

extern "C" DLLEXPORT int64_t UnixTimestampFromStr(const char *timeStr, int32_t timeLen, bool isNullTimeStr,
        const char *fmtStr, int32_t fmtLen, bool isNullFmtStr,
        const char *tzStr, int32_t tzLen, bool isNullTzStr,
        const char *policyStr, int32_t policyLen, bool isNullPolStr,
        bool *retIsNull)
{
   if (isNullTimeStr || isNullFmtStr || fmtLen == 0 || timeLen == 0) {
      *retIsNull = true;
      return 0;
   }
   if (!TimeUtil::IsTimeValid(timeStr, timeLen, fmtStr, fmtLen, policyStr)) {
      *retIsNull = true;
       return 0;
   }

   std::string timeStr1(timeStr, timeLen);
   std::string fmtStr1(fmtStr, fmtLen);
   int year = 1970, month = 1, day = 1, hour = 0, min = 0, sec = 0;
   size_t timePos = 0;
   size_t fmtPos = 0;

   while (fmtPos < fmtStr1.size() && timePos < timeStr1.size())
   {
        if (fmtStr1[fmtPos] == '%')
        {
            fmtPos++;
            int len = 0;
            int *target = nullptr;
                switch (fmtStr1[fmtPos])
                {
                    case 'Y':len = 4;target = &year;break;
                    case 'm':len = 2;target = &month;break;
                    case 'd':len = 2;target = &day;break;
                    case 'H':len = 2;target = &hour;break;case 'M':len = 2;target = &min;break;
                    case 'S':len = 2;target = &sec;break;
                    default:return 0;
                }
                if (timePos + len > timeStr1.size()) *retIsNull = true;return 0;
                if (!calPosDateValue(timeStr1.data() + timePos, len, *target)) *retIsNull = true;return 0;
                timePos += len;
                    fmtPos++;
        } else
        {
            if (timeStr1[timePos] != fmtStr1[fmtPos]) *retIsNull = true;
            return 0;
            timePos++;
            fmtPos++;
        }
   }

   if (timePos != timeStr1.size() || fmtPos != fmtStr1.size()) *retIsNull = true;return 0;
   time_t time;
   if (isNullTzStr)
   {
        std::tm tm{};
        tm.tm_year = year - 1900;
        tm.tm_mon = month - 1;
        tm.tm_mday = day;
        tm.tm_hour = hour;
        tm.tm_min = min;
        tm.tm_sec = sec;
        // if null use UTC
        time = timegm(&tm);
   } else
   {
        std::string tzStr1(tzStr, tzLen);
        if (tzStr1 == "Asia/Shanghai")
        {
            // default is UTC+8 boost
            std::tm tm{};
            tm.tm_year = year - 1900;
            tm.tm_mon = month - 1;
            tm.tm_mday = day;
            tm.tm_hour = hour - 8;
            tm.tm_min = min;
            tm.tm_sec = sec;
            // if null use UTC
            time = timegm(&tm);
        } else {
            absl::TimeZone tz;
            if (!absl::LoadTimeZone(tzStr1, &tz)) *retIsNull = true;
            return 0;
            // not null, initial calculate
            return toTimestamp(year, month, day, hour, min, sec, tz);
        }
   }
    return static_cast<int64_t>(time);
}

extern "C" DLLEXPORT int64_t UnixTimestampFromDate(int32_t date, const char *fmtStr, int32_t fmtLen,
   const char *tzStr, int32_t tzLen, const char *policyStr, int32_t policyLen, bool isNull)
{
    if (isNull) {
        return 0;
    }
    setenv("TZ", TimeZoneUtil::GetTZ(tzStr), 1);
    tzset();
    time_t desiredTime = type::SECOND_OF_DAY * date;
    struct tm ltm;
    localtime_r(&desiredTime, &ltm);
    time_t result = desiredTime - ltm.tm_gmtoff;
    result += TimeZoneUtil::AdjustDSTByUnixTimestampFromDate(tzStr, tzLen, &ltm, desiredTime) * type::SECOND_OF_HOUR;
    return static_cast<int64_t>(result);
}

extern "C" DLLEXPORT char *FromUnixTimeWithoutTz(int64_t contextPtr, int64_t timestamp, const char *fmtStr,
                                                   int32_t fmtLen, bool isNull, int32_t *outLen)
{
    if (isNull) {
        *outLen = 0;
        return nullptr;
    }

    struct tm ltm;
    int64_t adjusted_seconds = (timestamp >= 0) ? (timestamp / 1000) : ((timestamp - 999) / 1000);
    gmtime_r(&adjusted_seconds, &ltm);
    std::string fmt(fmtStr, fmtLen);
    int32_t resultLen = fmtLen + 3;
    auto result = ArenaAllocatorMalloc(contextPtr, resultLen);
    int ret = strftime(result, resultLen, fmt.c_str(), &ltm);
    *outLen = ret;
    return result;
}

extern "C" DLLEXPORT char *FromUnixTime(int64_t contextPtr, bool *isNull, int64_t timestamp, const char *fmtStr,
    int32_t fmtLen, const char *tzStr, int32_t tzLen, int32_t *outLen)
{
    time_t timeStampVal = timestamp;
    setenv("TZ", TimeZoneUtil::GetTZ(tzStr), 1);
    tzset();
    struct tm ltm;
    localtime_r(&timeStampVal, &ltm);
    if (!TimeZoneUtil::JudgeDSTByFromUnixTime(tzStr, tzLen, &ltm)) {
        timeStampVal -= type::SECOND_OF_HOUR;
        localtime_r(&timeStampVal, &ltm);
    }
    int32_t resultLen = fmtLen + 3;
    auto result = ArenaAllocatorMalloc(contextPtr, resultLen);
    std::string fmtStr1(fmtStr, fmtLen);
    std::string fmtOmniTimeStr = toOmniTimeFormat(fmtStr1);
    auto ret = strftime(result, resultLen, fmtOmniTimeStr.c_str(), &ltm);
    *isNull = static_cast<int32_t>(ret) == 0;
    *outLen = ret;
    return result;
}

std::string toOmniTimeFormat(const std::string &format)
{
    std::string result = format;
    const std::pair<std::string, std::string> replacements[] = {
        {"yyyy", "%Y"}, {"MM", "%m"}, {"dd", "%d"},
        {"HH", "%H"},   {"mm", "%M"}, {"ss", "%S"}};
    for (const auto &[from, to] : replacements) {
        size_t pos = 0;
        while ((pos = result.find(from, pos)) != std::string::npos) {
            result.replace(pos, from.length(), to);
            pos += to.length();
        }
    }
    return result;
}

extern "C" DLLEXPORT char *FromUnixTimeRetNull(int64_t contextPtr, bool *isNull, int64_t timestamp, const char *fmtStr,
    int32_t fmtLen, const char *tzStr, int32_t tzLen, int32_t *outLen)
{
    return FromUnixTime(contextPtr, isNull, timestamp, fmtStr, fmtLen, tzStr, tzLen, outLen);
}

extern "C" DLLEXPORT int32_t DateTrunc(int64_t contextPtr, int32_t days, const char *levelStr, int32_t len)
{
    type::DateTruncMode level = type::Date32::ParseTruncLevel(std::string(levelStr, len));
    int32_t result;
    if (type::Date32::TruncDate(days, level, result) != type::Status::CONVERT_SUCCESS) {
        std::ostringstream errorMessage;
        errorMessage << "The level is not supported yet: " << std::string(levelStr, len);
        SetError(contextPtr, errorMessage.str());
        return 0;
    }
    return result;
}

extern "C" DLLEXPORT int32_t DateTruncRetNull(bool *isNull, int32_t days, const char *levelStr, int32_t len)
{
    type::DateTruncMode level = type::Date32::ParseTruncLevel(std::string(levelStr, len));
    int32_t result;
    if (type::Date32::TruncDate(days, level, result) != type::Status::CONVERT_SUCCESS) {
        *isNull = true;
    }
    return result;
}

extern "C" DLLEXPORT int32_t GetHourFromTimestamp(int64_t timestamp, bool isNull)
{
    if (isNull) {
        return 0;
    }
    int64_t totalHours = timestamp / type::SECOND_OF_HOUR / 1000;
    int32_t result = totalHours % 24;

    return result;
}

extern "C" DLLEXPORT int32_t DateAdd(int32_t right, int32_t left)
{
    return right + left;
}

extern "C" DLLEXPORT char *DateFormat(int64_t contextPtr, int64_t timestamp, const char *fmtStr, int32_t fmtLen,
    bool isNull, int32_t *outLen)
{
    if (isNull) {
        *outLen = 0;
        return nullptr;
    }
    if (fmtStr == nullptr || std::memcpy(fmtStr, "yyyy-MM-dd", 10) != 0 ) {
        *outLen = 0;
        SetError(contextPtr, " Error: date_format now only support formatStr = \"yyyy-MM-dd\" ! ");
        return nullptr;
    }
    const char *tzStr = "UTC";
    time_t timeStampVal = timestamp / 1e6;
    setenv("TZ",TimeZoneUtil::GetTZ(tzStr), 1);
    tzset();
    struct tm ltm;
    localtime_r(&timeStampVal, &ltm);
    int32_t resultLen = fmtLen + 3;
    auto result = ArenaAllocatorMalloc(contextPtr, resultLen);
    std::string fmtStr1(fmtStr, fmtLen);
    std::string fmtOmniTimeStr = toOmniTimeFormat(fmtStr1);
    auto ret = strftime(result, resultLen, fmtOmniTimeStr.c_str(), &ltm);
    if (ret == 0) {
        SetError(contextPtr, " Error: date_format strftime failed ! ");
        *outLen = 0;
        return nullptr;
    }
    *outLen = ret;
    return result;
}

extern "C" DLLEXPORT int32_t DateDiff(int32_t endDate, bool endIsNull, int32_t startDate, bool startIsNull, bool *retIsNull)
{
    if (endIsNull || startIsNull) {
        *retIsNull = true;
        return 0;
    }
    return endDate - startDate;
}
}
