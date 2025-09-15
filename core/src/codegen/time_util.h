/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
 * Description: timezone util
 */

#ifndef OMNI_RUNTIME_TIMEZONE_UTIL_H
#define OMNI_RUNTIME_TIMEZONE_UTIL_H

#include <set>
#include <string>
#include <functional>
#include <map>
#include <stdlib.h>
#include "type/date_time_utils.h"
#include "type/date32.h"

namespace omniruntime::codegen::function {
static const int YEAR_LENGTH = 4;
static const int MONTH_LENGTH = 2;
static const int DAY_LENGTH = 2;
static const int HOUR_LENGTH = 2;
static const int MINUTE_LENGTH = 2;
static const int SECOND_LENGTH = 2;
// for example: "2020-12-12 00:00:00"
static const int TIME_LENGTH = 19;
// for example: "2020-12-12"
static const int DATE_LENGTH = 10;
// for example: "%Y-%m-%d %H:%M:%S"
static const int TIME_FORMAT_LENGTH = 17;
// for example: "%Y-%m-%d"
static const int DATE_FORMAT_LENGTH = 8;
static const int MIN_YEAR = 0;
static const int MAX_YEAR = 9999;
static const int MIN_MONTH = 1;
static const int MAX_MONTH = 12;
static const int MIN_DAY = 1;
static const int DAYS_PER_MONTH[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
static const int FEBRUARY_DAY_IN_LEAP_YEAR = 29;
static const int FEBRUARY = 2;
static const int MIN_HOUR = 0;
static const int MAX_HOUR = 23;
static const int MIN_MINUTE = 0;
static const int MAX_MINUTE = 59;
static const int MIN_SECOND = 0;
static const int MAX_SECOND = 59;
static const int GREGORIAN_CALENDAR_START_YEAR = 1582;

static const std::set<time_t> UNIX_TIMESTAMP_FROM_DATE_SHANGHAI_NON_DST_SET = {
    -18526 * type::SECOND_OF_DAY, -10806 * type::SECOND_OF_DAY,
    -10519 * type::SECOND_OF_DAY, -10197 * type::SECOND_OF_DAY,
    -8632 * type::SECOND_OF_DAY, -8297 * type::SECOND_OF_DAY,
    -7915 * type::SECOND_OF_DAY, -7550 * type::SECOND_OF_DAY,
    5967 * type::SECOND_OF_DAY, 6310 * type::SECOND_OF_DAY,
    6681 * type::SECOND_OF_DAY, 7045 * type::SECOND_OF_DAY,
    7409 * type::SECOND_OF_DAY, 7773 * type::SECOND_OF_DAY
};
static const std::set<time_t> UNIX_TIMESTAMP_FROM_DATE_SHANGHAI_DST_SET = {
    6100 * type::SECOND_OF_DAY, 6464 * type::SECOND_OF_DAY,
    6828 * type::SECOND_OF_DAY, 7199 * type::SECOND_OF_DAY,
    7563 * type::SECOND_OF_DAY, 7927 * type::SECOND_OF_DAY
};
static const std::set<std::string> UNIX_TIMESTAMP_FROM_STR_SHANGHAI_NON_DST_SET = {
    "1919-04-13", "1940-06-01", "1941-03-15", "1942-01-31", "1946-05-15", "1947-04-15", "1948-05-01", "1949-05-01"
};

using JudgeDSTActionByUnixTimestampFromDate = std::function<short(const struct tm *, time_t)>;
static std::map<std::string, JudgeDSTActionByUnixTimestampFromDate> AdjustDSTByUnixTimestampFromDateMap = {
    {"GMT+08:00", [](const struct tm *timeInfo, time_t desiredTime) {return timeInfo->tm_isdst;}},
    {"Asia/Shanghai", [](const struct tm *timeInfo, time_t desiredTime) {
            short flag = 0;
            flag = UNIX_TIMESTAMP_FROM_DATE_SHANGHAI_NON_DST_SET.find(desiredTime) !=
                    UNIX_TIMESTAMP_FROM_DATE_SHANGHAI_NON_DST_SET.end() ? 1 : flag;
            flag = UNIX_TIMESTAMP_FROM_DATE_SHANGHAI_DST_SET.find(desiredTime) !=
                    UNIX_TIMESTAMP_FROM_DATE_SHANGHAI_DST_SET.end() ? -1 : flag;
            return flag;
        }
    }
};

using JudgeDSTActionByUnixTimestampFromStr =
        std::function<bool(const struct tm *, const char *, int32_t, const char *, int32_t)>;
static std::map<std::string, JudgeDSTActionByUnixTimestampFromStr> JudgeDSTByUnixTimestampFromStrMap = {
    {"GMT+08:00", [](const struct tm *timeInfo, const char *timeStr, int32_t timeLen,
            const char *fmtStr, int32_t fmtLen) {return false;}},
    {"Asia/Shanghai", [](const struct tm *timeInfo, const char *timeStr, int32_t timeLen,
            const char *fmtStr, int32_t fmtLen) {
            bool flag = false;
            flag = timeInfo->tm_isdst > 0;
            std::string substr(timeStr, timeLen);
            flag = UNIX_TIMESTAMP_FROM_STR_SHANGHAI_NON_DST_SET.find(substr) ==
                    UNIX_TIMESTAMP_FROM_STR_SHANGHAI_NON_DST_SET.end() && flag;
            return flag;
        }
    }
};

using JudgeDSTActionByFromUnixTime = std::function<bool(const struct tm *)>;
static std::map<std::string, JudgeDSTActionByFromUnixTime> JudgeDSTByFromUnixTimeMap = {
    {"GMT+08:00", [](const struct tm *timeInfo) {return timeInfo->tm_isdst > 0 ? false : true;}},
    {"Asia/Shanghai", [](const struct tm *timeInfo) {return true;}}
};

class TimeZoneUtil {
public:
    static inline short AdjustDSTByUnixTimestampFromDate(const char *tzStr,
        int32_t tzLen, const struct tm *timeInfo, time_t desiredTime)
    {
        std::string timeZoneStr(tzStr, tzLen);
        auto it = AdjustDSTByUnixTimestampFromDateMap.find(timeZoneStr);
        return it ->second(timeInfo, desiredTime);
    }

    static inline bool JudgeDSTByUnixTimestampFromStr(const char *tzStr, int32_t tzLen, const struct tm *timeInfo,
        const char *timeStr, int32_t timeLen, const char *fmtStr, int32_t fmtLen)
    {
        std::string timeZoneStr(tzStr, tzLen);
        auto it = JudgeDSTByUnixTimestampFromStrMap.find(timeZoneStr);
        return it->second(timeInfo, timeStr, timeLen, fmtStr, fmtLen);
    }

    static inline bool JudgeDSTByFromUnixTime(const char *tzStr, int32_t tzLen, const struct tm * timeInfo)
    {
        std::string timeZoneStr(tzStr, tzLen);
        auto it = JudgeDSTByFromUnixTimeMap.find(timeZoneStr);
        return it->second(timeInfo);
    }

    static inline const char* GetTZ(const char *tzStr)
    {
        if (strcmp(tzStr, "GMT+08:00") == 0) {
            return "Etc/GMT-8";
        } else {
            return tzStr;
        }
    }
}; // class TimeZoneUtil
class TimeUtil {
public:
    // Verify that the format is %Y-%m-%d %H:%M:%S and %Y-%m-%d in the blue zone.
    static bool IsTimeValid(const char *timeStr, int timeLen, const char *fmtStr, int fmtLen, const char *policyStr)
    {
        if ((timeLen != TIME_LENGTH || fmtLen != TIME_FORMAT_LENGTH) &&
                (timeLen != DATE_LENGTH || fmtLen != DATE_FORMAT_LENGTH)) {
            return false;
        }
        int year = 0;
        int month = 0;
        int day = 0;
        int offset = 0;
        bool retYear = CheckAndGetNonNegativeInteger(timeStr, timeLen, offset, YEAR_LENGTH, year);
        if (!retYear || year < MIN_YEAR || year > MAX_YEAR) {
            return false;
        }
        offset = YEAR_LENGTH + 1;
        bool retMonth = CheckAndGetNonNegativeInteger(timeStr, timeLen, offset, MONTH_LENGTH, month);
        if (!retMonth || month < MIN_MONTH || month > MAX_MONTH) {
            return false;
        }
        offset += MONTH_LENGTH + 1;
        bool retDay = CheckAndGetNonNegativeInteger(timeStr, timeLen, offset, DAY_LENGTH, day);
        if (!retDay) {
            return false;
        }
        bool hasException = false;
        if (month == FEBRUARY) {
            if (LocalDate::IsGregorianLeapYear(year)) {
                if (day < MIN_DAY || day > FEBRUARY_DAY_IN_LEAP_YEAR) {
                    return false;
                }
            } else if (strcmp(policyStr, "EXCEPTION") == 0 && LocalDate::IsJulianLeapYear(year) &&
                year < GREGORIAN_CALENDAR_START_YEAR && year > 0 && day == FEBRUARY_DAY_IN_LEAP_YEAR) {
                hasException = true;
            } else {
                if (day < MIN_DAY || day > DAYS_PER_MONTH[month-1]) {
                    return false;
                }
            }
        } else {
            if (day < MIN_DAY || day > DAYS_PER_MONTH[month-1]) {
                return false;
            }
        }
        // It means that the format is "%Y-%m-%d %H:%M:%S“
        if (fmtLen == TIME_FORMAT_LENGTH) {
            int hour = 0;
            int minute = 0;
            int second = 0;
            offset += DAY_LENGTH + 1;
            bool retHour = CheckAndGetNonNegativeInteger(timeStr, timeLen, offset, HOUR_LENGTH, hour);
            if (!retHour || hour < MIN_HOUR || hour > MAX_HOUR) {
                return false;
            }
            offset += HOUR_LENGTH + 1;
            bool retMinute = CheckAndGetNonNegativeInteger(timeStr, timeLen, offset, MINUTE_LENGTH, minute);
            if (!retMinute || minute < MIN_MINUTE || minute > MAX_MINUTE) {
                return false;
            }
            offset += MINUTE_LENGTH + 1;
            bool retSecond = CheckAndGetNonNegativeInteger(timeStr, timeLen, offset, SECOND_LENGTH, second);
            if (!retSecond || second < MIN_SECOND || second > MAX_SECOND) {
                return false;
            }
        }
        if (hasException) {
            throw exception::OmniException("OPERATOR_RUNTIME_ERROR",
                "Invalid date 'February 29' as '" + std::to_string(year) + "' is not a leap year");
        }
        return true;
    }

    static size_t GetWallTimeMillis()
    {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
    }

private:
    static bool CheckAndGetNonNegativeInteger(const char *str, int strLen, int start, int substrLen, int &outValue)
    {
        outValue = 0;
        auto ptr = str + start;
        for (int i = 0; i < substrLen; i++) {
            auto value = ptr[i] - '0';
            if (value < 0 || value > 9) {
                return false;
            }
            outValue = outValue * 10 + value;
        }
        return true;
    }
}; // class TimeUtil
} // namespace codegen function

#endif // OMNI_RUNTIME_TIMEZONE_UTIL_H
