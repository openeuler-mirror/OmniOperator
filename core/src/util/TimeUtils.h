//
// Created by root on 12/11/25.
//

#pragma once
#include <ctime>
#include "compiler_util.h"
#include "omni_exception.h"
#include "type/Timestamp.h"

namespace omniruntime::util {
ALWAYS_INLINE int64_t GetSeconds(Timestamp timestamp, const tz::TimeZone *timeZone)
{
    if (timeZone != nullptr) {
        timestamp.toTimezone(*timeZone);
        return timestamp.getSeconds();
    } else {
        return timestamp.getSeconds();
    }
}

ALWAYS_INLINE std::tm GetDateTime(Timestamp timestamp, const tz::TimeZone *timeZone)
{
    int64_t seconds = GetSeconds(timestamp, timeZone);
    std::tm dateTime;
    OMNI_CHECK(Timestamp::epochToCalendarUtc(seconds, dateTime), "Timestamp is too large: {} seconds since epoch");
    return dateTime;
}
}
