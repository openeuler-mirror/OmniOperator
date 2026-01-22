/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: base operations implementation
 */
#pragma once
#include <optional>
#include "Timestamp.h"

namespace omniruntime::util {
constexpr const int32_t kHoursPerDay{24};
constexpr const int32_t kMinsPerHour{60};
constexpr const int32_t kSecsPerMinute{60};
constexpr const int64_t kMsecsPerSec{1000};

constexpr const int64_t kMillisPerSecond{1000};
constexpr const int64_t kMillisPerMinute{kMillisPerSecond * kSecsPerMinute};
constexpr const int64_t kMillisPerHour{kMillisPerMinute * kMinsPerHour};

constexpr const int64_t kMicrosPerMsec{1000};
constexpr const int64_t kMicrosPerSec{kMicrosPerMsec * kMsecsPerSec};
constexpr const int64_t kMicrosPerMinute{kMicrosPerSec * kSecsPerMinute};
constexpr const int64_t kMicrosPerHour{kMicrosPerMinute * kMinsPerHour};

constexpr const int64_t kNanosPerMicro{1000};

constexpr const int32_t kSecsPerHour{kSecsPerMinute * kMinsPerHour};
constexpr const int32_t kSecsPerDay{kSecsPerHour * kHoursPerDay};

// Max and min year correspond to Joda datetime min and max
constexpr const int32_t kMinYear{-292275055};
constexpr const int32_t kMaxYear{292278994};

constexpr const int32_t kYearInterval{400};
constexpr const int32_t kDaysPerYearInterval{146097};

struct ParsedTimestampWithTimeZone {
    Timestamp timestamp;
    const tz::TimeZone *timeZone;
    std::optional<int64_t> offsetMillis;

    // For ease of testing purposes.
    bool operator==(const ParsedTimestampWithTimeZone &other) const
    {
        return timestamp == other.timestamp && timeZone == other.timeZone && offsetMillis == other.offsetMillis;
    }
};

/// Parses a timestamp string using specified TimestampParseMode.
///
/// This is a timezone-aware version of the function above
/// `fromTimestampString()` which returns both the parsed timestamp and the
/// TimeZone pointer. It is up to the client to do the expected conversion based
/// on these two values.
///
/// The timezone information at the end of the string may contain a timezone
/// name (as defined in velox/type/tz/*), such as "UTC" or
/// "America/Los_Angeles", or a timezone offset, like "+06:00" or "-09:30". The
/// white space between the hour definition and timestamp is optional.
///
/// `nullptr` means the timezone was not recognized as a valid time zone or
/// was not present. In this case offsetMillis may be set with the milliseconds
/// timezone offset if an offset was found but was not a valid timezone.
///
/// Returns Unexpected with UserError status in case of parsing errors.
std::optional<ParsedTimestampWithTimeZone> fromTimestampWithTimezoneString(const char *buf, size_t len);

/// Converts ParsedTimestampWithTimeZone to Timestamp according to the
/// timezone-based adjustment. If no timezone information is available
/// in the first argument, respects the session timezone if configured.
Timestamp fromParsedTimestampWithTimeZone(ParsedTimestampWithTimeZone parsed, const tz::TimeZone *sessionTimeZone);
}
