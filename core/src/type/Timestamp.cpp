//
// Created by root on 12/11/25.
//

#include "Timestamp.h"
#include <algorithm>
#include "type/tzdb/exception.h"

namespace omniruntime {
// static
Timestamp Timestamp::fromDaysAndNanos(int32_t days, int64_t nanos)
{
    int64_t seconds = (days - kJulianToUnixEpochDays) * kSecondsInDay + nanos / kNanosInSecond;
    int64_t remainingNanos = nanos % kNanosInSecond;
    if (remainingNanos < 0) {
        remainingNanos += kNanosInSecond;
        seconds--;
    }
    return Timestamp(seconds, remainingNanos);
}

// static
Timestamp Timestamp::fromDate(int32_t date)
{
    int64_t seconds = (int64_t) date * kSecondsInDay;
    return Timestamp(seconds, 0);
}

// static
Timestamp Timestamp::now()
{
    auto now = std::chrono::system_clock::now();
    auto epochMs = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    return fromMillis(epochMs);
}

namespace {
constexpr int kTmYearBase = 1900;
constexpr int64_t kLeapYearOffset = 4000000000LL;
constexpr int64_t kSecondsPerHour = 3600;
constexpr int64_t kSecondsPerDay = 24 * kSecondsPerHour;

inline bool isLeap(int64_t y)
{
    return y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
}

inline int64_t leapThroughEndOf(int64_t y)
{
    // Add a large offset to make the calculation for negative years correct.
    y += kLeapYearOffset;
    return y / 4 - y / 100 + y / 400;
}

inline int64_t daysBetweenYears(int64_t y1, int64_t y2)
{
    return 365 * (y2 - y1) + leapThroughEndOf(y2 - 1) - leapThroughEndOf(y1 - 1);
}

const int16_t daysBeforeFirstDayOfMonth[][12] = {
    {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334},
    {0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335},
};
} // namespace

void Timestamp::toTimezone(const tz::TimeZone &zone)
{
    try {
        seconds_ = zone.to_local(std::chrono::seconds(seconds_)).count();
    } catch (const std::invalid_argument &e) {
        // Invalid argument means we hit a conversion not supported by
        // external/date. This is a special case where we intentionally throw
        // VeloxRuntimeError to avoid it being suppressed by TRY().
        OMNI_FAIL(e.what());
    }
}

void Timestamp::toGMT(const tz::TimeZone &zone)
{
    std::chrono::seconds sysSeconds;

    try {
        sysSeconds = zone.to_sys(std::chrono::seconds(seconds_));
    } catch (const std::invalid_argument &e) {
        // Invalid argument means we hit a conversion not supported by
        // external/date. Need to throw a RuntimeError so that try() statements do
        // not suppress it.
        OMNI_FAIL(e.what());
    }
    seconds_ = sysSeconds.count();
}

const tz::TimeZone &Timestamp::defaultTimezone()
{
    static const tz::TimeZone *kDefault = ( {
        // TODO: We are hard-coding PST/PDT here to be aligned with the current
        // behavior in DWRF reader/writer.  Once they are fixed, we can use
        // tzdb::current_zone() here.
        //
        // See https://github.com/facebookincubator/velox/issues/8127
        auto *tz = tz::locateZone("America/Los_Angeles");
        tz;
    });
    return *kDefault;
}

bool Timestamp::epochToCalendarUtc(int64_t epoch, std::tm &tm)
{
    constexpr int kDaysPerYear = 365;
    int64_t days = epoch / kSecondsPerDay;
    int64_t rem = epoch % kSecondsPerDay;
    while (rem < 0) {
        rem += kSecondsPerDay;
        --days;
    }
    tm.tm_hour = rem / kSecondsPerHour;
    rem = rem % kSecondsPerHour;
    tm.tm_min = rem / 60;
    tm.tm_sec = rem % 60;
    tm.tm_wday = (4 + days) % 7;
    if (tm.tm_wday < 0) {
        tm.tm_wday += 7;
    }
    int64_t y = 1970;
    if (y + days / kDaysPerYear <= -kLeapYearOffset + 10) {
        return false;
    }
    bool leapYear;
    while (days < 0 || days >= kDaysPerYear + (leapYear = isLeap(y))) {
        auto newy = y + days / kDaysPerYear - (days < 0);
        days -= daysBetweenYears(y, newy);
        y = newy;
    }
    y -= kTmYearBase;
    if (y > std::numeric_limits<decltype(tm.tm_year)>::max() || y < std::numeric_limits<decltype(tm.tm_year)>::min()) {
        return false;
    }
    tm.tm_year = y;
    tm.tm_yday = days;
    auto *months = daysBeforeFirstDayOfMonth[leapYear];
    tm.tm_mon = std::upper_bound(months, months + 12, days) - months - 1;
    tm.tm_mday = days - months[tm.tm_mon] + 1;
    tm.tm_isdst = 0;
    return true;
}

// static
int64_t Timestamp::calendarUtcToEpoch(const std::tm &tm)
{
    static_assert(sizeof(decltype(tm.tm_year)) == 4);
    // tm_year stores number of years since 1900.
    int64_t year = tm.tm_year + 1900LL;
    int64_t month = tm.tm_mon;
    if (UNLIKELY(month > 11)) {
        year += month / 12;
        month %= 12;
    } else if (UNLIKELY(month < 0)) {
        auto yearsDiff = (-month + 11) / 12;
        year -= yearsDiff;
        month += 12 * yearsDiff;
    }
    // Getting number of days since beginning of the year.
    auto dayOfYear = -1LL + daysBeforeFirstDayOfMonth[isLeap(year)][month] + tm.tm_mday;
    // Number of days since 1970-01-01.
    auto daysSinceEpoch = daysBetweenYears(1970, year) + dayOfYear;
    return kSecondsPerDay * daysSinceEpoch + kSecondsPerHour * tm.tm_hour + 60LL * tm.tm_min + tm.tm_sec;
}
}
