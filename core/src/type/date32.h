/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2024. All rights reserved.
 */

#ifndef OMNI_RUNTIME_DATE32_H
#define OMNI_RUNTIME_DATE32_H

#include <type_traits>
#include <cstdint>
#include <string>
#include <ctime>
#include <algorithm>
#include "base_operations.h"
#include "date_base.h"
#include "util/omni_exception.h"
#include "date_time_utils.h"

namespace omniruntime {
namespace type {
constexpr int64_t SECOND_OF_HOUR = 3600;
constexpr int64_t SECOND_OF_DAY = 86400;
constexpr int32_t MONDAY = 4;

constexpr int32_t LEAP_YEAR_OF_DAYS[] = {0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
constexpr int32_t NORMAL_YEAR_OF_DAYS[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
constexpr int32_t CUMULATIVE_DAYS[] = {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365};
constexpr int32_t CUMULATIVE_LEAP_DAYS[] = {0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366};

constexpr const int32_t MIN_YEAR{-292275055};
constexpr const int32_t MAX_YEAR{292278994};
constexpr const int32_t YEAR_INTERVAL{400};
constexpr const int32_t DAYS_PER_YEAR_INTERVAL{146097};
constexpr const int32_t MAX_DAY_ONLY_LENGTH{17};
constexpr int64_t LEAP_YEAR_OFFSET = 4000000000LL;
constexpr int TM_YEAR_BASE = 1900;

constexpr int MONTH_LENGTHS[][12] = {
    {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31},
    {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31},
};

constexpr int32_t CUMULATIVE_YEAR_DAYS[] = {
    0, 365, 730, 1096, 1461, 1826, 2191, 2557, 2922, 3287, 3652, 4018, 4383, 4748, 5113, 5479, 5844, 6209, 6574, 6940,
    7305, 7670, 8035, 8401, 8766, 9131, 9496, 9862, 10227, 10592, 10957, 11323, 11688, 12053, 12418, 12784, 13149,
    13514, 13879, 14245, 14610, 14975, 15340, 15706, 16071, 16436, 16801, 17167, 17532, 17897, 18262, 18628, 18993,
    19358, 19723, 20089, 20454, 20819, 21184, 21550, 21915, 22280, 22645, 23011, 23376, 23741, 24106, 24472, 24837,
    25202, 25567, 25933, 26298, 26663, 27028, 27394, 27759, 28124, 28489, 28855, 29220, 29585, 29950, 30316, 30681,
    31046, 31411, 31777, 32142, 32507, 32872, 33238, 33603, 33968, 34333, 34699, 35064, 35429, 35794, 36160, 36525,
    36890, 37255, 37621, 37986, 38351, 38716, 39082, 39447, 39812, 40177, 40543, 40908, 41273, 41638, 42004, 42369,
    42734, 43099, 43465, 43830, 44195, 44560, 44926, 45291, 45656, 46021, 46387, 46752, 47117, 47482, 47847, 48212,
    48577, 48942, 49308, 49673, 50038, 50403, 50769, 51134, 51499, 51864, 52230, 52595, 52960, 53325, 53691, 54056,
    54421, 54786, 55152, 55517, 55882, 56247, 56613, 56978, 57343, 57708, 58074, 58439, 58804, 59169, 59535, 59900,
    60265, 60630, 60996, 61361, 61726, 62091, 62457, 62822, 63187, 63552, 63918, 64283, 64648, 65013, 65379, 65744,
    66109, 66474, 66840, 67205, 67570, 67935, 68301, 68666, 69031, 69396, 69762, 70127, 70492, 70857, 71223, 71588,
    71953, 72318, 72684, 73049, 73414, 73779, 74145, 74510, 74875, 75240, 75606, 75971, 76336, 76701, 77067, 77432,
    77797, 78162, 78528, 78893, 79258, 79623, 79989, 80354, 80719, 81084, 81450, 81815, 82180, 82545, 82911, 83276,
    83641, 84006, 84371, 84736, 85101, 85466, 85832, 86197, 86562, 86927, 87293, 87658, 88023, 88388, 88754, 89119,
    89484, 89849, 90215, 90580, 90945, 91310, 91676, 92041, 92406, 92771, 93137, 93502, 93867, 94232, 94598, 94963,
    95328, 95693, 96059, 96424, 96789, 97154, 97520, 97885, 98250, 98615, 98981, 99346, 99711, 100076, 100442, 100807,
    101172, 101537, 101903, 102268, 102633, 102998, 103364, 103729, 104094, 104459, 104825, 105190, 105555, 105920,
    106286, 106651, 107016, 107381, 107747, 108112, 108477, 108842, 109208, 109573, 109938, 110303, 110669, 111034,
    111399, 111764, 112130, 112495, 112860, 113225, 113591, 113956, 114321, 114686, 115052, 115417, 115782, 116147,
    116513, 116878, 117243, 117608, 117974, 118339, 118704, 119069, 119435, 119800, 120165, 120530, 120895, 121260,
    121625, 121990, 122356, 122721, 123086, 123451, 123817, 124182, 124547, 124912, 125278, 125643, 126008, 126373,
    126739, 127104, 127469, 127834, 128200, 128565, 128930, 129295, 129661, 130026, 130391, 130756, 131122, 131487,
    131852, 132217, 132583, 132948, 133313, 133678, 134044, 134409, 134774, 135139, 135505, 135870, 136235, 136600,
    136966, 137331, 137696, 138061, 138427, 138792, 139157, 139522, 139888, 140253, 140618, 140983, 141349, 141714,
    142079, 142444, 142810, 143175, 143540, 143905, 144271, 144636, 145001, 145366, 145732, 146097};

enum class DateTruncMode {
    // The constants are visible for testing purpose only.
    TRUNC_INVALID = -1,
    // The levels from TRUNC_TO_MICROSECOND to TRUNC_TO_DAY are used in truncations
    // of TIMESTAMP values only.
    TRUNC_TO_MICROSECOND = 0,
    MIN_LEVEL_OF_TIMESTAMP_TRUNC = TRUNC_TO_MICROSECOND,
    TRUNC_TO_MILLISECOND = 1,
    TRUNC_TO_SECOND = 2,
    TRUNC_TO_MINUTE = 3,
    TRUNC_TO_HOUR = 4,
    TRUNC_TO_DAY = 5,
    // The levels from TRUNC_TO_WEEK to TRUNC_TO_YEAR are used in truncations
    // of DATE and TIMESTAMP values.
    TRUNC_TO_WEEK = 6,
    MIN_LEVEL_OF_DATE_TRUNC = TRUNC_TO_WEEK,
    TRUNC_TO_MONTH = 7,
    TRUNC_TO_QUARTER = 8,
    TRUNC_TO_YEAR = 9,
};

struct Timestamp {
public:
    static bool EpochToUtc(int64_t epoch, std::tm &tm);

    static int64_t TmToStringView(const std::tm &tmValue, char *const startPosition);
};

class Date32 : public BasicDate {
public:
    Date32(const Date32 &date) : Date32(date.value) {}

    explicit Date32(int32_t value) : BasicDate(), value(value) {}

    // Convert any integer value into a Date32.
    template <typename T,
        typename = typename std::enable_if<std::is_integral<T>::value && (sizeof(T) <= sizeof(int32_t)), T>::type>
    explicit constexpr Date32(T value) noexcept : Date32(static_cast<int32_t>(value))
    { // NOLINT
    }

    ~Date32() {}

    Date32 &operator = (const Date32 &right);

    Date32 &operator += (const Date32 &right);

    Date32 &operator -= (const Date32 &right);

    bool operator == (const Date32 &right) const;

    bool operator != (const Date32 &right) const;

    bool operator < (const Date32 &right) const;

    bool operator > (const Date32 &right) const;

    bool operator <= (const Date32 &right) const;

    bool operator >= (const Date32 &right) const;

    static int StringToTm(const char *s, int32_t strLen, tm &r);

    // The result might overflow, so use int64_t to store the intermediate result.
    static Status StringToDate32(const char *s, int32_t strLen, int64_t &result);

    size_t ToString(char *res, int32_t len) const;

    int32_t Value() const
    {
        return value;
    }

    static DateTruncMode ParseTruncLevel(const std::string &format);

    // date32 is with in range of integer.
    static bool ValidDate(int64_t daysSinceEpoch);

    // returns true if leap year, false otherwise
    static bool IsLeapYear(int32_t year);

    // returns true if year, month, day corresponds to valid date, false otherwise
    static bool IsValidDate(int32_t year, int32_t month, int32_t day);

    static bool ParseDigit(const char *buf, int32_t len, int32_t &pos, int32_t &result);

    /**
    * Computes the (signed) number of days since unix epoch (1970-01-01).
    * Returns UserError status if the date is invalid.
    */
    static bool DaysSinceEpochFromDate(int32_t year, int32_t month, int32_t day, int64_t &out);

    static Status TruncDate(int32_t days, DateTruncMode level, int32_t &result);

private:
    int32_t value;
};
Date32 operator + (const Date32 &left, const Date32 &right);

Date32 operator - (const Date32 &left, const Date32 &right);
}
}
#endif // OMNI_RUNTIME_DATE32_H
