/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 * @Description: width integer implementations
 */

#ifndef OMNI_RUNTIME_DATE_TIME_UTILS_H
#define OMNI_RUNTIME_DATE_TIME_UTILS_H

#include <iostream>

class LocalDate {
public:
    constexpr static int DAYS_PER_CYCLE = 146097;
    constexpr static long DAYS_0000_TO_1970 = (DAYS_PER_CYCLE * 5L) - (30L * 365L + 7L);

    static inline bool IsJulianLeapYear(int year)
    {
        return (year & 3) == 0;
    }

    static inline bool IsGregorianLeapYear(int year)
    {
        return (year & 3) == 0 && (year % 100 != 0 || year % 400 == 0);
    }

    LocalDate(int32_t year, int16_t month, int16_t day) : year_(year), month_(month), day_(day)
    {}

    explicit LocalDate(int epochDay)
    {
        long dayZero = epochDay + DAYS_0000_TO_1970;
        // find the march-based year
        dayZero -= 60;  // adjust to 0000-03-01 so leap day is at end of four year cycle
        long adjust = 0;
        if (dayZero < 0) {
            // adjust negative years to positive for calculation
            long adjustCycles = (dayZero + 1) / DAYS_PER_CYCLE - 1;
            adjust = adjustCycles * 400;
            dayZero += -adjustCycles * DAYS_PER_CYCLE;
        }
        long yearEst = (400 * dayZero + 591) / DAYS_PER_CYCLE;
        long doyEst = dayZero - (365 * yearEst + yearEst / 4 - yearEst / 100 + yearEst / 400);
        if (doyEst < 0) {
            // fix estimate
            yearEst--;
            doyEst = dayZero - (365 * yearEst + yearEst / 4 - yearEst / 100 + yearEst / 400);
        }
        yearEst += adjust;  // reset any negative year
        int marchDoy0 = (int) doyEst;

        // convert march-based values back to january-based
        int marchMonth0 = (marchDoy0 * 5 + 2) / 153;
        month_ = (marchMonth0 + 2) % 12 + 1;
        day_ = marchDoy0 - (marchMonth0 * 306 + 5) / 10 + 1;
        yearEst += marchMonth0 / 10;
        year_ = yearEst;
    }

    int32_t ToDays() const
    {
        long year = year_;
        long month = month_;
        long total = 0;
        total += 365 * year;
        if (year >= 0) {
            total += (year + 3) / 4 - (year + 99) / 100 + (year + 399) / 400;
        } else {
            total -= year / -4 - year / -100 + year / -400;
        }
        total += ((367 * month - 362) / 12);
        total += day_ - 1;
        if (month > 2) {
            total--;
            if (IsLeapYear() == false) {
                total--;
            }
        }
        return total - DAYS_0000_TO_1970;
    }

    bool IsLeapYear() const
    {
        return LocalDate::IsGregorianLeapYear(static_cast<int>(year_));
    }

    LocalDate &SetQuarter()
    {
        month_ = static_cast<int16_t>((month_ - 1) / 3 * 3 + 1);
        day_ = 1;
        return *this;
    }

    int32_t FirstDayOfYear() const
    {
        int32_t days = IsLeapYear() ? 1 : 0;
        switch (month_) {
            case 2:
                return 32;
            case 4:
                return 91 + days;
            case 6:
                return 152 + days;
            case 9:
                return 244 + days;
            case 11:
                return 305 + days;
            case 1:
                return 1;
            case 3:
                return 60 + days;
            case 5:
                return 121 + days;
            case 7:
                return 182 + days;
            case 8:
                return 213 + days;
            case 10:
                return 274 + days;
            case 12:
            default:
                return 335 + days;
        }
    }

    int32_t getDayOfYear() const
    {
        return FirstDayOfYear() + day_ - 1;
    }

    int16_t GetDay() const
    {
        return day_;
    }

private:
    int32_t year_;
    int16_t month_;
    int16_t day_;
};

#endif // OMNI_RUNTIME_DATE_TIME_UTILS_H
