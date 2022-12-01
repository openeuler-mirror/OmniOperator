/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#include "date32.h"

namespace omniruntime {
namespace type {
Date32 &Date32::operator = (const Date32 &right)
{
    value = right.value;
    return *this;
}

Date32 &Date32::operator += (const Date32 &right)
{
    value += right.value;
    return *this;
}

Date32 &Date32::operator -= (const Date32 &right)
{
    value += right.value;
    return *this;
}

bool Date32::operator == (const Date32 &right) const
{
    return value == right.value;
}

bool Date32::operator != (const Date32 &right) const
{
    return value != right.value;
}

bool Date32::operator < (const Date32 &right) const
{
    return value < right.value;
}

bool Date32::operator > (const Date32 &right) const
{
    return value > right.value;
}

bool Date32::operator <= (const Date32 &right) const
{
    return value <= right.value;
}

bool Date32::operator >= (const Date32 &right) const
{
    return value >= right.value;
}

int Date32::StringToTm(const char *s, int32_t len, tm &r)
{
    int offset = 0;
    int count = 0;
    int year = 0;
    int mouth = 0;
    int day = 0;

    while (s[offset] == ' ') {
        offset++;
    }
    while (s[len-1] == ' ') {
        len--;
    }
    // year
    while (offset < len) {
        if (count >= 4) {
            break;
        }
        if (!isdigit(s[offset])) {
            return -1;
        }

        year = year * 10 + s[offset] - 48;
        offset++;
        count++;
    }
    r.tm_year = year - 1900;
    if (offset == len && count == 4) {
        return 1;
    }
    if (s[offset] != '-' || count != 4) {
        return -1;
    }
    offset++;
    count = 0;

    // month
    while (offset < len) {
        if (count >= 2 || !isdigit(s[offset])) {
            break;
        }
        mouth = mouth * 10 + s[offset] - 48;
        offset++;
        count++;
    }
    r.tm_mon = mouth - 1;
    if (count == 1 || count == 2) {
        if (offset == len) {
            return 1;
        }
        if (s[offset] != '-') {
            return -1;
        }
    } else {
        return -1;
    }
    offset++;
    count = 0;

    // day
    while (offset < len) {
        if (count >= 2 || !isdigit(s[offset])) {
            break;
        }
        day = day * 10 + s[offset] - 48;
        offset++;
        count++;
    }
    r.tm_mday = day;

    if ((offset == len || s[offset] == ' ') && (count == 1 || count == 2)) {
        return 1;
    }
    return -1;
}

int Date32::StringToDate32(const char *s, int32_t strLen, int32_t &result)
{
    tm r { 0, 0, 0, 1, 0, 0 };
    if (StringToTm(s, strLen, r) == -1) {
        return -1;
    }
    tm epoch = { 0, 0, 0, 1, 0, 70 };
    time_t epochTime = mktime(&epoch);
    time_t desiredTime = mktime(&r);
    result = static_cast<int32_t>(difftime(desiredTime, epochTime) / SECOND_OF_DAY);
    return 1;
}

Date32 operator + (const Date32 &left, const Date32 &right)
{
    return Date32(left.Value() + right.Value());
}

Date32 operator - (const Date32 &left, const Date32 &right)
{
    return Date32(left.Value() - right.Value());
}
}
}