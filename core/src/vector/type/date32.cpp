/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#include "date32.h"

namespace omniruntime {
namespace vec {
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