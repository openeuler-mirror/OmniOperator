/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#ifndef OMNI_RUNTIME_DATE32_H
#define OMNI_RUNTIME_DATE32_H

#include <type_traits>
#include <cstdint>
#include <string>

#include "date_base.h"

namespace omniruntime {
namespace type {
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

    static int StringToDate32(const char *s, int32_t strLen, int32_t &result);

    static constexpr double SECOND_OF_DAY = 86400.0;

    int32_t Value() const
    {
        return value;
    }

private:
    int32_t value;
};
Date32 operator + (const Date32 &left, const Date32 &right);

Date32 operator - (const Date32 &left, const Date32 &right);
}
}
#endif // OMNI_RUNTIME_DATE32_H
