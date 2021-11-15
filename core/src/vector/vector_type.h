/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#ifndef OMNI_RUNTIME_VECTOR_TYPE_H
#define OMNI_RUNTIME_VECTOR_TYPE_H

#include <cstdint>
#include <iostream>
#include <nlohmann/json.hpp>
#include <climits>

namespace omniruntime {
namespace vec {
constexpr int32_t VEC_TYPE_MAX_COUNT = 18;

enum VecTypeId {
    OMNI_VEC_TYPE_NONE = 0,
    OMNI_VEC_TYPE_INT = 1,
    OMNI_VEC_TYPE_LONG = 2,
    OMNI_VEC_TYPE_DOUBLE = 3,
    OMNI_VEC_TYPE_BOOLEAN = 4,
    OMNI_VEC_TYPE_SHORT = 5,
    OMNI_VEC_TYPE_DECIMAL64 = 6,
    OMNI_VEC_TYPE_DECIMAL128 = 7,
    OMNI_VEC_TYPE_DATE32 = 8,
    OMNI_VEC_TYPE_DATE64 = 9,
    OMNI_VEC_TYPE_TIME32 = 10,
    OMNI_VEC_TYPE_TIME64 = 11,
    OMNI_VEC_TYPE_TIMESTAMP = 12,
    OMNI_VEC_TYPE_INTERVAL_MONTHS = 13,
    OMNI_VEC_TYPE_INTERVAL_DAY_TIME = 14,
    OMNI_VEC_TYPE_VARCHAR = 15,
    OMNI_VEC_TYPE_DICTIONARY = 16,
    OMNI_VEC_TYPE_CONTAINER = 17,
    OMNI_VEC_TYPE_LAZY = 18,
    OMNI_VEC_TYPE_INVALID
};

NLOHMANN_JSON_SERIALIZE_ENUM(VecTypeId, { { OMNI_VEC_TYPE_NONE, nullptr },
    { OMNI_VEC_TYPE_INT, "OMNI_VEC_TYPE_INT" },
    { OMNI_VEC_TYPE_LONG, "OMNI_VEC_TYPE_LONG" },
    { OMNI_VEC_TYPE_DOUBLE, "OMNI_VEC_TYPE_DOUBLE" },
    { OMNI_VEC_TYPE_BOOLEAN, "OMNI_VEC_TYPE_BOOLEAN" },
    { OMNI_VEC_TYPE_SHORT, "OMNI_VEC_TYPE_SHORT" },
    { OMNI_VEC_TYPE_DECIMAL64, "OMNI_VEC_TYPE_DECIMAL64" },
    { OMNI_VEC_TYPE_DECIMAL128, "OMNI_VEC_TYPE_DECIMAL128" },
    { OMNI_VEC_TYPE_DATE32, "OMNI_VEC_TYPE_DATE32" },
    { OMNI_VEC_TYPE_DATE64, "OMNI_VEC_TYPE_DATE64" },
    { OMNI_VEC_TYPE_TIME32, "OMNI_VEC_TYPE_TIME32" },
    { OMNI_VEC_TYPE_TIME64, "OMNI_VEC_TYPE_TIME64" },
    { OMNI_VEC_TYPE_TIMESTAMP, "OMNI_VEC_TYPE_TIMESTAMP" },
    { OMNI_VEC_TYPE_INTERVAL_MONTHS, "OMNI_VEC_TYPE_INTERVAL_MONTHS" },
    { OMNI_VEC_TYPE_INTERVAL_DAY_TIME, "OMNI_VEC_TYPE_INTERVAL_DAY_TIME" },
    { OMNI_VEC_TYPE_VARCHAR, "OMNI_VEC_TYPE_VARCHAR" },
    { OMNI_VEC_TYPE_DICTIONARY, "OMNI_VEC_TYPE_DICTIONARY" },
    { OMNI_VEC_TYPE_CONTAINER, "OMNI_VEC_TYPE_CONTAINER" },
    { OMNI_VEC_TYPE_INVALID, "OMNI_VEC_TYPE_INVALID" } })

enum DateUnit { DAY, MILLI };

NLOHMANN_JSON_SERIALIZE_ENUM(DateUnit, { { DAY, "DAY" }, { MILLI, "MILLI" } })

enum TimeUnit { SEC, MILLISEC, MICROSEC, NANOSEC };

NLOHMANN_JSON_SERIALIZE_ENUM(TimeUnit,
    { { SEC, "SEC" }, { MILLISEC, "MILLISEC" }, { MICROSEC, "MICROSEC" }, { NANOSEC, "NANOSEC" } })

class VecType {
public:
    VecType(const VecType &type)
        : id(type.id),
          width(type.width),
          precision(type.precision),
          scale(type.scale),
          dateUnit(type.dateUnit),
          timeUnit(type.timeUnit)
    {}

    VecType() : VecType(OMNI_VEC_TYPE_INVALID) {}

    explicit VecType(VecTypeId id) : id(id), width(0), precision(0), scale(0), dateUnit(DAY), timeUnit(SEC) {}

    template <typename T, typename = typename std::enable_if<std::is_integral<T>::value, T>::type>
    explicit constexpr VecType(T value) noexcept : VecType(static_cast<VecTypeId>(value))
    { // NOLINT
    }

    virtual ~VecType() {}

    VecTypeId GetId() const
    {
        return id;
    }

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(VecType, id, width, precision, scale, dateUnit, timeUnit);

    VecType &operator = (const VecType &right)
    {
        id = right.id;
        width = right.width;
        precision = right.precision;
        scale = right.scale;
        dateUnit = right.dateUnit;
        timeUnit = right.timeUnit;
        return *this;
    }

    bool operator != (const VecType &right) const
    {
        return !operator == (right);
    }

    bool operator == (const VecType &right) const
    {
        return id == right.id && width == right.width && precision == right.precision && scale == right.scale &&
            dateUnit == right.dateUnit && timeUnit == right.timeUnit;
    }

protected:
    VecTypeId id;
    uint32_t width;
    int32_t precision;
    int32_t scale;
    DateUnit dateUnit;
    TimeUnit timeUnit;
};

class IntVecType : public VecType {
public:
    IntVecType() : VecType(VecTypeId::OMNI_VEC_TYPE_INT) {}

    ~IntVecType() override {}

    const static IntVecType &Instance()
    {
        static IntVecType type;
        return type;
    }
};

class LongVecType : public VecType {
public:
    LongVecType() : VecType(VecTypeId::OMNI_VEC_TYPE_LONG) {}

    ~LongVecType() override {}

    const static LongVecType &Instance()
    {
        static LongVecType type;
        return type;
    }
};

class DoubleVecType : public VecType {
public:
    DoubleVecType() : VecType(VecTypeId::OMNI_VEC_TYPE_DOUBLE) {}

    ~DoubleVecType() override {}

    const static DoubleVecType &Instance()
    {
        static DoubleVecType type;
        return type;
    }
};

class BooleanVecType : public VecType {
public:
    BooleanVecType() : VecType(VecTypeId::OMNI_VEC_TYPE_BOOLEAN) {}

    ~BooleanVecType() override {}

    const static BooleanVecType &Instance()
    {
        static BooleanVecType type;
        return type;
    }
};

class ShortVecType : public VecType {
public:
    ShortVecType() : VecType(VecTypeId::OMNI_VEC_TYPE_SHORT) {}

    ~ShortVecType() override {}

    const static ShortVecType &Instance()
    {
        static ShortVecType type;
        return type;
    }
};

class Decimal64VecType : public VecType {
public:
    Decimal64VecType(int32_t precision, int32_t scale) : VecType(VecTypeId::OMNI_VEC_TYPE_DECIMAL64)
    {
        this->precision = precision;
        this->scale = scale;
    }

    ~Decimal64VecType() override {}

    int32_t GetPrecision() const
    {
        return precision;
    }

    int32_t GetScale() const
    {
        return scale;
    }

    const static Decimal64VecType &Instance()
    {
        static Decimal64VecType type(19, 0);
        return type;
    }
};

class Decimal128VecType : public VecType {
public:
    Decimal128VecType(int32_t precision, int32_t scale) : VecType(VecTypeId::OMNI_VEC_TYPE_DECIMAL128)
    {
        this->precision = precision;
        this->scale = scale;
    }

    ~Decimal128VecType() override {}

    int32_t GetPrecision() const
    {
        return precision;
    }

    int32_t GetScale() const
    {
        return scale;
    }

    const static Decimal128VecType &Instance()
    {
        static Decimal128VecType type(38, 0);
        return type;
    }
};

class Date32VecType : public VecType {
public:
    explicit Date32VecType(DateUnit dateUnit) : VecType(VecTypeId::OMNI_VEC_TYPE_DATE32)
    {
        this->dateUnit = dateUnit;
    }

    ~Date32VecType() override {}

    DateUnit GetDateUnit() const
    {
        return dateUnit;
    }

    const static Date32VecType &Instance()
    {
        static Date32VecType type(DAY);
        return type;
    }
};

class Date64VecType : public VecType {
public:
    explicit Date64VecType(DateUnit dateUnit) : VecType(VecTypeId::OMNI_VEC_TYPE_DATE64)
    {
        this->dateUnit = dateUnit;
    }

    ~Date64VecType() override {}

    DateUnit GetDateUnit() const
    {
        return dateUnit;
    }

    const static Date64VecType &Instance()
    {
        static Date64VecType type(DAY);
        return type;
    }
};

class Time32VecType : public VecType {
public:
    explicit Time32VecType() : VecType(VecTypeId::OMNI_VEC_TYPE_TIME32) {}

    ~Time32VecType() override {}

    const static Time32VecType &Instance()
    {
        static Time32VecType type;
        return type;
    }
};

class Time64VecType : public VecType {
public:
    explicit Time64VecType() : VecType(VecTypeId::OMNI_VEC_TYPE_TIME64) {}

    ~Time64VecType() override {}

    const static Time64VecType &Instance()
    {
        static Time64VecType type;
        return type;
    }
};

class ContainerVecType : public VecType {
public:
    explicit ContainerVecType() : VecType(VecTypeId::OMNI_VEC_TYPE_CONTAINER) {}

    ~ContainerVecType() override {}

    const static ContainerVecType &Instance()
    {
        static ContainerVecType type;
        return type;
    }
};

class VarcharVecType : public VecType {
public:
    explicit VarcharVecType(uint32_t width) : VecType(VecTypeId::OMNI_VEC_TYPE_VARCHAR)
    {
        this->width = width;
    }

    ~VarcharVecType() override {}

    uint32_t GetWidth() const
    {
        return width;
    }

    const static VarcharVecType &Instance()
    {
        static VarcharVecType type(INT_MAX);
        return type;
    }
};

class DictionaryVecType : public VecType {
public:
    explicit DictionaryVecType() : VecType(VecTypeId::OMNI_VEC_TYPE_DICTIONARY) {}

    ~DictionaryVecType() override {}

    const static DictionaryVecType &Instance()
    {
        static DictionaryVecType type;
        return type;
    }
};

class LazyVecType : public VecType {
public:
    LazyVecType() : VecType(VecTypeId::OMNI_VEC_TYPE_LAZY) {}

    ~LazyVecType() override {}

    const static LazyVecType &Instance()
    {
        static LazyVecType type;
        return type;
    }
};
} // namespace vec
} // namespace omniruntime
#endif // OMNI_RUNTIME_VECTOR_TYPE_H
