/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef OMNI_RUNTIME_DATA_TYPE_H
#define OMNI_RUNTIME_DATA_TYPE_H

#include <cstdint>
#include <iostream>
#include <nlohmann/json.hpp>
#include <climits>
#include "decimal128.h"
#include "util/debug.h"

namespace omniruntime {
namespace type {
constexpr int32_t DATA_TYPE_MAX_COUNT = 20;

enum DataTypeId {
    OMNI_NONE = 0,
    OMNI_INT = 1,
    OMNI_LONG = 2,
    OMNI_DOUBLE = 3,
    OMNI_BOOLEAN = 4,
    OMNI_SHORT = 5,
    OMNI_DECIMAL64 = 6,
    OMNI_DECIMAL128 = 7,
    OMNI_DATE32 = 8,
    OMNI_DATE64 = 9,
    OMNI_TIME32 = 10,
    OMNI_TIME64 = 11,
    OMNI_TIMESTAMP = 12,
    OMNI_INTERVAL_MONTHS = 13,
    OMNI_INTERVAL_DAY_TIME = 14,
    OMNI_VARCHAR = 15,
    OMNI_CHAR = 16,
    OMNI_CONTAINER = 17,
    OMNI_INVALID
};

template <DataTypeId dataTypeId> struct NativeType {};

template <> struct NativeType<DataTypeId::OMNI_INT> {
    using type = int32_t;
};

template <> struct NativeType<DataTypeId::OMNI_LONG> {
    using type = int64_t;
};

template <> struct NativeType<DataTypeId::OMNI_DOUBLE> {
    using type = double;
};

template <> struct NativeType<DataTypeId::OMNI_BOOLEAN> {
    using type = bool;
};

template <> struct NativeType<DataTypeId::OMNI_SHORT> {
    using type = int16_t;
};

template <> struct NativeType<DataTypeId::OMNI_DECIMAL64> {
    using type = int64_t;
};

template <> struct NativeType<DataTypeId::OMNI_DECIMAL128> {
    using type = Decimal128;
};

template <> struct NativeType<DataTypeId::OMNI_DATE32> {
    using type = int32_t;
};

template <> struct NativeType<DataTypeId::OMNI_DATE64> {
    using type = int64_t;
};

template <> struct NativeType<DataTypeId::OMNI_TIME64> {
    using type = int64_t;
};
template <> struct NativeType<DataTypeId::OMNI_VARCHAR> {
    using type = uint8_t;
};

template <> struct NativeType<DataTypeId::OMNI_CHAR> {
    using type = uint8_t;
};

template <> struct NativeType<DataTypeId::OMNI_CONTAINER> {
    using type = int64_t;
};

#define DYNAMIC_TYPE_DISPATCH(PREFIX, typeId, ...)                    \
    [&]() {                                                           \
        switch (typeId) {                                             \
            case OMNI_VEC_TYPE_INT: {                                 \
                return PREFIX<OMNI_VEC_TYPE_INT>(__VA_ARGS__);        \
            }                                                         \
            case OMNI_VEC_TYPE_LONG: {                                \
                return PREFIX<OMNI_VEC_TYPE_LONG>(__VA_ARGS__);       \
            }                                                         \
            case OMNI_VEC_TYPE_DOUBLE: {                              \
                return PREFIX<OMNI_VEC_TYPE_DOUBLE>(__VA_ARGS__);     \
            }                                                         \
            case OMNI_VEC_TYPE_BOOLEAN: {                             \
                return PREFIX<OMNI_VEC_TYPE_BOOLEAN>(__VA_ARGS__);    \
            }                                                         \
            case OMNI_VEC_TYPE_SHORT: {                               \
                return PREFIX<OMNI_VEC_TYPE_SHORT>(__VA_ARGS__);      \
            }                                                         \
            case OMNI_VEC_TYPE_DECIMAL64: {                           \
                return PREFIX<OMNI_VEC_TYPE_DECIMAL64>(__VA_ARGS__);  \
            }                                                         \
            case OMNI_VEC_TYPE_DECIMAL128: {                          \
                return PREFIX<OMNI_VEC_TYPE_DECIMAL128>(__VA_ARGS__); \
            }                                                         \
            case OMNI_VEC_TYPE_CHAR:                                  \
            case OMNI_VEC_TYPE_VARCHAR: {                             \
                return PREFIX<OMNI_VEC_TYPE_VARCHAR>(__VA_ARGS__);    \
            }                                                         \
            case OMNI_VEC_TYPE_CONTAINER: {                           \
                return PREFIX<OMNI_VEC_TYPE_CONTAINER>(__VA_ARGS__);  \
            }                                                         \
            case OMNI_VEC_TYPE_DICTIONARY: {                          \
                return PREFIX<OMNI_VEC_TYPE_DICTIONARY>(__VA_ARGS__); \
            }                                                         \
            case OMNI_VEC_TYPE_LAZY: {                                \
                return PREFIX<OMNI_VEC_TYPE_LAZY>(__VA_ARGS__);       \
            }                                                         \
            default:                                                  \
                LogError("Can not handle this type %d", typeId);      \
        }                                                             \
    }()

NLOHMANN_JSON_SERIALIZE_ENUM(DataTypeId, { { OMNI_NONE, nullptr },
    { OMNI_INT, "OMNI_INT" },
    { OMNI_LONG, "OMNI_LONG" },
    { OMNI_DOUBLE, "OMNI_DOUBLE" },
    { OMNI_BOOLEAN, "OMNI_BOOLEAN" },
    { OMNI_SHORT, "OMNI_SHORT" },
    { OMNI_DECIMAL64, "OMNI_DECIMAL64" },
    { OMNI_DECIMAL128, "OMNI_DECIMAL128" },
    { OMNI_DATE32, "OMNI_DATE32" },
    { OMNI_DATE64, "OMNI_DATE64" },
    { OMNI_TIME32, "OMNI_TIME32" },
    { OMNI_TIME64, "OMNI_TIME64" },
    { OMNI_TIMESTAMP, "OMNI_TIMESTAMP" },
    { OMNI_INTERVAL_MONTHS, "OMNI_INTERVAL_MONTHS" },
    { OMNI_INTERVAL_DAY_TIME, "OMNI_INTERVAL_DAY_TIME" },
    { OMNI_VARCHAR, "OMNI_VARCHAR" },
    { OMNI_CHAR, "OMNI_CHAR" },
    { OMNI_CONTAINER, "OMNI_CONTAINER" },
    { OMNI_INVALID, "OMNI_INVALID" } })

enum DateUnit { DAY, MILLI };

NLOHMANN_JSON_SERIALIZE_ENUM(DateUnit, { { DAY, "DAY" }, { MILLI, "MILLI" } })

enum TimeUnit { SEC, MILLISEC, MICROSEC, NANOSEC };

NLOHMANN_JSON_SERIALIZE_ENUM(TimeUnit,
    { { SEC, "SEC" }, { MILLISEC, "MILLISEC" }, { MICROSEC, "MICROSEC" }, { NANOSEC, "NANOSEC" } })

class DataType {
public:
    DataType(const DataType &type)
        : id(type.id),
          width(type.width),
          precision(type.precision),
          scale(type.scale),
          dateUnit(type.dateUnit),
          timeUnit(type.timeUnit)
    {}

    DataType() : DataType(OMNI_INVALID) {}

    explicit DataType(DataTypeId id) : id(id), width(0), precision(0), scale(0), dateUnit(DAY), timeUnit(SEC) {}

    template <typename T, typename = typename std::enable_if<std::is_integral<T>::value, T>::type>
    explicit constexpr DataType(T value) noexcept : DataType(static_cast<DataTypeId>(value))
    { // NOLINT
    }

    virtual ~DataType() {}

    DataTypeId GetId() const
    {
        return id;
    }

    uint32_t GetWidth() const
    {
        return width;
    }

    uint32_t GetPrecision() const
    {
        return precision;
    }

    uint32_t GetScale() const
    {
        return scale;
    }

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(DataType, id, width, precision, scale, dateUnit, timeUnit);

    DataType &operator = (const DataType &right)
    {
        id = right.id;
        width = right.width;
        precision = right.precision;
        scale = right.scale;
        dateUnit = right.dateUnit;
        timeUnit = right.timeUnit;
        return *this;
    }

    bool operator != (const DataType &right) const
    {
        return !operator == (right);
    }

    bool operator == (const DataType &right) const
    {
        return id == right.id && width == right.width && precision == right.precision && scale == right.scale &&
            dateUnit == right.dateUnit && timeUnit == right.timeUnit;
    }

protected:
    DataTypeId id;
    uint32_t width;
    int32_t precision;
    int32_t scale;
    DateUnit dateUnit;
    TimeUnit timeUnit;
};

class NoneDataType : public DataType {
public:
    NoneDataType() : DataType(DataTypeId::OMNI_NONE) {}

    ~NoneDataType() override {}

    const static NoneDataType &Instance()
    {
        static NoneDataType type;
        return type;
    }
};

class IntDataType : public DataType {
public:
    IntDataType() : DataType(DataTypeId::OMNI_INT) {}

    ~IntDataType() override {}

    const static IntDataType &Instance()
    {
        static IntDataType type;
        return type;
    }
};

class LongDataType : public DataType {
public:
    LongDataType() : DataType(DataTypeId::OMNI_LONG) {}

    ~LongDataType() override {}

    const static LongDataType &Instance()
    {
        static LongDataType type;
        return type;
    }
};

class DoubleDataType : public DataType {
public:
    DoubleDataType() : DataType(DataTypeId::OMNI_DOUBLE) {}

    ~DoubleDataType() override {}

    const static DoubleDataType &Instance()
    {
        static DoubleDataType type;
        return type;
    }
};

class BooleanDataType : public DataType {
public:
    BooleanDataType() : DataType(DataTypeId::OMNI_BOOLEAN) {}

    ~BooleanDataType() override {}

    const static BooleanDataType &Instance()
    {
        static BooleanDataType type;
        return type;
    }
};

class ShortDataType : public DataType {
public:
    ShortDataType() : DataType(DataTypeId::OMNI_SHORT) {}

    ~ShortDataType() override {}

    const static ShortDataType &Instance()
    {
        static ShortDataType type;
        return type;
    }
};

class Decimal64DataType : public DataType {
public:
    Decimal64DataType(int32_t precision, int32_t scale) : DataType(DataTypeId::OMNI_DECIMAL64)
    {
        this->precision = precision;
        this->scale = scale;
    }

    ~Decimal64DataType() override {}

    int32_t GetPrecision() const
    {
        return precision;
    }

    int32_t GetScale() const
    {
        return scale;
    }

    const static Decimal64DataType &Instance()
    {
        static Decimal64DataType type(19, 0);
        return type;
    }
};

class Decimal128DataType : public DataType {
public:
    Decimal128DataType(int32_t precision, int32_t scale) : DataType(DataTypeId::OMNI_DECIMAL128)
    {
        this->precision = precision;
        this->scale = scale;
    }

    ~Decimal128DataType() override {}

    int32_t GetPrecision() const
    {
        return precision;
    }

    int32_t GetScale() const
    {
        return scale;
    }

    const static Decimal128DataType &Instance()
    {
        static Decimal128DataType type(38, 0);
        return type;
    }
};

class Date32DataType : public DataType {
public:
    explicit Date32DataType(DateUnit dateUnit) : DataType(DataTypeId::OMNI_DATE32)
    {
        this->dateUnit = dateUnit;
    }

    ~Date32DataType() override {}

    DateUnit GetDateUnit() const
    {
        return dateUnit;
    }

    const static Date32DataType &Instance()
    {
        static Date32DataType type(DAY);
        return type;
    }
};

class Date64DataType : public DataType {
public:
    explicit Date64DataType(DateUnit dateUnit) : DataType(DataTypeId::OMNI_DATE64)
    {
        this->dateUnit = dateUnit;
    }

    ~Date64DataType() override {}

    DateUnit GetDateUnit() const
    {
        return dateUnit;
    }

    const static Date64DataType &Instance()
    {
        static Date64DataType type(DAY);
        return type;
    }
};

class Time32DataType : public DataType {
public:
    explicit Time32DataType() : DataType(DataTypeId::OMNI_TIME32) {}

    ~Time32DataType() override {}

    const static Time32DataType &Instance()
    {
        static Time32DataType type;
        return type;
    }
};

class Time64DataType : public DataType {
public:
    explicit Time64DataType() : DataType(DataTypeId::OMNI_TIME64) {}

    ~Time64DataType() override {}

    const static Time64DataType &Instance()
    {
        static Time64DataType type;
        return type;
    }
};

class ContainerDataType : public DataType {
public:
    explicit ContainerDataType() : DataType(DataTypeId::OMNI_CONTAINER) {}

    ~ContainerDataType() override {}

    const static ContainerDataType &Instance()
    {
        static ContainerDataType type;
        return type;
    }
};

class VarcharDataType : public DataType {
public:
    explicit VarcharDataType(uint32_t width) : DataType(DataTypeId::OMNI_VARCHAR)
    {
        this->width = width;
    }

    virtual ~VarcharDataType() override {}

    const static VarcharDataType &Instance()
    {
        static VarcharDataType type(INT_MAX);
        return type;
    }

protected:
    explicit VarcharDataType(uint32_t width, DataTypeId dataTypeId) : DataType(dataTypeId)
    {
        this->width = width;
    }
};

class CharDataType : public VarcharDataType {
public:
    explicit CharDataType(uint32_t width) : VarcharDataType(width, DataTypeId::OMNI_CHAR) {}

    ~CharDataType() override {}

    const static CharDataType &Instance()
    {
        static CharDataType type(MAX_WIDTH);
        return type;
    }

private:
    const static int32_t MAX_WIDTH = 65536;
};
} // namespace vec
} // namespace omniruntime
#endif // OMNI_RUNTIME_DATA_TYPE_H
