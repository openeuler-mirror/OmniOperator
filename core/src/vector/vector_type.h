/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#ifndef OMNI_RUNTIME_VECTOR_TYPE_H
#define OMNI_RUNTIME_VECTOR_TYPE_H

#include <cstdint>
#include <iostream>
#include <nlohmann/json.hpp>
#include <climits>
#include "type/decimal128.h"
#include "../util/debug.h"

namespace omniruntime {
namespace vec {
constexpr int32_t VEC_TYPE_MAX_COUNT = 20;

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
    OMNI_VEC_TYPE_CHAR = 16,
    OMNI_VEC_TYPE_DICTIONARY = 17,
    OMNI_VEC_TYPE_CONTAINER = 18,
    OMNI_VEC_TYPE_LAZY = 19,
    OMNI_VEC_TYPE_INVALID
};

template <VecTypeId typeId> struct NativeType {};

template <> struct NativeType<VecTypeId::OMNI_VEC_TYPE_INT> {
    using type = int32_t;
};

template <> struct NativeType<VecTypeId::OMNI_VEC_TYPE_LONG> {
    using type = int64_t;
};

template <> struct NativeType<VecTypeId::OMNI_VEC_TYPE_DOUBLE> {
    using type = double;
};

template <> struct NativeType<VecTypeId::OMNI_VEC_TYPE_BOOLEAN> {
    using type = bool;
};

template <> struct NativeType<VecTypeId::OMNI_VEC_TYPE_SHORT> {
    using type = int16_t;
};

template <> struct NativeType<VecTypeId::OMNI_VEC_TYPE_DECIMAL64> {
    using type = int64_t;
};

template <> struct NativeType<VecTypeId::OMNI_VEC_TYPE_DECIMAL128> {
    using type = Decimal128;
};

template <> struct NativeType<VecTypeId::OMNI_VEC_TYPE_DATE32> {
    using type = int32_t;
};

template <> struct NativeType<VecTypeId::OMNI_VEC_TYPE_DATE64> {
    using type = int64_t;
};

template <> struct NativeType<VecTypeId::OMNI_VEC_TYPE_TIME64> {
    using type = int64_t;
};
template <> struct NativeType<VecTypeId::OMNI_VEC_TYPE_VARCHAR> {
    using type = uint8_t;
};

template <> struct NativeType<VecTypeId::OMNI_VEC_TYPE_CHAR> {
    using type = uint8_t;
};

template <> struct NativeType<VecTypeId::OMNI_VEC_TYPE_CONTAINER> {
    using type = int64_t;
};

template <> struct NativeType<VecTypeId::OMNI_VEC_TYPE_LAZY> {
    using type = void;
};

template <> struct NativeType<VecTypeId::OMNI_VEC_TYPE_DICTIONARY> {
    using type = void;
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
    { OMNI_VEC_TYPE_CHAR, "OMNI_VEC_TYPE_CHAR" },
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

class NoneVecType : public VecType {
public:
    NoneVecType() : VecType(VecTypeId::OMNI_VEC_TYPE_NONE) {}

    ~NoneVecType() override {}

    const static NoneVecType &Instance()
    {
        static NoneVecType type;
        return type;
    }
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

    virtual ~VarcharVecType() override {}

    const static VarcharVecType &Instance()
    {
        static VarcharVecType type(INT_MAX);
        return type;
    }

protected:
    explicit VarcharVecType(uint32_t width, VecTypeId vecTypeId) : VecType(vecTypeId)
    {
        this->width = width;
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

class CharVecType : public VarcharVecType {
public:
    explicit CharVecType(uint32_t width) : VarcharVecType(width, VecTypeId::OMNI_VEC_TYPE_CHAR) {}

    ~CharVecType() override {}

    const static CharVecType &Instance()
    {
        static CharVecType type(MAX_WIDTH);
        return type;
    }

private:
    const static int32_t MAX_WIDTH = 65536;
};
} // namespace vec
} // namespace omniruntime
#endif // OMNI_RUNTIME_VECTOR_TYPE_H
