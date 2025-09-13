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
const std::string ID = "id";
const std::string WIDTH = "width";
const std::string PRECISION = "precision";
const std::string SCALE = "scale";
const std::string DATE_UNIT = "dateUnit";
const std::string TIME_UNIT = "timeUnit";
const std::string FIELD_TYPES = "fieldTypes";
const static uint32_t CHAR_MAX_WIDTH = 65536;
const static int32_t DECIMAL128_DEFAULT_PRECISION = 38;
const static int32_t DECIMAL64_DEFAULT_PRECISION = 18;
const static int32_t DECIMAL128_DEFAULT_SCALE = 0;
const static int32_t DECIMAL64_DEFAULT_SCALE = 0;

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
    OMNI_INVALID = 18,
    OMNI_TIME_WITHOUT_TIME_ZONE = 19,
    OMNI_TIMESTAMP_WITHOUT_TIME_ZONE = 20,
    OMNI_TIMESTAMP_WITH_TIME_ZONE = 21,
    OMNI_TIMESTAMP_WITH_LOCAL_TIME_ZONE = 22,
    OMNI_MULTISET = 24,
    OMNI_ARRAY = 30,
    OMNI_MAP = 31,
    OMNI_ROW = 32,
    OMNI_UNKNOWN = 33,
    OMNI_FUNCTION = 34,
    OMNI_OPAQUE = 35
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
    using type = std::string_view;
};

template <> struct NativeType<DataTypeId::OMNI_CHAR> {
    using type = std::string_view;
};

template <> struct NativeType<DataTypeId::OMNI_CONTAINER> {
    using type = int64_t;
};

template <> struct NativeType<DataTypeId::OMNI_TIMESTAMP> {
    using type = int64_t;
};

#define DYNAMIC_TYPE_DISPATCH(CALLBACK, typeId, ...)                                          \
    [&]() {                                                                                   \
        switch (typeId) {                                                                     \
            case OMNI_INT: {                                                                  \
                return CALLBACK<omniruntime::type::DataTypeId::OMNI_INT>(__VA_ARGS__);        \
            }                                                                                 \
            case OMNI_DATE32: {                                                               \
                return CALLBACK<omniruntime::type::DataTypeId::OMNI_DATE32>(__VA_ARGS__);     \
            }                                                                                 \
            case OMNI_LONG: {                                                                 \
                return CALLBACK<omniruntime::type::DataTypeId::OMNI_LONG>(__VA_ARGS__);       \
            }                                                                                 \
            case OMNI_DATE64: {                                                               \
                return CALLBACK<omniruntime::type::DataTypeId::OMNI_DATE64>(__VA_ARGS__);     \
            }                                                                                 \
            case OMNI_DECIMAL64: {                                                            \
                return CALLBACK<omniruntime::type::DataTypeId::OMNI_DECIMAL64>(__VA_ARGS__);  \
            }                                                                                 \
            case OMNI_DOUBLE: {                                                               \
                return CALLBACK<omniruntime::type::DataTypeId::OMNI_DOUBLE>(__VA_ARGS__);     \
            }                                                                                 \
            case OMNI_BOOLEAN: {                                                              \
                return CALLBACK<omniruntime::type::DataTypeId::OMNI_BOOLEAN>(__VA_ARGS__);    \
            }                                                                                 \
            case OMNI_SHORT: {                                                                \
                return CALLBACK<omniruntime::type::DataTypeId::OMNI_SHORT>(__VA_ARGS__);      \
            }                                                                                 \
            case OMNI_DECIMAL128: {                                                           \
                return CALLBACK<omniruntime::type::DataTypeId::OMNI_DECIMAL128>(__VA_ARGS__); \
            }                                                                                 \
            case OMNI_CHAR: {                                                                 \
                return CALLBACK<omniruntime::type::DataTypeId::OMNI_CHAR>(__VA_ARGS__);       \
            }                                                                                 \
            case OMNI_VARCHAR: {                                                              \
                return CALLBACK<omniruntime::type::DataTypeId::OMNI_VARCHAR>(__VA_ARGS__);    \
            }                                                                                 \
            case OMNI_TIMESTAMP: {                                                            \
                return CALLBACK<omniruntime::type::DataTypeId::OMNI_TIMESTAMP>(__VA_ARGS__);  \
            }                                                                                 \
            default:                                                                          \
                LogError("Can not handle this type %d", typeId);                              \
        }                                                                                     \
    }()

template <typename T> constexpr DataTypeId TYPE_ID = DataTypeId::OMNI_INVALID;
template <> inline constexpr DataTypeId TYPE_ID<int16_t> = DataTypeId::OMNI_SHORT;
template <> inline constexpr DataTypeId TYPE_ID<int32_t> = DataTypeId::OMNI_INT;
template <> inline constexpr DataTypeId TYPE_ID<int64_t> = DataTypeId::OMNI_LONG;
template <> inline constexpr DataTypeId TYPE_ID<double> = DataTypeId::OMNI_DOUBLE;
template <> inline constexpr DataTypeId TYPE_ID<bool> = DataTypeId::OMNI_BOOLEAN;
template <> inline constexpr DataTypeId TYPE_ID<Decimal128> = DataTypeId::OMNI_DECIMAL128;
template <> inline constexpr DataTypeId TYPE_ID<std::string_view> = DataTypeId::OMNI_CHAR;

enum DateUnit {
    DAY = 0,
    MILLI = 1
};

enum TimeUnit {
    SEC = 0,
    MILLISEC = 1,
    MICROSEC = 2,
    NANOSEC = 3
};

class DataType {
public:
    DataType(const DataType &type) : DataType(type.id) {}
    DataType() : DataType(OMNI_INVALID) {}

    explicit DataType(DataTypeId id) : id(id) {}

    template <typename T, typename = typename std::enable_if<std::is_integral<T>::value, T>::type>
    explicit constexpr DataType(T value) noexcept : DataType(static_cast<DataTypeId>(value))
    { // NOLINT
    }

    virtual ~DataType() {}

    DataTypeId GetId() const
    {
        return id;
    }

    virtual void Serialize(nlohmann::json &nlohmannJson) const
    {
        nlohmannJson = nlohmannJson = nlohmann::json { { ID, id } };
    }

    friend void to_json(nlohmann::json &nlohmannJson, const std::shared_ptr<DataType> &dataType)
    {
        dataType->Serialize(nlohmannJson);
    }

    bool operator != (const DataType &right) const
    {
        return !operator == (right);
    }

    virtual bool operator == (const DataType &right) const
    {
        return id == right.id;
    }

    DataType &operator = (const DataType &right)
    {
        id = right.id;
        return *this;
    }

protected:
    DataTypeId id;
};

using DataTypePtr = std::shared_ptr<DataType>;

template <DataTypeId typeId> class FixedWidthDataType : public DataType {
public:
    FixedWidthDataType() : DataType(typeId) {}
    ~FixedWidthDataType() override = default;
    static DataTypePtr Instance()
    {
        return std::make_shared<FixedWidthDataType<typeId>>();
    }
};

using IntDataType = FixedWidthDataType<OMNI_INT>;
using ShortDataType = FixedWidthDataType<OMNI_SHORT>;
using DoubleDataType = FixedWidthDataType<OMNI_DOUBLE>;
using LongDataType = FixedWidthDataType<OMNI_LONG>;
using BooleanDataType = FixedWidthDataType<OMNI_BOOLEAN>;
using TimestampDataType = FixedWidthDataType<OMNI_TIMESTAMP>;
using InvalidDataType = FixedWidthDataType<OMNI_INVALID>;
using NoneDataType = FixedWidthDataType<OMNI_NONE>;

class RowType : public DataType {
public:
    RowType(std::vector<std::shared_ptr<DataType>> &types): DataType(OMNI_ROW), children(std::move(types)) {}

    bool operator ==(const DataType &right) const override
    {
        if (&right == this) {
            return true;
        }
        auto otherTyped = reinterpret_cast<const RowType*>(&right);
        if (otherTyped->Size() != Size()) {
            return false;
        }
        for (size_t i = 0; i < Size(); ++i) {
            if (children[i] != otherTyped->children[i]) {
                return false;
            }
        }
        return true;
    }

    size_t Size() const
    {
        return children.size();
    }

    const std::vector<std::shared_ptr<DataType>> &Children() const
    {
        return children;
    }

    void Serialize(nlohmann::json &nlohmannJson) const override {}

protected:
    const std::vector<std::shared_ptr<DataType>> children;
};

class DecimalDataType : public DataType {
public:
    int32_t GetPrecision() const
    {
        return precision;
    }

    int32_t GetScale() const
    {
        return scale;
    }

    bool operator == (const DataType &right) const override
    {
        if (id != right.GetId()) {
            return false;
        } else if (precision != static_cast<const DecimalDataType &>(right).GetPrecision()) {
            return false;
        } else if (scale != static_cast<const DecimalDataType &>(right).GetScale()) {
            return false;
        } else {
            return true;
        }
    }

    void Serialize(nlohmann::json &nlohmannJson) const override
    {
        nlohmannJson = nlohmann::json { { ID, id }, { SCALE, scale }, { PRECISION, precision } };
    }

protected:
    DecimalDataType(DataTypeId id, int32_t precision, int32_t scale) : DataType(id), precision(precision), scale(scale)
    {}
    int32_t precision;
    int32_t scale;
};

class Decimal64DataType : public DecimalDataType {
public:
    Decimal64DataType() : Decimal64DataType(DECIMAL64_DEFAULT_PRECISION, DECIMAL64_DEFAULT_SCALE) {}
    Decimal64DataType(int32_t precision, int32_t scale) : DecimalDataType(DataTypeId::OMNI_DECIMAL64, precision, scale)
    {}

    ~Decimal64DataType() override = default;

    static DataTypePtr Instance()
    {
        return std::make_shared<Decimal64DataType>(DECIMAL64_DEFAULT_PRECISION, DECIMAL64_DEFAULT_SCALE);
    }

    Decimal64DataType &operator = (const Decimal64DataType &right)
    {
        precision = right.GetPrecision();
        scale = right.GetScale();
        return *this;
    }
};

class Decimal128DataType : public DecimalDataType {
public:
    Decimal128DataType() : Decimal128DataType(DECIMAL128_DEFAULT_PRECISION, DECIMAL128_DEFAULT_SCALE) {}
    Decimal128DataType(int32_t precision, int32_t scale)
        : DecimalDataType(DataTypeId::OMNI_DECIMAL128, precision, scale)
    {}

    ~Decimal128DataType() override {}

    static DataTypePtr Instance()
    {
        return std::make_shared<Decimal128DataType>(DECIMAL128_DEFAULT_PRECISION, DECIMAL128_DEFAULT_SCALE);
    }

    Decimal128DataType &operator = (const Decimal128DataType &right)
    {
        precision = right.GetPrecision();
        scale = right.GetScale();
        return *this;
    }
};

class DateDataType : public DataType {
public:
    ~DateDataType() override {}

    DateUnit GetDateUnit() const
    {
        return dateUnit;
    }

    bool operator == (const DataType &right) const override
    {
        if (id != right.GetId()) {
            return false;
        } else if (dateUnit != static_cast<const DateDataType &>(right).GetDateUnit()) {
            return false;
        } else {
            return true;
        }
    }

    void Serialize(nlohmann::json &nlohmannJson) const override
    {
        nlohmannJson = nlohmann::json { { ID, id }, { DATE_UNIT, dateUnit } };
    }

protected:
    DateDataType(DataTypeId id, DateUnit dateUnit) : DataType(id), dateUnit(dateUnit) {}

    DateUnit dateUnit;
};

class Date32DataType : public DateDataType {
public:
    explicit Date32DataType() : Date32DataType(DAY) {}
    explicit Date32DataType(DateUnit dateUnit) : DateDataType(DataTypeId::OMNI_DATE32, dateUnit) {}

    ~Date32DataType() override {}

    static DataTypePtr Instance()
    {
        return std::make_shared<Date32DataType>(DAY);
    }

    Date32DataType &operator = (const Date32DataType &right)
    {
        dateUnit = right.GetDateUnit();
        return *this;
    }
};

class Date64DataType : public DateDataType {
public:
    explicit Date64DataType() : Date64DataType(DAY) {}
    explicit Date64DataType(DateUnit dateUnit) : DateDataType(DataTypeId::OMNI_DATE64, dateUnit) {}

    ~Date64DataType() override {}

    static DataTypePtr Instance()
    {
        return std::make_shared<Date64DataType>(DAY);
    }

    Date64DataType &operator = (const Date64DataType &right)
    {
        dateUnit = right.GetDateUnit();
        return *this;
    }
};


class TimeDataType : public DataType {
public:
    ~TimeDataType() override {}

    TimeUnit GetTimeUnit() const
    {
        return timeUnit;
    }

    bool operator == (const DataType &right) const override
    {
        if (id != right.GetId()) {
            return false;
        } else if (timeUnit != static_cast<const TimeDataType &>(right).GetTimeUnit()) {
            return false;
        } else {
            return true;
        }
    }

    void Serialize(nlohmann::json &nlohmannJson) const override
    {
        nlohmannJson = nlohmann::json { { ID, id }, { TIME_UNIT, timeUnit } };
    }

protected:
    TimeDataType(DataTypeId id, TimeUnit timeUnit) : DataType(id), timeUnit(timeUnit) {}

    TimeUnit timeUnit;
};

class Time32DataType : public TimeDataType {
public:
    explicit Time32DataType(TimeUnit timeUnit) : TimeDataType(DataTypeId::OMNI_TIME32, timeUnit) {}

    ~Time32DataType() override {}

    static DataTypePtr Instance()
    {
        return std::make_shared<Time32DataType>(SEC);
    }

    Time32DataType &operator = (const Time32DataType &right)
    {
        timeUnit = right.GetTimeUnit();
        return *this;
    }
};

class Time64DataType : public TimeDataType {
public:
    explicit Time64DataType(TimeUnit timeUnit) : TimeDataType(DataTypeId::OMNI_TIME64, timeUnit) {}

    ~Time64DataType() override {}

    static DataTypePtr Instance()
    {
        return std::make_shared<Time64DataType>(SEC);
    }

    Time64DataType &operator = (const Time64DataType &right)
    {
        timeUnit = right.GetTimeUnit();
        return *this;
    }
};

class ContainerDataType : public DataType {
public:
    explicit ContainerDataType() : DataType(DataTypeId::OMNI_CONTAINER) {}
    explicit ContainerDataType(std::vector<DataTypePtr> &fieldTypes)
        : DataType(DataTypeId::OMNI_CONTAINER), fieldTypes(std::move(fieldTypes))
    {}

    ~ContainerDataType() override = default;

    static DataTypePtr Instance()
    {
        return std::make_shared<ContainerDataType>();
    }

    std::vector<DataTypePtr> &GetFieldTypes()
    {
        return fieldTypes;
    }

    const DataTypePtr &GetFieldType(int32_t index) const
    {
        return fieldTypes[index];
    }

    int32_t GetSize() const
    {
        return fieldTypes.size();
    }

    void GetIds(std::vector<int32_t> &ids) const
    {
        ids.reserve(fieldTypes.size());
        for (const auto &fieldType : fieldTypes) {
            ids.push_back(fieldType->GetId());
        }
    }

    ContainerDataType &operator = (ContainerDataType &right)
    {
        fieldTypes = right.GetFieldTypes();
        return *this;
    }

    bool operator == (const DataType &right) const override
    {
        if (id != right.GetId()) {
            return false;
        }
        if (fieldTypes.size() !=
            const_cast<ContainerDataType &>(static_cast<const ContainerDataType &>(right)).GetFieldTypes().size()) {
            return false;
        }
        const auto &rightType = static_cast<const ContainerDataType &>(right);
        for (uint32_t i = 0; i < fieldTypes.size(); i++) {
            if (*fieldTypes[i] != *rightType.GetFieldType(i)) {
                return false;
            }
        }
        return true;
    }

    void Serialize(nlohmann::json &nlohmannJson) const override
    {
        nlohmannJson = nlohmann::json { { ID, id } };
        for (const auto &fieldType : fieldTypes) {
            nlohmann::json fieldTypeJson;
            fieldType->Serialize(fieldTypeJson);
            nlohmannJson[FIELD_TYPES].push_back(fieldTypeJson);
        }
    }

private:
    std::vector<DataTypePtr> fieldTypes;
};

class VarcharDataType : public DataType {
public:
    VarcharDataType(const VarcharDataType &type) : VarcharDataType(type.GetWidth()) {}
    explicit VarcharDataType() : VarcharDataType(INT_MAX) {}
    explicit VarcharDataType(uint32_t width) : DataType(DataTypeId::OMNI_VARCHAR)
    {
        this->width = width;
    }

    ~VarcharDataType() override = default;

    static DataTypePtr Instance()
    {
        return std::make_shared<VarcharDataType>(INT_MAX);
    }

    uint32_t GetWidth() const
    {
        return width;
    }

    void SetWidth(const uint32_t newWidth)
    {
        width = newWidth;
    }

    bool operator == (const DataType &right) const override
    {
        if (id != right.GetId()) {
            return false;
        } else if (width != static_cast<const VarcharDataType &>(right).GetWidth()) {
            return false;
        } else {
            return true;
        }
    }

    VarcharDataType &operator = (const VarcharDataType &right)
    {
        id = right.GetId();
        width = static_cast<const VarcharDataType &>(right).GetWidth();
        return *this;
    }

    void Serialize(nlohmann::json &nlohmannJson) const override
    {
        nlohmannJson = nlohmann::json { { ID, id }, { WIDTH, width } };
    }

protected:
    uint32_t width;

    explicit VarcharDataType(uint32_t width, DataTypeId dataTypeId) : DataType(dataTypeId)
    {
        this->width = width;
    }
};

class CharDataType : public VarcharDataType {
public:
    CharDataType(CharDataType &type) : CharDataType(type.width) {}
    explicit CharDataType() : CharDataType(CHAR_MAX_WIDTH) {}
    explicit CharDataType(uint32_t width) : VarcharDataType(width, DataTypeId::OMNI_CHAR) {}

    ~CharDataType() override = default;

    static DataTypePtr Instance()
    {
        return std::make_shared<CharDataType>(CHAR_MAX_WIDTH);
    }
};
} // namespace type
} // namespace omniruntime
#endif // OMNI_RUNTIME_DATA_TYPE_H
