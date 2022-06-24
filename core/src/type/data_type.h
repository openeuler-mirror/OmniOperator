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
const static uint32_t MAX_WIDTH = 65536;
const static int32_t DECIMAL128_DEFAULT_PRECISION= 38;
const static int32_t DECIMAL64_DEFAULT_PRECISION= 19;
const static int32_t DECIMAL128_DEFAULT_SCALE= 0;
const static int32_t DECIMAL64_DEFAULT_SCALE= 0;

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

    virtual uint32_t GetWidth() const
    {
        return 0;
    }

    virtual uint32_t GetPrecision() const
    {
        return 0;
    }

    virtual uint32_t GetScale() const
    {
        return 0;
    }

    virtual DateUnit GetDateUnit() const
    {
        return DAY;
    }

    virtual TimeUnit GetTimeUnit() const
    {
        return SEC;
    }

    virtual const std::vector<std::shared_ptr<DataType>> &GetFieldTypes() const
    {
        static std::vector<std::shared_ptr<DataType>> emptyFields;
        return emptyFields;
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

    virtual DataType &operator = (const DataType &right)
    {
        id = right.id;
        return *this;
    }

protected:
    DataTypeId id;
};

using DataTypePtr = std::shared_ptr<DataType>;

template<DataTypeId typeId>
class ScalarDataType : public DataType {
public:
    ScalarDataType() : DataType(typeId) {}
    ~ScalarDataType() override = default;
    static DataTypePtr Instance() {
        static std::shared_ptr<ScalarDataType<typeId>> type = std::make_shared<ScalarDataType<typeId>>();
        return type;
    }
};

using InvalidDataType = ScalarDataType<OMNI_INVALID>;
using NoneDataType = ScalarDataType<OMNI_NONE>;
using IntDataType = ScalarDataType<OMNI_INT>;
using ShortDataType = ScalarDataType<OMNI_SHORT>;
using DoubleDataType = ScalarDataType<OMNI_DOUBLE>;
using LongDataType = ScalarDataType<OMNI_LONG>;
using BooleanDataType = ScalarDataType<OMNI_BOOLEAN>;

class DecimalDataType : public DataType {
public:
    DecimalDataType(int32_t precision, int32_t scale, DataTypeId id) : DataType(id)
    {
        this->precision = precision;
        this->scale = scale;
    }

    uint32_t GetPrecision() const override
    {
        return precision;
    }

    uint32_t GetScale() const override
    {
        return scale;
    }

    DataType &operator = (const DataType &right) override
    {
        id = right.GetId();
        precision = right.GetPrecision();
        scale = right.GetScale();
        return *this;
    }

    bool operator == (const DataType &right) const override
    {
        return id == right.GetId() && precision == right.GetPrecision() && scale == right.GetScale();
    }

    void Serialize(nlohmann::json &nlohmannJson) const override
    {
        nlohmannJson = nlohmann::json { { ID, id }, { SCALE, scale }, { PRECISION, precision } };
    }

private:
    int32_t precision;
    int32_t scale;
};

class Decimal64DataType : public DecimalDataType {
public:
    Decimal64DataType() : Decimal64DataType(DECIMAL64_DEFAULT_PRECISION, DECIMAL64_DEFAULT_SCALE) {}
    Decimal64DataType(int32_t precision, int32_t scale) : DecimalDataType(precision, scale, DataTypeId::OMNI_DECIMAL64)
    {}

    ~Decimal64DataType() override = default;

    static DataTypePtr Instance()
    {
        static std::shared_ptr<Decimal64DataType> type = std::make_shared<Decimal64DataType>(DECIMAL64_DEFAULT_PRECISION, DECIMAL64_DEFAULT_SCALE);
        return type;
    }

};

class Decimal128DataType : public DecimalDataType {
public:
    Decimal128DataType() : Decimal128DataType(DECIMAL128_DEFAULT_PRECISION, DECIMAL128_DEFAULT_SCALE) {}
    Decimal128DataType(int32_t precision, int32_t scale)
        : DecimalDataType(precision, scale, DataTypeId::OMNI_DECIMAL128)
    {}

    ~Decimal128DataType() override {}

    static DataTypePtr Instance()
    {
        static std::shared_ptr<Decimal128DataType> type = std::make_shared<Decimal128DataType>(DECIMAL128_DEFAULT_PRECISION, DECIMAL128_DEFAULT_SCALE);
        return type;
    }

};

class DateDataType : public DataType {
public:

    ~DateDataType() override {}

    DateUnit GetDateUnit() const override
    {
        return dateUnit;
    }

    bool operator == (const DataType &right) const override
    {
        return id == right.GetId() && dateUnit == right.GetDateUnit();
    }

    DataType &operator = (const DataType &right) override
    {
        id = right.GetId();
        dateUnit = right.GetDateUnit();
        return *this;
    }

    void Serialize(nlohmann::json &nlohmannJson) const override
    {
        nlohmannJson = nlohmann::json { { ID, id }, { DATE_UNIT, dateUnit } };
    }

protected:
    DateDataType(DateUnit dateUnit, DataTypeId id) : DataType(id)
    {
        this->dateUnit = dateUnit;
    }

private:
    DateUnit dateUnit;
};

class Date32DataType : public DateDataType {
public:
    explicit Date32DataType() : Date32DataType(DAY) {}
    explicit Date32DataType(DateUnit dateUnit) : DateDataType(dateUnit, DataTypeId::OMNI_DATE32) {}

    ~Date32DataType() override {}

    static DataTypePtr Instance()
    {
        static std::shared_ptr<Date32DataType> type = std::make_shared<Date32DataType>(DAY);
        return type;
    }
};

class Date64DataType : public DateDataType {
public:
    explicit Date64DataType() : Date64DataType(DAY) {}
    explicit Date64DataType(DateUnit dateUnit) : DateDataType(dateUnit, DataTypeId::OMNI_DATE64) {}

    ~Date64DataType() override {}

    static DataTypePtr Instance()
    {
        static std::shared_ptr<Date64DataType> type = std::make_shared<Date64DataType>(DAY);
        return type;
    }
};


class TimeDataType : public DataType {
public:
    ~TimeDataType() override {}

    TimeUnit GetTimeUnit() const override
    {
        return timeUnit;
    }

    bool operator == (const DataType &right) const override
    {
        return id == right.GetId() && timeUnit == right.GetTimeUnit();
    }

    void Serialize(nlohmann::json &nlohmannJson) const override
    {
        nlohmannJson = nlohmann::json { { ID, id }, { TIME_UNIT, timeUnit } };
    }

protected:
    TimeDataType(TimeUnit timeUnit, DataTypeId id) : DataType(id)
    {
        this->timeUnit = timeUnit;
    }

private:
    TimeUnit timeUnit;
};
class Time32DataType : public TimeDataType {
public:
    explicit Time32DataType(TimeUnit timeUnit) : TimeDataType(timeUnit, DataTypeId::OMNI_TIME32) {}

    ~Time32DataType() override {}

    static DataTypePtr Instance()
    {
        static std::shared_ptr<Time32DataType> type = std::make_shared<Time32DataType>(SEC);
        return type;
    }
};

class Time64DataType : public TimeDataType {
public:
    explicit Time64DataType(TimeUnit timeUnit) : TimeDataType(timeUnit, DataTypeId::OMNI_TIME64) {}

    ~Time64DataType() override {}

    static DataTypePtr Instance()
    {
        static std::shared_ptr<Time64DataType> type = std::make_shared<Time64DataType>(SEC);
        return type;
    }
};

class ContainerDataType : public DataType {
public:
    explicit ContainerDataType() : DataType(DataTypeId::OMNI_CONTAINER) {}
    explicit ContainerDataType(std::vector<DataTypePtr> &fieldTypes) : DataType(DataTypeId::OMNI_CONTAINER), fieldTypes(std::move(fieldTypes))
    {
    }

    ~ContainerDataType() override = default;

    static DataTypePtr Instance()
    {
        static std::shared_ptr<ContainerDataType> type = std::make_shared<ContainerDataType>();
        return type;
    }

    const std::vector<DataTypePtr> &GetFieldTypes() const override
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
        for (const auto & fieldType : fieldTypes) {
            ids.push_back(fieldType->GetId());
        }
    }

    ContainerDataType &operator = (const DataType &right) override
    {
        id = right.GetId();
        fieldTypes = right.GetFieldTypes();
        return *this;
    }

    bool operator == (const DataType &right) const override
    {
        return id == right.GetId() && fieldTypes == right.GetFieldTypes();
    }

    void Serialize(nlohmann::json &nlohmannJson) const override
    {
        nlohmannJson = nlohmann::json { { ID, id } };
        for (const auto& fieldType : fieldTypes) {
            nlohmann::json fieldTypeJson;
            fieldType->Serialize(fieldTypeJson);
            nlohmannJson[FIELD_TYPES].push_back(fieldTypeJson);
        }
    }

private:
    std::vector<DataTypePtr> fieldTypes;
};

using ContainerDataTypePtr = std::shared_ptr<ContainerDataType>;

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
        static std::shared_ptr<VarcharDataType> type = std::make_shared<VarcharDataType>(INT_MAX);
        return type;
    }


    uint32_t GetWidth() const override
    {
        return width;
    }

    bool operator == (const DataType &right) const override
    {
        return id == right.GetId() && width == right.GetWidth();
    }

    DataType *operator = (const DataType *right)
    {
        id = right->GetId();
        width = right->GetWidth();
        return this;
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
    explicit CharDataType() : CharDataType(MAX_WIDTH) {}
    explicit CharDataType(uint32_t width) : VarcharDataType(width, DataTypeId::OMNI_CHAR) {}

    ~CharDataType() override = default;

    static DataTypePtr Instance()
    {
        static std::shared_ptr<CharDataType> type = std::make_shared<CharDataType>(MAX_WIDTH);
        return type;
    }

};
} // namespace type
} // namespace omniruntime
#endif // OMNI_RUNTIME_DATA_TYPE_H
