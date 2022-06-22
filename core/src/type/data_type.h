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

    virtual std::vector<DataType *> GetFieldTypes() const
    {
        std::vector<DataType *> emptyFieldTypes = std::vector<DataType *> {};
        return emptyFieldTypes;
    }

    virtual void Serialize(nlohmann::json &nlohmannJson) const
    {
        nlohmannJson = nlohmannJson = nlohmann::json { { ID, id } };
    }

    friend void to_json(nlohmann::json &nlohmannJson, const DataType *dataType)
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

using DataTypeRawPtr = DataType *;

template<DataTypeId typeId>
class ScalarDataType : public DataType {
public:
    ScalarDataType() : DataType(typeId) {}
    ~ScalarDataType() override = default;
    static DataTypeRawPtr Instance() {
        static std::shared_ptr<ScalarDataType<typeId>> type = std::make_shared<ScalarDataType<typeId>>();
        return type.get();
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

    ~Decimal64DataType() override {}

    static DataTypeRawPtr Instance()
    {
        static std::shared_ptr<Decimal64DataType> type = std::make_shared<Decimal64DataType>(DECIMAL64_DEFAULT_PRECISION, DECIMAL64_DEFAULT_SCALE);
        return type.get();
    }

//private:
//    const static int32_t DEFAULT_PRECISION = 19;
//    const static int32_t DEFAULT_SCALE = 0;
};

class Decimal128DataType : public DecimalDataType {
public:
    Decimal128DataType() : Decimal128DataType(DECIMAL128_DEFAULT_PRECISION, DECIMAL128_DEFAULT_SCALE) {}
    Decimal128DataType(int32_t precision, int32_t scale)
        : DecimalDataType(precision, scale, DataTypeId::OMNI_DECIMAL128)
    {}

    ~Decimal128DataType() override {}

    static DataTypeRawPtr Instance()
    {
        static std::shared_ptr<Decimal128DataType> type = std::make_shared<Decimal128DataType>(DECIMAL128_DEFAULT_PRECISION, DECIMAL128_DEFAULT_SCALE);
        return type.get();
    }

//private:
//    const static int32_t DEFAULT_PRECISION = 38;
//    const static int32_t DEFAULT_SCALE = 0;
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

    static DataTypeRawPtr Instance()
    {
        static std::shared_ptr<Date32DataType> type = std::make_shared<Date32DataType>(DAY);
        return type.get();
    }
};

class Date64DataType : public DateDataType {
public:
    explicit Date64DataType() : Date64DataType(DAY) {}
    explicit Date64DataType(DateUnit dateUnit) : DateDataType(dateUnit, DataTypeId::OMNI_DATE64) {}

    ~Date64DataType() override {}

    static DataTypeRawPtr Instance()
    {
        static std::shared_ptr<Date64DataType> type = std::make_shared<Date64DataType>(DAY);
        return type.get();
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

    static DataTypeRawPtr Instance()
    {
        static std::shared_ptr<Time32DataType> type = std::make_shared<Time32DataType>(SEC);
        return type.get();
    }
};

class Time64DataType : public TimeDataType {
public:
    explicit Time64DataType(TimeUnit timeUnit) : TimeDataType(timeUnit, DataTypeId::OMNI_TIME64) {}

    ~Time64DataType() override {}

    static DataTypeRawPtr Instance()
    {
        static std::shared_ptr<Time64DataType> type = std::make_shared<Time64DataType>(SEC);
        return type.get();
    }
};

class ContainerDataType : public DataType {
public:
    ContainerDataType(const ContainerDataType &type) : ContainerDataType(type.GetFieldTypes()) {}
    explicit ContainerDataType() : DataType(DataTypeId::OMNI_CONTAINER) {}
    explicit ContainerDataType(std::vector<DataTypeRawPtr> fieldTypes) : DataType(DataTypeId::OMNI_CONTAINER)
    {
        this->fieldTypes = fieldTypes;
    }

    ~ContainerDataType() override {
        for (auto fieldType : fieldTypes)
        {
            delete fieldType;
        }
    }

    static DataTypeRawPtr Instance()
    {
        static std::shared_ptr<ContainerDataType> type = std::make_shared<ContainerDataType>();
        return type.get();
    }

    std::vector<DataTypeRawPtr> GetFieldTypes() const override
    {
        return fieldTypes;
    }

    DataType &operator = (const DataType &right) override
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
        for (auto fieldType : this->GetFieldTypes()) {
            nlohmann::json fieldTypeJson;
            fieldType->Serialize(fieldTypeJson);
            nlohmannJson[FIELD_TYPES].push_back(fieldTypeJson);
        }
    }

private:
    std::vector<DataTypeRawPtr> fieldTypes;
};

class VarcharDataType : public DataType {
public:
    VarcharDataType(const VarcharDataType &type) : VarcharDataType(type.GetWidth()) {}
    explicit VarcharDataType() : VarcharDataType(INT_MAX) {}
    explicit VarcharDataType(uint32_t width) : DataType(DataTypeId::OMNI_VARCHAR)
    {
        this->width = width;
    }

    virtual ~VarcharDataType() override {}

    static DataTypeRawPtr Instance()
    {
        static std::shared_ptr<VarcharDataType> type = std::make_shared<VarcharDataType>(INT_MAX);
        return type.get();
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

    ~CharDataType() override {}

    static DataTypeRawPtr Instance()
    {
        static std::shared_ptr<CharDataType> type = std::make_shared<CharDataType>(MAX_WIDTH);
        return type.get();
    }

private:
    const static int32_t MAX_WIDTH = 65536;
};
} // namespace type
} // namespace omniruntime
#endif // OMNI_RUNTIME_DATA_TYPE_H
