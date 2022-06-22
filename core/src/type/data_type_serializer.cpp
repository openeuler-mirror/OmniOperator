/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#include "data_type_serializer.h"

#include <nlohmann/json.hpp>

namespace omniruntime {
namespace type {
using json = nlohmann::json;

std::string Serialize(const std::vector<DataTypeRawPtr> &types)
{
    return json(types).dump();
}

std::string SerializeSingle(const DataType *type)
{
    return json(type).dump();
}

DataTypes Deserialize(const std::string &dataTypes)
{
    auto dataTypeJsons = nlohmann::json::parse(dataTypes);
    std::vector<DataTypeRawPtr> types = std::vector<DataTypeRawPtr> {};
    for (auto dataTypeJson : dataTypeJsons) {
        types.push_back(DataTypeJsonParser(dataTypeJson));
    }
    return DataTypes(types);
}

DataTypeRawPtr DeserializeSingle(const std::string &dataType)
{
    auto dataTypeJson = nlohmann::json::parse(dataType);
    return omniruntime::type::DataTypeJsonParser(dataTypeJson);
}

DataTypeRawPtr DataTypeJsonParser(const nlohmann::json &dataTypeJson)
{
    DataTypeRawPtr dataTypePtr = nullptr;
    int dataTypeId = dataTypeJson[ID].get<int>();
    switch (dataTypeId) {
        case OMNI_NONE:
            dataTypePtr = new NoneDataType();
            break;
        case OMNI_INVALID:
            dataTypePtr = new InvalidDataType();
            break;
        case OMNI_INT:
            dataTypePtr = new IntDataType();
            break;
        case OMNI_LONG:
            dataTypePtr = new LongDataType();
            break;
        case OMNI_DOUBLE:
            dataTypePtr = new DoubleDataType();
            break;
        case OMNI_BOOLEAN:
            dataTypePtr = new BooleanDataType();
            break;
        case OMNI_SHORT:
            dataTypePtr = new ShortDataType();
            break;
        case OMNI_DECIMAL64:
            dataTypePtr =
                new Decimal64DataType(dataTypeJson[PRECISION].get<int32_t>(), dataTypeJson[SCALE].get<int32_t>());
            break;
        case OMNI_DECIMAL128:
            dataTypePtr =
                new Decimal128DataType(dataTypeJson[PRECISION].get<int32_t>(), dataTypeJson[SCALE].get<int32_t>());
            break;
        case OMNI_DATE32:
            dataTypePtr = new Date32DataType(dataTypeJson[DATE_UNIT].get<DateUnit>());
            break;
        case OMNI_DATE64:
            dataTypePtr = new Date64DataType(dataTypeJson[DATE_UNIT].get<DateUnit>());
            break;
        case OMNI_TIME32:
            dataTypePtr = new Time32DataType(dataTypeJson[TIME_UNIT].get<TimeUnit>());
            break;
        case OMNI_TIME64:
            dataTypePtr = new Time64DataType(dataTypeJson[TIME_UNIT].get<TimeUnit>());
            break;
        case OMNI_VARCHAR:
            dataTypePtr = new VarcharDataType(dataTypeJson[WIDTH].get<uint32_t>());
            break;
        case OMNI_CHAR:
            dataTypePtr = new CharDataType(dataTypeJson[WIDTH].get<uint32_t>());
            break;
        case OMNI_CONTAINER: {
            std::vector<DataTypeRawPtr> fieldTypes = std::vector<DataType *> {};
            for (auto fieldJson : dataTypeJson[FIELD_TYPES]) {
                fieldTypes.push_back(DataTypeJsonParser(fieldJson));
            }
            dataTypePtr = new ContainerDataType(fieldTypes);
            break;
        }
        case OMNI_TIMESTAMP:
        case OMNI_INTERVAL_MONTHS:
        case OMNI_INTERVAL_DAY_TIME:
        default:
            LogError("Not Supported Data Type : %d", dataTypeId);
    }
    return dataTypePtr;
}
}
}
