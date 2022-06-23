/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#include "data_type_serializer.h"

#include <nlohmann/json.hpp>

namespace omniruntime {
namespace type {
using json = nlohmann::json;

std::string Serialize(const std::vector<DataTypePtr> &types)
{
    return json(types).dump();
}

std::string SerializeSingle(const DataTypePtr &type)
{
    return json(type).dump();
}

ContainerDataTypePtr Deserialize(const std::string &dataTypes)
{
    auto dataTypeJsons = nlohmann::json::parse(dataTypes);
    std::vector<DataTypePtr> types;
    for (const auto& dataTypeJson : dataTypeJsons) {
        types.push_back(DataTypeJsonParser(dataTypeJson));
    }
    return std::make_shared<ContainerDataType>(types);
}

DataTypePtr DeserializeSingle(const std::string &dataType)
{
    auto dataTypeJson = nlohmann::json::parse(dataType);
    return omniruntime::type::DataTypeJsonParser(dataTypeJson);
}

DataTypePtr DataTypeJsonParser(const nlohmann::json &dataTypeJson)
{
    DataTypePtr dataTypePtr = nullptr;
    int dataTypeId = dataTypeJson[ID].get<int>();
    switch (dataTypeId) {
        case OMNI_NONE:
            dataTypePtr = NoneDataType::Instance();
            break;
        case OMNI_INVALID:
            dataTypePtr = InvalidDataType::Instance();
            break;
        case OMNI_INT:
            dataTypePtr = IntDataType::Instance();
            break;
        case OMNI_LONG:
            dataTypePtr = LongDataType::Instance();
            break;
        case OMNI_DOUBLE:
            dataTypePtr = DoubleDataType::Instance();
            break;
        case OMNI_BOOLEAN:
            dataTypePtr = BooleanDataType::Instance();
            break;
        case OMNI_SHORT:
            dataTypePtr = ShortDataType::Instance();
            break;
        case OMNI_DECIMAL64:
            dataTypePtr =
                std::make_shared<Decimal64DataType>(dataTypeJson[PRECISION].get<int32_t>(), dataTypeJson[SCALE].get<int32_t>());
            break;
        case OMNI_DECIMAL128:
            dataTypePtr =
                    std::make_shared<Decimal128DataType>(dataTypeJson[PRECISION].get<int32_t>(), dataTypeJson[SCALE].get<int32_t>());
            break;
        case OMNI_DATE32:
            dataTypePtr = std::make_shared<Date32DataType>(dataTypeJson[DATE_UNIT].get<DateUnit>());
            break;
        case OMNI_DATE64:
            dataTypePtr = std::make_shared<Date64DataType>(dataTypeJson[DATE_UNIT].get<DateUnit>());
            break;
        case OMNI_TIME32:
            dataTypePtr = std::make_shared<Time32DataType>(dataTypeJson[TIME_UNIT].get<TimeUnit>());
            break;
        case OMNI_TIME64:
            dataTypePtr = std::make_shared<Time64DataType>(dataTypeJson[TIME_UNIT].get<TimeUnit>());
            break;
        case OMNI_VARCHAR:
            dataTypePtr = std::make_shared<VarcharDataType>(dataTypeJson[WIDTH].get<uint32_t>());
            break;
        case OMNI_CHAR:
            dataTypePtr = std::make_shared<CharDataType>(dataTypeJson[WIDTH].get<uint32_t>());
            break;
        case OMNI_CONTAINER: {
            std::vector<DataTypePtr> fieldTypes;
            for (const auto& fieldJson : dataTypeJson[FIELD_TYPES]) {
                fieldTypes.push_back(DataTypeJsonParser(fieldJson));
            }
            dataTypePtr = std::make_shared<ContainerDataType>(fieldTypes);
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
