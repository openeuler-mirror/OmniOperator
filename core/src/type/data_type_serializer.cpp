/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#include "data_type_serializer.h"
#include <nlohmann/json.hpp>
#include "util/type_util.h"

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

DataTypes Deserialize(const std::string &dataTypes)
{
    auto dataTypeJsons = nlohmann::json::parse(dataTypes);
    std::vector<DataTypePtr> types;
    for (const auto &dataTypeJson : dataTypeJsons) {
        types.push_back(DataTypeJsonParser(dataTypeJson));
    }
    return DataTypes(types);
}

DataTypePtr DeserializeSingle(const std::string &dataType)
{
    auto dataTypeJson = nlohmann::json::parse(dataType);
    return omniruntime::type::DataTypeJsonParser(dataTypeJson);
}

DataTypePtr DataTypeJsonParser(const nlohmann::json &dataTypeJson)
{
    int dataTypeId = dataTypeJson[ID].get<int>();
    switch (dataTypeId) {
        case OMNI_NONE:
            return NoneType();
        case OMNI_INVALID:
            return InvalidType();
        case OMNI_INT:
            return IntType();
        case OMNI_LONG:
            return LongType();
        case OMNI_TIMESTAMP:
            return TimestampType();
        case OMNI_DOUBLE:
            return DoubleType();
        case OMNI_FLOAT:
            return FloatType();
        case OMNI_BOOLEAN:
            return BooleanType();
        case OMNI_BYTE:
            return ByteType();
        case OMNI_SHORT:
            return ShortType();
        case OMNI_DECIMAL64:
            return Decimal64Type(dataTypeJson[PRECISION].get<int32_t>(), dataTypeJson[SCALE].get<int32_t>());
        case OMNI_DECIMAL128:
            return Decimal128Type(dataTypeJson[PRECISION].get<int32_t>(), dataTypeJson[SCALE].get<int32_t>());
        case OMNI_DATE32:
            return Date32Type(dataTypeJson[DATE_UNIT].get<DateUnit>());
        case OMNI_DATE64:
            return Date64Type(dataTypeJson[DATE_UNIT].get<DateUnit>());
        case OMNI_TIME32:
            return Time32Type(dataTypeJson[TIME_UNIT].get<TimeUnit>());
        case OMNI_TIME64:
            return Time64Type(dataTypeJson[TIME_UNIT].get<TimeUnit>());
        case OMNI_VARBINARY:
            return VarBinaryType(dataTypeJson[WIDTH].get<uint32_t>());
        case OMNI_VARCHAR:
            return VarcharType(dataTypeJson[WIDTH].get<uint32_t>());
        case OMNI_CHAR:
            return CharType(dataTypeJson[WIDTH].get<uint32_t>());
        case OMNI_CONTAINER: {
            std::vector<DataTypePtr> fieldTypes;
            for (const auto &fieldJson : dataTypeJson[FIELD_TYPES]) {
                fieldTypes.push_back(DataTypeJsonParser(fieldJson));
            }
            return ContainerType(fieldTypes);
        }
        case OMNI_MAP: {
            DataTypePtr keyType = DataTypeJsonParser(dataTypeJson["keyType"]);
            DataTypePtr valueType = DataTypeJsonParser(dataTypeJson["valueType"]);
            return std::make_shared<MapType>(keyType, valueType);
        }
        case OMNI_ROW: {
            std::vector<DataTypePtr> fieldTypes;
            for (const auto &fieldJson: dataTypeJson[FIELD_TYPES]) {
                fieldTypes.push_back(DataTypeJsonParser(fieldJson));
            }
            std::vector<std::string> fieldNames;
            if (dataTypeJson.contains(FIELD_NAMES) && dataTypeJson[FIELD_NAMES].is_array()) {
                for (const auto& nameJson : dataTypeJson[FIELD_NAMES]) {
                    if (nameJson.is_string()) {
                        fieldNames.push_back(nameJson.get<std::string>());
                    }
                }
            }
            // If upstream doesn't provide names (e.g. some plan/type sources),
            // fall back to ordinal-based names to avoid crashing scan-spec building.
            if (fieldNames.empty() && !fieldTypes.empty()) {
                fieldNames.reserve(fieldTypes.size());
                for (size_t i = 0; i < fieldTypes.size(); ++i) {
                    fieldNames.push_back("field" + std::to_string(i));
                }
            }
            return std::make_shared<RowType>(fieldTypes, fieldNames);
        }
        case OMNI_ARRAY: {
            DataTypePtr child = DataTypeJsonParser(dataTypeJson[ELEMENT_TYPE]);
            return std::make_shared<ArrayType>(child);
        }
        default:
            LogError("Not Supported Data Type : %d", dataTypeId);
            return nullptr;
    }
}
}
}
