/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#include "data_type_serializer.h"

#include <nlohmann/json.hpp>

namespace omniruntime {
namespace type {
using json = nlohmann::json;

std::string Serialize(const std::vector<DataType> &types)
{
    return json(types).dump();
}

std::string SerializeSingle(const DataType &type)
{
    return json(type).dump();
}

DataTypes Deserialize(const std::string &dataTypes)
{
    return DataTypes(json::parse(dataTypes));
}

DataType DeserializeSingle(const std::string &dataType)
{
    return json::parse(dataType);
}
}
}
