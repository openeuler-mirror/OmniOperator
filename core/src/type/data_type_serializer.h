/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#ifndef OMNI_RUNTIME_DATA_TYPE_SERIALIZER_H
#define OMNI_RUNTIME_DATA_TYPE_SERIALIZER_H

#include "data_type.h"
#include "data_types.h"

namespace omniruntime {
namespace type {
std::string Serialize(const std::vector<DataTypePtr> &types);

std::string SerializeSingle(const DataTypePtr &type);

DataTypes Deserialize(const std::string &dataTypes);

DataTypePtr DeserializeSingle(const std::string &dataType);

DataTypePtr DataTypeJsonParser(const nlohmann::json &dataTypeJson);
}
}

#endif // OMNI_RUNTIME_DATA_TYPE_SERIALIZER_H
