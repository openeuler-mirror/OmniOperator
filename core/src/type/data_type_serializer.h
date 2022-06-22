/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#ifndef OMNI_RUNTIME_DATA_TYPE_SERIALIZER_H
#define OMNI_RUNTIME_DATA_TYPE_SERIALIZER_H

#include "data_type.h"
#include "data_types.h"

namespace omniruntime {
namespace type {
std::string Serialize(const std::vector<DataTypeRawPtr> &types);

std::string SerializeSingle(const DataType *type);

DataTypes Deserialize(const std::string &dataTypes);

DataTypeRawPtr DeserializeSingle(const std::string &dataType);

DataTypeRawPtr DataTypeJsonParser(const nlohmann::json &dataTypeJson);
}
}

#endif // OMNI_RUNTIME_DATA_TYPE_SERIALIZER_H
