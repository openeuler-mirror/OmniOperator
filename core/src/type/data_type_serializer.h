/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#ifndef OMNI_RUNTIME_DATA_TYPE_SERIALIZER_H
#define OMNI_RUNTIME_DATA_TYPE_SERIALIZER_H

#include "data_type.h"
#include "data_types.h"

namespace omniruntime {
namespace type {
std::string Serialize(const std::vector<DataType> &types);

std::string SerializeSingle(const DataType &type);

DataTypes Deserialize(const std::string &dataTypes);

DataType DeserializeSingle(const std::string &dataType);
}
}

#endif // OMNI_RUNTIME_DATA_TYPE_SERIALIZER_H
