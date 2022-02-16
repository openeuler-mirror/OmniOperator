/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#ifndef OMNI_RUNTIME_DATA_TYPE_SERIALIZER_H
#define OMNI_RUNTIME_DATA_TYPE_SERIALIZER_H

#include "vector_type.h"
#include "data_types.h"

namespace omniruntime {
namespace type {
std::string Serialize(const std::vector<VecType> &types);

std::string SerializeSingle(const VecType &type);

VecTypes Deserialize(const std::string &vecTypes);

VecType DeserializeSingle(const std::string &vecTypeExt);
}
}

#endif // OMNI_RUNTIME_DATA_TYPE_SERIALIZER_H
