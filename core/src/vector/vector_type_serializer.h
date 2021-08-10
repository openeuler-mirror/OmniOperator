/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#ifndef OMNI_RUNTIME_VECTOR_TYPE_SERIALIZER_H
#define OMNI_RUNTIME_VECTOR_TYPE_SERIALIZER_H

#include "vector_type.h"
#include "vector_types.h"

namespace omniruntime {
namespace vec {
std::string Serialize(const std::vector<VecType> &types);

std::string SerializeSingle(const VecType &type);

const VecTypesPtr Deserialize(const std::string &vecTypes);

VecType DeserializeSingle(const std::string &vecTypeExt);
}
}

#endif // OMNI_RUNTIME_VECTOR_TYPE_SERIALIZER_H
