/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#include <nlohmann/json.hpp>
#include "vector_type_serializer.h"

namespace omniruntime {
namespace vec {
using json = nlohmann::json;

std::string Serialize(const std::vector<VecType> &types)
{
    return json(types).dump();
}

std::string SerializeSingle(const VecType &type)
{
    return json(type).dump();
}

const VecTypesPtr Deserialize(const std::string &vecTypes)
{
    return std::make_shared<VecTypes>(json::parse(vecTypes));
}

VecType DeserializeSingle(const std::string &vecType)
{
    return json::parse(vecType);
}
}
}
