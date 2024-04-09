/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2024. All rights reserved.
 * Description: aggregator tool class
 */

#ifndef AGGREGATOR_UTIL_H
#define AGGREGATOR_UTIL_H

#include <memory>

#include "operator/aggregation/definitions.h"
#include "type/data_types.h"
#include "vector/vector.h"
#include "vector/vector_common.h"
#include "operator/execution_context.h"
#include "operator/util/function_type.h"

namespace omniruntime {
namespace op {
using namespace omniruntime::vec;

class AggregatorUtil {
public:
    static std::vector<type::DataTypes> WrapWithVector(const type::DataTypes &value);

    static std::vector<std::vector<uint32_t>> WrapWithVector(const std::vector<uint32_t> &value);

    static std::vector<std::vector<int32_t>> WrapWithVector(std::vector<int32_t> &value);

    static std::unique_ptr<type::DataTypes> WrapWithDataTypes(const type::DataTypePtr &value);
};
}
}
#endif // AGGREGATOR_UTIL_H
