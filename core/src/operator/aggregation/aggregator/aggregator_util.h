/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
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
    static std::vector<int32_t> WrapWithVector(int32_t value);

    static std::vector<DataTypes> WrapWithVector(const DataTypes &value);

    static std::vector<bool> WrapWithVector(bool value, int num);

    static std::vector<std::vector<uint32_t>> WrapWithVector(std::vector<uint32_t> value);

    static std::vector<std::vector<int32_t>> WrapWithVector(std::vector<int32_t> value);

    static DataTypesPtr WrapWithDataTypes(DataTypePtr value);

    static bool IsHMPPMaxMinSupportDataTypeId(DataTypeId dataTypeId);

protected:
};
}
}
#endif // OMNI_RUNTIME_AGGREGATOR_UTIL_H
