/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2024. All rights reserved.
 * @Description: FunctionType enum
 */

#ifndef OMNI_RUNTIME_FUNCTION_TYPE_H
#define OMNI_RUNTIME_FUNCTION_TYPE_H

namespace omniruntime {
namespace op {
using FunctionType = enum FunctionType {
    OMNI_AGGREGATION_TYPE_SUM = 0,
    OMNI_AGGREGATION_TYPE_COUNT_COLUMN,
    OMNI_AGGREGATION_TYPE_COUNT_ALL,
    OMNI_AGGREGATION_TYPE_AVG,
    OMNI_AGGREGATION_TYPE_SAMP,
    OMNI_AGGREGATION_TYPE_MAX,
    OMNI_AGGREGATION_TYPE_MIN,
    OMNI_AGGREGATION_TYPE_DNV,
    OMNI_AGGREGATION_TYPE_FIRST_IGNORENULL,
    OMNI_AGGREGATION_TYPE_FIRST_INCLUDENULL,
    OMNI_AGGREGATION_TYPE_INVALID,
    OMNI_WINDOW_TYPE_ROW_NUMBER,
    OMNI_WINDOW_TYPE_RANK,
    OMNI_AGGREGATION_TYPE_TRY_SUM,
    OMNI_AGGREGATION_TYPE_TRY_AVG
};

template <typename Enumeration>
auto as_integer(Enumeration const value) -> typename std::underlying_type<Enumeration>::type
{
    return static_cast<typename std::underlying_type<Enumeration>::type>(value);
}
}
}
#endif // OMNI_RUNTIME_FUNCTION_TYPE_H
