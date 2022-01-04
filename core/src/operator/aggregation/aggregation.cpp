/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Aggregation source file
 */
#include "aggregation.h"
namespace omniruntime {
namespace op {
AggregationCommonOperator::AggregationCommonOperator(std::vector<std::unique_ptr<Aggregator>> aggs, bool inputRaw,
    bool outputPartial)
    : aggregators(std::move(aggs)), inputRaw(inputRaw), outputPartial(outputPartial) {}
}
}
