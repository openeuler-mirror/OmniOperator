/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Aggregation Base Class
 * Author: Songling Liu
 * Create: 2021-07-01
 * Notes: None
 */
#ifndef AGGREGATION_H
#define AGGREGATION_H

#include "../operator_factory.h"
#include "aggregator.h"
#include "../../util/debug.h"
#include "../../memory/memory_pool.h"

#include <vector>
#include <cstdint>
#include <thread>

namespace omniruntime {
namespace op {
class AggregationCommonOperator : public Operator {
public:
    AggregationCommonOperator(std::vector<unique_ptr<Aggregator>> aggs, bool inputRaw, bool outputPartial)
        : aggregators(aggs), inputRaw(inputRaw), outputPartial(outputPartial)
    {}
    ~AggregationCommonOperator() override {}

protected:
    std::vector<unique_ptr<Aggregator>> aggregators;
    int inputRaw;
    int outputPartial;
};

class AggregationCommonOperatorFactory : public OperatorFactory {
public:
    AggregationCommonOperatorFactory(bool inputRaw, bool outputPartial)
        : inputRaw(inputRaw), outputPartial(outputPartial)
    {}
    virtual ~AggregationCommonOperatorFactory() override {}

protected:
    int inputRaw;
    int outputPartial;
};
}
}
#endif