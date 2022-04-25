/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Description: Aggregation Base Class
 */
#ifndef AGGREGATION_H
#define AGGREGATION_H

#include <vector>
#include <thread>
#include "operator/operator_factory.h"
#include "operator/aggregation/aggregator/aggregator.h"
#include "memory/memory_pool.h"
#include "operator/status.h"

namespace omniruntime {
namespace op {
class AggregationCommonOperator : public Operator {
public:
    explicit AggregationCommonOperator(std::vector<std::unique_ptr<Aggregator>> aggs, bool inputRaw, bool outputPartial)
        : aggregators(std::move(aggs)), inputRaw(inputRaw), outputPartial(outputPartial)
    {}

    ~AggregationCommonOperator() override {};

protected:
    std::vector<std::unique_ptr<Aggregator>> aggregators;
    int inputRaw;
    int outputPartial;
};

class AggregationCommonOperatorFactory : public OperatorFactory {
public:
    AggregationCommonOperatorFactory(bool inputRaw, bool outputPartial)
        : inputRaw(inputRaw), outputPartial(outputPartial)
    {}
    ~AggregationCommonOperatorFactory() override {};
    virtual OmniStatus Init() = 0;
    virtual OmniStatus Close() = 0;

protected:
    int inputRaw;
    int outputPartial;
};
}
}
#endif