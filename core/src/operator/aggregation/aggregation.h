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
#include "operator/aggregation/aggregator/aggregator_factory.h"
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
    AggregationCommonOperatorFactory(bool inputRaw, bool outputPartial, PrepareContext maskColsContext)
        : inputRaw(inputRaw), outputPartial(outputPartial)
    {
        for (size_t i = 0; i < maskColsContext.len; ++i) {
            maskCols.push_back(maskColsContext.context[i]);
        }
    }

    ~AggregationCommonOperatorFactory() override {};

    std::vector<int32_t> &GetMaskColumns()
    {
        return maskCols;
    }

    virtual OmniStatus Init() = 0;
    virtual OmniStatus Close() = 0;

    template <class T>
    void CreateAggregatorFactory(std::vector<std::unique_ptr<AggregatorFactory>> &aggregatorFactories, int32_t maskCol)
    {
        if (maskCol == Aggregator::INVALID_MASK_COL) {
            aggregatorFactories.push_back(std::make_unique<T>());
        } else {
            aggregatorFactories.push_back(std::make_unique<MaskAggregatorFactory<T>>(maskCol));
        }
    }

protected:
    int inputRaw;
    int outputPartial;
    std::vector<int32_t> maskCols;
};
}
}
#endif