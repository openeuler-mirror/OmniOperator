/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Sum aggregate for short decimal
 */
#ifndef OMNI_RUNTIME_SUM_SHORT_DECIMAL_AGGREGATOR_H
#define OMNI_RUNTIME_SUM_SHORT_DECIMAL_AGGREGATOR_H
#include "aggregator.h"
namespace omniruntime {
namespace op {
class SumShortDecimalAggregator : public Aggregator {
public:
    SumShortDecimalAggregator(int32_t in, int32_t out, int32_t channel) : Aggregator(OMNI_AGGREGATION_TYPE_MAX, channel, in, out) {}

    SumShortDecimalAggregator(int32_t in, int32_t out, int32_t channel, bool inputRaw, bool outputPartial)
        : Aggregator(OMNI_AGGREGATION_TYPE_MIN, in, out, channel, inputRaw, outputPartial)
    {}

    ~SumShortDecimalAggregator() override {}

    void ProcessGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex) override {}

    void InitiateGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex) override {}

    // TODO extract common function for sum/min/max
    void ExtractValue(AggregateState &state, Vector *vector, int32_t rowIndex) override {}
};
}
}
#endif // OMNI_RUNTIME_SUM_SHORT_DECIMAL_AGGREGATOR_H
