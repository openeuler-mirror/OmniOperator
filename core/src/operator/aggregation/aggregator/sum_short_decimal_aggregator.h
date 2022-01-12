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
    SumShortDecimalAggregator(int32_t in, int32_t out) : Aggregator(OMNI_AGGREGATION_TYPE_MAX, in, out) {}

    SumShortDecimalAggregator(int32_t in, int32_t out, bool inputRaw, bool outputPartial)
        : Aggregator(OMNI_AGGREGATION_TYPE_MIN, in, out, inputRaw, outputPartial)
    {}

    ~SumShortDecimalAggregator() override {}

    void ProcessGroup(AggregateState &state, Vector *vector, uint32_t offset) override {}

    void InitiateGroup(AggregateState &state, Vector *vector, uint32_t offset) override {}

    // TODO extract common function for sum/min/max
    void ExtractValue(AggregateState &state, Vector *vector, int32_t rowIndex) override {}
};
}
}
#endif // OMNI_RUNTIME_SUM_SHORT_DECIMAL_AGGREGATOR_H
