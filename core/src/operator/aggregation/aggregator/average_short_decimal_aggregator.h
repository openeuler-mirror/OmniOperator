/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Average aggregate for short decimal
 */
#ifndef OMNI_RUNTIME_AVERAGE_SHORT_DECIMAL_AGGREGATOR_H
#define OMNI_RUNTIME_AVERAGE_SHORT_DECIMAL_AGGREGATOR_H
#include "aggregator.h"
namespace omniruntime {
namespace op {
class AverageShortDecimalAggregator : public Aggregator {
public:
    AverageShortDecimalAggregator(int32_t in, int32_t out, int32_t channel) : Aggregator(OMNI_AGGREGATION_TYPE_MAX, in, out, channel) {}

    AverageShortDecimalAggregator(int32_t in, int32_t out, int32_t channel, bool inputRaw, bool outputPartial)
        : Aggregator(OMNI_AGGREGATION_TYPE_MIN, in, out, channel, inputRaw, outputPartial)
    {}

    ~AverageShortDecimalAggregator() override {}

    void ProcessGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex) override {}

    void InitiateGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex) override {}

    void ExtractValue(AggregateState &state, Vector *vector, int32_t rowIndex) override {}
};
}
}
#endif // OMNI_RUNTIME_AVERAGE_SHORT_DECIMAL_AGGREGATOR_H
