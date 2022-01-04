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
        AverageShortDecimalAggregator(int32_t in, int32_t out) : Aggregator(OMNI_AGGREGATION_TYPE_MAX, in, out) {}

        AverageShortDecimalAggregator(int32_t in, int32_t out, bool inputRaw, bool outputPartial)
                : Aggregator(OMNI_AGGREGATION_TYPE_MIN, in, out, inputRaw, outputPartial) {}

        ~AverageShortDecimalAggregator() override {}

        void ProcessGroup(AggregateState &state, Vector *vector, uint32_t offset) override
        {
        }

        void ProcessNonGroup(Vector *vector, uint32_t offset) override {

        }

        void InitiateGroup(AggregateState &state, Vector *vector, uint32_t offset) override
        {

        }

        void InitiateNonGroup(Vector *vector, uint32_t offset) override
        {

        }

        void* Evaluate(const AggregateState &state) override
        {
            return nullptr;
        }

        void ExtractValue(Vector *vector, AggregateState &state, int32_t rowIndex) override
        {

        }
    };
}
}
#endif //OMNI_RUNTIME_AVERAGE_SHORT_DECIMAL_AGGREGATOR_H
