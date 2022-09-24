/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Description: Count aggregate
 */
#ifndef OMNI_RUNTIME_COUNT_ALL_AGGREGATOR_H
#define OMNI_RUNTIME_COUNT_ALL_AGGREGATOR_H

#include "aggregator.h"

namespace omniruntime {
namespace op {
class CountAllAggregator : public Aggregator {
public:
    CountAllAggregator(DataTypesPtr outputTypes, std::vector<int32_t> &channels)
        : Aggregator(OMNI_AGGREGATION_TYPE_COUNT_ALL, DataTypes::NoneDataTypesInstance(), outputTypes, channels)
    {}

    CountAllAggregator(DataTypesPtr outputTypes, std::vector<int32_t> &channels, bool inputRaw, bool outputPartial)
        : Aggregator(OMNI_AGGREGATION_TYPE_COUNT_ALL, DataTypes::NoneDataTypesInstance(), outputTypes, channels,
        inputRaw, outputPartial)
    {}

    ~CountAllAggregator() override {}

    void ProcessGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex) override
    {
        if (inputRaw) {
            state.count++;
        } else {
            int32_t offset;
            Vector *vector = VectorHelper::ExpandVectorAndIndex(vectorBatch->GetVector(channels[0]), rowIndex, offset);
            state.count += (static_cast<LongVector *>(vector))->GetValue(offset);
        }
    }

    void InitiateGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex) override
    {
        if (inputRaw) {
            state.count = 1;
            return;
        }
        int32_t offset;
        Vector *vector = VectorHelper::ExpandVectorAndIndex(vectorBatch->GetVector(channels[0]), rowIndex, offset);
        state.count = (static_cast<LongVector *>(vector))->GetValue(offset);
    }

    void ExtractValues(AggregateState &state, std::vector<Vector *> &vectors, int32_t rowIndex) override
    {
        int32_t offset;
        Vector *vector = VectorHelper::ExpandVectorAndIndex(vectors[0], rowIndex, offset);
        auto v = static_cast<LongVector *>(vector);
        v->SetValue(rowIndex, state.count);
    }
};
}
}
#endif // OMNI_RUNTIME_COUNT_ALL_AGGREGATOR_H
