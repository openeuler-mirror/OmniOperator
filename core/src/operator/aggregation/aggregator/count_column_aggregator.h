/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Description: Count aggregate
 */
#ifndef OMNI_RUNTIME_COUNT_COLUMN_AGGREGATOR_H
#define OMNI_RUNTIME_COUNT_COLUMN_AGGREGATOR_H

#include "aggregator.h"

namespace omniruntime {
namespace op {
class CountColumnAggregator : public Aggregator {
public:
    CountColumnAggregator(const DataTypes &outputTypes, std::vector<int32_t> &channels)
        : Aggregator(OMNI_AGGREGATION_TYPE_COUNT_COLUMN, *DataTypes::NoneDataTypesInstance(), outputTypes, channels)
    {}

    CountColumnAggregator(const DataTypes &outputTypes, std::vector<int32_t> &channels, bool inputRaw, bool outputPartial)
        : Aggregator(OMNI_AGGREGATION_TYPE_COUNT_COLUMN, *DataTypes::NoneDataTypesInstance(), outputTypes, channels,
        inputRaw, outputPartial)
    {}

    ~CountColumnAggregator() override {}

    void ProcessGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex) override
    {
        int32_t offset;
        Vector *vector = VectorHelper::ExpandVectorAndIndex(vectorBatch->GetVector(channels[0]), rowIndex, offset);
        if (vector->IsValueNull(offset)) {
            return;
        }
        if (inputRaw) {
            state.count++;
        } else {
            state.count += (static_cast<LongVector *>(vector))->GetValue(offset);
        }
    }

    void InitiateGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex) override
    {
        int32_t offset;
        Vector *vector = VectorHelper::ExpandVectorAndIndex(vectorBatch->GetVector(channels[0]), rowIndex, offset);
        // It is only effective when COUNT(col). When COUNT(*) or COUNT(1) should directly accumulate vector size;
        if (vector->IsValueNull(offset)) {
            state.count = 0;
            return;
        }

        if (inputRaw) {
            state.count = 1;
            return;
        }

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

#endif // OMNI_RUNTIME_COUNT_COLUMN_AGGREGATOR_H
