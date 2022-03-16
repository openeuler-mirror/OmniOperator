/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Count aggregate
 */
#ifndef OMNI_RUNTIME_COUNT_ALL_AGGREGATOR_H
#define OMNI_RUNTIME_COUNT_ALL_AGGREGATOR_H
#include "aggregator.h"
namespace omniruntime {
namespace op {
class CountAllAggregator : public Aggregator {
public:
    CountAllAggregator(const DataType &out)
        : Aggregator(OMNI_AGGREGATION_TYPE_COUNT_ALL, NoneDataType::Instance(), out, INVALID_INPUT_COL)
    {}

    CountAllAggregator(const DataType &out, bool inputRaw, bool outputPartial)
        : Aggregator(OMNI_AGGREGATION_TYPE_COUNT_ALL, NoneDataType::Instance(), out, INVALID_INPUT_COL, inputRaw,
        outputPartial)
    {}

    ~CountAllAggregator() override {}

    void ProcessGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex) override
    {
        if (inputRaw) {
            state.count++;
        } else {
            int32_t offset;
            Vector *vector = VectorHelper::ExpandVectorAndIndex(vectorBatch->GetVector(channel), rowIndex, offset);
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
        Vector *vector = VectorHelper::ExpandVectorAndIndex(vectorBatch->GetVector(channel), rowIndex, offset);
        state.count = (static_cast<LongVector *>(vector))->GetValue(offset);
    }

    void ExtractValue(AggregateState &state, Vector *vector, int32_t rowIndex) override
    {
        auto v = static_cast<LongVector *>(vector);
        v->SetValue(rowIndex, state.count);
    }
};
}
}
#endif // OMNI_RUNTIME_COUNT_ALL_AGGREGATOR_H
