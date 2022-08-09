/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Description: Max aggregate
 */
#ifndef OMNI_RUNTIME_MAX_AGGREGATOR_H
#define OMNI_RUNTIME_MAX_AGGREGATOR_H

#include "aggregator.h"

namespace omniruntime {
namespace op {
template <typename InputVecType, typename OutputVecType, typename ResultType> class MaxAggregator : public Aggregator {
public:
    MaxAggregator(DataTypePtr in, DataTypePtr out, int32_t channel)
        : Aggregator(OMNI_AGGREGATION_TYPE_MAX, in, out, channel)
    {}

    MaxAggregator(DataTypePtr in, DataTypePtr out, int32_t channel, bool inputRaw, bool outputPartial)
        : Aggregator(OMNI_AGGREGATION_TYPE_MAX, in, out, channel, inputRaw, outputPartial)
    {}

    ~MaxAggregator() override {}

    void ProcessGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex) override
    {
        int32_t offset;
        Vector *vector = VectorHelper::ExpandVectorAndIndex(vectorBatch->GetVector(channel), rowIndex, offset);
        if (vector->IsValueNull(offset)) {
            return;
        }
        if (state.val == nullptr) {
            this->InitiateGroup(state, vectorBatch, rowIndex);
            return;
        }
        auto rowVal = static_cast<ResultType>((static_cast<InputVecType *>(vector))->GetValue(offset));
        auto leftVal = static_cast<ResultType *>(state.val);
        *leftVal = (Compare(*leftVal, rowVal) == 1) ? *leftVal : rowVal;
    }

    void InitiateGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex) override
    {
        int32_t offset;
        Vector *vector = VectorHelper::ExpandVectorAndIndex(vectorBatch->GetVector(channel), rowIndex, offset);
        if (vector->IsValueNull(offset)) {
            return;
        }
        auto rowVal = static_cast<InputVecType *>(vector)->GetValue(offset);
        auto ptr = executionContext->GetArena()->Allocate(sizeof(ResultType));
        *reinterpret_cast<ResultType *>(ptr) = rowVal;
        state.val = ptr;
    }

    // TOResultTypeO extract common function for sum/min/max
    void ExtractValue(AggregateState &state, Vector *vector, int32_t rowIndex) override
    {
        auto v = static_cast<OutputVecType *>(vector);
        if (state.val == nullptr) {
            v->SetValueNull(rowIndex);
            return;
        }
        v->SetValue(rowIndex, *static_cast<ResultType *>(state.val));
    }
};
}
}
#endif // OMNI_RUNTIME_MAX_AGGREGATOR_H
