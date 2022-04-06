/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Sum aggregator
 * Author: Songling Liu
 * Create: 2021-12-24
 * Notes: None
 */

#ifndef OMNI_RUNTIME_SUM_AGGREGATOR_H
#define OMNI_RUNTIME_SUM_AGGREGATOR_H
#include "aggregator.h"
namespace omniruntime {
namespace op {
template <typename V, typename IN, typename ResultType> class SumAggregator : public Aggregator {
public:
    SumAggregator(const DataType &in, const DataType &out, int32_t channel)
        : Aggregator(OMNI_AGGREGATION_TYPE_SUM, in, out, channel)
    {}
    SumAggregator(const DataType &in, const DataType &out, int32_t channel, bool inputRaw, bool outputPartial)
        : Aggregator(OMNI_AGGREGATION_TYPE_SUM, in, out, channel, inputRaw, outputPartial)
    {}
    ~SumAggregator() override {}

    void ProcessGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex) override
    {
        int32_t offset;
        Vector *vector = VectorHelper::ExpandVectorAndIndex(vectorBatch->GetVector(channel), rowIndex, offset);
        if (UNLIKELY(vector->IsValueNull(offset))) {
            return;
        }
        if (state.val == nullptr) {
            InitiateGroup(state, vectorBatch, rowIndex);
            return;
        }
        *(static_cast<ResultType *>(state.val)) += (static_cast<V *>(vector))->GetValue(offset);
    }

    void InitiateGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex) override
    {
        int32_t offset;
        Vector *vector = VectorHelper::ExpandVectorAndIndex(vectorBatch->GetVector(channel), rowIndex, offset);
        if (UNLIKELY(vector->IsValueNull(offset))) {
            return;
        }
        auto curVal = (static_cast<V *>(vector))->GetValue(offset);
        auto ptr = executionContext->GetArena()->Allocate(sizeof(ResultType));
        *reinterpret_cast<ResultType *>(ptr) = curVal;
        state.val = ptr;
    }

    void ExtractValue(AggregateState &state, Vector *vector, int32_t rowIndex) override
    {
        auto v = static_cast<V *>(vector);
        if (state.val == nullptr) {
            v->SetValueNull(rowIndex);
            return;
        }
        v->SetValue(rowIndex, *static_cast<ResultType *>(state.val));
    }
};
}
}
#endif // OMNI_RUNTIME_SUM_AGGREGATOR_H
