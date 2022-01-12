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
    SumAggregator(int32_t in, int32_t out) : Aggregator(OMNI_AGGREGATION_TYPE_SUM, in, out) {}
    SumAggregator(int32_t in, int32_t out, bool inputRaw, bool outputPartial)
        : Aggregator(OMNI_AGGREGATION_TYPE_SUM, out, inputRaw, outputPartial)
    {}
    ~SumAggregator() override {}

    void ProcessGroup(AggregateState &state, Vector *colPtr, uint32_t offset) override
    {
        if (UNLIKELY(colPtr->IsValueNull(offset))) {
            return;
        }
        if (state.val == nullptr) {
            InitiateGroup(state, colPtr, offset);
            return;
        }
        *(static_cast<ResultType *>(state.val)) += (static_cast<V *>(colPtr))->GetValue(offset);
    }

    void InitiateGroup(AggregateState &state, Vector *colPtr, uint32_t offset) override
    {
        if (UNLIKELY(colPtr->IsValueNull(offset))) {
            return;
        }
        auto curVal = (static_cast<V *>(colPtr))->GetValue(offset);
        int32_t len = sizeof(ResultType);
        auto ptr = executionContext->getArena()->Allocate(len);
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
