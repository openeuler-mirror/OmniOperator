/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Description: Sum aggregator
 */
#ifndef OMNI_RUNTIME_SUM_AGGREGATOR_H
#define OMNI_RUNTIME_SUM_AGGREGATOR_H

#include "aggregator.h"
#ifdef ENABLE_HMPP
#include "HMPP/hmpps.h"
#endif

namespace omniruntime {
namespace op {
template <typename V, typename IN, typename ResultType> class SumAggregator : public Aggregator {
public:
    SumAggregator(DataTypePtr in, DataTypePtr out, int32_t channel)
        : Aggregator(OMNI_AGGREGATION_TYPE_SUM, in, out, channel)
    {}
    SumAggregator(DataTypePtr in, DataTypePtr out, int32_t channel, bool inputRaw, bool outputPartial)
        : Aggregator(OMNI_AGGREGATION_TYPE_SUM, in, out, channel, inputRaw, outputPartial)
    {}
    ~SumAggregator() override {}

#ifdef ENABLE_HMPP
    void ProcessGroupWithHMPP(AggregateState &state, VectorBatch *vectorBatch) override
    {
        auto vector = vectorBatch->GetVector(channel);

        auto vectorValues = vector->GetValues();
        auto positionOffset = vector->GetPositionOffset();
        auto rowCount = vector->GetSize();
        auto nullAddr = vector->GetValueNulls();
        bool overflow = false;
        auto sumVal = reinterpret_cast<ResultType *>(executionContext->GetArena()->Allocate(sizeof(ResultType)));

        auto inputTypeId = inputType->GetId();
        HmppResult result = HMPP_STS_NO_ERR;
        switch (inputTypeId) {
            case OMNI_LONG: {
                LogDebug("HMPP-Agg-sum");
                result = HMPPS_Sum_64s(static_cast<int64_t *>(static_cast<int64_t *>(vectorValues) + positionOffset),
                    rowCount, static_cast<int8_t *>(static_cast<int8_t *>(nullAddr) + positionOffset), &overflow,
                    reinterpret_cast<int64_t *>(sumVal));
                break;
            }
            default: {
                throw OmniException("NOT SUPPORT", "Unsupported input type for sum aggregate");
                break;
            }
        }

        if (result != HMPP_STS_NO_ERR) {
            throw OmniException("HMPP ERROR", "sum failed for hmpp error");
        }
        if (state.val == nullptr) {
            state.val = sumVal;
        } else {
            *(static_cast<ResultType *>(state.val)) += *static_cast<ResultType *>(sumVal);
        }
    }
#endif

    void ProcessGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex) override
    {
        int32_t offset;
        Vector *vector = VectorHelper::ExpandVectorAndIndex(vectorBatch->GetVector(channel), rowIndex, offset);
        if (vector->IsValueNull(offset)) {
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
        if (vector->IsValueNull(offset)) {
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
