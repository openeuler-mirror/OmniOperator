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
    SumAggregator(DataTypesPtr inputTypes, DataTypesPtr outputTypes, std::vector<int32_t> &channels)
        : Aggregator(OMNI_AGGREGATION_TYPE_SUM, inputTypes, outputTypes, channels)
    {}
    SumAggregator(DataTypesPtr inputTypes, DataTypesPtr outputTypes, std::vector<int32_t> &channels, bool inputRaw,
        bool outputPartial)
        : Aggregator(OMNI_AGGREGATION_TYPE_SUM, inputTypes, outputTypes, channels, inputRaw, outputPartial)
    {}
    ~SumAggregator() override {}

#ifdef ENABLE_HMPP
    void ProcessGroupWithHMPP(AggregateState &state, VectorBatch *vectorBatch) override
    {
        auto vector = vectorBatch->GetVector(channels[0]);

        auto vectorValues = vector->GetValues();
        auto positionOffset = vector->GetPositionOffset();
        auto rowCount = vector->GetSize();
        auto nullAddr = vector->GetValueNulls();
        bool overflow = false;
        auto sumVal = reinterpret_cast<ResultType *>(executionContext->GetArena()->Allocate(sizeof(ResultType)));

        auto inputTypeId = inputTypes->GetType(0)->GetId();
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

    bool CanProcessWithHMPP(AggregateState &state, VectorBatch *vectorBatch) override
    {
        // not accept dictionnary vector
        if (vectorBatch->GetVector(channels[0])->GetEncoding() == OMNI_VEC_ENCODING_DICTIONARY) {
            return false;
        }
        // only OMNI_LONG type input support
        return (inputTypes->GetType(0)->GetId() == OMNI_LONG);
    }
#endif

    void ProcessGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex) override
    {
        int32_t offset;
        Vector *vector = VectorHelper::ExpandVectorAndIndex(vectorBatch->GetVector(channels[0]), rowIndex, offset);
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
        Vector *vector = VectorHelper::ExpandVectorAndIndex(vectorBatch->GetVector(channels[0]), rowIndex, offset);
        if (vector->IsValueNull(offset)) {
            return;
        }
        auto curVal = (static_cast<V *>(vector))->GetValue(offset);
        auto ptr = executionContext->GetArena()->Allocate(sizeof(ResultType));
        *reinterpret_cast<ResultType *>(ptr) = curVal;
        state.val = ptr;
    }

    void ExtractValues(AggregateState &state, std::vector<Vector *> &vectors, int32_t rowIndex) override
    {
        int32_t offset;
        auto v = static_cast<V *>(VectorHelper::ExpandVectorAndIndex(vectors[0], rowIndex, offset));
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
