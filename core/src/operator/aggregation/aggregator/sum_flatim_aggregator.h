/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: For non-decimal type
 */
#ifndef OMNI_RUNTIME_SUM_FLAT_IM_AGGREGATOR_H
#define OMNI_RUNTIME_SUM_FLAT_IM_AGGREGATOR_H

#include "aggregator.h"

namespace omniruntime {
namespace op {
template <typename RawInputVectorType, typename IntermediateVectorType, typename ResultType>
class SumFlatIMAggregator : public Aggregator {
public:
    SumFlatIMAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels)
        : Aggregator(OMNI_AGGREGATION_TYPE_SUM, inputTypes, outputTypes, channels)
    {}
    SumFlatIMAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels,
        bool inputRaw, bool outputPartial, bool isOverflowAsNull)
        : Aggregator(OMNI_AGGREGATION_TYPE_SUM, inputTypes, outputTypes, channels, inputRaw, outputPartial,
        isOverflowAsNull)
    {}
    ~SumFlatIMAggregator() override {}

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
        if (inputRaw) {
            *(static_cast<ResultType *>(state.val)) += (static_cast<RawInputVectorType *>(vector))->GetValue(offset);
        } else {
            *(static_cast<ResultType *>(state.val)) +=
                (static_cast<IntermediateVectorType *>(vector))->GetValue(offset);
        }
    }

    void InitiateGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex) override
    {
        int32_t offset;
        Vector *vector = VectorHelper::ExpandVectorAndIndex(vectorBatch->GetVector(channels[0]), rowIndex, offset);
        if (vector->IsValueNull(offset)) {
            return;
        }
        if (inputRaw) {
            auto curVal = (static_cast<RawInputVectorType *>(vector))->GetValue(offset);
            auto ptr = executionContext->GetArena()->Allocate(sizeof(ResultType));
            *reinterpret_cast<ResultType *>(ptr) = curVal;
            state.val = ptr;
        } else {
            auto curVal = (static_cast<IntermediateVectorType *>(vector))->GetValue(offset);
            auto ptr = executionContext->GetArena()->Allocate(sizeof(ResultType));
            *reinterpret_cast<ResultType *>(ptr) = curVal;
            state.val = ptr;
        }
    }

    void ExtractValues(AggregateState &state, std::vector<Vector *> &vectors, int32_t rowIndex) override
    {
        int32_t offset;
        auto v =
            static_cast<IntermediateVectorType *>(VectorHelper::ExpandVectorAndIndex(vectors[0], rowIndex, offset));
        // all null as null
        if (state.val == nullptr) {
            v->SetValueNull(rowIndex);
            return;
        }
        v->SetValue(rowIndex, *static_cast<ResultType *>(state.val));
    }
};
}
}
#endif // OMNI_RUNTIME_SUM_FLAT_IM_AGGREGATOR_H
