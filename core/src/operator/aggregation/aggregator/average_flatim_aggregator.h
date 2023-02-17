/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: average aggregate for intermedia data vector are multi vectors
 *
 *
 */
#ifndef OMNI_RUNTIME_AVERAGE_FLAT_IM_AGGREGATOR_H
#define OMNI_RUNTIME_AVERAGE_FLAT_IM_AGGREGATOR_H

#include "aggregator.h"

namespace omniruntime {
namespace op {
template <bool INPUT_RAW, bool OUT_PARTIAL, typename RawInputVectorType, typename ResultType = double>
class AverageFlatIMAggregator : public Aggregator {
public:
    AverageFlatIMAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels)
        : Aggregator(OMNI_AGGREGATION_TYPE_AVG, inputTypes, outputTypes, channels)
    {}

    AverageFlatIMAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels,
        bool inputRaw, bool outputPartial, bool isOverflowAsNull)
        : Aggregator(OMNI_AGGREGATION_TYPE_AVG, inputTypes, outputTypes, channels, inputRaw, outputPartial,
        isOverflowAsNull)
    {}

    ~AverageFlatIMAggregator() override {}

    void ProcessGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex) override
    {
        int32_t offset;
        Vector *vector = VectorHelper::ExpandVectorAndIndex(vectorBatch->GetVector(channels[0]), rowIndex, offset);
        if (vector->IsValueNull(offset)) {
            return;
        }

        if (state.val == nullptr) {
            this->InitiateGroup(state, vectorBatch, rowIndex);
            return;
        }

        if constexpr (INPUT_RAW) {
            auto currentVal = static_cast<ResultType *>(state.val);
            *reinterpret_cast<ResultType *>(state.val) =
                (static_cast<RawInputVectorType *>(vector))->GetValue(offset) + *currentVal;
            ++state.count;
        } else {
            int32_t avgValOffset;
            auto avgValVector = reinterpret_cast<DoubleVector *>(
                VectorHelper::ExpandVectorAndIndex(vectorBatch->GetVector(channels[0]), rowIndex, avgValOffset));
            double avgVal = avgValVector->GetValue(avgValOffset);

            int32_t avgCountOffset;
            auto avgCountVector = reinterpret_cast<LongVector *>(
                VectorHelper::ExpandVectorAndIndex(vectorBatch->GetVector(channels[1]), rowIndex, avgCountOffset));
            int64_t avgCnt = avgCountVector->GetValue(avgCountOffset);

            auto currentVal = static_cast<ResultType *>(state.val);
            state.count += avgCnt;
            *currentVal += avgVal;
        }
    }

    void InitiateGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex) override
    {
        int32_t offset;
        Vector *vector = VectorHelper::ExpandVectorAndIndex(vectorBatch->GetVector(channels[0]), rowIndex, offset);
        if (vector->IsValueNull(offset)) {
            return;
        }

        // for partial aggregation
        if constexpr (INPUT_RAW) {
            auto rowVal = (static_cast<RawInputVectorType *>(vector))->GetValue(offset);
            auto ptr = executionContext->GetArena()->Allocate(sizeof(ResultType));
            *reinterpret_cast<ResultType *>(ptr) = rowVal;
            state.val = ptr;
            state.count = 1;
        } else {
            int32_t avgValOffset;
            auto avgValVector = reinterpret_cast<DoubleVector *>(
                VectorHelper::ExpandVectorAndIndex(vectorBatch->GetVector(channels[0]), rowIndex, avgValOffset));
            double avgVal = avgValVector->GetValue(avgValOffset);

            int32_t avgCountOffset;
            auto avgCountVector = reinterpret_cast<LongVector *>(
                VectorHelper::ExpandVectorAndIndex(vectorBatch->GetVector(channels[1]), rowIndex, avgCountOffset));
            int64_t avgCnt = avgCountVector->GetValue(avgCountOffset);

            auto ptr = executionContext->GetArena()->Allocate(sizeof(ResultType));
            *reinterpret_cast<ResultType *>(ptr) = avgVal;
            state.val = ptr;
            state.count = avgCnt;
        }
    }

    void ExtractValues(const AggregateState &state, std::vector<Vector *> &vectors, int32_t rowIndex) override
    {
        if constexpr (OUT_PARTIAL) {
            int32_t avgValOffset;
            auto avgValVector = reinterpret_cast<DoubleVector *>(
                VectorHelper::ExpandVectorAndIndex(vectors[0], rowIndex, avgValOffset));

            int32_t avgCountOffset;
            auto avgCountVector = reinterpret_cast<LongVector *>(
                VectorHelper::ExpandVectorAndIndex(vectors[1], rowIndex, avgCountOffset));
            // all input are nulls, return 0
            if (state.val == nullptr) {
                avgValVector->SetValue(rowIndex, 0);
                avgCountVector->SetValue(rowIndex, 0);
                return;
            }

            avgValVector->SetValue(rowIndex, *static_cast<ResultType *>(state.val));
            avgCountVector->SetValue(rowIndex, state.count);
        } else {
            int32_t avgValOffset;
            auto avgValVector = reinterpret_cast<DoubleVector *>(
                VectorHelper::ExpandVectorAndIndex(vectors[0], rowIndex, avgValOffset));
            if (state.count <= 0 || state.val == nullptr) {
                avgValVector->SetValueNull(rowIndex);
                return;
            }
            auto currentVal = *(static_cast<ResultType *>(state.val));
            auto result = currentVal / state.count;
            avgValVector->SetValue(rowIndex, result);
        }
    }
};
}
}
#endif // OMNI_RUNTIME_AVERAGE_FLAT_IM_AGGREGATOR_H
