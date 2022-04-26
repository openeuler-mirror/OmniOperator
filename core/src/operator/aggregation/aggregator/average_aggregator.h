/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Description: Average aggregate
 */
#ifndef OMNI_RUNTIME_AVERAGE_AGGREGATOR_H
#define OMNI_RUNTIME_AVERAGE_AGGREGATOR_H

#include "aggregator.h"

namespace omniruntime {
namespace op {
template <typename V, typename ResultType = double> class AverageAggregator : public Aggregator {
public:
    AverageAggregator(const DataType &in, const DataType &out, int32_t channel)
        : Aggregator(OMNI_AGGREGATION_TYPE_AVG, in, out, channel)
    {}

    AverageAggregator(const DataType &in, const DataType &out, int32_t channel, bool inputRaw, bool outputPartial)
        : Aggregator(OMNI_AGGREGATION_TYPE_AVG, in, out, channel, inputRaw, outputPartial)
    {}

    ~AverageAggregator() override {}

    void ProcessGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex) override
    {
        int32_t offset;
        Vector *vector = VectorHelper::ExpandVectorAndIndex(vectorBatch->GetVector(channel), rowIndex, offset);
        if (vector->IsValueNull(offset)) {
            return;
        }
        if (inputRaw == true) {
            if (state.val == nullptr) {
                this->InitiateGroup(state, vectorBatch, rowIndex);
                return;
            }
            auto currentVal = static_cast<ResultType *>(state.avgVal);
            *reinterpret_cast<ResultType *>(state.avgVal) = (static_cast<V *>(vector))->GetValue(offset) + *currentVal;
            ++state.avgCnt;
        } else {
            if (state.val == nullptr) {
                this->InitiateGroup(state, vectorBatch, rowIndex);
                return;
            }
            auto containerVector = static_cast<ContainerVector *>(vector);
            auto avgValVector = reinterpret_cast<DoubleVector *>(containerVector->GetValue(0));
            auto avgCountVector = reinterpret_cast<LongVector *>(containerVector->GetValue(1));
            double avgVal = avgValVector->GetValue(offset);
            int64_t avgCnt = avgCountVector->GetValue(offset);
            auto currentVal = static_cast<double *>(state.avgVal);
            auto currentCnt = static_cast<int64_t>(state.avgCnt);
            if (avgCnt == 0) {
                // Fixme use error code
                LogError("Divisor should not be zero! Offset = %d", offset);
            }
            state.avgCnt += avgCnt;
            *currentVal += avgVal;
        }
    }

    void InitiateGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex) override
    {
        int32_t offset;
        Vector *vector = VectorHelper::ExpandVectorAndIndex(vectorBatch->GetVector(channel), rowIndex, offset);
        if (vector->IsValueNull(offset)) {
            return;
        }
        // for partial aggregation
        if (inputRaw == true) {
            auto rowVal = (static_cast<V *>(vector))->GetValue(offset);
            auto len = sizeof(ResultType);
            auto ptr = executionContext->GetArena()->Allocate(len);
            *reinterpret_cast<ResultType *>(ptr) = rowVal;
            state.avgVal = ptr;
            state.avgCnt = 1;
        } else {
            auto containerVector = static_cast<ContainerVector *>(vector);
            auto avgValVector = reinterpret_cast<DoubleVector *>(containerVector->GetValue(0));
            auto avgCountVector = reinterpret_cast<LongVector *>(containerVector->GetValue(1));
            double avgVal = avgValVector->GetValue(offset);
            int64_t avgCnt = avgCountVector->GetValue(offset);
            auto ptr = executionContext->GetArena()->Allocate(sizeof(double));
            *reinterpret_cast<double *>(ptr) = avgVal;
            state.avgVal = ptr;
            state.avgCnt = avgCnt;
        }
    }

    void ExtractValue(AggregateState &state, Vector *vector, int32_t rowIndex) override
    {
        if (outputPartial == true) {
            ContainerVector *v = static_cast<ContainerVector *>(vector);
            if (state.val == nullptr) {
                v->SetValueNull(rowIndex);
                auto doubleVector = reinterpret_cast<DoubleVector *>(v->GetValue(0));
                doubleVector->SetValue(rowIndex, 0);
                auto longVector = reinterpret_cast<LongVector *>(v->GetValue(1));
                longVector->SetValue(rowIndex, 0);
                return;
            }
            if (state.avgCnt == 0) {
                LogError("Divisor is zero!");
            }
            auto doubleVector = reinterpret_cast<DoubleVector *>(v->GetValue(0));
            doubleVector->SetValue(rowIndex, *static_cast<double *>(state.avgVal));
            auto longVector = reinterpret_cast<LongVector *>(v->GetValue(1));
            longVector->SetValue(rowIndex, state.avgCnt);
        } else {
            auto v = static_cast<DoubleVector *>(vector);
            if (state.avgCnt <= 0 || state.val == nullptr) {
                v->SetValueNull(rowIndex);
                return;
            }
            auto currentVal = *(static_cast<ResultType *>(state.avgVal));
            auto result = currentVal / state.avgCnt;
            v->SetValue(rowIndex, result);
        }
    }
};
}
}
#endif // OMNI_RUNTIME_AVERAGE_AGGREGATOR_H
