/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2023. All rights reserved.
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
using FixedVector = Vector<RawInputVectorType>;
using DicVector = Vector<DictionaryContainer<RawInputVectorType>>;
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
        BaseVector *vector = vectorBatch->Get(channels[0]);
        if (vector->IsNull(rowIndex)) {
            return;
        }

        if (state.val == nullptr) {
            this->InitiateGroup(state, vectorBatch, rowIndex);
            return;
        }

        if constexpr (INPUT_RAW) {
            auto currentVal = static_cast<ResultType *>(state.val);
            if (vector->GetEncoding() == OMNI_DICTIONARY) {
                *reinterpret_cast<ResultType *>(state.val) =
                    (static_cast<DicVector *>(vector))->GetValue(rowIndex) + *currentVal;
            } else {
                *reinterpret_cast<ResultType *>(state.val) =
                    (static_cast<FixedVector *>(vector))->GetValue(rowIndex) + *currentVal;
            }
            ++state.count;
        } else {
            double avgVal;
            if (vector->GetEncoding() == OMNI_DICTIONARY) {
                avgVal = static_cast<Vector<DictionaryContainer<double>> *>(vector)->GetValue(rowIndex);
            } else {
                avgVal = static_cast<Vector<double> *>(vector)->GetValue(rowIndex);
            }

            auto avgCountVector = vectorBatch->Get(channels[1]);
            int64_t avgCnt;
            if (avgCountVector->GetEncoding() == OMNI_DICTIONARY) {
                avgCnt = static_cast<Vector<DictionaryContainer<int64_t>> *>(avgCountVector)->GetValue(rowIndex);
            } else {
                avgCnt = static_cast<Vector<int64_t> *>(avgCountVector)->GetValue(rowIndex);
            }

            auto currentVal = static_cast<ResultType *>(state.val);
            state.count += avgCnt;
            *currentVal += avgVal;
        }
    }

    void InitiateGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex) override
    {
        BaseVector *vector = vectorBatch->Get(channels[0]);
        if (vector->IsNull(rowIndex)) {
            return;
        }

        // for partial aggregation
        if constexpr (INPUT_RAW) {
            auto ptr = executionContext->GetArena()->Allocate(sizeof(ResultType));
            if (vector->GetEncoding() == OMNI_DICTIONARY) {
                auto rowVal = (static_cast<DicVector *>(vector))->GetValue(rowIndex);
                *reinterpret_cast<ResultType *>(ptr) = rowVal;
            } else {
                auto rowVal = (static_cast<FixedVector *>(vector))->GetValue(rowIndex);
                *reinterpret_cast<ResultType *>(ptr) = rowVal;
            }
            state.val = ptr;
            state.count = 1;
        } else {
            double avgVal;
            if (vector->GetEncoding() == OMNI_DICTIONARY) {
                avgVal = static_cast<Vector<DictionaryContainer<double>> *>(vector)->GetValue(rowIndex);
            } else {
                avgVal = static_cast<Vector<double> *>(vector)->GetValue(rowIndex);
            }

            auto avgCountVector = vectorBatch->Get(channels[1]);
            int64_t avgCnt;
            if (avgCountVector->GetEncoding() == OMNI_DICTIONARY) {
                avgCnt = static_cast<Vector<DictionaryContainer<int64_t>> *>(avgCountVector)->GetValue(rowIndex);
            } else {
                avgCnt = static_cast<Vector<int64_t> *>(avgCountVector)->GetValue(rowIndex);
            }
            auto ptr = executionContext->GetArena()->Allocate(sizeof(ResultType));
            *reinterpret_cast<ResultType *>(ptr) = avgVal;
            state.val = ptr;
            state.count = avgCnt;
        }
    }

    void ExtractValues(const AggregateState &state, std::vector<BaseVector *> &vectors, int32_t rowIndex) override
    {
        if constexpr (OUT_PARTIAL) {
            auto avgValVector = static_cast<Vector<double> *>(vectors[0]);
            auto avgCountVector = static_cast<Vector<int64_t> *>(vectors[1]);
            // all input are nulls, return 0
            if (state.val == nullptr) {
                avgValVector->SetValue(rowIndex, 0);
                avgCountVector->SetValue(rowIndex, 0);
                return;
            }

            avgValVector->SetValue(rowIndex, *static_cast<ResultType *>(state.val));
            avgCountVector->SetValue(rowIndex, state.count);
        } else {
            auto avgValVector = static_cast<Vector<double> *>(vectors[0]);
            if (state.count <= 0 || state.val == nullptr) {
                avgValVector->SetNull(rowIndex);
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
