/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2023. All rights reserved.
 * Description: For non-decimal type
 */
#ifndef OMNI_RUNTIME_SUM_FLAT_IM_AGGREGATOR_H
#define OMNI_RUNTIME_SUM_FLAT_IM_AGGREGATOR_H

#include "aggregator.h"

namespace omniruntime {
namespace op {
template <bool INPUT_RAW, bool OUTPUT_PARTIAL, typename RawInputVectorType, typename ResultType>
class SumFlatIMAggregator : public Aggregator {
using VECTOR = Vector<RawInputVectorType>;
using DICTVECTOR = Vector<DictionaryContainer<RawInputVectorType>>;
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
        BaseVector *vector = vectorBatch->Get(channels[0]);
        if (vector->IsNull(rowIndex)) {
            return;
        }
        if (state.val == nullptr) {
            InitiateGroup(state, vectorBatch, rowIndex);
            return;
        }
        if constexpr (INPUT_RAW) {
            if (vector->GetEncoding() == OMNI_DICTIONARY) {
                *(static_cast<ResultType *>(state.val)) += (static_cast<DICTVECTOR *>(vector))->GetValue(rowIndex);
            } else {
                *(static_cast<ResultType *>(state.val)) += (static_cast<VECTOR *>(vector))->GetValue(rowIndex);
            }
        } else {
            if (vector->GetEncoding() == OMNI_DICTIONARY) {
                *(static_cast<ResultType *>(state.val)) +=
                    (static_cast<Vector<DictionaryContainer<ResultType>> *>(vector))->GetValue(rowIndex);
            } else {
                *(static_cast<ResultType *>(state.val)) +=
                    (static_cast<Vector<ResultType> *>(vector))->GetValue(rowIndex);
            }
        }
    }

    void InitiateGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex) override
    {
        BaseVector *vector = vectorBatch->Get(channels[0]);
        if (vector->IsNull(rowIndex)) {
            return;
        }
        if constexpr (INPUT_RAW) {
            auto ptr = executionContext->GetArena()->Allocate(sizeof(ResultType));
            if (vector->GetEncoding() == OMNI_DICTIONARY) {
                auto curVal = (static_cast<DICTVECTOR *>(vector))->GetValue(rowIndex);
                *reinterpret_cast<ResultType *>(ptr) = curVal;
                state.val = ptr;
            } else {
                auto curVal = (static_cast<VECTOR *>(vector))->GetValue(rowIndex);
                *reinterpret_cast<ResultType *>(ptr) = curVal;
                state.val = ptr;
            }
        } else {
            auto ptr = executionContext->GetArena()->Allocate(sizeof(ResultType));
            ResultType curVal;
            if (vector->GetEncoding() == OMNI_DICTIONARY) {
                curVal = (static_cast<Vector<DictionaryContainer<ResultType>> *>(vector))->GetValue(rowIndex);
            } else {
                curVal = (static_cast<Vector<ResultType> *>(vector))->GetValue(rowIndex);
            }
            *reinterpret_cast<ResultType *>(ptr) = curVal;
            state.val = ptr;
        }
    }

    void ExtractValues(const AggregateState &state, std::vector<BaseVector *> &vectors, int32_t rowIndex) override
    {
        auto v = static_cast<Vector<ResultType> *>(vectors[0]);
        // all null as null
        if (state.val == nullptr) {
            v->SetNull(rowIndex);
            return;
        }
        v->SetValue(rowIndex, *static_cast<ResultType *>(state.val));
    }
};
}
}
#endif // OMNI_RUNTIME_SUM_FLAT_IM_AGGREGATOR_H
