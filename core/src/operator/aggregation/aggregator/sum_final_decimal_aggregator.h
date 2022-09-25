/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: sum aggregate for intermedia data vector are multi vectors
 */

#ifndef OMNI_RUNTIME_SUM_FINAL_DECIMAL_AGGREGATOR_H
#define OMNI_RUNTIME_SUM_FINAL_DECIMAL_AGGREGATOR_H

#include "aggregator.h"
#include "type/decimal_operations.h"


namespace omniruntime {
namespace op {
/**
 * For ProcessGroup the input vector type is LongVec and output vector type is VarcharVec
 */
class SumFinalDecimalAggregator : public Aggregator {
public:
    SumFinalDecimalAggregator(DataTypesPtr inputTypes, DataTypesPtr outputTypes, std::vector<int32_t> &channels)
        : Aggregator(OMNI_AGGREGATION_TYPE_SUM, inputTypes, outputTypes, channels)
    {}

    SumFinalDecimalAggregator(DataTypesPtr inputTypes, DataTypesPtr outputTypes, std::vector<int32_t> &channels,
        bool inputRaw, bool outputPartial)
        : Aggregator(OMNI_AGGREGATION_TYPE_SUM, inputTypes, outputTypes, channels, inputRaw, outputPartial)
    {}

    ~SumFinalDecimalAggregator() override = default;

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

        // 1. get a new intermediate value
        uint8_t *otherState = nullptr;
        int64_t oldOverflow = 0;
        int64_t otherOverflow = 0;
        static_cast<VarcharVector *>(vector)->GetValue(offset, &otherState);
        // 2. decode current state and intermediate state
        Decimal128 leftVal;
        Decimal128 curVal;
        DecimalOperations::DecodeSumDecimal(static_cast<DecimalSumState *>(state.val), leftVal, oldOverflow);
        DecimalOperations::DecodeSumDecimal(reinterpret_cast<DecimalSumState *>(otherState), curVal, otherOverflow);
        // 3. do calculation
        int64_t newOverflow = DecimalOperations::AddWithOverflow(leftVal, curVal, leftVal);
        oldOverflow += newOverflow;
        // 4. encode to state
        DecimalOperations::EncodeSumDecimal(static_cast<DecimalSumState *>(state.val), leftVal, oldOverflow);
    }

    void InitiateGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex) override
    {
        int32_t offset;
        Vector *vector = VectorHelper::ExpandVectorAndIndex(vectorBatch->GetVector(channels[0]), rowIndex, offset);
        if (vector->IsValueNull(offset)) {
            return;
        }

        // input vector is expected as VarcharVec
        uint8_t *otherState = nullptr;
        auto length = (static_cast<VarcharVector *>(vector))->GetValue(offset, &otherState);

        state.val = executionContext->GetArena()->Allocate(length);
        memcpy_s(state.val, length, otherState, length);

        Decimal128 curVal;
        int64_t oldOverflow = 0;
        DecimalOperations::DecodeSumDecimal(static_cast<DecimalSumState *>(state.val), curVal, oldOverflow);
    }

    void ExtractValues(AggregateState &state, std::vector<Vector *> &vectors, int32_t rowIndex) override
    {
        int32_t offset;
        Vector *vector = VectorHelper::ExpandVectorAndIndex(vectors[0], rowIndex, offset);
        if (state.val == nullptr) {
            vector->SetValueNull(rowIndex);
            return;
        }

        // write decimal if not overflow. otherwise throw exception
        int64_t isOverflow = 0;
        Decimal128 result;
        DecimalOperations::DecodeSumDecimal(static_cast<DecimalSumState *>(state.val), result, isOverflow);
        if (isOverflow != 0) {
            throw OmniException("Decimal overflow", "Sum aggregate exceeds maximum.");
        }
        DecimalOperations::ThrowIfOverflows(result);

        static_cast<Decimal128Vector *>(vector)->SetValue(rowIndex, result);
    }
};
}
}

#endif // OMNI_RUNTIME_SUM_FINAL_DECIMAL_AGGREGATOR_H
