/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Description: Sum aggregate for short decimal
 */
#ifndef OMNI_RUNTIME_SUM_SHORT_DECIMAL_AGGREGATOR_H
#define OMNI_RUNTIME_SUM_SHORT_DECIMAL_AGGREGATOR_H

#include "aggregator.h"
#include "type/decimal_operations.h"

namespace omniruntime {
namespace op {
/**
 * For ProcessGroup the input vector type is LongVec and output vector type is VarcharVec
 */

class SumShortDecimalAggregator : public Aggregator {
public:
    SumShortDecimalAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels)
        : Aggregator(OMNI_AGGREGATION_TYPE_SUM, inputTypes, outputTypes, channels)
    {}

    SumShortDecimalAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels,
        bool inputRaw, bool outputPartial)
        : Aggregator(OMNI_AGGREGATION_TYPE_SUM, inputTypes, outputTypes, channels, inputRaw, outputPartial)
    {}

    ~SumShortDecimalAggregator() override = default;

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
        // val and state to sum. The value of state.val transforms to overflowFlag(8 bytes) + decimal(16 bytes)
        // 1. get a new value
        int64_t oldOverflow = 0;
        int128 curVal = static_cast<LongVector *>(vector)->GetValue(offset);
        int128 leftVal;
        // 2. decode current state
        DecimalOperations::DecodeSumDecimal(static_cast<DecimalSumState *>(state.val), leftVal, oldOverflow);
        // 3. do calculation
        int64_t newOverflow = static_cast<int64_t>(AddCheckedOverflow(leftVal, curVal, leftVal));
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
        // input vector is expected as LongVec
        auto curVal = (static_cast<LongVector *>(vector))->GetValue(offset);

        state.val = executionContext->GetArena()->Allocate(PARTIAL_SUM_OUTPUT_LENGTH);
        int128 initState = curVal;
        DecimalOperations::EncodeSumDecimal(static_cast<DecimalSumState *>(state.val), initState, 0);
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
        int128 result;
        DecimalOperations::DecodeSumDecimal(static_cast<DecimalSumState *>(state.val), result, isOverflow);
        if (isOverflow != 0) {
            throw OmniException("Decimal overflow", "Sum aggregate exceeds maximum.");
        }

        if (outputPartial) {
            static_cast<VarcharVector *>(vector)->SetValue(rowIndex, static_cast<uint8_t *>(state.val),
                PARTIAL_SUM_OUTPUT_LENGTH);
        } else {
            // this branch is for window operator
            static_cast<Decimal128Vector *>(vector)->SetValue(rowIndex, Decimal128(result));
        }
    }
};
}
}
#endif // OMNI_RUNTIME_SUM_SHORT_DECIMAL_AGGREGATOR_H
