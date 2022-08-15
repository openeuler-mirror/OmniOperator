/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Description: Sum aggregate for long decimal
 */
#ifndef OMNI_RUNTIME_SUM_LONG_DECIMAL_AGGREGATOR_H
#define OMNI_RUNTIME_SUM_LONG_DECIMAL_AGGREGATOR_H

#include "aggregator.h"
#include "type/decimal_operations.h"

namespace omniruntime {
namespace op {
class SumLongDecimalAggregator : public Aggregator {
public:
    SumLongDecimalAggregator(DataTypePtr in, DataTypePtr out, int32_t channel)
        : Aggregator(OMNI_AGGREGATION_TYPE_SUM, in, out, channel)
    {}

    SumLongDecimalAggregator(DataTypePtr in, DataTypePtr out, int32_t channel, bool inputRaw, bool outputPartial)
        : Aggregator(OMNI_AGGREGATION_TYPE_SUM, in, out, channel, inputRaw, outputPartial)
    {}

    ~SumLongDecimalAggregator() override {}

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
        // val and state to sum. The value of state.val transforms to overflowFlag(8 bytes) + decimal(16 bytes)
        // 1. get a new value
        int64_t oldOverflow = 0;
        Decimal128 curVal = static_cast<Decimal128Vector *>(vector)->GetValue(offset);
        Decimal128 leftVal;
        // 2. decode current state
        DecimalOperations::DecodeSumDecimal(static_cast<DecimalSumState *>(state.val), leftVal, oldOverflow);
        // 3. do calculation
        int64_t newOverflow = DecimalOperations::AddWithOverflow(leftVal, curVal, leftVal);
        oldOverflow += newOverflow;
        // 4. encode to state
        DecimalOperations::EncodeSumDecimal(static_cast<DecimalSumState *>(state.val), leftVal, oldOverflow);
    }


    void InitiateGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex) override
    {
        int32_t offset;
        Vector *vector = VectorHelper::ExpandVectorAndIndex(vectorBatch->GetVector(channel), rowIndex, offset);
        if (vector->IsValueNull(offset)) {
            return;
        }
        // input vector is expected as LongVec
        auto curVal = (static_cast<Decimal128Vector *>(vector))->GetValue(offset);

        state.val = executionContext->GetArena()->Allocate(PARTIAL_SUM_OUTPUT_LENGTH);
        int64_t overflow = 0;
        Decimal128 initState(curVal);
        DecimalOperations::EncodeSumDecimal(static_cast<DecimalSumState *>(state.val), initState, overflow);
    }

    void ExtractValue(AggregateState &state, Vector *vector, int32_t rowIndex) override
    {
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

        if (outputPartial) {
            static_cast<VarcharVector *>(vector)->SetValue(rowIndex, static_cast<uint8_t *>(state.val),
                PARTIAL_SUM_OUTPUT_LENGTH);
        } else {
            // this branch is for window operator
            static_cast<Decimal128Vector *>(vector)->SetValue(rowIndex, result);
        }
    }
};
}
}
#endif // OMNI_RUNTIME_SUM_LONG_DECIMAL_AGGREGATOR_H
