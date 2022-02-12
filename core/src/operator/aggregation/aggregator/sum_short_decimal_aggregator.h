/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Sum aggregate for short decimal
 */
#ifndef OMNI_RUNTIME_SUM_SHORT_DECIMAL_AGGREGATOR_H
#define OMNI_RUNTIME_SUM_SHORT_DECIMAL_AGGREGATOR_H
#include "aggregator.h"
#include "vector/type/decimalOperations.h"

namespace omniruntime {
namespace op {
/**
 * For ProcessGroup the input vector type is LongVec and output vector type is VarcharVec
 */
//static constexpr int32_t PARTIAL_SUM_OUTPUT_LENGTH = 24;

class SumShortDecimalAggregator : public Aggregator {
public:
    SumShortDecimalAggregator(const VecType &in, const VecType &out, int32_t channel) : Aggregator(OMNI_AGGREGATION_TYPE_SUM, in, out, channel) {}

    SumShortDecimalAggregator(const VecType &in, const VecType &out, int32_t channel, bool inputRaw, bool outputPartial)
        : Aggregator(OMNI_AGGREGATION_TYPE_SUM, in, out, channel, inputRaw, outputPartial)
    {}

    ~SumShortDecimalAggregator() override = default;

    void ProcessGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex) override
    {
        int32_t offset;
        Vector *vector = VectorHelper::ExpandVectorAndIndex(vectorBatch->GetVector(channel), rowIndex, offset);
        if (UNLIKELY(vector->IsValueNull(offset))) {
            return;
        }
        if (state.val == nullptr) {
            InitiateGroup(state, vectorBatch, offset);
            return;
        }
        if (inputRaw) {
            // val and state to sum. The value of state.val transforms to overflowFlag(8 bytes) + decimal(16 bytes)
            // 1. get a new value
            int64_t oldOverflow = 0;
            Decimal128 curVal = DecimalOperations::UnscaledDecimal(static_cast<LongVector *>(vector)->GetValue(offset));
            Decimal128 leftVal;
            // 2. decode current state
            DecimalOperations::DecodeSumDecimal(state.val, leftVal, oldOverflow);
            // 3. do calculation
            int64_t newOverflow = DecimalOperations::AddWithOverflow(leftVal, curVal, leftVal);
            oldOverflow += newOverflow;
            // 4. encode to state
            DecimalOperations::EncodeSumDecimal(state.val, leftVal, oldOverflow);
        } else {
            // 1. get a new intermediate value
            uint8_t *otherState = nullptr;
            int64_t oldOverflow = 0;
            int64_t otherOverflow = 0;
            auto length = static_cast<VarcharVector *>(vector)->GetValue(offset, &otherState);
            if (length != PARTIAL_SUM_OUTPUT_LENGTH) {
                LogError("Intermediate decimal length should be 24 bytes");
            }
            // 2. decode current state and intermediate state
            Decimal128 leftVal;
            Decimal128 curVal;
            DecimalOperations::DecodeSumDecimal(state.val, leftVal, oldOverflow);
            DecimalOperations::DecodeSumDecimal(otherState, curVal, otherOverflow);
            // 3. do calculation
            int64_t newOverflow = DecimalOperations::AddWithOverflow(leftVal, curVal, leftVal);
            oldOverflow += newOverflow;
            // 4. encode to state
            DecimalOperations::EncodeSumDecimal(state.val, leftVal, oldOverflow);
        }
    }

    void InitiateGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex) override
    {
        int32_t offset;
        Vector *vector = VectorHelper::ExpandVectorAndIndex(vectorBatch->GetVector(channel), rowIndex, offset);
        if (UNLIKELY(vector->IsValueNull(offset))) {
            return;
        }
        if (inputRaw) {
            // input vector is expected as LongVec
            auto curVal = (static_cast<LongVector *>(vector))->GetValue(offset);

            state.val = executionContext->getArena()->Allocate(PARTIAL_SUM_OUTPUT_LENGTH);
            int64_t overflow = 0;
            Decimal128 initState(curVal);
            DecimalOperations::EncodeSumDecimal(state.val, initState, overflow);
        } else {
            // input vector is expected as VarcharVec
            uint8_t *otherState = nullptr;
            auto length = (static_cast<VarcharVector *>(vector))->GetValue(offset, &otherState);
            if (length != PARTIAL_SUM_OUTPUT_LENGTH) {
                LogError("Intermediate decimal length should be 24 bytes");
            }
            state.val = executionContext->getArena()->Allocate(length);
            state.val = otherState;
        }
    }

    // TODO extract common function for sum/min/max
    void ExtractValue(AggregateState &state, Vector *vector, int32_t rowIndex) override
    {
        if (state.val == nullptr) {
            vector->SetValueNull(rowIndex);
            return;
        }
        if (outputPartial) {
            static_cast<VarcharVector *>(vector)->SetValue(rowIndex, static_cast<uint8_t *>(state.val),
                PARTIAL_SUM_OUTPUT_LENGTH);
        } else {
            // write decimal if not overflow. otherwise throw exception
            int64_t isOverflow = 0;
            Decimal128 result;
            DecimalOperations::DecodeSumDecimal(state.val, result, isOverflow);
            if (isOverflow != 0) {
                LogError("Sum decimal causes overflow!");
            }
            static_cast<Decimal128Vector *>(vector)->SetValue(rowIndex, result);
        }
    }
};
}
}
#endif // OMNI_RUNTIME_SUM_SHORT_DECIMAL_AGGREGATOR_H
