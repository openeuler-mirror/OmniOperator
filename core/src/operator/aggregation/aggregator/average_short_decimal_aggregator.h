/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Average aggregate for short decimal
 */
#ifndef OMNI_RUNTIME_AVERAGE_SHORT_DECIMAL_AGGREGATOR_H
#define OMNI_RUNTIME_AVERAGE_SHORT_DECIMAL_AGGREGATOR_H
#include "aggregator.h"
namespace omniruntime {
namespace op {
static constexpr int32_t PARTIAL_AVG_OUTPUT_LENGTH = 32;

class AverageShortDecimalAggregator : public Aggregator {
public:
    AverageShortDecimalAggregator(const DataType &in, const DataType &out, int32_t channel)
        : Aggregator(OMNI_AGGREGATION_TYPE_AVG, in, out, channel)
    {}

    AverageShortDecimalAggregator(const DataType &in, const DataType &out, int32_t channel, bool inputRaw,
        bool outputPartial)
        : Aggregator(OMNI_AGGREGATION_TYPE_AVG, in, out, channel, inputRaw, outputPartial)
    {}

    ~AverageShortDecimalAggregator() override {}

    void ProcessGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex) override
    {
        int32_t offset;
        Vector *vector = VectorHelper::ExpandVectorAndIndex(vectorBatch->GetVector(channel), rowIndex, offset);
        if (UNLIKELY(vector->IsValueNull(offset))) {
            return;
        }
        if (state.val == nullptr) {
            InitiateGroup(state, vectorBatch, rowIndex);
            return;
        }
        if (inputRaw) {
            if (vector->GetTypeId() != OMNI_LONG) {
                LogError("Partial short decimal average should input long.");
            }
            // val and state to sum. The value of state.val transforms to overflowFlag(8 bytes) + decimal(16 bytes)
            // 1. get a new value
            int64_t oldOverflow = 0;
            int64_t oldCount = 0;
            Decimal128 curVal = DecimalOperations::UnscaledDecimal(static_cast<LongVector *>(vector)->GetValue(offset));
            Decimal128 leftVal;
            // 2. decode current state
            DecimalOperations::DecodeAvgDecimal(state.val, leftVal, oldOverflow, oldCount);
            // 3. do calculation
            int64_t newOverflow = DecimalOperations::AddWithOverflow(leftVal, curVal, leftVal);
            oldOverflow += newOverflow;
            ++oldCount;
            // 4. encode to state
            DecimalOperations::EncodeAvgDecimal(state.val, leftVal, oldOverflow, oldCount);
        } else {
            if (vector->GetTypeId() != OMNI_VARCHAR) {
                LogError("Partial short decimal average should input long.");
            }
            // 1. get a new intermediate value
            uint8_t *otherState = nullptr;
            int64_t oldOverflow = 0;
            int64_t oldCount = 0;
            int64_t otherOverflow = 0;
            int64_t otherCount = 0;
            auto length = static_cast<VarcharVector *>(vector)->GetValue(offset, &otherState);
            if (length != PARTIAL_AVG_OUTPUT_LENGTH) {
                LogError("Intermediate decimal length should be 24 bytes");
            }
            // 2. decode current state and intermediate state
            Decimal128 leftVal;
            Decimal128 curVal;
            DecimalOperations::DecodeAvgDecimal(state.val, leftVal, oldOverflow, oldCount);
            DecimalOperations::DecodeAvgDecimal(otherState, curVal, otherOverflow, otherCount);
            // 3. do calculation
            int64_t newOverflow = DecimalOperations::AddWithOverflow(leftVal, curVal, leftVal);
            oldOverflow += newOverflow;
            oldCount += otherCount;
            // 4. encode to state
            DecimalOperations::EncodeAvgDecimal(state.val, leftVal, oldOverflow, oldCount);
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
            if (vector->GetTypeId() != OMNI_LONG) {
                LogError("Partial short decimal average should input long.");
            }
            // input vector is expected as LongVec
            auto curVal = (static_cast<LongVector *>(vector))->GetValue(offset);

            state.val = executionContext->GetArena()->Allocate(PARTIAL_AVG_OUTPUT_LENGTH);
            Decimal128 initState = DecimalOperations::UnscaledDecimal(curVal);
            DecimalOperations::EncodeAvgDecimal(state.val, initState, 0, 1);
        } else {
            if (vector->GetTypeId() != OMNI_VARCHAR) {
                LogError("Final short decimal average should input varbinary.");
            }
            // input vector is expected as VarcharVec
            uint8_t *otherState = nullptr;
            auto length = (static_cast<VarcharVector *>(vector))->GetValue(offset, &otherState);
            if (length != PARTIAL_AVG_OUTPUT_LENGTH) {
                LogError("Intermediate decimal length should be 24 bytes");
            }
            state.val = executionContext->GetArena()->Allocate(length);
            memcpy_s(state.val, length, otherState, length);
        }
    }

    void ExtractValue(AggregateState &state, Vector *vector, int32_t rowIndex) override
    {
        if (state.val == nullptr) {
            vector->SetValueNull(rowIndex);
            return;
        }
        if (outputPartial) {
            if (vector->GetTypeId() != OMNI_VARCHAR) {
                LogError("Partial short decimal average should output varbinary.");
            }
            static_cast<VarcharVector *>(vector)->SetValue(rowIndex, static_cast<uint8_t *>(state.val),
                PARTIAL_AVG_OUTPUT_LENGTH);
        } else {
            if (vector->GetTypeId() != OMNI_LONG) {
                LogError("Final short decimal average should output long.");
            }
            // write decimal if not overflow. otherwise throw exception
            int64_t overflowAccumulator = 0;
            int64_t count = 0;
            Decimal128 decodedDec;
            DecimalOperations::DecodeAvgDecimal(state.val, decodedDec, overflowAccumulator, count);
            // TODO we do not support Decimal256 now. thus we cannot handle overflow.
            if (overflowAccumulator != 0) {
                LogError("The sum of short decimal average is overflow.");
            }

            Decimal128 resultDec;
            Decimal128 remainder;
            Decimal128 countDec = count;
            decodedDec.Divide(countDec, resultDec, remainder);
            DecimalOperations::RoundUp(decodedDec, countDec, resultDec, remainder);
            static_cast<LongVector *>(vector)->SetValue(rowIndex, resultDec.LowBits());
        }
    }
};
}
}
#endif // OMNI_RUNTIME_AVERAGE_SHORT_DECIMAL_AGGREGATOR_H
