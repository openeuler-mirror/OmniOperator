/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Description: Average aggregate for short decimal
 */
#ifndef OMNI_RUNTIME_AVERAGE_DECIMAL_AGGREGATOR_H
#define OMNI_RUNTIME_AVERAGE_DECIMAL_AGGREGATOR_H

#include "aggregator.h"

namespace omniruntime {
namespace op {
class AverageDecimalAggregator : public Aggregator {
public:
    AverageDecimalAggregator(const DataType &in, const DataType &out, int32_t channel)
        : Aggregator(OMNI_AGGREGATION_TYPE_AVG, in, out, channel)
    {}

    AverageDecimalAggregator(const DataType &in, const DataType &out, int32_t channel, bool inputRaw,
        bool outputPartial)
        : Aggregator(OMNI_AGGREGATION_TYPE_AVG, in, out, channel, inputRaw, outputPartial)
    {}

    ~AverageDecimalAggregator() override {}

    void ProcessGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex) override
    {
        int32_t offset;
        Vector *vector = VectorHelper::ExpandVectorAndIndex(vectorBatch->GetVector(channel), rowIndex, offset);
        // null rows dont count
        if (vector->IsValueNull(offset)) {
            return;
        }
        if (state.val == nullptr) {
            InitiateGroup(state, vectorBatch, rowIndex);
            return;
        }
        if (inputRaw) {
            // val and state to sum. The value of state.val transforms to overflowFlag(8 bytes) + decimal(16 bytes)
            // 1. get a new value
            int64_t oldOverflow = 0;
            int64_t oldCount = 0;
            Decimal128 curVal;
            if (inputType.GetId() == OMNI_DECIMAL64) {
                curVal = DecimalOperations::UnscaledDecimal(static_cast<LongVector *>(vector)->GetValue(offset));
            } else if (inputType.GetId() == OMNI_DECIMAL128) {
                curVal = static_cast<Decimal128Vector *>(vector)->GetValue(offset);
            }
            Decimal128 leftVal;
            // 2. decode current state
            DecimalOperations::DecodeAvgDecimal(static_cast<DecimalAverageState *>(state.val), leftVal, oldOverflow,
                oldCount);
            // 3. do calculation
            int64_t newOverflow = DecimalOperations::AddWithOverflow(leftVal, curVal, leftVal);
            oldOverflow += newOverflow;
            ++oldCount;
            // 4. encode to state
            DecimalOperations::EncodeAvgDecimal(static_cast<DecimalAverageState *>(state.val), leftVal, oldOverflow,
                oldCount);
        } else {
            // 1. get a new intermediate value
            uint8_t *otherState = nullptr;
            int64_t oldOverflow = 0;
            int64_t oldCount = 0;
            int64_t otherOverflow = 0;
            int64_t otherCount = 0;
            static_cast<VarcharVector *>(vector)->GetValue(offset, &otherState);
            // 2. decode current state and intermediate state
            Decimal128 leftVal;
            Decimal128 curVal;
            DecimalOperations::DecodeAvgDecimal(static_cast<DecimalAverageState *>(state.val), leftVal, oldOverflow,
                oldCount);
            DecimalOperations::DecodeAvgDecimal(reinterpret_cast<DecimalAverageState *>(otherState), curVal,
                otherOverflow, otherCount);
            // 3. do calculation
            int64_t newOverflow = DecimalOperations::AddWithOverflow(leftVal, curVal, leftVal);
            oldOverflow += newOverflow;
            oldCount += otherCount;
            // 4. encode to state
            DecimalOperations::EncodeAvgDecimal(static_cast<DecimalAverageState *>(state.val), leftVal, oldOverflow,
                oldCount);
        }
    }

    void InitiateGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex) override
    {
        int32_t offset;
        Vector *vector = VectorHelper::ExpandVectorAndIndex(vectorBatch->GetVector(channel), rowIndex, offset);
        if (vector->IsValueNull(offset)) {
            return;
        }
        if (inputRaw) {
            Decimal128 initState;
            if (inputType.GetId() == OMNI_DECIMAL64) {
                initState = DecimalOperations::UnscaledDecimal((static_cast<LongVector *>(vector))->GetValue(offset));
            } else if (inputType.GetId() == OMNI_DECIMAL128) {
                initState = (static_cast<Decimal128Vector *>(vector))->GetValue(offset);
            }

            state.val = executionContext->GetArena()->Allocate(PARTIAL_AVG_OUTPUT_LENGTH);
            DecimalOperations::EncodeAvgDecimal(static_cast<DecimalAverageState *>(state.val), initState, 0, 1);
        } else {
            // input vector is expected as VarcharVec
            uint8_t *otherState = nullptr;
            auto length = (static_cast<VarcharVector *>(vector))->GetValue(offset, &otherState);
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

        int64_t overflowAccumulator = 0;
        int64_t count = 0;
        Decimal128 decodedDec;
        DecimalOperations::DecodeAvgDecimal(static_cast<DecimalAverageState *>(state.val), decodedDec,
            overflowAccumulator, count);

        // currently if intermediate sum overflow decimal 128, will throw exception. To fix it once support decimal 256.
        if (overflowAccumulator != 0) {
            throw OmniException("Decimal overflow", "Sum for average aggregate exceeds maximum.");
        }
        DecimalOperations::ThrowIfOverflows(decodedDec);

        if (outputPartial) {
            static_cast<VarcharVector *>(vector)->SetValue(rowIndex, static_cast<uint8_t *>(state.val),
                PARTIAL_AVG_OUTPUT_LENGTH);
        } else {
            Decimal128 resultDec;
            Decimal128 countDec = count;
            // only support output scale >= input scale
            int32_t scaleDiff = 0;
            // for spark, input type is always decimal. for olk, input type is varbinary and the precision
            // and scale are zero.
            auto outType = outputType.GetId();
            auto inType = inputType.GetId();
            if (inType == OMNI_DECIMAL64 || inType == OMNI_DECIMAL128) {
                scaleDiff = outputType.GetScale() - inputType.GetScale();
            }
            Decimal128 rescaledDividend;
            // rescale dividend and divisor to output scale
            DecimalOperations::Rescale128(decodedDec, scaleDiff, rescaledDividend);
            resultDec = DecimalOperations::DivideRoundUp(rescaledDividend, countDec, 0, 0);
            if (outType == OMNI_DECIMAL64) {
                // restore sign
                int64_t low = resultDec.LowBits();
                int64_t shortResult = DecimalOperations::IsNegative(resultDec) ? -low : low;
                static_cast<LongVector *>(vector)->SetValue(rowIndex, shortResult);
            } else {
                static_cast<Decimal128Vector *>(vector)->SetValue(rowIndex, resultDec);
            }
        }
    }
};
}
}
#endif // OMNI_RUNTIME_AVERAGE_DECIMAL_AGGREGATOR_H
