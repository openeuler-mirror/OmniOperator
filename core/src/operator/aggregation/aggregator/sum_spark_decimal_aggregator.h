/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Aggregate factories
 */

#ifndef OMNI_RUNTIME_SUM_SPARK_DECIMAL_AGGREGATOR_H
#define OMNI_RUNTIME_SUM_SPARK_DECIMAL_AGGREGATOR_H

#include "aggregator.h"
#include "type/decimal_operations.h"


namespace omniruntime {
namespace op {
/**
 * SUM agg data type
 * input: decimal
 * middle: decimal+boolean
 * final: decimal
 */
class SumSparkDecimalAggregator : public Aggregator {
public:
    SumSparkDecimalAggregator(DataTypesPtr inputTypes, DataTypesPtr outputTypes, std::vector<int32_t> &channels)
        : Aggregator(OMNI_AGGREGATION_TYPE_SUM, inputTypes, outputTypes, channels)
    {}

    SumSparkDecimalAggregator(DataTypesPtr inputTypes, DataTypesPtr outputTypes, std::vector<int32_t> &channels,
        bool inputRaw, bool outputPartial, bool isOverflowAsNull)
        : Aggregator(OMNI_AGGREGATION_TYPE_SUM, inputTypes, outputTypes, channels, inputRaw, outputPartial,
        isOverflowAsNull)
    {}

    ~SumSparkDecimalAggregator() override = default;

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

        auto inputType = inputTypes->GetIds()[0];
        if (inputRaw) {
            // val and state to sum. The value of state.val transforms to overflowFlag(8 bytes) + decimal(16 bytes)
            // 1. get a new value
            int64_t oldOverflow = 0;
            Decimal128 curVal;
            if (inputType == OMNI_DECIMAL64) {
                curVal = DecimalOperations::UnscaledDecimal(static_cast<LongVector *>(vector)->GetValue(offset));
            } else if (inputType == OMNI_DECIMAL128) {
                curVal = static_cast<Decimal128Vector *>(vector)->GetValue(offset);
            }
            Decimal128 leftVal;
            // 2. decode current state
            DecimalOperations::DecodeSumDecimal(static_cast<DecimalSumState *>(state.val), leftVal, oldOverflow);
            // 3. if overflowed, no need to do calculation
            if (oldOverflow > 0) {
                return;
            }
            // 4. do calculation
            int64_t newOverflow = DecimalOperations::AddWithOverflow(leftVal, curVal, leftVal);
            oldOverflow += newOverflow;
            // 5. encode to state
            DecimalOperations::EncodeSumDecimal(static_cast<DecimalSumState *>(state.val), leftVal, oldOverflow);
        } else {
            // get value from containerVector
            Decimal128 curVal;
            if (inputType == OMNI_DECIMAL64) {
                curVal = DecimalOperations::UnscaledDecimal(reinterpret_cast<LongVector *>(vector)->GetValue(offset));
            } else if (inputType == OMNI_DECIMAL128) {
                curVal = reinterpret_cast<Decimal128Vector *>(vector)->GetValue(offset);
            }
            int32_t sumOverflowOffset;
            Vector *sumOverflowVector =
                VectorHelper::ExpandVectorAndIndex(vectorBatch->GetVector(channels[1]), rowIndex, sumOverflowOffset);
            int64_t sumOverflow =
                reinterpret_cast<BooleanVector *>(sumOverflowVector)->GetValue(sumOverflowOffset) ? 1 : 0;

            // 2. decode current state and intermediate state
            Decimal128 leftVal;
            int64_t oldOverflow = sumOverflow;
            DecimalOperations::DecodeSumDecimal(static_cast<DecimalSumState *>(state.val), leftVal, oldOverflow);
            // 3. if overflowed, no need to do calculation
            if (oldOverflow > 0) {
                return;
            }
            // 4. do calculation
            int64_t newOverflow = DecimalOperations::AddWithOverflow(leftVal, curVal, leftVal);
            oldOverflow += newOverflow;
            // 5. encode to state
            DecimalOperations::EncodeSumDecimal(static_cast<DecimalSumState *>(state.val), leftVal, oldOverflow);
        }
    }

    void InitiateGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex) override
    {
        int32_t offset;
        Vector *vector = VectorHelper::ExpandVectorAndIndex(vectorBatch->GetVector(channels[0]), rowIndex, offset);
        if (vector->IsValueNull(offset)) {
            return;
        }

        if (inputRaw) {
            Decimal128 initState;
            if (inputTypes->GetIds()[0] == OMNI_DECIMAL64) {
                initState = DecimalOperations::UnscaledDecimal((static_cast<LongVector *>(vector))->GetValue(offset));
            } else if (inputTypes->GetIds()[0] == OMNI_DECIMAL128) {
                initState = (static_cast<Decimal128Vector *>(vector))->GetValue(offset);
            }
            int64_t oldOverflow = 0;
            state.val = executionContext->GetArena()->Allocate(PARTIAL_SUM_OUTPUT_LENGTH);
            DecimalOperations::EncodeSumDecimal(static_cast<DecimalSumState *>(state.val), initState, oldOverflow);
        } else {
            // get value from containerVector
            Decimal128 curVal;
            if (inputTypes->GetIds()[0] == OMNI_DECIMAL64) {
                curVal = DecimalOperations::UnscaledDecimal(reinterpret_cast<LongVector *>(vector)->GetValue(offset));
            } else if (inputTypes->GetIds()[0] == OMNI_DECIMAL128) {
                curVal = reinterpret_cast<Decimal128Vector *>(vector)->GetValue(offset);
            }
            int32_t sumOverflowOffset;
            Vector *sumOverflowVector =
                VectorHelper::ExpandVectorAndIndex(vectorBatch->GetVector(channels[1]), rowIndex, sumOverflowOffset);
            int64_t sumOverflow =
                reinterpret_cast<BooleanVector *>(sumOverflowVector)->GetValue(sumOverflowOffset) ? 1 : 0;

            state.val = executionContext->GetArena()->Allocate(PARTIAL_SUM_OUTPUT_LENGTH);
            DecimalOperations::EncodeSumDecimal(static_cast<DecimalSumState *>(state.val), curVal, sumOverflow);
        }
    }

    void ExtractValues(AggregateState &state, std::vector<Vector *> &vectors, int32_t rowIndex) override
    {
        int32_t offset;
        Vector *vector = VectorHelper::ExpandVectorAndIndex(vectors[0], rowIndex, offset);
        if (state.val == nullptr) {
            vector->SetValueNull(rowIndex);
            return;
        }

        int64_t overflowAccumulator = 0;
        Decimal128 decodedDec;
        DecimalOperations::DecodeSumDecimal(static_cast<DecimalSumState *>(state.val), decodedDec, overflowAccumulator);

        Decimal128 resultDec;
        // only support output scale >= input scale
        // for spark, input type is always decimal. for olk, input type is varbinary and the precision
        // and scale are zero.
        int32_t scaleDiff = static_cast<DecimalDataType *>(outputTypes->GetType(0).get())->GetScale() -
            static_cast<DecimalDataType *>(inputTypes->GetType(0).get())->GetScale();
        auto outputType = outputTypes->GetIds()[0];
        // rescale dividend and divisor to output scale
        OpStatus status = DecimalOperations::Rescale128(decodedDec, scaleDiff, resultDec);
        bool isOverflow = overflowAccumulator > 0 || status == OP_OVERFLOW;
        if (isOverflow) {
            SetNullOrThrowException(vector, rowIndex);
        }

        if (outputPartial) {
            if (outputType == OMNI_DECIMAL64) {
                auto longVector = reinterpret_cast<LongVector *>(vector);
                int64_t low = resultDec.LowBits();
                int64_t shortResult = DecimalOperations::IsNegative(resultDec) ? -low : low;
                longVector->SetValue(rowIndex, shortResult);
            } else if (outputType == OMNI_DECIMAL128) {
                auto decimal128Vector = reinterpret_cast<Decimal128Vector *>(vector);
                static_cast<Decimal128Vector *>(decimal128Vector)->SetValue(rowIndex, resultDec);
            }

            int32_t sumOverflowOffset;
            Vector *sumOverflowVector = VectorHelper::ExpandVectorAndIndex(vectors[1], rowIndex, sumOverflowOffset);
            reinterpret_cast<BooleanVector *>(sumOverflowVector)->SetValue(rowIndex, isOverflow ? true : false);
        } else {
            if (outputType == OMNI_DECIMAL64) {
                // restore sign
                int64_t low = resultDec.LowBits();
                int64_t shortResult = DecimalOperations::IsNegative(resultDec) ? -low : low;
                static_cast<LongVector *>(vector)->SetValue(rowIndex, shortResult);
            } else {
                static_cast<Decimal128Vector *>(vector)->SetValue(rowIndex, resultDec);
            }
        }
    }

private:
    // set vector value null or throw exception when overflow
    void SetNullOrThrowException(Vector *vector, int index)
    {
        if (!IsOverflowAsNull()) {
            throw OmniException("OPERATOR_RUNTIME_ERROR", "Overflow in sum of decimals");
        }
        vector->SetValueNull(index);
    }
};
}
}

#endif // OMNI_RUNTIME_SUM_SPARK_DECIMAL_AGGREGATOR_H
