/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: Average aggregate for short decimal
 */
#ifndef OMNI_RUNTIME_AVERAGE_SPARK_DECIMAL_AGGREGATOR_H
#define OMNI_RUNTIME_AVERAGE_SPARK_DECIMAL_AGGREGATOR_H

#include "aggregator.h"
#include "type/decimal_operations.h"

namespace omniruntime {
namespace op {
class AverageSparkDecimalAggregator : public Aggregator {
public:
    AverageSparkDecimalAggregator(DataTypesPtr inputTypes, DataTypesPtr outputTypes, std::vector<int32_t> &channels)
        : Aggregator(OMNI_AGGREGATION_TYPE_AVG, inputTypes, outputTypes, channels)
    {}

    AverageSparkDecimalAggregator(DataTypesPtr inputTypes, DataTypesPtr outputTypes, std::vector<int32_t> &channels,
        bool inputRaw, bool outputPartial, bool isOverflowAsNull)
        : Aggregator(OMNI_AGGREGATION_TYPE_AVG, inputTypes, outputTypes, channels, inputRaw, outputPartial,
        isOverflowAsNull)
    {}

    ~AverageSparkDecimalAggregator() override {}

    void ProcessGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex) override
    {
        int32_t offset;
        Vector *vector = VectorHelper::ExpandVectorAndIndex(vectorBatch->GetVector(channels[0]), rowIndex, offset);
        // null rows dont count
        if (vector->IsValueNull(offset)) {
            return;
        }
        if (state.val == nullptr) {
            InitiateGroup(state, vectorBatch, rowIndex);
            return;
        }

        if (inputRaw) {
            ProcessGroupInputRaw(state, vector, offset);
        } else {
            // get value from containerVector
            Decimal128 curVal;
            if (inputTypes->GetIds()[0] == OMNI_DECIMAL64) {
                curVal = DecimalOperations::UnscaledDecimal(
                    reinterpret_cast<LongVector *>(vectorBatch->GetVector(channels[0]))->GetValue(offset));
            } else if (inputTypes->GetIds()[0] == OMNI_DECIMAL128) {
                curVal = reinterpret_cast<Decimal128Vector *>(vector)->GetValue(offset);
            }
            int32_t avgCountOffset;
            Vector *avgCountVector =
                VectorHelper::ExpandVectorAndIndex(vectorBatch->GetVector(channels[1]), rowIndex, avgCountOffset);
            int64_t avgCnt = reinterpret_cast<LongVector *>(avgCountVector)->GetValue(avgCountOffset);

            // 2. decode current state and intermediate state
            Decimal128 leftVal;
            int64_t oldOverflow = 0;
            int64_t oldCount = 0;
            DecimalOperations::DecodeAvgDecimal(static_cast<DecimalAverageState *>(state.val), leftVal, oldOverflow,
                oldCount);
            // 3. if overflowed, no need to do calculation
            if (oldOverflow > 0) {
                oldCount += avgCnt;
                DecimalOperations::EncodeAvgDecimal(static_cast<DecimalAverageState *>(state.val), leftVal, oldOverflow,
                    oldCount);
                return;
            }
            // 4. do calculation
            int64_t newOverflow = DecimalOperations::AddWithOverflow(leftVal, curVal, leftVal);
            oldCount += avgCnt;
            oldOverflow += newOverflow;

            DecimalOperations::EncodeAvgDecimal(static_cast<DecimalAverageState *>(state.val), leftVal, oldOverflow,
                oldCount);
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

            state.val = executionContext->GetArena()->Allocate(PARTIAL_AVG_OUTPUT_LENGTH);
            DecimalOperations::EncodeAvgDecimal(static_cast<DecimalAverageState *>(state.val), initState, 0, 1);
        } else {
            // get value from containerVector
            Decimal128 curVal;
            if (inputTypes->GetIds()[0] == OMNI_DECIMAL64) {
                curVal = DecimalOperations::UnscaledDecimal(reinterpret_cast<LongVector *>(vector)->GetValue(offset));
            } else if (inputTypes->GetIds()[0] == OMNI_DECIMAL128) {
                curVal = reinterpret_cast<Decimal128Vector *>(vector)->GetValue(offset);
            }

            int32_t avgCountOffset;
            Vector *avgCountVector =
                VectorHelper::ExpandVectorAndIndex(vectorBatch->GetVector(channels[1]), rowIndex, avgCountOffset);
            int64_t avgCnt = reinterpret_cast<LongVector *>(avgCountVector)->GetValue(avgCountOffset);

            state.val = executionContext->GetArena()->Allocate(PARTIAL_AVG_OUTPUT_LENGTH);
            DecimalOperations::EncodeAvgDecimal(static_cast<DecimalAverageState *>(state.val), curVal, 0, avgCnt);
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
        int64_t count = 0;
        Decimal128 decodedDec;
        DecimalOperations::DecodeAvgDecimal(static_cast<DecimalAverageState *>(state.val), decodedDec,
            overflowAccumulator, count);
        Decimal128 countDec = count;

        auto outputDecimalType = static_cast<DecimalDataType *>(outputTypes->GetType(0).get());
        auto inputDecimalType = static_cast<DecimalDataType *>(inputTypes->GetType(0).get());

        if (overflowAccumulator > 0) {
            SetNullOrThrowException(vector, rowIndex);
            return;
        }

        if (outputPartial) {
            int32_t scaleDiff = outputDecimalType->GetScale() - inputDecimalType->GetScale();
            Decimal128 resultDec;
            // rescale dividend and divisor to output scale.
            // In Partial mode, if input type is Decimal(p,s), then, output type is Decimal(p+10,s).
            // So, it can not be overflowed.
            DecimalOperations::Rescale128(decodedDec, scaleDiff, resultDec);

            if (outputTypes->GetIds()[0] == OMNI_DECIMAL64) {
                auto longVector = reinterpret_cast<LongVector *>(vector);
                int64_t low = resultDec.LowBits();
                int64_t shortResult = DecimalOperations::IsNegative(resultDec) ? -low : low;
                longVector->SetValue(rowIndex, shortResult);
            } else {
                auto decimal128Vector = reinterpret_cast<Decimal128Vector *>(vector);
                static_cast<Decimal128Vector *>(decimal128Vector)->SetValue(rowIndex, resultDec);
            }
            int32_t avgCountOffset;
            Vector *avgCountVector = VectorHelper::ExpandVectorAndIndex(vectors[1], rowIndex, avgCountOffset);
            reinterpret_cast<LongVector *>(avgCountVector)
                ->SetValue(rowIndex, static_cast<DecimalAverageState *>(state.val)->count);
        } else {
            Decimal128 finalResultDec;
            OpStatus status = CalcAvg(inputDecimalType, decodedDec, countDec, outputDecimalType, finalResultDec);
            if (status == OpStatus::OP_OVERFLOW) {
                SetNullOrThrowException(vector, rowIndex);
                return;
            }
            if (outputTypes->GetIds()[0] == OMNI_DECIMAL64) {
                int64_t low = finalResultDec.LowBits();
                int64_t shortResult = DecimalOperations::IsNegative(finalResultDec) ? -low : low;
                static_cast<LongVector *>(vector)->SetValue(rowIndex, shortResult);
            } else {
                static_cast<Decimal128Vector *>(vector)->SetValue(rowIndex, finalResultDec);
            }
        }
    }

private:
    // ProcessGroup in inputRaw mode
    void ProcessGroupInputRaw(AggregateState &state, Vector *vector, int32_t offset)
    {
        // val and state to sum. The value of state.val transforms to overflowFlag(8 bytes) + decimal(16 bytes)
        // 1. get a new value
        int64_t oldOverflow = 0;
        int64_t oldCount = 0;
        Decimal128 curVal;
        if (inputTypes->GetIds()[0] == OMNI_DECIMAL64) {
            curVal = DecimalOperations::UnscaledDecimal(static_cast<LongVector *>(vector)->GetValue(offset));
        } else if (inputTypes->GetIds()[0] == OMNI_DECIMAL128) {
            curVal = static_cast<Decimal128Vector *>(vector)->GetValue(offset);
        }
        Decimal128 leftVal;
        // 2. decode current state
        DecimalOperations::DecodeAvgDecimal(static_cast<DecimalAverageState *>(state.val), leftVal, oldOverflow,
            oldCount);
        // 3. if overflowed, no need to do calculation
        if (oldOverflow > 0) {
            ++oldCount;
            DecimalOperations::EncodeAvgDecimal(static_cast<DecimalAverageState *>(state.val), leftVal, oldOverflow,
                oldCount);
            return;
        }
        // 4. do calculation
        int64_t newOverflow = DecimalOperations::AddWithOverflow(leftVal, curVal, leftVal);
        oldOverflow += newOverflow;
        ++oldCount;
        // 5. encode to state
        DecimalOperations::EncodeAvgDecimal(static_cast<DecimalAverageState *>(state.val), leftVal, oldOverflow,
            oldCount);
    }

    // calculate avg=sum/count, and rescale to the result Decimal Type
    OpStatus CalcAvg(DecimalDataType *sumType, Decimal128 &sumDec, Decimal128 &countDec,
        DecimalDataType *outputDecimalType, Decimal128 &finalResultDec)
    {
        int32_t sumPrec = sumType->GetPrecision();
        int32_t sumScale = sumType->GetScale();
        // for raw input type Decimal(p,s)
        // in final mode, aggregator's input type(sumType) is Decimal(p+10,s)
        // window operator only has one stage, and it's input type(sumType) is Decimal(p,s), so precision need to +10
        if (inputRaw && !outputPartial) {
            sumPrec += 10;
            sumPrec = std::min(sumPrec, MAX_PRECISION);
        }
        // before calculate avg, try to check if overflowed when cast sum and count to the wider type
        if (IsCastToWiderTypeOverflow(sumDec, sumPrec, sumScale, countDec)) {
            return OpStatus::OP_OVERFLOW;
        }

        int32_t divideResultPrec = 0;
        int32_t divideResultScale = 0;
        GetDivideResultDecimalType(sumPrec, sumScale, divideResultPrec, divideResultScale);
        Decimal128 dividend;
        // rescale dividend and divisor to divideResultScale(see GetDivideResultDecimalType)
        OpStatus dividendRescalStatus = DecimalOperations::Rescale128(sumDec, divideResultScale - sumScale, dividend);
        Decimal128 avgResultDec;
        OpStatus divideStatus = DecimalOperations::DivideRoundUp(dividend, countDec, 0, 0, avgResultDec);

        // avg = sum/count 's result should rescale to the output DecimalType
        OpStatus resultRescaleStatus = DecimalOperations::Rescale128(avgResultDec,
            outputDecimalType->GetScale() - divideResultScale, finalResultDec);

        bool isOverflow =
            dividendRescalStatus == OP_OVERFLOW || divideStatus == OP_OVERFLOW || resultRescaleStatus == OP_OVERFLOW;

        return isOverflow ? OpStatus::OP_OVERFLOW : OpStatus::SUCCESS;
    }
    // set vector value null or throw exception when overflow
    void SetNullOrThrowException(Vector *vector, int index)
    {
        if (!IsOverflowAsNull()) {
            throw OmniException("OPERATOR_RUNTIME_ERROR", "Overflow in avg of decimals");
        }
        vector->SetValueNull(index);
    }

    // avg = (sum/count).cast(targetDecimalType)
    // GetDivideResultDecimalType get the sum/count result decimal type, and count's type is always Decimal(20,0)
    void GetDivideResultDecimalType(int32_t sumPrec, int32_t sumScale, int32_t &resultPrec, int32_t &resultScale)
    {
        int32_t scale = std::max(MINIMUM_ADJUSTED_SCALE, sumScale + COUNT_PRECISION + 1);
        int32_t prec = sumPrec - sumScale + COUNT_SCALE + scale;

        if (prec <= MAX_PRECISION) {
            // Adjustment only needed when we exceed max precision
            resultPrec = prec;
            resultScale = scale;
            return;
        }

        // Precision/scale exceed maximum precision. Result must be adjusted to MAX_PRECISION.
        int32_t intDigits = prec - scale;
        // If original scale is less than MINIMUM_ADJUSTED_SCALE, use original scale value; otherwise
        // preserve at least MINIMUM_ADJUSTED_SCALE fractional digits
        int32_t minScaleValue = std::min(scale, MINIMUM_ADJUSTED_SCALE);
        // The resulting scale is the maximum between what is available without causing a loss of
        // digits for the integer part of the decimal and the minimum guaranteed scale, which is
        // computed above
        resultScale = std::max(MAX_PRECISION - intDigits, minScaleValue);
        resultPrec = MAX_PRECISION;
    }

    // try to cast sum and count to the wider Decimal Type, return true if overflowed; return false if normal
    // the input sum and count will never be changed.
    bool IsCastToWiderTypeOverflow(Decimal128 &sum, int32_t sumPrec, int32_t sumScale, Decimal128 &count)
    {
        int32_t scale = std::max(sumScale, COUNT_SCALE);
        int32_t range = std::max(sumPrec - sumScale, COUNT_PRECISION - COUNT_SCALE);
        int32_t widerPrec = std::min(range + scale, MAX_PRECISION);
        int32_t widerScale = std::min(scale, MAX_SCALE);

        Decimal128 sumRescale;
        OpStatus sumStatus = DecimalOperations::Rescale128(sum, widerScale - sumScale, sumRescale);
        if (sumStatus == OpStatus::OP_OVERFLOW) {
            return true;
        }
        Decimal128 countRescale;
        OpStatus countStatus = DecimalOperations::Rescale128(count, widerScale - COUNT_SCALE, countRescale);
        return countStatus == OpStatus::OP_OVERFLOW;
    }

private:
    inline static constexpr int32_t COUNT_PRECISION = 20;
    inline static constexpr int32_t COUNT_SCALE = 0;
    inline static constexpr int32_t MINIMUM_ADJUSTED_SCALE = 6;
};
}
}
#endif // OMNI_RUNTIME_AVERAGE_DECIMAL_AGGREGATOR_H
