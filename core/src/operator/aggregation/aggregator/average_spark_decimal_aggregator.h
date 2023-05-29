/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2023. All rights reserved.
 * Description: Average aggregate for short decimal
 */
#ifndef OMNI_RUNTIME_AVERAGE_SPARK_DECIMAL_AGGREGATOR_H
#define OMNI_RUNTIME_AVERAGE_SPARK_DECIMAL_AGGREGATOR_H

#include "sum_spark_decimal_aggregator.h"
#include "type/decimal_operations.h"
#include "operations_aggregator.h"

namespace omniruntime {
namespace op {
static constexpr int32_t PARTIAL_AVG_OUTPUT_LENGTH = sizeof(DecimalAverageState);
template <DataTypeId InDecimalId, DataTypeId OutDecimalId>
class AverageSparkDecimalAggregator : public SumSparkDecimalAggregator<InDecimalId,OutDecimalId> {
public:
    using ResultType = typename AggNativeAndVectorType<OutDecimalId>::type;
    using InRawType = typename AggNativeAndVectorType<InDecimalId>::type;
    using ResultIntType = std::conditional_t<std::is_same_v<ResultType,Decimal128>,int128_t,int64_t>;

    AverageSparkDecimalAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes,
        std::vector<int32_t> &channels, bool inputRaw, bool outputPartial, bool isOverflowAsNull)
        : SumSparkDecimalAggregator<InDecimalId,OutDecimalId>(OMNI_AGGREGATION_TYPE_AVG, inputTypes, outputTypes, channels, inputRaw,
        outputPartial, isOverflowAsNull)
    {}

    ~AverageSparkDecimalAggregator() override {}
    // todo: will be deleted
    void ProcessGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex) override
    {
//        BaseVector *vector = vectorBatch->Get(SumSparkDecimalAggregator<InDecimalId,OutDecimalId>::channels[0]);
//        // null rows dont count
//        if (vector->IsNull(rowIndex)) {
//            return;
//        }
//
//        if (SumSparkDecimalAggregator<InDecimalId,OutDecimalId>::inputRaw) {
//            ProcessGroupInputRaw(state, vector, rowIndex);
//        } else {
//            // get value from containerVector
//            int128_t curVal;
//            GetDecimalValue(vector, SumSparkDecimalAggregator<InDecimalId,OutDecimalId>::inputTypes.GetIds()[0], rowIndex, curVal);
//
//            BaseVector *avgCountVector = vectorBatch->Get(SumSparkDecimalAggregator<InDecimalId,OutDecimalId>::channels[1]);
//            int64_t avgCnt;
//            if (avgCountVector->GetEncoding() == OMNI_DICTIONARY) {
//                avgCnt = reinterpret_cast<Vector<DictionaryContainer<long>> *>(avgCountVector)->GetValue(rowIndex);
//            } else {
//                avgCnt = reinterpret_cast<Vector<int64_t> *>(avgCountVector)->GetValue(rowIndex);
//            }
//
//            // 2. decode current state and intermediate state
//            int128_t leftVal;
//            int64_t oldOverflow = 0;
//            int64_t oldCount = 0;
//            DecodeAvgDecimal(static_cast<DecimalAverageState *>(state.val), leftVal, oldOverflow, oldCount);
//            // 3. if overflowed, no need to do calculation
//            if (oldOverflow > 0) {
//                oldCount += avgCnt;
//                EncodeAvgDecimal(static_cast<DecimalAverageState *>(state.val), leftVal, oldOverflow, oldCount);
//                return;
//            }
//            // 4. do calculation
//            oldCount += avgCnt;
//            oldOverflow += static_cast<int64_t>(AddCheckedOverflow(leftVal, curVal, leftVal));
//
//            EncodeAvgDecimal(static_cast<DecimalAverageState *>(state.val), leftVal, oldOverflow, oldCount);
//        }
    }

    void ProcessGroupInternal(std::vector<AggregateState *> &rowStates, const size_t aggIdx, BaseVector *vector,
        const int32_t rowOffset, const uint8_t *nullMap, const int32_t *indexMap)
    {
        if (SumSparkDecimalAggregator<InDecimalId,OutDecimalId>::inputRaw) {
            SumSparkDecimalAggregator<InDecimalId,OutDecimalId>::ProcessGroupInternal(rowStates, aggIdx, vector, rowOffset, nullMap, indexMap);
        } else {
            auto *sumVector = SumSparkDecimalAggregator<InDecimalId,OutDecimalId>::curVectorBatch->Get(SumSparkDecimalAggregator<InDecimalId,OutDecimalId>::channels[0]);
            auto *ptr = reinterpret_cast<InRawType *>(GetValuesFromVector<InDecimalId>(sumVector));

            auto *avgCountVector = SumSparkDecimalAggregator<InDecimalId,OutDecimalId>::curVectorBatch->Get(SumSparkDecimalAggregator<InDecimalId,OutDecimalId>::channels[1]);
            auto *cntPtr = reinterpret_cast<int64_t *>(GetValuesFromVector<OMNI_LONG>(avgCountVector));
            ptr += rowOffset;
            cntPtr += rowOffset;

            if (nullMap == nullptr) {
                AddUseRowIndexAvg<InRawType , ResultType, SumOp<InRawType, ResultType>>(rowStates, aggIdx, ptr, cntPtr);
            } else {
                // Reza: can we use customize float operation similar to sumConditionalFloat
                AddConditionalUseRowIndexAvg<InRawType, ResultType, SumConditionalOp<InRawType, ResultType, false>>(rowStates,
                    aggIdx, ptr, cntPtr, nullMap);
            }
        }
    }

    void ProcessSingleInternal(AggregateState &state, BaseVector *vector, const int32_t rowOffset,
        const int32_t rowCount, const uint8_t *nullMap, const int32_t *indexMap)
    {
        auto *res = reinterpret_cast<ResultType *>(state.val);
        if (SumSparkDecimalAggregator<InDecimalId,OutDecimalId>::inputRaw) {
            SumSparkDecimalAggregator<InDecimalId,OutDecimalId>::ProcessSingleInternal(state, vector, rowOffset, rowCount, nullMap, indexMap);
        } else {
            auto *sumVector = SumSparkDecimalAggregator<InDecimalId,OutDecimalId>::curVectorBatch->Get(SumSparkDecimalAggregator<InDecimalId,OutDecimalId>::channels[0]);
            auto *ptr = reinterpret_cast<InRawType *>(GetValuesFromVector<InDecimalId>(sumVector));

            auto *avgCountVector = SumSparkDecimalAggregator<InDecimalId,OutDecimalId>::curVectorBatch->Get(SumSparkDecimalAggregator<InDecimalId,OutDecimalId>::channels[1]);
            auto *cntPtr = reinterpret_cast<int64_t *>(GetValuesFromVector<OMNI_LONG>(avgCountVector));
            ptr += rowOffset;
            cntPtr += rowOffset;

            if (nullMap == nullptr) {
                AddAvg<InRawType, ResultType, SumOp<InRawType, ResultType>>(res, state.count, ptr, cntPtr, rowCount);
            } else {
                AddConditionalAvg<InRawType, ResultType, SumConditionalOp<InRawType, ResultType, false>>(res, state.count,
                    ptr, cntPtr, rowCount, nullMap);
            }
        }
    }

    void ExtractValues(const AggregateState &state, std::vector<BaseVector *> &vectors, int32_t rowIndex) override
    {
        BaseVector *vector = vectors[0];
        //        int64_t overflowAccumulator = 0;
        int64_t count = 0;

        int128_t decodedDec;
        if constexpr(std::is_same_v<ResultType,Decimal128>) {
            decodedDec = (static_cast<ResultType*>(state.val))->ToInt128();
        } else {
            decodedDec = *(static_cast<ResultType*>(state.val));
        }
        //        DecodeAvgDecimal(static_cast<DecimalAverageState *>(state.val), decodedDec, overflowAccumulator,
        //        count);
        int128_t countDec = state.count;

        auto outputDecimalType =
            static_cast<DecimalDataType *>(SumSparkDecimalAggregator<InDecimalId,OutDecimalId>::outputTypes.GetType(0).get());
        auto inputDecimalType = static_cast<DecimalDataType *>(SumSparkDecimalAggregator<InDecimalId,OutDecimalId>::inputTypes.GetType(0).get());

        if (state.count < 0) {
            // overflow
            this->SetNullOrThrowException(vector, rowIndex);
            return;
        }

        if (SumSparkDecimalAggregator<InDecimalId,OutDecimalId>::outputPartial) {
            int32_t scaleDiff = outputDecimalType->GetScale() - inputDecimalType->GetScale();
            int128_t resultDec;
            // rescale dividend and divisor to output scale.
            // In Partial mode, if input type is Decimal(p,s), then, output type is Decimal(p+10,s).
            // So, it can not be overflowed.
            MulCheckedOverflow(decodedDec, TenOfInt128[scaleDiff], resultDec);

            if constexpr (std::is_same_v<ResultType ,Decimal128>) {
                auto decimal128Vector = reinterpret_cast<Vector<Decimal128> *>(vector);
                Decimal128 decimal128Result(resultDec);
                static_cast<Vector<Decimal128> *>(decimal128Vector)->SetValue(rowIndex, decimal128Result);
            } else {
                auto longVector = reinterpret_cast<Vector<int64_t> *>(vector);
                int64_t shortResult = static_cast<int64_t>(resultDec);
                longVector->SetValue(rowIndex, shortResult);
            }

            BaseVector *avgCountVector = vectors[1];
            reinterpret_cast<Vector<int64_t> *>(avgCountVector)
                ->SetValue(rowIndex, state.count);
        } else {
            int128_t finalResultDec;
            // if count is zero, it means all input is null
            if (countDec == 0) {
                vector->SetNull(rowIndex);
                return;
            }
            OpStatus status = CalcAvg(inputDecimalType, decodedDec, countDec, outputDecimalType, finalResultDec);
            if (status == OpStatus::OP_OVERFLOW) {
                this->SetNullOrThrowException(vector, rowIndex);
                return;
            }
            // we can not use template std::is_same_v<resultType,Decimal128> here
            // for average, Decimal128 / n = Decimal64 is possible,
            // but all intermediate types such as ResultType are Decimal128,
            // so we have to get type from outputTypes , rather than ResultTyp
            if (SumSparkDecimalAggregator<InDecimalId,OutDecimalId>::outputTypes.GetIds()[0] == OMNI_DECIMAL128) {
                Decimal128 decimal128Result(finalResultDec);
                static_cast<Vector<Decimal128> *>(vector)->SetValue(rowIndex, decimal128Result);
            } else {
                int64_t shortResult = static_cast<int64_t>(finalResultDec);
                static_cast<Vector<int64_t> *>(vector)->SetValue(rowIndex, shortResult);
            }
        }
    }

private:
    // ProcessGroup in inputRaw mode
//    void ProcessGroupInputRaw(AggregateState &state, BaseVector *vector, int32_t rowIndex)
//    {
//        // val and state to sum. The value of state.val transforms to overflowFlag(8 bytes) + decimal(16 bytes)
//        // 1. get a new value
//        int64_t oldOverflow = 0;
//        int64_t oldCount = 0;
//        int128_t curVal;
//        GetDecimalValue(vector, SumSparkDecimalAggregator<InDecimalId,OutDecimalId>::inputTypes.GetIds()[0], rowIndex, curVal);
//
//        int128_t leftVal;
//        // 2. decode current state
//        DecodeAvgDecimal(static_cast<DecimalAverageState *>(state.val), leftVal, oldOverflow, oldCount);
//        // 3. if overflowed, no need to do calculation
//        if (oldOverflow > 0) {
//            ++oldCount;
//            EncodeAvgDecimal(static_cast<DecimalAverageState *>(state.val), leftVal, oldOverflow, oldCount);
//            return;
//        }
//        // 4. do calculation
//        oldOverflow += static_cast<int64_t>(AddCheckedOverflow(leftVal, curVal, leftVal));
//        ++oldCount;
//        // 5. encode to state
//        EncodeAvgDecimal(static_cast<DecimalAverageState *>(state.val), leftVal, oldOverflow, oldCount);
//    }

    // calculate avg=sum/count, and rescale to the result Decimal Type
    OpStatus CalcAvg(DecimalDataType *sumType, int128_t &sumDec, int128_t &countDec, DecimalDataType *outputDecimalType,
        int128_t &finalResultDec)
    {
        int32_t sumPrec = sumType->GetPrecision();
        int32_t sumScale = sumType->GetScale();
        // for raw input type Decimal(p,s)
        // in final mode, aggregator's input type(sumType) is Decimal(p+10,s)
        // window operator only has one stage, and it's input type(sumType) is Decimal(p,s), so precision need to +10
        if (SumSparkDecimalAggregator<InDecimalId,OutDecimalId>::inputRaw
            && !SumSparkDecimalAggregator<InDecimalId,OutDecimalId>::outputPartial) {
            sumPrec += 10;
            sumPrec = std::min(sumPrec, 38);
        }
        // before calculate avg, try to check if overflowed when cast sum and count to the wider type
        if (IsCastToWiderTypeOverflow(sumDec, sumPrec, sumScale, countDec)) {
            return OpStatus::OP_OVERFLOW;
        }

        int32_t divideResultPrec = 0;
        int32_t divideResultScale = 0;
        GetDivideResultDecimalType(sumPrec, sumScale, divideResultPrec, divideResultScale);
        int128_t dividend;
        // rescale dividend and divisor to divideResultScale(see GetDivideResultDecimalType)
        bool isOverflow = MulCheckedOverflow(sumDec, TenOfInt128[divideResultScale - sumScale], dividend);
        int128_t avgResultDec;
        DivideRoundUp(dividend, countDec, avgResultDec);

        // avg = sum/count 's result should rescale to the output DecimalType
        int32_t diffScale = outputDecimalType->GetScale() - divideResultScale;
        if (diffScale >= 0) {
            isOverflow = isOverflow || MulCheckedOverflow(avgResultDec, TenOfInt128[diffScale], finalResultDec);
        } else {
            DivideRoundUp(avgResultDec, TenOfInt128[-diffScale], finalResultDec);
        }

        return isOverflow ? OpStatus::OP_OVERFLOW : OpStatus::SUCCESS;
    }
    // set vector value null or throw exception when overflow
    void SetNullOrThrowException(BaseVector *vector, int index)
    {
        if (!SumSparkDecimalAggregator<InDecimalId,OutDecimalId>::IsOverflowAsNull()) {
            throw OmniException("OPERATOR_RUNTIME_ERROR", "Overflow in avg of decimals");
        }
        vector->SetNull(index);
    }

    // GetDivideResultDecimalType get the sum/count result decimal type, and count's type is always Decimal(20,0)
    void GetDivideResultDecimalType(int32_t sumPrec, int32_t sumScale, int32_t &resultPrec, int32_t &resultScale)
    {
        int32_t scale = std::max(MINIMUM_ADJUSTED_SCALE, sumScale + COUNT_PRECISION + 1);
        int32_t prec = sumPrec - sumScale + COUNT_SCALE + scale;

        if (prec <= 38) {
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
        resultScale = std::max(38 - intDigits, minScaleValue);
        resultPrec = 38;
    }

    // try to cast sum and count to the wider Decimal Type, return true if overflowed; return false if normal
    // the input sum and count will never be changed.
    bool IsCastToWiderTypeOverflow(int128_t &sum, int32_t sumPrec, int32_t sumScale, int128_t &count)
    {
        int32_t scale = std::max(sumScale, COUNT_SCALE);
        int32_t widerScale = std::min(scale, 38);

        int128_t sumRescale;
        bool sumStatus = MulCheckedOverflow(sum, TenOfInt128[widerScale - sumScale], sumRescale);
        if (sumStatus) {
            return true;
        }
        int128_t countRescale;
        bool countStatus = MulCheckedOverflow(count, TenOfInt128[widerScale - COUNT_SCALE], countRescale);
        return countStatus;
    }

private:
    inline static constexpr int32_t COUNT_PRECISION = 20;
    inline static constexpr int32_t COUNT_SCALE = 0;
    inline static constexpr int32_t MINIMUM_ADJUSTED_SCALE = 6;
};
}
}
#endif // OMNI_RUNTIME_AVERAGE_DECIMAL_AGGREGATOR_H
