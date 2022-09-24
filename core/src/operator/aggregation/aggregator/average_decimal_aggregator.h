/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Description: Average aggregate for short decimal
 */
#ifndef OMNI_RUNTIME_AVERAGE_DECIMAL_AGGREGATOR_H
#define OMNI_RUNTIME_AVERAGE_DECIMAL_AGGREGATOR_H

#include "aggregator.h"
#ifdef ENABLE_HMPP
#include "HMPP/hmpps.h"
#endif

namespace omniruntime {
namespace op {
class AverageDecimalAggregator : public Aggregator {
public:
    AverageDecimalAggregator(DataTypesPtr inputTypes, DataTypesPtr outputTypes, std::vector<int32_t> &channels)
        : Aggregator(OMNI_AGGREGATION_TYPE_AVG, inputTypes, outputTypes, channels)
    {}

    AverageDecimalAggregator(DataTypesPtr inputTypes, DataTypesPtr outputTypes, std::vector<int32_t> &channels,
        bool inputRaw, bool outputPartial)
        : Aggregator(OMNI_AGGREGATION_TYPE_AVG, inputTypes, outputTypes, channels, inputRaw, outputPartial)
    {}

    ~AverageDecimalAggregator() override {}

#ifdef ENABLE_HMPP
    void ProcessGroupWithHMPP(AggregateState &state, VectorBatch *vectorBatch) override
    {
        auto vector = vectorBatch->GetVector(channels[0]);

        auto vectorValues = vector->GetValues();
        auto positionOffset = vector->GetPositionOffset();
        auto rowCount = vector->GetSize();
        auto nullAddr = vector->GetValueNulls();
        bool overflow = false;
        int32_t count = 0;

        auto inputTypeId = inputTypes->GetType(0)->GetId();
        HmppResult result = HMPP_STS_NO_ERR;
        auto sumVal =
            reinterpret_cast<HmppDecimal128 *>(executionContext->GetArena()->Allocate(sizeof(HmppDecimal128)));
        switch (inputTypeId) {
            case OMNI_DECIMAL128: {
                LogDebug("HMPP-Agg-avg");
                result = HMPPS_Mean_decimal128(
                    static_cast<HmppDecimal128 *>(static_cast<HmppDecimal128 *>(vectorValues) + 2 * positionOffset),
                    rowCount, static_cast<int8_t *>(static_cast<int8_t *>(nullAddr) + positionOffset), &overflow,
                    reinterpret_cast<HmppDecimal128 *>(sumVal), &count);
                break;
            }
            default: {
                throw OmniException("NOT SUPPORT", "Unsupported input type for avg aggregate");
                break;
            }
        }

        if (result != HMPP_STS_NO_ERR) {
            throw OmniException("HMPP ERROR", "avg failed for hmpp error");
        }

        if (state.val == nullptr) {
            state.val = executionContext->GetArena()->Allocate(PARTIAL_AVG_OUTPUT_LENGTH);
            int64_t newOverflow = overflow == false ? 0 : 1 << 63;
            DecimalOperations::EncodeAvgDecimal(static_cast<DecimalAverageState *>(state.val),
                Decimal128(sumVal->high, sumVal->low), newOverflow, static_cast<int64_t>(count));
        } else {
            Decimal128 preSumVal;
            int64_t oldOverflow = 0;
            int64_t oldCount = 0;
            DecimalOperations::DecodeAvgDecimal(static_cast<DecimalAverageState *>(state.val), preSumVal, oldOverflow,
                oldCount);

            auto currSumVal = Decimal128(sumVal->high, sumVal->low);
            int64_t newOverflow = DecimalOperations::AddWithOverflow(preSumVal, currSumVal, preSumVal);
            oldOverflow += newOverflow;
            oldCount += static_cast<int64_t>(count);
            DecimalOperations::EncodeAvgDecimal(static_cast<DecimalAverageState *>(state.val), preSumVal, oldOverflow,
                oldCount);
        }
    }

    bool CanProcessWithHMPP(AggregateState &state, VectorBatch *vectorBatch) override
    {
        // just support raw input data
        if (!inputRaw) {
            return false;
        }
        // not accept dictionnary vector
        if (vectorBatch->GetVector(channels[0])->GetEncoding() == OMNI_VEC_ENCODING_DICTIONARY) {
            return false;
        }
        // only OMNI_DECIMAL128 type input support
        return (inputTypes->GetType(0)->GetId() == OMNI_DECIMAL128);
    }
#endif

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
            // input vector is expected as VarcharVec
            uint8_t *otherState = nullptr;
            auto length = (static_cast<VarcharVector *>(vector))->GetValue(offset, &otherState);
            state.val = executionContext->GetArena()->Allocate(length);
            memcpy_s(state.val, length, otherState, length);
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
            auto outType = outputTypes->GetIds()[0];
            auto inType = inputTypes->GetIds()[0];
            if (inType == OMNI_DECIMAL64 || inType == OMNI_DECIMAL128) {
                scaleDiff = static_cast<DecimalDataType *>(outputTypes->GetType(0).get())->GetScale() -
                    static_cast<DecimalDataType *>(inputTypes->GetType(0).get())->GetScale();
            }
            Decimal128 rescaledDividend;
            // rescale dividend and divisor to output scale
            DecimalOperations::Rescale128(decodedDec, scaleDiff, rescaledDividend);
            DecimalOperations::DivideRoundUp(rescaledDividend, countDec, 0, 0, resultDec);
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
