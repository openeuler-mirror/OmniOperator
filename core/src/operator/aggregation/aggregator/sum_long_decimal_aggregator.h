/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Description: Sum aggregate for long decimal
 */
#ifndef OMNI_RUNTIME_SUM_LONG_DECIMAL_AGGREGATOR_H
#define OMNI_RUNTIME_SUM_LONG_DECIMAL_AGGREGATOR_H

#include "aggregator.h"
#include "type/decimal_operations.h"
#ifdef ENABLE_HMPP
#include "HMPP/hmpps.h"
#endif

namespace omniruntime {
namespace op {
class SumLongDecimalAggregator : public Aggregator {
public:
    SumLongDecimalAggregator(DataTypesPtr inputTypes, DataTypesPtr outputTypes, std::vector<int32_t> &channels)
        : Aggregator(OMNI_AGGREGATION_TYPE_SUM, inputTypes, outputTypes, channels)
    {}

    SumLongDecimalAggregator(DataTypesPtr inputTypes, DataTypesPtr outputTypes, std::vector<int32_t> &channels,
        bool inputRaw, bool outputPartial)
        : Aggregator(OMNI_AGGREGATION_TYPE_SUM, inputTypes, outputTypes, channels, inputRaw, outputPartial)
    {}

    ~SumLongDecimalAggregator() override {}

#ifdef ENABLE_HMPP
    void ProcessGroupWithHMPP(AggregateState &state, VectorBatch *vectorBatch) override
    {
        auto vector = vectorBatch->GetVector(channels[0]);

        auto vectorValues = vector->GetValues();
        auto positionOffset = vector->GetPositionOffset();
        auto rowCount = vector->GetSize();
        auto nullAddr = vector->GetValueNulls();
        bool overflow = false;
        auto sumVal =
            reinterpret_cast<HmppDecimal128 *>(executionContext->GetArena()->Allocate(sizeof(HmppDecimal128)));

        LogDebug("HMPP-Agg-sum");
        auto result = HMPPS_Sum_decimal128(
            static_cast<HmppDecimal128 *>(static_cast<HmppDecimal128 *>(vectorValues) + 2 * positionOffset), rowCount,
            static_cast<int8_t *>(static_cast<int8_t *>(nullAddr) + positionOffset), &overflow, sumVal);
        if (result != HMPP_STS_NO_ERR) {
            throw OmniException("HMPP ERROR", "sum failed for hmpp error");
        }

        if (state.val == nullptr) {
            state.val = executionContext->GetArena()->Allocate(PARTIAL_SUM_OUTPUT_LENGTH);
            int64_t newOverflow = overflow == false ? 0 : 1 << 63;
            DecimalOperations::EncodeSumDecimal(static_cast<DecimalSumState *>(state.val),
                Decimal128(sumVal->high, sumVal->low), newOverflow);
        } else {
            Decimal128 preSumVal;
            int64_t oldOverflow = 0;
            DecimalOperations::DecodeSumDecimal(static_cast<DecimalSumState *>(state.val), preSumVal, oldOverflow);

            auto currSumVal = Decimal128(sumVal->high, sumVal->low);
            int64_t newOverflow = DecimalOperations::AddWithOverflow(preSumVal, currSumVal, preSumVal);
            oldOverflow += newOverflow;
            DecimalOperations::EncodeSumDecimal(static_cast<DecimalSumState *>(state.val), preSumVal, oldOverflow);
        }
    }

    bool CanProcessWithHMPP(AggregateState &state, VectorBatch *vectorBatch) override
    {
        // just support row Raw data
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
        Vector *vector = VectorHelper::ExpandVectorAndIndex(vectorBatch->GetVector(channels[0]), rowIndex, offset);
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
