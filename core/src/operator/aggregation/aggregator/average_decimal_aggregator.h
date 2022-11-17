#pragma once

/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Description: Average aggregate for short decimal
 */

#include "sum_decimal_aggregator.h"

namespace omniruntime {
namespace op {
template <bool RAW_IN, bool PARTIAL_OUT, bool NULL_OVERFLOW, DataTypeId IN_ID, DataTypeId OUT_ID>
class AverageDecimalAggregator
    : public SumDecimalAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OUT_ID> {
    using InVector = typename NativeAndVectorType<IN_ID>::vector;
    using InType = std::conditional_t<IN_ID == OMNI_VARCHAR,
        DecimalPartialResult, typename NativeAndVectorType<IN_ID>::type>;
    using OutVector = typename NativeAndVectorType<OUT_ID>::vector;
    using OutType = std::conditional_t<OUT_ID == OMNI_VARCHAR,
        DecimalPartialResult, typename NativeAndVectorType<OUT_ID>::type>;
public:
    AverageDecimalAggregator(const DataTypes & inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels)
        : SumDecimalAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW, IN_ID, OUT_ID>(
            OMNI_AGGREGATION_TYPE_AVG, inputTypes, outputTypes, channels)
    {}

    ~AverageDecimalAggregator() override = default;

#ifdef ENABLE_HMPP
    void ProcessGroupWithHMPP(AggregateState &state, VectorBatch *vectorBatch) override
    {
        auto vector = vectorBatch->GetVector(this->channels[0]);

        auto vectorValues = vector->GetValues();
        auto positionOffset = vector->GetPositionOffset();
        auto rowCount = vector->GetSize();
        auto nullAddr = vector->GetValueNulls();
        bool overflow = false;
        int32_t count = 0;

        auto inputTypeId = inputTypes.GetType(0)->GetId();
        HmppResult result = HMPP_STS_NO_ERR;
        HmppDecimal128 sumVal {};

        switch (inputTypeId) {
            case OMNI_DECIMAL128: {
                LogDebug("HMPP-Agg-avg");
                result = HMPPS_Mean_decimal128(
                    static_cast<HmppDecimal128 *>(static_cast<HmppDecimal128 *>(vectorValues) + positionOffset),
                    rowCount, static_cast<int8_t *>(static_cast<int8_t *>(nullAddr) + positionOffset), &overflow,
                    &sumVal, &count);
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
            state.val = this->executionContext->GetArena()->Allocate(sizeof(Decimal128));
            *reinterpret_cast<Decimal128 *>(state.val) = Decimal128(sumVal.high, sumVal.low);
        } else {
            Decimal128 preSumDec = *(reinterpret_cast<Decimal128 *>(state.val));
            Decimal128Wrapper preSumVal(preSumDec);
            preSumVal=preSumVal.Add(Decimal128Wrapper(sumVal.high, sumVal.low));
            overflow |= (static_cast<int64_t>(preSumVal.IsOverflow()) != 0);
        }
        if (overflow) {
            state.count = -1;
        } else if (state.count >= 0) {
            state.count += static_cast<int64_t>(count);
        }
    }

    bool CanProcessWithHMPP(AggregateState &state, VectorBatch *vectorBatch) override
    {
        // just support raw input data
        if constexpr (!RAW_IN) {
            return false;
        } else {
            // not accept dictionnary vector
            if (vectorBatch->GetVector(this->channels[0])->GetEncoding() == OMNI_VEC_ENCODING_DICTIONARY) {
                return false;
            }
            // only OMNI_DECIMAL128 type input support
            return (this->inputTypes.GetType(0)->GetId() == OMNI_DECIMAL128);
        }
    }
#endif

    void ExtractFinalValues(const AggregateState &state, std::vector<Vector *> &vectors, int32_t rowIndex) override
    {
        if constexpr (PARTIAL_OUT) {
            throw OmniException("Logical Error",
                "ExtractFinalValues with outputPartial=true for sum decimal aggregator");
        } else if constexpr (OUT_ID != OMNI_DECIMAL128 && OUT_ID != OMNI_DECIMAL64) {
            throw OmniException("Logical Error",
                "ExtractFinalValues output type is not decimal sum decimal aggregator");
        } else {
            int32_t offset;
            Vector *v = VectorHelper::ExpandVectorAndIndex(vectors[0], rowIndex, offset);
            OutVector *vector = static_cast<OutVector *>(v);

            OutType result {};
            bool overflow = state.count < 0;

            if (state.count > 0 && state.val != nullptr) {
                Decimal128Wrapper result128 = Decimal128Wrapper(*reinterpret_cast<Decimal128 *>(state.val)).Divide(
                    Decimal128Wrapper(state.count), 0);
                result = this->template CastWithOverflow<Decimal128, OutType>(result128.ToDecimal128(), overflow);
            }

            vector->SetValue(offset, result);

            if (overflow) {
                this->SetNullOrThrowException(v, offset, "average_decimal_aggregator overflow.");
            } else if (state.count == 0 || state.val == nullptr) {
                v->SetValueNull(offset);
            }
        }
    }
};
}
}
