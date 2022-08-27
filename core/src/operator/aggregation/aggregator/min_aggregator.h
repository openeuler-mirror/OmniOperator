/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Description: Min aggregate
 */
#ifndef OMNI_RUNTIME_MIN_AGGREGATOR_H
#define OMNI_RUNTIME_MIN_AGGREGATOR_H

#include "aggregator.h"
#ifdef ENABLE_HMPP
#include "HMPP/hmpps.h"
#endif

namespace omniruntime {
namespace op {
template <typename InputVecType, typename OutputVecType, typename ResultType> class MinAggregator : public Aggregator {
public:
    MinAggregator(DataTypePtr in, DataTypePtr out, int32_t channel)
        : Aggregator(OMNI_AGGREGATION_TYPE_MIN, in, out, channel)
    {}

    MinAggregator(DataTypePtr in, DataTypePtr out, int32_t channel, bool inputRaw, bool outputPartial)
        : Aggregator(OMNI_AGGREGATION_TYPE_MIN, in, out, channel, inputRaw, outputPartial)
    {}

    ~MinAggregator() override {}

#ifdef ENABLE_HMPP
    void ProcessGroupWithHMPP(AggregateState &state, VectorBatch *vectorBatch) override
    {
        auto vector = vectorBatch->GetVector(channel);

        auto vectorValues = vector->GetValues();
        auto positionOffset = vector->GetPositionOffset();
        auto rowCount = vector->GetSize();
        auto inputTypeId = inputType->GetId();

        HmppResult result = HMPP_STS_NO_ERR;
        auto minVal = reinterpret_cast<ResultType *>(executionContext->GetArena()->Allocate(sizeof(ResultType)));
        switch (inputTypeId) {
            case OMNI_SHORT: {
                result = HMPPS_Min_16s(static_cast<int16_t *>(static_cast<int16_t *>(vectorValues) + positionOffset),
                    rowCount, reinterpret_cast<int16_t *>(minVal));
                break;
            }
            case OMNI_INT:
            case OMNI_DATE32: {
                result = HMPPS_Min_32s(static_cast<int32_t *>(static_cast<int32_t *>(vectorValues) + positionOffset),
                    rowCount, reinterpret_cast<int32_t *>(minVal));
                break;
            }
            case OMNI_LONG:
            case OMNI_DECIMAL64: {
                result = HMPPS_Min_64s(static_cast<int64_t *>(static_cast<int64_t *>(vectorValues) + positionOffset),
                    rowCount, reinterpret_cast<int64_t *>(minVal));
                break;
            }
            case OMNI_DOUBLE: {
                result = HMPPS_Min_64f(static_cast<double *>(static_cast<double *>(vectorValues) + positionOffset),
                    rowCount, reinterpret_cast<double *>(minVal));
                break;
            }
            case OMNI_DECIMAL128: {
                result = HMPPS_Min_decimal(
                    static_cast<HmppDecimal128 *>(static_cast<HmppDecimal128 *>(vectorValues) + 2 * positionOffset),
                    rowCount, reinterpret_cast<HmppDecimal128 *>(minVal));
                break;
            }
            default: {
                throw OmniException("NOT SUPPORT", "Unsupported input type for min aggregate");
                break;
            }
        }

        if (result != HMPP_STS_NO_ERR) {
            throw OmniException("HMPP ERROR", "min failed for hmpp error");
        }
        if (state.val == nullptr) {
            state.val = minVal;
        } else {
            auto preMinVal = static_cast<ResultType *>(state.val);
            auto currMinVal = reinterpret_cast<ResultType *>(minVal);
            *static_cast<ResultType *>(state.val) = (Compare(*preMinVal, *currMinVal) == -1) ? *preMinVal : *currMinVal;
        }
    }
#endif

    void ProcessGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex) override
    {
        int32_t offset;
        Vector *vector = VectorHelper::ExpandVectorAndIndex(vectorBatch->GetVector(channel), rowIndex, offset);
        if (vector->IsValueNull(offset)) {
            return;
        }
        if (state.val == nullptr) {
            this->InitiateGroup(state, vectorBatch, rowIndex);
            return;
        }
        auto rowVal = static_cast<ResultType>((static_cast<InputVecType *>(vector))->GetValue(offset));
        auto leftVal = static_cast<ResultType *>(state.val);
        *leftVal = (Compare(*leftVal, rowVal) == -1) ? *leftVal : rowVal;
    }

    void InitiateGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex) override
    {
        int32_t offset;
        Vector *vector = VectorHelper::ExpandVectorAndIndex(vectorBatch->GetVector(channel), rowIndex, offset);
        if (vector->IsValueNull(offset)) {
            return;
        }
        auto rowVal = static_cast<InputVecType *>(vector)->GetValue(offset);
        auto ptr = executionContext->GetArena()->Allocate(sizeof(ResultType));
        *reinterpret_cast<ResultType *>(ptr) = rowVal;
        state.val = ptr;
    }

    // TODO extract common function for sum/min/max
    void ExtractValue(AggregateState &state, Vector *vector, int32_t rowIndex) override
    {
        auto v = static_cast<OutputVecType *>(vector);
        if (state.val == nullptr) {
            v->SetValueNull(rowIndex);
            return;
        }
        v->SetValue(rowIndex, *static_cast<ResultType *>(state.val));
    }
};
}
}
#endif // OMNI_RUNTIME_MIN_AGGREGATOR_H
