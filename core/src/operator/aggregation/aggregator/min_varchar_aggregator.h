/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Description: Min aggregate for varchar
 */
#ifndef OMNI_RUNTIME_MIN_VARCHAR_AGGREGATOR_H
#define OMNI_RUNTIME_MIN_VARCHAR_AGGREGATOR_H

#include "aggregator.h"
#ifdef ENABLE_HMPP
#include "HMPP/hmpps.h"
#endif

namespace omniruntime {
namespace op {
class MinVarcharAggregator : public Aggregator {
public:
    MinVarcharAggregator(DataTypePtr in, DataTypePtr out, int32_t channel)
        : Aggregator(OMNI_AGGREGATION_TYPE_MIN, in, out, channel)
    {}

    MinVarcharAggregator(DataTypePtr in, DataTypePtr out, int32_t channel, bool inputRaw, bool outputPartial)
        : Aggregator(OMNI_AGGREGATION_TYPE_MIN, in, out, channel, inputRaw, outputPartial)
    {}

    ~MinVarcharAggregator() override {}

#ifdef ENABLE_HMPP
    void ProcessGroupWithHMPP(AggregateState &state, VectorBatch *vectorBatch) override
    {
        auto vector = vectorBatch->GetVector(channel);

        auto offsets =
            static_cast<int32_t *>(static_cast<int32_t *>(vector->GetValueOffsets()) + vector->GetPositionOffset());
        auto width = static_cast<VarcharDataType *>(inputType.get())->GetWidth();
        int32_t minLen = 3 * width;
        uint8_t *minVal = executionContext->GetArena()->Allocate(3 * width);

        auto result =
            HMPPS_Min_varchar(static_cast<uint8_t *>(vector->GetValues()), offsets, vector->GetSize(), minVal, &minLen);
        if (result != HMPP_STS_NO_ERR) {
            throw OmniException("HMPP ERROR", "min failed for hmpp error");
        }

        if (state.val == nullptr) {
            state.strVal = minVal;
            state.strLen = minLen;
        } else {
            auto preMinVal = reinterpret_cast<char *>(state.strVal);

            int32_t result = memcmp(preMinVal, reinterpret_cast<char *>(minVal), std::min(state.strLen, minLen));
            if (result > 0 || (result == 0 && state.strLen > minLen)) {
                state.strVal = minVal;
                state.strLen = minLen;
            }
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
        uint8_t *rowVal = nullptr;
        int valLen = (static_cast<VarcharVector *>(vector))->GetValue(offset, &rowVal);
        auto leftVal = reinterpret_cast<char *>(state.strVal);

        int32_t result = memcmp(leftVal, (char *)rowVal, std::min(state.strLen, valLen));
        if (result > 0 && state.strLen == valLen) {
            auto err = memcpy_s(leftVal, valLen, rowVal, valLen);
            if (err != EOK) {
                LogError("set data failed in variable vector. %d", err);
            }
        }
        if ((result > 0 && state.strLen != valLen) || (result == 0 && state.strLen > valLen)) {
            uint8_t *ptr = executionContext->GetArena()->Allocate(valLen);
            auto err = memcpy_s(ptr, valLen, rowVal, valLen);
            if (err != EOK) {
                LogError("set data failed in variable vector. %d", err);
            }

            state.strVal = ptr;
            state.strLen = valLen;
        }
    }

    void InitiateGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex) override
    {
        int32_t offset;
        Vector *vector = VectorHelper::ExpandVectorAndIndex(vectorBatch->GetVector(channel), rowIndex, offset);
        if (vector->IsValueNull(offset)) {
            return;
        }
        uint8_t *data = nullptr;
        int valLen = static_cast<VarcharVector *>(vector)->GetValue(offset, &data);
        uint8_t *ptr = executionContext->GetArena()->Allocate(valLen);
        auto err = memcpy_s(ptr, valLen, data, valLen);
        if (err != EOK) {
            LogError("set data failed in variable vector. %d", err);
        }
        state.strVal = ptr;
        state.strLen = valLen;
    }

    void ExtractValue(AggregateState &state, Vector *vector, int32_t rowIndex) override
    {
        auto v = static_cast<VarcharVector *>(vector);
        if (state.val == nullptr) {
            v->SetValueNull(rowIndex);
            return;
        }
        v->SetValue(rowIndex, reinterpret_cast<uint8_t *>(state.strVal), state.strLen);
    }
};
}
}
#endif // OMNI_RUNTIME_MIN_VARCHAR_AGGREGATOR_H
