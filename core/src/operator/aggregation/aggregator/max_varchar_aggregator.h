/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Description: Max aggregate for varchar
 */
#ifndef OMNI_RUNTIME_MAX_VARCHAR_AGGREGATOR_H
#define OMNI_RUNTIME_MAX_VARCHAR_AGGREGATOR_H

#include "aggregator.h"
#ifdef ENABLE_HMPP
#include "HMPP/hmpps.h"
#endif

namespace omniruntime {
namespace op {
class MaxVarcharAggregator : public Aggregator {
public:
    MaxVarcharAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels)
        : Aggregator(OMNI_AGGREGATION_TYPE_MAX, inputTypes, outputTypes, channels)
    {}

    MaxVarcharAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels,
        bool inputRaw, bool outputPartial)
        : Aggregator(OMNI_AGGREGATION_TYPE_MAX, inputTypes, outputTypes, channels, inputRaw, outputPartial)
    {}

    ~MaxVarcharAggregator() override {}

#ifdef ENABLE_HMPP
    void ProcessGroupWithHMPP(AggregateState &state, VectorBatch *vectorBatch) override
    {
        auto vector = vectorBatch->GetVector(channels[0]);

        auto offsets =
            static_cast<int32_t *>(static_cast<int32_t *>(vector->GetValueOffsets()) + vector->GetPositionOffset());
        auto width = static_cast<VarcharDataType *>(inputTypes.GetType(0).get())->GetWidth();
        int32_t maxLen = 0;
        uint8_t *maxVal = executionContext->GetArena()->Allocate(3 * width);

        LogDebug("HMPP-Agg-max");
        auto result =
            HMPPS_Max_varchar(static_cast<uint8_t *>(vector->GetValues()), offsets, vector->GetSize(), maxVal, &maxLen);
        if (result != HMPP_STS_NO_ERR) {
            throw OmniException("HMPP ERROR", "max failed for hmpp error");
        }

        if (state.strVal == nullptr) {
            state.strVal = maxVal;
            state.strLen = maxLen;
        } else {
            auto preMaxVal = reinterpret_cast<char *>(state.strVal);

            int32_t result = memcmp(preMaxVal, reinterpret_cast<char *>(maxVal), std::min(state.strLen, maxLen));
            if (result < 0 || (result == 0 && state.strLen < maxLen)) {
                state.strVal = maxVal;
                state.strLen = maxLen;
            }
        }
    }

    bool CanProcessWithHMPP(AggregateState &state, VectorBatch *vectorBatch) override
    {
        // must no null inpout
        if (vectorBatch->GetVector(channels[0])->MayHaveNull()) {
            return false;
        }
        // not accept dictionnary vector
        if (vectorBatch->GetVector(channels[0])->GetEncoding() == OMNI_VEC_ENCODING_DICTIONARY) {
            return false;
        }
        return true;
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
            this->InitiateGroup(state, vectorBatch, rowIndex);
            return;
        }
        uint8_t *rowVal = nullptr;
        int valLen = (static_cast<VarcharVector *>(vector))->GetValue(offset, &rowVal);
        auto leftVal = reinterpret_cast<char *>(state.strVal);

        int32_t result = memcmp(leftVal, (char *)rowVal, std::min(state.strLen, valLen));
        if (result < 0 && state.strLen == valLen) {
            auto err = memcpy_s(leftVal, valLen, rowVal, valLen);
            if (err != EOK) {
                LogError("set data failed in variable vector. %d", err);
            }
        }
        if ((result < 0 && state.strLen != valLen) || (result == 0 && state.strLen < valLen)) {
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
        Vector *vector = VectorHelper::ExpandVectorAndIndex(vectorBatch->GetVector(channels[0]), rowIndex, offset);
        if (vector->IsValueNull(offset)) {
            return;
        }
        uint8_t *data = nullptr;
        int valLen = static_cast<VarcharVector *>(vector)->GetValue(offset, &data);
        auto ptr = executionContext->GetArena()->Allocate(valLen);
        auto err = memcpy_s(ptr, valLen, data, valLen);
        if (err != EOK) {
            LogError("set data failed in variable vector. %d", err);
        }
        state.strVal = ptr;
        state.strLen = valLen;
    }

    void ExtractValues(AggregateState &state, std::vector<Vector *> &vectors, int32_t rowIndex) override
    {
        int32_t offset;
        Vector *vector = VectorHelper::ExpandVectorAndIndex(vectors[0], rowIndex, offset);
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
#endif // OMNI_RUNTIME_MAX_VARCHAR_AGGREGATOR_H
