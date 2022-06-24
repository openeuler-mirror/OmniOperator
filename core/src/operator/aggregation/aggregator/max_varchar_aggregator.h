/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Description: Max aggregate for varchar
 */
#ifndef OMNI_RUNTIME_MAX_VARCHAR_AGGREGATOR_H
#define OMNI_RUNTIME_MAX_VARCHAR_AGGREGATOR_H

#include "aggregator.h"

namespace omniruntime {
namespace op {
class MaxVarcharAggregator : public Aggregator {
public:
    MaxVarcharAggregator(const DataTypePtr in, const DataTypePtr out, int32_t channel)
        : Aggregator(OMNI_AGGREGATION_TYPE_MAX, in, out, channel)
    {}

    MaxVarcharAggregator(const DataTypePtr in, const DataTypePtr out, int32_t channel, bool inputRaw, bool outputPartial)
        : Aggregator(OMNI_AGGREGATION_TYPE_MAX, in, out, channel, inputRaw, outputPartial)
    {}

    ~MaxVarcharAggregator() override {}

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
        Vector *vector = VectorHelper::ExpandVectorAndIndex(vectorBatch->GetVector(channel), rowIndex, offset);
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
#endif // OMNI_RUNTIME_MAX_VARCHAR_AGGREGATOR_H
