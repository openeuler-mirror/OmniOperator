/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Max aggregate for varchar
 */
#ifndef OMNI_RUNTIME_MAX_VARCHAR_AGGREGATOR_H
#define OMNI_RUNTIME_MAX_VARCHAR_AGGREGATOR_H
#include "aggregator.h"
namespace omniruntime {
namespace op {
    class MaxVarcharAggregator : public Aggregator {
    public:
        MaxVarcharAggregator(int32_t in, int32_t out) : Aggregator(OMNI_AGGREGATION_TYPE_MAX, in, out) {}

        MaxVarcharAggregator(int32_t in, int32_t out, bool inputRaw, bool outputPartial)
        : Aggregator(OMNI_AGGREGATION_TYPE_MIN, in, out, inputRaw, outputPartial) {}

        ~MaxVarcharAggregator() override {}

        void ProcessGroup(AggregateState &state, Vector *vector, uint32_t offset) override
        {
            if (UNLIKELY(vector->IsValueNull(offset))) {
                return;
            }
            if (state.val == nullptr) {
                this->InitiateGroup(state, vector, offset);
                return;
            }
            uint8_t *rowVal = nullptr;
            int valLen = (static_cast<VarcharVector *>(vector))->GetValue(offset, &rowVal);
            auto leftVal = reinterpret_cast<char *>(state.strVal);
            if (memcmp(leftVal, (char *) rowVal, std::min(valLen, state.strLen)) < 0) {
                auto err = memcpy_s(leftVal, valLen, rowVal, valLen);
                if (err != EOK) {
                    LogError("set data failed in variable vector. %d", err);
                }
            }
            return;
        }

        void ProcessNonGroup(Vector *vector, uint32_t offset) override {
            if (UNLIKELY(vector->IsValueNull(offset))) {
                return;
            }

            if (nonGroupState.val == nullptr) {
                this->InitiateNonGroup(vector, offset);
                return;
            }
            uint8_t *rowVal = nullptr;
            int valLen = (static_cast<VarcharVector *>(vector))->GetValue(offset, &rowVal);
            auto leftVal = reinterpret_cast<char *>(nonGroupState.strVal);
            if (memcmp(leftVal, (char *) rowVal, std::min(valLen, nonGroupState.strLen)) < 0) {
                auto err = memcpy_s(leftVal, valLen, rowVal, valLen);
                if (err != EOK) {
                    LogError("set data failed in variable vector. %d", err);
                }
            }
        }

        void InitiateGroup(AggregateState &state, Vector *vector, uint32_t offset) override
        {
            if (UNLIKELY(vector->IsValueNull(offset))) {
                return;
            }
            uint8_t *data = nullptr;
            int valLen = static_cast<VarcharVector *>(vector)->GetValue(offset, &data);
            auto ptr = executionContext->getArena()->Allocate(valLen);
            auto err = memcpy_s(ptr, valLen, data, valLen);
            if (err != EOK) {
                LogError("set data failed in variable vector. %d", err);
            }
            state.strVal = ptr;
            state.strLen = valLen;
        }

        void InitiateNonGroup(Vector *vector, uint32_t offset) override
        {
            if (UNLIKELY(vector->IsValueNull(offset))) {
                return;
            }

            uint8_t *data = nullptr;
            int valLen = static_cast<VarcharVector *>(vector)->GetValue(offset, &data);
            uint8_t *state = executionContext->getArena()->Allocate(valLen);
            auto err = memcpy_s(state, valLen, data, valLen);
            if (err != EOK) {
                LogError("set data failed in variable vector. %d", err);
            }
            nonGroupState.strVal = state;
            nonGroupState.strLen = valLen;
        }

        void* Evaluate(const AggregateState &state) override
        {
            return new std::string(reinterpret_cast<char *>(state.strVal), 0, state.strLen);
        }

        void ExtractValue(Vector *vector, AggregateState &state, int32_t rowIndex) override
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
#endif //OMNI_RUNTIME_MAX_VARCHAR_AGGREGATOR_H
