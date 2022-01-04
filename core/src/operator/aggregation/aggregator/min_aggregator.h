/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Min aggregate
 */
#ifndef OMNI_RUNTIME_MIN_AGGREGATOR_H
#define OMNI_RUNTIME_MIN_AGGREGATOR_H
#include "aggregator.h"
namespace omniruntime {
namespace op {
    template<typename V, typename ResultType>
    class MinAggregator : public Aggregator {
    public:
        MinAggregator(int32_t in, int32_t out) : Aggregator(OMNI_AGGREGATION_TYPE_MIN, in, out) {}

        MinAggregator(int32_t in, int32_t out, bool inputRaw, bool outputPartial)
                : Aggregator(OMNI_AGGREGATION_TYPE_MIN, in, out, inputRaw, outputPartial) {}

        ~MinAggregator() override {}

        void ProcessGroup(AggregateState &state, Vector *vector, uint32_t offset) override {
            if (UNLIKELY(vector->IsValueNull(offset))) {
                return;
            }
            if (state.val == nullptr) {
                this->InitiateGroup(state, vector, offset);
                return;
            }
            auto rowVal = (static_cast<V *>(vector))->GetValue(offset);
            auto leftVal = static_cast<ResultType *>(state.val);
            *leftVal = (Compare(*leftVal, rowVal) == -1) ? *leftVal : rowVal;
        }

        void ProcessNonGroup(Vector *vector, uint32_t offset) override {
            if (UNLIKELY(vector->IsValueNull(offset))) {
                return;
            }

            if (nonGroupState.val == nullptr) {
                this->InitiateNonGroup(vector, offset);
                return;
            }
            auto rowVal = (static_cast<V *>(vector))->GetValue(offset);
            auto leftVal = static_cast<ResultType *>(nonGroupState.val);
            *leftVal = (Compare(*leftVal, rowVal) == -1) ? *leftVal : rowVal;
        }

        void InitiateGroup(AggregateState &state, Vector *vector, uint32_t offset) override {
            if (UNLIKELY(vector->IsValueNull(offset))) {
                return;
            }
            auto rowVal = static_cast<V *>(vector)->GetValue(offset);
            int32_t len = sizeof(ResultType);
            auto ptr = executionContext->getArena()->Allocate(len);
            *reinterpret_cast<ResultType *>(ptr) = rowVal;
            state.val = ptr;
        }

        void InitiateNonGroup(Vector *vector, uint32_t offset) override {
            if (UNLIKELY(vector->IsValueNull(offset))) {
                return;
            }

            auto curVal = (static_cast<V *>(vector))->GetValue(offset);
            auto ptr = executionContext->getArena()->Allocate(sizeof(ResultType));
            *reinterpret_cast<ResultType *>(ptr) = curVal;
            nonGroupState.val = ptr;
        }

        void* Evaluate(const AggregateState &state) override {
            return state.val;
        }

        // TODO extract common function for sum/min/max
        void ExtractValue(Vector *vector, AggregateState &state, int32_t rowIndex) override {
            auto v = static_cast<V *>(vector);
            if (state.val == nullptr) {
                v->SetValueNull(rowIndex);
                return;
            }
            v->SetValue(rowIndex, *static_cast<ResultType *>(state.val));
        }
    };

}
}
#endif //OMNI_RUNTIME_MIN_AGGREGATOR_H
