/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Count aggregate
 */
#ifndef OMNI_RUNTIME_COUNT_AGGREGATOR_H
#define OMNI_RUNTIME_COUNT_AGGREGATOR_H
#include "aggregator.h"
namespace omniruntime {
namespace op {
    class CountAggregator : public Aggregator {
    public:
        CountAggregator(int32_t in, int32_t out) : Aggregator(OMNI_AGGREGATION_TYPE_COUNT, in, out) {}

        CountAggregator(int32_t in, int32_t out, bool inputRaw, bool outputPartial)
                : Aggregator(OMNI_AGGREGATION_TYPE_COUNT, in, out, inputRaw, outputPartial) {}

        ~CountAggregator() override {}

        void ProcessGroup(AggregateState &state, Vector *vector, uint32_t offset) override {
            if (UNLIKELY(vector->IsValueNull(offset))) {
                return;
            }
            if (inputRaw) {
                state.count++;
            } else {
                state.count += (static_cast<LongVector *>(vector))->GetValue(offset);
            }
        }

        void ProcessNonGroup(Vector *vector, uint32_t offset) override {
            if (!initiated) {
                InitiateNonGroup(vector, offset);
                return;
            }
            if (UNLIKELY(vector->IsValueNull(offset))) {
                return;
            }
            if (inputRaw) {
                nonGroupState.count++;
            } else {
                nonGroupState.count += reinterpret_cast<LongVector *>(vector)->GetValue(offset);
            }
        }

        void InitiateGroup(AggregateState &state, Vector *vector, uint32_t offset) override {
            // It is only effective when COUNT(col). When COUNT(*) or COUNT(1) should directly accumulate vector size;
            if (UNLIKELY(vector->IsValueNull(offset))) {
                state.count = 0;
                return;
            }

            if (inputRaw) {
                state.count = 1;
                return;
            }

            state.count = (static_cast<LongVector *>(vector))->GetValue(offset);
        }

        void InitiateNonGroup(Vector *vector, uint32_t offset) override {
            // It is only effective when COUNT(col). When COUNT(*) or COUNT(1) should directly accumulate vector size;
            if (UNLIKELY(vector->IsValueNull(offset))) {
                nonGroupState.count = 0;
                return;
            }

            if (inputRaw) {
                nonGroupState.count = 1;
                initiated = true;
                return;
            }
            nonGroupState.count = (static_cast<LongVector *>(vector))->GetValue(offset);
            initiated = true;
        }

        void *Evaluate(const AggregateState &state) override {
            return &(const_cast<AggregateState &>(state).count);
        }

        void ExtractValue(Vector *vector, AggregateState &state, int32_t rowIndex) override {
            auto v = static_cast<LongVector *>(vector);
            if (state.val == nullptr) {
                v->SetValueNull(rowIndex);
                return;
            }
            v->SetValue(rowIndex, state.count);
        }
    };
}
}

#endif //OMNI_RUNTIME_COUNT_AGGREGATOR_H
