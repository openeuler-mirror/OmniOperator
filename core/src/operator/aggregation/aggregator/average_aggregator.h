/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Average aggregate
 */
#ifndef OMNI_RUNTIME_AVERAGE_AGGREGATOR_H
#define OMNI_RUNTIME_AVERAGE_AGGREGATOR_H
#include "aggregator.h"
namespace omniruntime {
namespace op {
    template<typename V, typename ResultType = double>
    class AverageAggregator : public Aggregator {
    public:
        AverageAggregator(int32_t in, int32_t out) : Aggregator(OMNI_AGGREGATION_TYPE_AVG, in, out) {}

        AverageAggregator(int32_t in, int32_t out, bool inputRaw, bool outputPartial)
                : Aggregator(OMNI_AGGREGATION_TYPE_AVG, in, out, inputRaw, outputPartial) {}

        ~AverageAggregator() override {}

        void ProcessGroup(AggregateState &state, Vector *vector, uint32_t offset) override {
            if (UNLIKELY(vector->IsValueNull(offset))) {
                return;
            }
            if (inputRaw == true) {
                if (state.val == nullptr) {
                    this->InitiateGroup(state, vector, offset);
                    return;
                }
                auto currentVal = static_cast<ResultType *>(state.avgVal);
                *reinterpret_cast<ResultType *>(state.avgVal) =
                        (static_cast<V *>(vector))->GetValue(offset) + *currentVal;
                ++state.avgCnt;
            } else {
                if (state.val == nullptr) {
                    this->InitiateGroup(state, vector, offset);
                    return;
                }
                auto containerVector = static_cast<ContainerVector *>(vector);
                DoubleVector *avgValVector = reinterpret_cast<DoubleVector *>(containerVector->GetValue(0));
                LongVector *avgCountVector = reinterpret_cast<LongVector *>(containerVector->GetValue(1));
                double avgVal = avgValVector->GetValue(offset);
                int64_t avgCnt = avgCountVector->GetValue(offset);
                auto currentVal = static_cast<double *>(state.avgVal);
                auto currentCnt = static_cast<int64_t>(state.avgCnt);
                if (avgCnt == 0) {
                    // Fixme use error code
                    LogError("Divisor should not be zero! Offset = %d", offset);
                }
                state.avgCnt += avgCnt;
                *currentVal += avgVal;
            }
        }

        void ProcessNonGroup(Vector *vector, uint32_t offset) override {
            if (UNLIKELY(vector->IsValueNull(offset))) {
                return;
            }

            if (nonGroupState.val == nullptr) {
                this->InitiateNonGroup(vector, offset);
                return;
            }
            auto currentVal = static_cast<ResultType *>(nonGroupState.avgVal);
            *currentVal += (static_cast<V *>(vector))->GetValue(offset);
            ++nonGroupState.avgCnt;
        }

        void InitiateGroup(AggregateState &state, Vector *vector, uint32_t offset) override {
            if (UNLIKELY(vector->IsValueNull(offset))) {
                return;
            }
            // for partial aggregation
            if (inputRaw == true) {
                auto rowVal = (static_cast<V *>(vector))->GetValue(offset);
                auto len = sizeof(ResultType);
                auto ptr = executionContext->getArena()->Allocate(len);
                *reinterpret_cast<ResultType *>(ptr) = rowVal;
                state.avgVal = ptr;
                state.avgCnt = 1;
            } else {
                auto containerVector = static_cast<ContainerVector *>(vector);
                auto *avgValVector = reinterpret_cast<DoubleVector *>(containerVector->GetValue(0));
                auto *avgCountVector = reinterpret_cast<LongVector *>(containerVector->GetValue(1));
                double avgVal = avgValVector->GetValue(offset);
                int64_t avgCnt = avgCountVector->GetValue(offset);
                if (avgCnt == 0) {
                    // Fixme use error code
                    LogError("Divisor should not be zero! Offset = %d", offset);
                }
                auto ptr = executionContext->getArena()->Allocate(sizeof(double));
                *reinterpret_cast<double *>(ptr) = avgVal;
                state.avgVal = ptr;
                state.avgCnt = avgCnt;
            }
        }

        void InitiateNonGroup(Vector *vector, uint32_t offset) override {
            if (UNLIKELY(vector->IsValueNull(offset))) {
                return;
            }

            auto rowVal = (static_cast<V *>(vector))->GetValue(offset);
            auto ptr = executionContext->getArena()->Allocate(sizeof(ResultType));
            *reinterpret_cast<ResultType *>(ptr) = rowVal;
            nonGroupState.avgVal = ptr;
            nonGroupState.avgCnt = 1;
        }

        void *Evaluate(const AggregateState &state) override {
            if (state.val == nullptr) {
                return nullptr;
            }
            if (state.avgCnt <= 0) {
                LogError("Divisor has to be larger than 0!");
                return nullptr;
            }
            auto currentVal = static_cast<ResultType *>(state.avgVal);
            auto ptr = executionContext->getArena()->Allocate(sizeof(ResultType));
            auto finalState = reinterpret_cast<ResultType *>(ptr);
            *finalState = *currentVal / state.avgCnt;
            return finalState;
        }

        void ExtractValue(Vector *vector, AggregateState &state, int32_t rowIndex) override {
            if (outputPartial == true) {
                ContainerVector *v = static_cast<ContainerVector *>(vector);
                if (state.val == nullptr) {
                    v->SetValueNull(rowIndex);
                    return;
                }
                if (state.avgCnt == 0) {
                    LogError("Divisor is zero!");
                }
                DoubleVector *doubleVector = reinterpret_cast<DoubleVector *>(v->GetValue(0));
                doubleVector->SetValue(rowIndex, *static_cast<double *>(state.avgVal));
                LongVector *longVector = reinterpret_cast<LongVector *>(v->GetValue(1));
                longVector->SetValue(rowIndex, state.avgCnt);
            } else {
                DoubleVector *v = static_cast<DoubleVector *>(vector);
                if (state.val == nullptr) {
                    v->SetValueNull(rowIndex);
                    return;
                }
                if (state.avgCnt == 0) {
                    LogError("Divisor is zero!");
                }
                v->SetValue(rowIndex, *static_cast<ResultType *>(this->Evaluate(state)));
            }
        }
    };
}
}
#endif //OMNI_RUNTIME_AVERAGE_AGGREGATOR_H
