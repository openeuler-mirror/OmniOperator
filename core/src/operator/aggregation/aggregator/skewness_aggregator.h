/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: Skewness aggregate
 */
#ifndef OMNI_RUNTIME_SKEWNESS_AGGREGATOR_H
#define OMNI_RUNTIME_SKEWNESS_AGGREGATOR_H

#include "typed_aggregator.h"
#include "central_moment.h"

namespace omniruntime {
    namespace op {
        template<DataTypeId IN, DataTypeId OUT>
        class SkewnessAggregator : public TypedAggregator {
            using MomentValue = double;
            using Value = typename AggNativeAndVectorType<IN>::type;
            using ValueOut = typename AggNativeAndVectorType<OUT>::type;
            using ValueVector = typename AggNativeAndVectorType<IN>::vector;
            using ValueOutVector = typename AggNativeAndVectorType<OUT>::vector;

#pragma pack(push, 1)

            struct SkewnessState {
                CentralMomentState<MomentValue> centralMomentState;
                AggValueState valueState;

                static const SkewnessAggregator<IN, OUT>::SkewnessState *
                ConstCastState(const AggregateState *state) {
                    return reinterpret_cast<const SkewnessAggregator<IN, OUT>::SkewnessState *>(state);
                }

                static SkewnessAggregator<IN, OUT>::SkewnessState *
                CastState(AggregateState *state) {
                    return reinterpret_cast<SkewnessAggregator<IN, OUT>::SkewnessState *>(state);
                }

                template<typename Value>
                static void UpdateState(AggregateState *state, const Value &value) {
                    auto *pSkewnessState = CastState(state);
                    UpdateCentralMomentOp<MomentValue, Value>(&(pSkewnessState->centralMomentState.count),
                                                              &(pSkewnessState->centralMomentState.centralMoment1),
                                                              &(pSkewnessState->centralMomentState.centralMoment2),
                                                              &(pSkewnessState->centralMomentState.centralMoment3),
                                                              &(pSkewnessState->centralMomentState.centralMoment4),
                                                              &(pSkewnessState->centralMomentState.momentOrder),
                                                              pSkewnessState->valueState, value);
                }

                template<typename Value, bool addIf>
                static void
                UpdateStateWithCondition(AggregateState *state, const Value &value, const uint8_t &condition) {
                    if (condition == addIf) {
                        UpdateState<Value>(state, value);
                    }
                }
            };

#pragma pack(pop)

        public:
            ~SkewnessAggregator() override = default;

            void
            ExtractValues(const AggregateState *state, std::vector<BaseVector *> &vectors, int32_t rowIndex) override;

            void ExtractValuesBatch(std::vector<AggregateState *> &groupStates, std::vector<BaseVector *> &vectors,
                                    int32_t rowOffset, int32_t rowCount) override;

            void ExtractValuesForSpill(std::vector<AggregateState *> &groupStates,
                                       std::vector<BaseVector *> &vectors) override;

            void InitState(AggregateState *state) override;

            void InitStates(std::vector<AggregateState *> &groupStates) override;

            std::vector<DataTypePtr> GetSpillType() override;

            size_t GetStateSize() override {
                return sizeof(SkewnessState);
            }

            static std::unique_ptr<Aggregator> Create(const DataTypes &inputTypes, const DataTypes &outputTypes,
                                                      std::vector<int32_t> &channels, bool rawIn, bool partialOut,
                                                      bool isOverflowAsNull) {
                if constexpr ((IN == OMNI_DOUBLE || IN == OMNI_LONG || IN == OMNI_SHORT || IN == OMNI_FLOAT || IN ==
                               OMNI_INT)
                              && (OUT == OMNI_DOUBLE || OUT == OMNI_LONG)) {
                    return std::unique_ptr<SkewnessAggregator<IN, OUT> >(
                        new SkewnessAggregator<IN, OUT>(
                            inputTypes,
                            outputTypes, channels, rawIn, partialOut, isOverflowAsNull));
                } else if constexpr (IN != OMNI_DOUBLE && IN != OMNI_LONG && IN != OMNI_SHORT && IN != OMNI_FLOAT && IN
                                     != OMNI_INT) {
                    LogError("Error in SkewnessAggregator aggregator: Unsupported value type %s",
                             TypeUtil::TypeToStringLog(IN).c_str());
                    return nullptr;
                } else if constexpr (OUT != OMNI_DOUBLE && OUT != OMNI_LONG) {
                    LogError("Error in SkewnessAggregator aggregator: Unsupported value type %s",
                             TypeUtil::TypeToStringLog(OUT).c_str());
                    return nullptr;
                }
            }

            void ProcessGroupUnspill(std::vector<UnspillRowInfo> &unspillRows, int32_t rowCount,
                                     int32_t &vectorIndex) override;

            void ProcessAlignAggSchema(VectorBatch *result, BaseVector *originVector,
                                       const std::shared_ptr<NullsHelper> nullMap, const bool aggFilter) override;

        protected:
            SkewnessAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes,
                               std::vector<int32_t> &channels,
                               const bool inputRaw, const bool outputPartial, const bool isOverflowAsNull);

            void ProcessSingleInternal(AggregateState *state, BaseVector *vector, const int32_t rowOffset,
                                       const int32_t rowCount, const std::shared_ptr<NullsHelper> nullMap) override;

            void
            ProcessGroupInternal(std::vector<AggregateState *> &rowStates, BaseVector *vector, const int32_t rowOffset,
                                 const std::shared_ptr<NullsHelper> nullMap) override;

            void ProcessAlignAggSchemaInternal(VectorBatch *result, BaseVector *originVector,
                                               const std::shared_ptr<NullsHelper> nullMap);
        };
    }
}
#endif // OMNI_RUNTIME_SKEWNESS_AGGREGATOR_H
