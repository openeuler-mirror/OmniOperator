/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: Kurtosis aggregate
 */
#ifndef OMNI_RUNTIME_KURTOSIS_AGGREGATOR_H
#define OMNI_RUNTIME_KURTOSIS_AGGREGATOR_H

#include "typed_aggregator.h"
#include "central_moment.h"

namespace omniruntime {
    namespace op {
        template<DataTypeId IN, DataTypeId OUT>
        class KurtosisAggregator : public TypedAggregator {
            using MomentValue = double;
            using Value = typename AggNativeAndVectorType<IN>::type;
            using ValueOut = typename AggNativeAndVectorType<OUT>::type;
            using ValueVector = typename AggNativeAndVectorType<IN>::vector;
            using ValueOutVector = typename AggNativeAndVectorType<OUT>::vector;

#pragma pack(push, 1)

            struct KurtosisState {
                CentralMomentState<MomentValue> centralMomentState;
                AggValueState valueState;

                static const KurtosisAggregator<IN, OUT>::KurtosisState *
                ConstCastState(const AggregateState *state) {
                    return reinterpret_cast<const KurtosisAggregator<IN, OUT>::KurtosisState *>(state);
                }

                static KurtosisAggregator<IN, OUT>::KurtosisState *
                CastState(AggregateState *state) {
                    return reinterpret_cast<KurtosisAggregator<IN, OUT>::KurtosisState *>(state);
                }

                template<typename Value>
                static void UpdateState(AggregateState *state, const Value &value) {
                    auto *pKurtosisState = CastState(state);
                    UpdateCentralMomentOp<MomentValue, Value>(&(pKurtosisState->centralMomentState.count),
                                                              &(pKurtosisState->centralMomentState.centralMoment1),
                                                              &(pKurtosisState->centralMomentState.centralMoment2),
                                                              &(pKurtosisState->centralMomentState.centralMoment3),
                                                              &(pKurtosisState->centralMomentState.centralMoment4),
                                                              &(pKurtosisState->centralMomentState.momentOrder),
                                                              pKurtosisState->valueState, value);
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
            ~KurtosisAggregator() override = default;

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
                return sizeof(KurtosisState);
            }

            static std::unique_ptr<Aggregator> Create(const DataTypes &inputTypes, const DataTypes &outputTypes,
                                                      std::vector<int32_t> &channels, bool rawIn, bool partialOut,
                                                      bool isOverflowAsNull) {
                if constexpr ((IN == OMNI_DOUBLE || IN == OMNI_LONG || IN == OMNI_SHORT || IN == OMNI_FLOAT || IN ==
                               OMNI_INT)
                              && (OUT == OMNI_DOUBLE || OUT == OMNI_LONG)) {
                    return std::unique_ptr<KurtosisAggregator<IN, OUT> >(
                        new KurtosisAggregator<IN, OUT>(
                            inputTypes,
                            outputTypes, channels, rawIn, partialOut, isOverflowAsNull));
                } else if constexpr (IN != OMNI_DOUBLE && IN != OMNI_LONG && IN != OMNI_SHORT && IN != OMNI_FLOAT && IN
                                     != OMNI_INT) {
                    LogError("Error in KurtosisAggregator aggregator: Unsupported value type %s",
                             TypeUtil::TypeToStringLog(IN).c_str());
                    return nullptr;
                } else if constexpr (OUT != OMNI_DOUBLE && OUT != OMNI_LONG) {
                    LogError("Error in KurtosisAggregator aggregator: Unsupported value type %s",
                             TypeUtil::TypeToStringLog(OUT).c_str());
                    return nullptr;
                }
            }

            void ProcessGroupUnspill(std::vector<UnspillRowInfo> &unspillRows, int32_t rowCount,
                                     int32_t &vectorIndex) override;

            void ProcessAlignAggSchema(VectorBatch *result, BaseVector *originVector,
                                       const std::shared_ptr<NullsHelper> nullMap, const bool aggFilter) override;

        protected:
            KurtosisAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes,
                               std::vector<int32_t> &channels,
                               const bool inputRaw, const bool outputPartial, const bool isOverflowAsNull);

            void ProcessSingleInternal(AggregateState *state, BaseVector *vector, const int32_t rowOffset,
                                       const int32_t rowCount, const std::shared_ptr<NullsHelper> nullMap) override;

            void
            ProcessGroupInternal(std::vector<AggregateState *> &rowStates, BaseVector *vector, const int32_t rowOffset,
                                 const std::shared_ptr<NullsHelper> nullMap) override;

            template<typename T>
            void ProcessAlignAggSchemaInternal(VectorBatch *result, BaseVector *originVector,
                                               const std::shared_ptr<NullsHelper> nullMap);
        };
    }
}
#endif // OMNI_RUNTIME_KURTOSIS_AGGREGATOR_H
