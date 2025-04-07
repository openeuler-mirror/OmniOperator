/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: For non-decimal type
 */
#ifndef OMNI_RUNTIME_TRY_SUM_FLAT_IM_AGGREGATOR_H
#define OMNI_RUNTIME_TRY_SUM_FLAT_IM_AGGREGATOR_H

#include "aggregator.h"
#include "sum_aggregator.h"
#include "simd/func/reduce.h"

namespace omniruntime {
namespace op {
template <DataTypeId IN_ID, DataTypeId OUT_ID> class TrySumFlatIMAggregator : public TypedAggregator {
    using InType = typename AggNativeAndVectorType<IN_ID>::type;
    using ResultType = typename AggNativeAndVectorType<OUT_ID>::type;

    // inner class for aggregate state, the member depends on ResultType of Aggregator
#pragma pack(push, 1)
    struct TrySumFlatState : BaseCountState<ResultType> {
        static const TrySumFlatIMAggregator<IN_ID, OUT_ID>::TrySumFlatState *ConstCastState(const AggregateState *state)
        {
            return reinterpret_cast<const TrySumFlatIMAggregator<IN_ID, OUT_ID>::TrySumFlatState *>(state);
        }

        static TrySumFlatIMAggregator<IN_ID, OUT_ID>::TrySumFlatState *CastState(AggregateState *state)
        {
            return reinterpret_cast<TrySumFlatIMAggregator<IN_ID, OUT_ID>::TrySumFlatState *>(state);
        }

        template <typename TypeIn, typename TypeOut> static void UpdateState(AggregateState *state, const TypeIn &in)
        {
            auto *maxState = CastState(state);
            SumOp<TypeIn, TypeOut, int64_t, StateCountHandler, true>(&(maxState->value),
                maxState->count, in, 1ULL);
        }

        template <typename TypeIn, typename TypeOut, bool addIf>
        static void UpdateStateWithCondition(AggregateState *state, const TypeIn &in, const uint8_t &condition)
        {
            if (condition == addIf) {
                UpdateState<TypeIn, TypeOut>(state, in);
            }
        }
    };
#pragma pack(pop)

public:
    TrySumFlatIMAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels,
        bool inputRaw, bool outputPartial, bool isOverflowAsNull)
        : TypedAggregator(OMNI_AGGREGATION_TYPE_SUM, inputTypes, outputTypes, channels, inputRaw, outputPartial,
        isOverflowAsNull)
    {}
    TrySumFlatIMAggregator(const FunctionType aggFunc, const DataTypes &inputTypes, const DataTypes &outputTypes,
        std::vector<int32_t> &channels, bool inputRaw, bool outputPartial, bool isOverflowAsNull)
        : TypedAggregator(aggFunc, inputTypes, outputTypes, channels, inputRaw, outputPartial, isOverflowAsNull)
    {}
    ~TrySumFlatIMAggregator() override = default;

    void ProcessGroupInternalFinal(std::vector<AggregateState *> &rowStates, BaseVector *vector,
        const int32_t rowOffset, const std::shared_ptr<NullsHelper> nullMap)
    {
        // final stage : input vector will be Vector<ResultType>
        auto *ptr = reinterpret_cast<ResultType *>(GetValuesFromVector<OUT_ID>(vector));
        ptr += rowOffset;
        if (nullMap == nullptr) {
            AddUseRowIndex<ResultType, TrySumFlatState::template UpdateState<ResultType, ResultType>>(rowStates,
                aggStateOffset, ptr);
        } else {
            AddConditionalUseRowIndex<ResultType,
                TrySumFlatState::template UpdateStateWithCondition<ResultType, ResultType, false>>(rowStates,
                aggStateOffset, ptr, *nullMap);
        }
    }

    void ProcessGroupInternal(std::vector<AggregateState *> &rowStates, BaseVector *vector, const int32_t rowOffset,
        const std::shared_ptr<NullsHelper> nullMap)
    {
        if (inputRaw) {
            if (vector->GetEncoding() != vec::OMNI_DICTIONARY) {
                auto *ptr = reinterpret_cast<InType *>(GetValuesFromVector<IN_ID>(vector));
                ptr += rowOffset;
                if (nullMap == nullptr) {
                    AddUseRowIndex<InType, TrySumFlatState::template UpdateState<InType, ResultType>>(rowStates,
                        aggStateOffset, ptr);
                } else {
                    AddConditionalUseRowIndex<InType,
                        TrySumFlatState::template UpdateStateWithCondition<InType, ResultType, false>>(rowStates,
                        aggStateOffset, ptr, *nullMap);
                }
            } else {
                auto *ptr = reinterpret_cast<InType *>(GetValuesFromDict<IN_ID>(vector));
                auto *indexMap = GetIdsFromDict<IN_ID>(vector) + rowOffset;
                if (nullMap == nullptr) {
                    AddDictUseRowIndex<InType, TrySumFlatState::template UpdateState<InType, ResultType>>(rowStates,
                        aggStateOffset, ptr, indexMap);
                } else {
                    AddDictConditionalUseRowIndex<InType, ResultType,
                        TrySumFlatState::template UpdateStateWithCondition<InType, ResultType, false>>(rowStates,
                        aggStateOffset, ptr, *nullMap, indexMap);
                }
            }
        } else {
            // no dictionary in input when stage is not partial
            ProcessGroupInternalFinal(rowStates, vector, rowOffset, nullMap);
        }
    }

    void ProcessGroupUnspill(std::vector<UnspillRowInfo> &unspillRows, int32_t rowCount, int32_t &vectorIndex) override
    {
        auto firstVecIdx = vectorIndex++;
        for (int32_t rowIdx = 0; rowIdx < rowCount; rowIdx++) {
            auto &row = unspillRows[rowIdx];
            auto index = row.rowIdx;
            auto sumVector = static_cast<Vector<ResultType> *>(row.batch->Get(firstVecIdx));
            TrySumFlatState *state = TrySumFlatState::CastState(row.state + aggStateOffset);
            auto value = sumVector->GetValue(index);
            if (!sumVector->IsNull(index)) {
                SumOp<ResultType, ResultType, int64_t, StateCountHandler, true>((ResultType *)(&state->value),
                    state->count, value, 1LL);
            } else if (value == SPILL_OVERFLOW_VALUE) {
                state->count=-1;
            }
        }
    }

    void ProcessSingleInternalFinal(AggregateState *state, BaseVector *vector, const int32_t rowOffset,
        const int32_t rowCount, const std::shared_ptr<NullsHelper> nullMap)
    {
        TrySumFlatState *trySumFlatState = TrySumFlatState::CastState(state);
        auto *ptr = reinterpret_cast<ResultType *>(GetValuesFromVector<OUT_ID>(vector));
        ptr += rowOffset;
        if (nullMap == nullptr) {
                Add<ResultType, ResultType, int64_t, SumOp<ResultType, ResultType, int64_t, StateCountHandler>>(
                    reinterpret_cast<ResultType *>(&trySumFlatState->value), trySumFlatState->count, ptr, rowCount);
        } else {
                AddConditional<ResultType , ResultType, int64_t, SumConditionalOp<ResultType, ResultType, int64_t, StateCountHandler, false>>(
                    reinterpret_cast<ResultType *>(&trySumFlatState->value), trySumFlatState->count, ptr, rowCount, *nullMap);
        }
    }

    void ProcessSingleInternal(AggregateState *state, BaseVector *vector, const int32_t rowOffset,
        const int32_t rowCount, const std::shared_ptr<NullsHelper> nullMap)
    {
        TrySumFlatState *trySumFlatState = TrySumFlatState::CastState(state);
        if (inputRaw) {
            if (vector->GetEncoding() != vec::OMNI_DICTIONARY) {
                auto *ptr = reinterpret_cast<InType *>(GetValuesFromVector<IN_ID>(vector));
                ptr += rowOffset;
                if (nullMap == nullptr) {
                        Add<InType, ResultType, int64_t, SumOp<InType, ResultType, int64_t, StateCountHandler>>(
                            reinterpret_cast<ResultType *>(&trySumFlatState->value), trySumFlatState->count, ptr, rowCount);
                } else {
                        AddConditional<InType, ResultType, int64_t, SumConditionalOp<InType, ResultType, int64_t, StateCountHandler, false>>(
                            reinterpret_cast<ResultType *>(&trySumFlatState->value), trySumFlatState->count, ptr, rowCount, *nullMap);
                }
            } else {
                auto *ptr = reinterpret_cast<InType *>(GetValuesFromDict<IN_ID>(vector));
                auto *indexMap = GetIdsFromDict<IN_ID>(vector) + rowOffset;
                if (nullMap == nullptr) {
                        AddDict<InType, ResultType, int64_t, SumOp<InType, ResultType, int64_t, StateCountHandler>>(
                            reinterpret_cast<ResultType *>(&trySumFlatState->value), trySumFlatState->count, ptr, rowCount, indexMap);
                } else {
                        AddDictConditional<InType, ResultType, int64_t,
                            SumConditionalOp<InType, ResultType, int64_t, StateCountHandler, false>>(
                            &trySumFlatState->value, trySumFlatState->count, ptr, rowCount, *nullMap, indexMap);
                }
            }
        } else {
            ProcessSingleInternalFinal(state, vector, rowOffset, rowCount, nullMap);
        }
    }

    size_t GetStateSize() override
    {
        return sizeof(TrySumFlatState);
    }

    void InitState(AggregateState *state) override
    {
        TrySumFlatState *trySumFlatState = TrySumFlatState::CastState(state + aggStateOffset);
        trySumFlatState->value = ResultType{};
        trySumFlatState->count=0;
    }

    void InitStates(std::vector<AggregateState *> &groupStates) override
    {
        for (auto groupState : groupStates) {
            InitState(groupState);
        }
    }

    std::vector<DataTypePtr> GetSpillType() override
    {
        std::vector<DataTypePtr> spillTypes;
        spillTypes.emplace_back(outputTypes.GetType(0));
        return spillTypes;
    }

    void ExtractValuesForSpill(std::vector<AggregateState *> &groupStates, std::vector<BaseVector *> &vectors) override
    {
        auto spillValueVec = static_cast<Vector<ResultType> *>(vectors[0]);
        auto rowCount = static_cast<int32_t>(groupStates.size());
        for (int32_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            TrySumFlatState *state = TrySumFlatState::CastState(groupStates[rowIndex] + aggStateOffset);
            auto count = state->count;
            bool isOverflow = count<0;
            bool isEmpty = count==0;
            if (isEmpty) {
                // set null for empty group(all rows are NULL) when spill to ensure skip empty group when unspill
                spillValueVec->SetNull(rowIndex);
                spillValueVec->SetValue(rowIndex, SPILL_EMPTY_VALUE);
            } else if (isOverflow) {
                spillValueVec->SetNull(rowIndex);
                spillValueVec->SetValue(rowIndex, SPILL_OVERFLOW_VALUE);
            } else {
                spillValueVec->SetValue(rowIndex, state->value);
            }
        }
    }

    void ExtractValues(const AggregateState *state, std::vector<BaseVector *> &vectors, int32_t rowIndex) override
    {
        auto v = static_cast<Vector<ResultType> *>(vectors[0]);
        const TrySumFlatState *trySumFlatState = TrySumFlatState::ConstCastState(state + aggStateOffset);
        bool isOverflow = trySumFlatState->count<0;
        bool isEmpty = trySumFlatState->count==0;
        if (isOverflow || isEmpty) {
            v->SetNull(rowIndex);
        } else {
            v->SetValue(rowIndex, static_cast<ResultType>(trySumFlatState->value));
        }
    }

    void ExtractValuesBatch(std::vector<AggregateState *> &groupStates, std::vector<BaseVector *> &vectors,
        int32_t rowOffset, int32_t rowCount) override
    {
        for (int32_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            ExtractValues(groupStates[rowIndex] + aggStateOffset, vectors, rowIndex);
        }
    }

    void ProcessAlignAggSchema(VectorBatch *result, BaseVector *originVector,
        const std::shared_ptr<NullsHelper> nullMap, const bool aggFilter) override
    {
        int rowCount = originVector->GetSize();
        // opt branch
        if constexpr (std::is_same_v<InType, ResultType>) {
            if (!aggFilter) {
                auto sumVector = VectorHelper::SliceVector(originVector, 0, rowCount);
                result->Append(sumVector);
                return;
            }
        }

        if (originVector->GetEncoding() == OMNI_DICTIONARY) {
            ProcessAlignAggSchemaInternal<Vector<DictionaryContainer<InType>>>(result, originVector, nullMap);
        } else {
            ProcessAlignAggSchemaInternal<Vector<InType>>(result, originVector, nullMap);
        }
    }

    template<typename T>
    void ProcessAlignAggSchemaInternal(VectorBatch *result, BaseVector *originVector,
        const std::shared_ptr<NullsHelper> nullMap)
    {
        int rowCount = originVector->GetSize();
        auto sumVector = reinterpret_cast<Vector<ResultType> *>(VectorHelper::CreateFlatVector(OUT_ID, rowCount));
        // The varchar type is converted to the double type in advance, so InType can't be the varchar type.
        auto vector = reinterpret_cast<T *>(originVector);
        if (nullMap != nullptr) {
            for (int index = 0; index < rowCount; ++index) {
                if ((*nullMap)[index]) {
                    sumVector->SetNull(index);
                } else {
                    sumVector->SetValue(index, (ResultType)vector->GetValue(index));
                }
            }
        } else {
            for (int index = 0; index < rowCount; ++index) {
                sumVector->SetValue(index, (ResultType)vector->GetValue(index));
            }
        }
        result->Append(sumVector);
    }

private:
    static constexpr ResultType SPILL_EMPTY_VALUE{0};
    static constexpr ResultType SPILL_OVERFLOW_VALUE{-1};
};
}
}
#endif // OMNI_RUNTIME_SUM_FLAT_IM_AGGREGATOR_H
