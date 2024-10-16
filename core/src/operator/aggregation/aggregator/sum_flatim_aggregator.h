/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2024. All rights reserved.
 * Description: For non-decimal type
 */
#ifndef OMNI_RUNTIME_SUM_FLAT_IM_AGGREGATOR_H
#define OMNI_RUNTIME_SUM_FLAT_IM_AGGREGATOR_H

#include "aggregator.h"
#include "operator/aggregation/neon_aggregation/simd_aggregation_external.h"

namespace omniruntime {
namespace op {
template <DataTypeId IN_ID, DataTypeId OUT_ID> class SumFlatIMAggregator : public TypedAggregator {
    using InType = typename AggNativeAndVectorType<IN_ID>::type;
    using ResultType = typename AggNativeAndVectorType<OUT_ID>::type;

public:
    SumFlatIMAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels,
        bool inputRaw, bool outputPartial, bool isOverflowAsNull)
        : TypedAggregator(OMNI_AGGREGATION_TYPE_SUM, inputTypes, outputTypes, channels, inputRaw, outputPartial,
        isOverflowAsNull)
    {}
    SumFlatIMAggregator(const FunctionType aggFunc, const DataTypes &inputTypes, const DataTypes &outputTypes,
        std::vector<int32_t> &channels, bool inputRaw, bool outputPartial, bool isOverflowAsNull)
        : TypedAggregator(aggFunc, inputTypes, outputTypes, channels, inputRaw, outputPartial, isOverflowAsNull)
    {}
    ~SumFlatIMAggregator() override = default;

    void ProcessGroupInternalFinal(std::vector<AggregateState *> &rowStates, const size_t aggIdx, BaseVector *vector,
        const int32_t rowOffset, const uint8_t *nullMap)
    {
        // final stage : input vector will be Vector<ResultType>
        auto *ptr = reinterpret_cast<ResultType *>(GetValuesFromVector<OUT_ID>(vector));
        ptr += rowOffset;
        if (nullMap == nullptr) {
            AddUseRowIndex<ResultType, ResultType, SumOp<ResultType, ResultType, false>>(rowStates, aggIdx, ptr);
        } else {
            AddConditionalUseRowIndex<ResultType, ResultType, SumConditionalOp<ResultType, ResultType, false, false>>(
                rowStates, aggIdx, ptr, nullMap);
        }
    }

    void ProcessGroupInternal(std::vector<AggregateState *> &rowStates, const size_t aggIdx, BaseVector *vector,
        const int32_t rowOffset, const uint8_t *nullMap)
    {
        if (inputRaw) {
            if (vector->GetEncoding() != vec::OMNI_DICTIONARY) {
                auto *ptr = reinterpret_cast<InType *>(GetValuesFromVector<IN_ID>(vector));
                ptr += rowOffset;
                if (nullMap == nullptr) {
                    AddUseRowIndex<InType, ResultType, SumOp<InType, ResultType, false>>(rowStates, aggIdx, ptr);
                } else {
                    AddConditionalUseRowIndex<InType, ResultType, SumConditionalOp<InType, ResultType, false, false>>(
                        rowStates, aggIdx, ptr, nullMap);
                }
            } else {
                auto *ptr = reinterpret_cast<InType *>(GetValuesFromDict<IN_ID>(vector));
                auto *indexMap = GetIdsFromDict<IN_ID>(vector) + rowOffset;
                if (nullMap == nullptr) {
                    AddDictUseRowIndex<InType, ResultType, SumOp<InType, ResultType, false>>(rowStates, aggIdx, ptr,
                        indexMap);
                } else {
                    AddDictConditionalUseRowIndex<InType, ResultType,
                        SumConditionalOp<InType, ResultType, false, false>>(rowStates, aggIdx, ptr, nullMap, indexMap);
                }
            }
        } else {
            // no dictionary in input when stage is not partial
            ProcessGroupInternalFinal(rowStates, aggIdx, vector, rowOffset, nullMap);
        }
    }

    void ProcessGroupUnspill(std::vector<UnspillRowInfo> &unspillRows, int32_t rowCount, const size_t aggIdx,
        int32_t &vectorIndex) override
    {
        auto firstVecIdx = vectorIndex++;
        for (int32_t rowIdx = 0; rowIdx < rowCount; rowIdx++) {
            auto &row = unspillRows[rowIdx];
            auto index = row.rowIdx;
            auto sumVector = static_cast<Vector<ResultType> *>(row.batch->Get(firstVecIdx));
            if (!sumVector->IsNull(index)) {
                auto &state = row.state[aggIdx];
                auto value = sumVector->GetValue(index);
                if constexpr (std::is_floating_point_v<ResultType>) {
                    SumOp<ResultType, ResultType, false>(reinterpret_cast<ResultType *>(state.val), state.count, value,
                        1LL);
                } else {
                    SumOp<ResultType, ResultType, false>((ResultType *)(&state.val), state.count, value, 1LL);
                }
            }
        }
    }

    void ProcessSingleInternalFinal(AggregateState &state, BaseVector *vector, const int32_t rowOffset,
        const int32_t rowCount, const uint8_t *nullMap)
    {
        auto *ptr = reinterpret_cast<ResultType *>(GetValuesFromVector<OUT_ID>(vector));
        ptr += rowOffset;
        if (nullMap == nullptr) {
            if constexpr (std::is_floating_point_v<ResultType>) {
                simd::SIMDAdd<ResultType, ResultType, simd::BasicOp::Sum>(reinterpret_cast<ResultType *>(state.val),
                    state.count, ptr, rowCount);
            } else {
                simd::SIMDAdd<ResultType, ResultType, simd::BasicOp::Sum>(reinterpret_cast<ResultType *>(&state.val),
                    state.count, ptr, rowCount);
            }
        } else {
            if constexpr (std::is_floating_point_v<ResultType>) {
                simd::SIMDAddConditional<ResultType, ResultType, simd::BasicOp::Sum>(
                    reinterpret_cast<ResultType *>(state.val), state.count, ptr, rowCount, nullMap);
            } else {
                simd::SIMDAddConditional<ResultType, ResultType, simd::BasicOp::Sum>(
                    reinterpret_cast<ResultType *>(&state.val), state.count, ptr, rowCount, nullMap);
            }
        }
    }

    void ProcessSingleInternal(AggregateState &state, BaseVector *vector, const int32_t rowOffset,
        const int32_t rowCount, const uint8_t *nullMap)
    {
        if (inputRaw) {
            if (vector->GetEncoding() != vec::OMNI_DICTIONARY) {
                auto *ptr = reinterpret_cast<InType *>(GetValuesFromVector<IN_ID>(vector));
                ptr += rowOffset;
                if (nullMap == nullptr) {
                    if constexpr (std::is_floating_point_v<ResultType>) {
                        simd::SIMDAdd<InType, ResultType, simd::BasicOp::Sum>(reinterpret_cast<ResultType *>(state.val),
                            state.count, ptr, rowCount);
                    } else {
                        simd::SIMDAdd<InType, ResultType, simd::BasicOp::Sum>(
                            reinterpret_cast<ResultType *>(&state.val), state.count, ptr, rowCount);
                    }
                } else {
                    if constexpr (std::is_floating_point_v<ResultType>) {
                        simd::SIMDAddConditional<InType, ResultType, simd::BasicOp::Sum>(
                            reinterpret_cast<ResultType *>(state.val), state.count, ptr, rowCount, nullMap);
                    } else {
                        simd::SIMDAddConditional<InType, ResultType, simd::BasicOp::Sum>(
                            reinterpret_cast<ResultType *>(&state.val), state.count, ptr, rowCount, nullMap);
                    }
                }
            } else {
                auto *ptr = reinterpret_cast<InType *>(GetValuesFromDict<IN_ID>(vector));
                auto *indexMap = GetIdsFromDict<IN_ID>(vector) + rowOffset;
                if (nullMap == nullptr) {
                    if constexpr (std::is_floating_point_v<ResultType>) {
                        simd::SIMDAddDict<InType, ResultType, simd::BasicOp::Sum>(
                            reinterpret_cast<ResultType *>(state.val), state.count, ptr, rowCount, indexMap);
                    } else {
                        simd::SIMDAddDict<InType, ResultType, simd::BasicOp::Sum>(
                            reinterpret_cast<ResultType *>(&state.val), state.count, ptr, rowCount, indexMap);
                    }
                } else {
                    if constexpr (std::is_floating_point_v<ResultType>) {
                        AddDictConditional<InType, ResultType, SumConditionalOp<InType, ResultType, false, false>>(
                            reinterpret_cast<ResultType *>(state.val), state.count, ptr, rowCount, nullMap, indexMap);
                    } else {
                        AddDictConditional<InType, ResultType, SumConditionalOp<InType, ResultType, false, false>>(
                            reinterpret_cast<ResultType *>(&state.val), state.count, ptr, rowCount, nullMap, indexMap);
                    }
                }
            }
        } else {
            ProcessSingleInternalFinal(state, vector, rowOffset, rowCount, nullMap);
        }
    }

    void InitState(AggregateState &state) override
    {
        if constexpr (std::is_floating_point_v<ResultType>) {
            state.val = reinterpret_cast<int64_t>(arenaAllocator->Allocate(sizeof(ResultType)));
            *reinterpret_cast<ResultType *>(state.val) = ResultType {};
        } else {
            state.val = 0;
        }
        state.count = 0;
    }

    void InitStates(std::vector<AggregateState *> groupStates, const size_t aggIdx) override
    {
        for (auto groupState : groupStates) {
            auto &state = groupState[aggIdx];
            InitState(state);
        }
    }

    std::vector<DataTypePtr> GetSpillType() override
    {
        std::vector<DataTypePtr> spillTypes;
        spillTypes.emplace_back(outputTypes.GetType(0));
        return spillTypes;
    }

    void ExtractValuesForSpill(std::vector<AggregateState *> &groupStates, const size_t aggIdx,
        std::vector<BaseVector *> &vectors) override
    {
        auto spillValueVec = static_cast<Vector<ResultType> *>(vectors[0]);
        auto rowCount = static_cast<int32_t>(groupStates.size());
        for (int32_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            auto &state = groupStates[rowIndex][aggIdx];
            auto count = state.count;
            if (count == 0) {
                // set null for empty group(all rows are NULL) when spill to ensure skip empty group when unspill
                spillValueVec->SetNull(rowIndex);
            } else {
                if constexpr (std::is_floating_point_v<ResultType>) {
                    spillValueVec->SetValue(rowIndex, *reinterpret_cast<ResultType *>(state.val));
                } else {
                    spillValueVec->SetValue(rowIndex, static_cast<ResultType>(state.val));
                }
            }
        }
    }

    void ExtractValues(const AggregateState &state, std::vector<BaseVector *> &vectors, int32_t rowIndex) override
    {
        auto v = static_cast<Vector<ResultType> *>(vectors[0]);

        // state.count == 0 means all data is null or no data be accumulated,
        // we will set null
        if (state.count == 0) {
            v->SetNull(rowIndex);
            return;
        }

        // we can not distinguish whether value is overflow when stage.val is null
        if constexpr (std::is_floating_point_v<ResultType>) {
            v->SetValue(rowIndex, *reinterpret_cast<ResultType *>(state.val));
        } else {
            v->SetValue(rowIndex, static_cast<ResultType>(state.val));
        }
    }

    void ExtractValuesBatch(std::vector<AggregateState *> &groupStates, const size_t aggIdx,
        std::vector<BaseVector *> &vectors, int32_t rowOffset, int32_t rowCount) override
    {
        auto v = static_cast<Vector<ResultType> *>(vectors[0]);
        for (int32_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            auto &state = groupStates[rowIndex][aggIdx];
            if (state.count <= 0) {
                v->SetNull(rowIndex);
                continue;
            }
            // we can not distinguish whether value is overflow when stage.val is null
            if constexpr (std::is_floating_point_v<ResultType>) {
                v->SetValue(rowIndex, *reinterpret_cast<ResultType *>(state.val));
            } else {
                v->SetValue(rowIndex, static_cast<ResultType>(state.val));
            }
        }
    }

    void ProcessAlignAggSchema(VectorBatch *result, BaseVector *originVector, const uint8_t *nullMap,
        const bool aggFilter) override
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
    void ProcessAlignAggSchemaInternal(VectorBatch *result, BaseVector *originVector, const uint8_t *nullMap)
    {
        int rowCount = originVector->GetSize();
        auto sumVector = reinterpret_cast<Vector<ResultType> *>(VectorHelper::CreateFlatVector(OUT_ID, rowCount));
        // The varchar type is converted to the double type in advance, so InType can't be the varchar type.
        auto vector = reinterpret_cast<T *>(originVector);
        if (nullMap != nullptr) {
            for (int index = 0; index < rowCount; ++index) {
                if (nullMap[index]) {
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
};
}
}
#endif // OMNI_RUNTIME_SUM_FLAT_IM_AGGREGATOR_H
