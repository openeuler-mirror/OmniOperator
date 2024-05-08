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
    ~SumFlatIMAggregator() override {}

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

    void ProcessGroupAfterSpill(AggregateState &state, VectorBatch *vectorBatch, int32_t &vectorIndex,
        int32_t rowIdx) override
    {
        auto sumVector = vectorBatch->Get(vectorIndex++);
        auto *sum = reinterpret_cast<ResultType *>(GetValuesFromVector<OUT_ID>(sumVector));
        auto countVector = vectorBatch->Get(vectorIndex++);
        auto *cntPtr = reinterpret_cast<int64_t *>(GetValuesFromVector<OMNI_LONG>(countVector));
        sum = (ResultType *)__builtin_assume_aligned(sum, ARRAY_ALIGNMENT);
        cntPtr = (int64_t *)__builtin_assume_aligned(cntPtr, ARRAY_ALIGNMENT);

        int64_t cnt = cntPtr[rowIdx];
        if (cnt == 0 || sumVector->IsNull(rowIdx)) {
            return;
        } else {
            if constexpr (std::is_floating_point_v<ResultType>) {
                SumOp<ResultType, ResultType, false>(reinterpret_cast<ResultType *>(state.val), state.count,
                    sum[rowIdx], cnt);
            } else {
                SumOp<ResultType, ResultType, false>((ResultType *)(&state.val), state.count, sum[rowIdx], cnt);
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

    void InitState(AggregateState &state)
    {
        if constexpr (std::is_floating_point_v<ResultType>) {
            state.val = reinterpret_cast<int64_t>(arenaAllocator->Allocate(sizeof(ResultType)));
            *reinterpret_cast<ResultType *>(state.val) = ResultType {};
        } else {
            state.val = 0;
        }
        state.count = 0;
    }

    void GetSpillType(std::vector<DataTypePtr> &spillTypes) override
    {
        if constexpr (IN_ID == OMNI_SHORT || IN_ID == OMNI_INT || IN_ID == OMNI_LONG) {
            spillTypes.push_back(std::make_shared<DataType>(OMNI_LONG));
        } else {
            spillTypes.push_back(std::make_shared<DataType>(OMNI_DOUBLE));
        }
        spillTypes.push_back(std::make_shared<DataType>(OMNI_LONG));
    }

    void ExtractSpillValues(const AggregateState &state, std::vector<BaseVector *> &vectors, int32_t rowIndex) override
    {
        auto spillValueVec = static_cast<Vector<ResultType> *>(vectors[0]);
        auto spillCountVec = static_cast<Vector<long> *>(vectors[1]);
        if (state.count == 0) {
            spillValueVec->SetNull(rowIndex);
            spillCountVec->SetValue(rowIndex, state.count);
            return;
        }
        if constexpr (std::is_floating_point_v<ResultType>) {
            spillValueVec->SetValue(rowIndex, *reinterpret_cast<ResultType *>(state.val));
        } else {
            spillValueVec->SetValue(rowIndex, static_cast<ResultType>(state.val));
        }
        spillCountVec->SetValue(rowIndex, state.count);
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

        // we will set state.count to -1 if overflow happened
        if (state.count < 0) {
            SetNullOrThrowException(v, rowIndex, "");
            return;
        }
        // we can not distinguish whether value is overflow when stage.val is null
        if constexpr (std::is_floating_point_v<ResultType>) {
            v->SetValue(rowIndex, *reinterpret_cast<ResultType *>(state.val));
        } else {
            v->SetValue(rowIndex, static_cast<ResultType>(state.val));
        }
    }
};
}
}
#endif // OMNI_RUNTIME_SUM_FLAT_IM_AGGREGATOR_H
