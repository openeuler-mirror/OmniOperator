/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2023. All rights reserved.
 * Description: For non-decimal type
 */
#ifndef OMNI_RUNTIME_SUM_FLAT_IM_AGGREGATOR_H
#define OMNI_RUNTIME_SUM_FLAT_IM_AGGREGATOR_H

#include "aggregator.h"

namespace omniruntime {
namespace op {
template <DataTypeId IN_ID, DataTypeId OUT_ID> class SumFlatIMAggregator : public TypedAggregator {
    using InType = typename AggNativeAndVectorType<IN_ID>::type;
    using ResultType = typename AggNativeAndVectorType<OUT_ID>::type;
    using FixedVector = typename AggNativeAndVectorType<IN_ID>::vector;
    using DictVector = Vector<DictionaryContainer<InType>>;

public:
    static std::unique_ptr<Aggregator> Create(const DataTypes &inputTypes, const DataTypes &outputTypes,
                                              std::vector<int32_t> &channels, bool rawIn, bool partialOut, bool isOverflowAsNull)
    {
        if constexpr (IN_ID == OMNI_VARCHAR || IN_ID == OMNI_CHAR) {
            LogError("Error in sum flatim aggregator: Unsupported output type %s", TypeUtil::TypeToStringLog(IN_ID).c_str());
            return nullptr;
        } else if constexpr (!(IN_ID == OMNI_SHORT || IN_ID == OMNI_INT || IN_ID == OMNI_LONG || IN_ID == OMNI_DOUBLE ||
                               IN_ID == OMNI_DECIMAL64 || IN_ID == OMNI_BOOLEAN)) {
            LogError("Error in sum flatim aggregator: Unsupported input type %s", TypeUtil::TypeToStringLog(IN_ID).c_str());
            return nullptr;
        } else if constexpr (!(OUT_ID == OMNI_SHORT || OUT_ID == OMNI_INT || OUT_ID == OMNI_LONG ||
                               OUT_ID == OMNI_DOUBLE || OUT_ID == OMNI_BOOLEAN)) {
            LogError("Error in sum flatim aggregator: Unsupported output type %s", TypeUtil::TypeToStringLog(OUT_ID).c_str());
            return nullptr;
        } else {
            return std::unique_ptr<Aggregator>(new SumFlatIMAggregator<IN_ID, OUT_ID>(inputTypes,
                                               outputTypes, channels, rawIn, partialOut, isOverflowAsNull));
        }
    }

    SumFlatIMAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels,
        bool inputRaw, bool outputPartial, bool isOverflowAsNull)
        : TypedAggregator(OMNI_AGGREGATION_TYPE_SUM, inputTypes, outputTypes, channels, inputRaw, outputPartial,
        isOverflowAsNull)
    {}
    SumFlatIMAggregator(const FunctionType aggFunc, const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels,
                        bool inputRaw, bool outputPartial, bool isOverflowAsNull)
            : TypedAggregator(aggFunc, inputTypes, outputTypes, channels, inputRaw, outputPartial,
                              isOverflowAsNull)
    {}
    ~SumFlatIMAggregator() override {}

    // todo: will delete
    void ProcessGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex) override
    {
        BaseVector *vector = vectorBatch->Get(channels[0]);
        if (vector->IsNull(rowIndex)) {
            return;
        }
        if (state.val == nullptr) {
            InitiateGroup(state, vectorBatch, rowIndex);
            return;
        }
        if (inputRaw) {
            if (vector->GetEncoding() == OMNI_DICTIONARY) {
                *(static_cast<ResultType *>(state.val)) += (static_cast<DictVector *>(vector))->GetValue(rowIndex);
            } else {
                *(static_cast<ResultType *>(state.val)) += (static_cast<FixedVector *>(vector))->GetValue(rowIndex);
            }
        } else {
            *(static_cast<ResultType *>(state.val)) += (static_cast<Vector<ResultType> *>(vector))->GetValue(rowIndex);
        }
    }

    void ProcessGroupInternalFinal(std::vector<AggregateState *> &rowStates, const size_t aggIdx, BaseVector *vector,
        const int32_t rowOffset, const uint8_t *nullMap)
    {
        // final stage : input vector will be Vector<ResultType>
        auto *ptr = reinterpret_cast<ResultType *>(GetValuesFromVector<OUT_ID>(vector));
        ptr += rowOffset;
        if (nullMap == nullptr) {
            AddUseRowIndex<ResultType, ResultType, SumOp<ResultType, ResultType>>(rowStates, aggIdx, ptr);
        } else {
            // Reza: can we use customize float operation similar to sumConditionalFloat
            AddConditionalUseRowIndex<ResultType, ResultType, SumConditionalOp<ResultType, ResultType, false>>(
                rowStates, aggIdx, ptr, nullMap);
        }
    }

    void ProcessGroupInternal(std::vector<AggregateState *> &rowStates, const size_t aggIdx, BaseVector *vector,
        const int32_t rowOffset, const uint8_t *nullMap, const int32_t *indexMap)
    {
        if (inputRaw) {
            if (indexMap == nullptr) {
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
                if (nullMap == nullptr) {
                    AddDictUseRowIndex<InType, ResultType, SumOp<InType, ResultType, false>>(rowStates, aggIdx, ptr, indexMap);
                } else {
                    AddDictConditionalUseRowIndex<InType, ResultType, SumConditionalOp<InType, ResultType, false, false>>(
                        rowStates, aggIdx, ptr, nullMap, indexMap);
                }
            }
        } else {
            // no dictionary in input when stage is not partial
            ProcessGroupInternalFinal(rowStates, aggIdx, vector, rowOffset, nullMap);
        }
    }

    void ProcessSingleInternalFinal(AggregateState &state, BaseVector *vector, const int32_t rowOffset,
        const int32_t rowCount, const uint8_t *nullMap)
    {
        auto *res = reinterpret_cast<ResultType *>(state.val);
        auto *ptr = reinterpret_cast<ResultType *>(GetValuesFromVector<OUT_ID>(vector));
        ptr += rowOffset;
        if (nullMap == nullptr) {
            Add<ResultType, ResultType, SumOp<ResultType, ResultType, false>>(res, state.count, ptr, rowCount);
        } else {
            if constexpr (std::is_floating_point_v<ResultType>) {
                SumConditionalFloat<ResultType, ResultType, false>(res, state.count, ptr, rowCount, nullMap);
            } else {
                AddConditional<ResultType, ResultType, SumConditionalOp<ResultType, ResultType, false, false>>(res,
                    state.count, ptr, rowCount, nullMap);
            }
        }
    }

    void ProcessSingleInternal(AggregateState &state, BaseVector *vector, const int32_t rowOffset,
        const int32_t rowCount, const uint8_t *nullMap, const int32_t *indexMap)
    {
        auto *res = reinterpret_cast<ResultType *>(state.val);
        if (inputRaw) {
            if (indexMap == nullptr) {
                auto *ptr = reinterpret_cast<InType *>(GetValuesFromVector<IN_ID>(vector));
                ptr += rowOffset;
                if (nullMap == nullptr) {
                    Add<InType, ResultType, SumOp<InType, ResultType, false>>(res, state.count, ptr, rowCount);
                } else {
                    if constexpr (std::is_floating_point_v<InType>) {
                        SumConditionalFloat<InType, ResultType, false>(res, state.count, ptr, rowCount, nullMap);
                    } else {
                        AddConditional<InType, ResultType, SumConditionalOp<InType, ResultType, false, false>>(res,
                            state.count, ptr, rowCount, nullMap);
                    }
                }
            } else {
                auto *ptr = reinterpret_cast<InType *>(GetValuesFromDict<IN_ID>(vector));
                if (nullMap == nullptr) {
                    AddDict<InType, ResultType, SumOp<InType, ResultType, false>>(res, state.count, ptr, rowCount, indexMap);
                } else {
                    AddDictConditional<InType, ResultType, SumConditionalOp<InType, ResultType, false, false>>(res,
                        state.count, ptr, rowCount, nullMap, indexMap);
                }
            }
        } else {
            ProcessSingleInternalFinal(state, vector, rowOffset, rowCount, nullMap);
        }
    }

    // todo: will delete
    void InitiateGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex) override
    {
        BaseVector *vector = vectorBatch->Get(channels[0]);
        if (vector->IsNull(rowIndex)) {
            return;
        }
        if (inputRaw) {
            auto ptr = executionContext->GetArena()->Allocate(sizeof(ResultType));
            if (vector->GetEncoding() == OMNI_DICTIONARY) {
                auto curVal = (static_cast<DictVector *>(vector))->GetValue(rowIndex);
                *reinterpret_cast<ResultType *>(ptr) = curVal;
                state.val = ptr;
            } else {
                auto curVal = (static_cast<FixedVector *>(vector))->GetValue(rowIndex);
                *reinterpret_cast<ResultType *>(ptr) = curVal;
                state.val = ptr;
            }
        } else {
            auto ptr = executionContext->GetArena()->Allocate(sizeof(ResultType));
            ResultType curVal;
            if (vector->GetEncoding() == OMNI_DICTIONARY) {
                curVal = (static_cast<Vector<DictionaryContainer<ResultType>> *>(vector))->GetValue(rowIndex);
            } else {
                curVal = (static_cast<Vector<ResultType> *>(vector))->GetValue(rowIndex);
            }
            *reinterpret_cast<ResultType *>(ptr) = curVal;
            state.val = ptr;
        }
    }

    void InitState(AggregateState &state)
    {
        state.val = this->executionContext->GetArena()->Allocate(sizeof(ResultType));
        *reinterpret_cast<ResultType *>(state.val) = ResultType{};
        state.count = 0;
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
        v->SetValue(rowIndex, *static_cast<ResultType *>(state.val));
    }
};
}
}
#endif // OMNI_RUNTIME_SUM_FLAT_IM_AGGREGATOR_H
