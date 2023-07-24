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
            AddUseRowIndex<ResultType, ResultType, SumOp<ResultType, ResultType>>(rowStates, aggIdx, ptr);
        } else {
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
                    AddDict<InType, ResultType, SumOp<InType, ResultType, false>>(res, state.count, ptr, rowCount,
                        indexMap);
                } else {
                    AddDictConditional<InType, ResultType, SumConditionalOp<InType, ResultType, false, false>>(res,
                        state.count, ptr, rowCount, nullMap, indexMap);
                }
            }
        } else {
            ProcessSingleInternalFinal(state, vector, rowOffset, rowCount, nullMap);
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
