/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2024. All rights reserved.
 * Description: Sum aggregator
 */

#include "sum_aggregator.h"

namespace omniruntime {
namespace op {
template <DataTypeId IN_ID, DataTypeId OUT_ID>
void SumAggregator<IN_ID, OUT_ID>::ExtractValues(const AggregateState &state, std::vector<BaseVector *> &vectors,
    int32_t rowIndex)
{
    auto v = static_cast<OutVector *>(vectors[0]);

    OutType result{};
    bool overflow = state.count < 0;

    if constexpr (OUT_ID == OMNI_VARCHAR) {
        if (state.count > 0 && state.val != nullptr) {
            result.sum = this->template CastWithOverflow<Decimal128, Decimal128>(
                *reinterpret_cast<Decimal128 *>(state.val), overflow);
        }
        result.count = overflow ? 0 : state.count;
        std::string_view decimal2Str(reinterpret_cast<char *>(&result), sizeof(OutType));
        v->SetValue(rowIndex, decimal2Str);
        if (overflow && !this->IsOverflowAsNull()) {
            throw OmniException("OPERATOR_RUNTIME_ERROR", "sum_aggregator overflow.");
        }
    } else {
        if (state.count > 0 && state.val != nullptr) {
            result = this->template CastWithOverflow<ResultType, OutType>(*reinterpret_cast<ResultType *>(state.val),
                overflow);
        }
        v->SetValue(rowIndex, result);
        if (overflow) {
            this->SetNullOrThrowException(v, rowIndex, "sum_aggregator overflow.");
        } else if (state.count == 0 || state.val == nullptr) {
            v->SetNull(rowIndex);
        }
    }
}
template <DataTypeId IN_ID, DataTypeId OUT_ID>
void SumAggregator<IN_ID, OUT_ID>::GetSpillType(std::vector<DataTypeId>& spillTypes)
{
    if constexpr (IN_ID == OMNI_SHORT || IN_ID == OMNI_INT || IN_ID == OMNI_LONG) {
        spillTypes.push_back(OMNI_LONG);
    } else if constexpr (IN_ID == OMNI_DOUBLE || IN_ID == OMNI_CONTAINER) {
        spillTypes.push_back(OMNI_DOUBLE);
    } else {
        spillTypes.push_back(OMNI_DECIMAL128);
    }
    spillTypes.push_back(OMNI_LONG);
}


template <DataTypeId IN_ID, DataTypeId OUT_ID>
void SumAggregator<IN_ID, OUT_ID>::ExtractSpillValues(const omniruntime::op::AggregateState &state,
    std::vector<BaseVector *> &vectors, int32_t rowIndex)
{
    auto v = static_cast<Vector<ResultType> *>(vectors[0]);
    auto v1 = static_cast<Vector<long> *>(vectors[1]);
    if (state.count == 0 || state.val == nullptr) {
        v->SetNull(rowIndex);
        v1->SetValue(rowIndex, state.count);
        return;
    }
    v->SetValue(rowIndex, *reinterpret_cast<ResultType *>(state.val));
    v1->SetValue(rowIndex, state.count);
}

template <DataTypeId IN_ID, DataTypeId OUT_ID> void SumAggregator<IN_ID, OUT_ID>::InitState(AggregateState &state)
{
    state.val = this->executionContext->GetArena()->Allocate(sizeof(ResultType));
    *reinterpret_cast<ResultType *>(state.val) = ResultType{};
    state.count = 0;
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void SumAggregator<IN_ID, OUT_ID>::ProcessSingleInternal(AggregateState &state, BaseVector *vector,
    const int32_t rowOffset, const int32_t rowCount, const uint8_t *nullMap)
{
    if (state.val == nullptr) {
        this->InitState(state);
    }
    auto *res = reinterpret_cast<ResultType *>(state.val);

    if (vector->GetEncoding() != vec::OMNI_DICTIONARY) {
        auto *ptr = reinterpret_cast<InType *>(GetValuesFromVector<IN_ID>(vector));
        ptr += rowOffset;
        if (nullMap == nullptr) {
            Add<InType, ResultType, SumOp<InType, ResultType>>(res, state.count, ptr, rowCount);
        } else {
            if constexpr (std::is_floating_point_v<InType>) {
                SumConditionalFloat<InType, ResultType, false>(res, state.count, ptr, rowCount, nullMap);
            } else {
                AddConditional<InType, ResultType, SumConditionalOp<InType, ResultType, false>>(res, state.count, ptr,
                    rowCount, nullMap);
            }
        }
    } else {
        auto *ptr = reinterpret_cast<InType *>(GetValuesFromDict<IN_ID>(vector));
        auto *indexMap = GetIdsFromDict<IN_ID>(vector) + rowOffset;
        if (nullMap == nullptr) {
            AddDict<InType, ResultType, SumOp<InType, ResultType>>(res, state.count, ptr, rowCount, indexMap);
        } else {
            AddDictConditional<InType, ResultType, SumConditionalOp<InType, ResultType, false>>(res, state.count, ptr,
                rowCount, nullMap, indexMap);
        }
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void SumAggregator<IN_ID, OUT_ID>::ProcessGroupInternal(std::vector<AggregateState *> &rowStates, const size_t aggIdx,
    BaseVector *vector, const int32_t rowOffset, const uint8_t *nullMap)
{
    if (vector->GetEncoding() != vec::OMNI_DICTIONARY) {
        auto *ptr = reinterpret_cast<InType *>(GetValuesFromVector<IN_ID>(vector));
        ptr += rowOffset;
        if (nullMap == nullptr) {
            AddUseRowIndex<InType, ResultType, SumOp<InType, ResultType>>(rowStates, aggIdx, ptr);
        } else {
            // Reza: can we use customize float operation similar to sumConditionalFloat
            AddConditionalUseRowIndex<InType, ResultType, SumConditionalOp<InType, ResultType, false>>(rowStates,
                aggIdx, ptr, nullMap);
        }
    } else {
        auto *ptr = reinterpret_cast<InType *>(GetValuesFromDict<IN_ID>(vector));
        auto *indexMap = GetIdsFromDict<IN_ID>(vector) + rowOffset;
        if (nullMap == nullptr) {
            AddDictUseRowIndex<InType, ResultType, SumOp<InType, ResultType>>(rowStates, aggIdx, ptr, indexMap);
        } else {
            AddDictConditionalUseRowIndex<InType, ResultType, SumConditionalOp<InType, ResultType, false>>(rowStates,
                aggIdx, ptr, nullMap, indexMap);
        }
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void SumAggregator<IN_ID, OUT_ID>::ProcessGroupAfterSpill(AggregateState &state, VectorBatch *vectorBatch,
    int32_t &vectorIndex, int32_t rowIdx)
{
    auto sumVector = vectorBatch->Get(vectorIndex++);
    auto countVector = vectorBatch->Get(vectorIndex++);
    auto *cntPtr = reinterpret_cast<int64_t *>(GetValuesFromVector<OMNI_LONG>(countVector));
    cntPtr = (int64_t *)__builtin_assume_aligned(cntPtr, ARRAY_ALIGNMENT);

    int64_t cnt = cntPtr[rowIdx];
    if (cnt == 0 || sumVector->IsNull(rowIdx)) {
        return;
    } else {
        if constexpr (IN_ID == OMNI_VARCHAR || IN_ID == OMNI_CHAR) {
            auto sum = reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(sumVector);
            Decimal128 data = Decimal128(sum->GetValue(rowIdx));
            SumOp<Decimal128, Decimal128>(reinterpret_cast<Decimal128 *>(state.val), state.count, data, cnt);
        } else if constexpr (IN_ID == OMNI_SHORT || IN_ID == OMNI_INT || IN_ID == OMNI_LONG) {
            auto *sum = reinterpret_cast<ResultType *>(GetValuesFromVector<OMNI_LONG>(sumVector));
            sum = (ResultType *)__builtin_assume_aligned(sum, ARRAY_ALIGNMENT);
            SumOp<ResultType, ResultType>(reinterpret_cast<ResultType *>(state.val), state.count, sum[rowIdx], cnt);
        } else if constexpr (IN_ID == OMNI_DOUBLE || IN_ID == OMNI_CONTAINER) {
            auto *sum = reinterpret_cast<ResultType *>(GetValuesFromVector<OMNI_DOUBLE>(sumVector));
            sum = (ResultType *)__builtin_assume_aligned(sum, ARRAY_ALIGNMENT);
            SumOp<ResultType, ResultType>(reinterpret_cast<ResultType *>(state.val), state.count, sum[rowIdx], cnt);
        } else {
            auto *sum = reinterpret_cast<ResultType *>(GetValuesFromVector<OMNI_DECIMAL128>(sumVector));
            sum = (ResultType *)__builtin_assume_aligned(sum, ARRAY_ALIGNMENT);
            SumOp<ResultType, ResultType>(reinterpret_cast<ResultType *>(state.val), state.count, sum[rowIdx], cnt);
        }
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
SumAggregator<IN_ID, OUT_ID>::SumAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes,
    std::vector<int32_t> &channels, const bool inputRaw, const bool outputPartial, const bool isOverflowAsNull)
    : TypedAggregator(OMNI_AGGREGATION_TYPE_SUM, inputTypes, outputTypes, channels, inputRaw, outputPartial,
    isOverflowAsNull)
{}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
SumAggregator<IN_ID, OUT_ID>::SumAggregator(FunctionType aggregateType, const DataTypes &inputTypes,
    const DataTypes &outputTypes, std::vector<int32_t> &channels, const bool inputRaw, const bool outputPartial,
    const bool isOverflowAsNull)
    : TypedAggregator(aggregateType, inputTypes, outputTypes, channels, inputRaw, outputPartial, isOverflowAsNull)
{}

// Explicit template instantiation
// Defining templated aggregators in header file consume a lot of memory during compilation
// since, compiler needs to generate each individual template instance wherever aggregator header is include
// to reduce time and memory usage during compilation moved templated aggregator implementation into .cpp files
// and used explicit template instantiation to generate template instances
template class SumAggregator<OMNI_SHORT, OMNI_SHORT>;

template class SumAggregator<OMNI_SHORT, OMNI_INT>;

template class SumAggregator<OMNI_SHORT, OMNI_LONG>;

template class SumAggregator<OMNI_SHORT, OMNI_DOUBLE>;

template class SumAggregator<OMNI_SHORT, OMNI_DECIMAL128>;

template class SumAggregator<OMNI_SHORT, OMNI_DECIMAL64>;

template class SumAggregator<OMNI_SHORT, OMNI_VARCHAR>;

template class SumAggregator<OMNI_SHORT, OMNI_CONTAINER>;

template class SumAggregator<OMNI_INT, OMNI_SHORT>;

template class SumAggregator<OMNI_INT, OMNI_INT>;

template class SumAggregator<OMNI_INT, OMNI_LONG>;

template class SumAggregator<OMNI_INT, OMNI_DOUBLE>;

template class SumAggregator<OMNI_INT, OMNI_DECIMAL128>;

template class SumAggregator<OMNI_INT, OMNI_DECIMAL64>;

template class SumAggregator<OMNI_INT, OMNI_VARCHAR>;

template class SumAggregator<OMNI_INT, OMNI_CONTAINER>;

template class SumAggregator<OMNI_LONG, OMNI_SHORT>;

template class SumAggregator<OMNI_LONG, OMNI_INT>;

template class SumAggregator<OMNI_LONG, OMNI_LONG>;

template class SumAggregator<OMNI_LONG, OMNI_DOUBLE>;

template class SumAggregator<OMNI_LONG, OMNI_DECIMAL128>;

template class SumAggregator<OMNI_LONG, OMNI_DECIMAL64>;

template class SumAggregator<OMNI_LONG, OMNI_VARCHAR>;

template class SumAggregator<OMNI_LONG, OMNI_CONTAINER>;

template class SumAggregator<OMNI_DOUBLE, OMNI_SHORT>;

template class SumAggregator<OMNI_DOUBLE, OMNI_INT>;

template class SumAggregator<OMNI_DOUBLE, OMNI_LONG>;

template class SumAggregator<OMNI_DOUBLE, OMNI_DOUBLE>;

template class SumAggregator<OMNI_DOUBLE, OMNI_DECIMAL128>;

template class SumAggregator<OMNI_DOUBLE, OMNI_DECIMAL64>;

template class SumAggregator<OMNI_DOUBLE, OMNI_VARCHAR>;

template class SumAggregator<OMNI_DOUBLE, OMNI_CONTAINER>;

template class SumAggregator<OMNI_DECIMAL128, OMNI_SHORT>;

template class SumAggregator<OMNI_DECIMAL128, OMNI_INT>;

template class SumAggregator<OMNI_DECIMAL128, OMNI_LONG>;

template class SumAggregator<OMNI_DECIMAL128, OMNI_DOUBLE>;

template class SumAggregator<OMNI_DECIMAL128, OMNI_DECIMAL128>;

template class SumAggregator<OMNI_DECIMAL128, OMNI_DECIMAL64>;

template class SumAggregator<OMNI_DECIMAL128, OMNI_VARCHAR>;

template class SumAggregator<OMNI_DECIMAL128, OMNI_CONTAINER>;

template class SumAggregator<OMNI_DECIMAL64, OMNI_SHORT>;

template class SumAggregator<OMNI_DECIMAL64, OMNI_INT>;

template class SumAggregator<OMNI_DECIMAL64, OMNI_LONG>;

template class SumAggregator<OMNI_DECIMAL64, OMNI_DOUBLE>;

template class SumAggregator<OMNI_DECIMAL64, OMNI_DECIMAL128>;

template class SumAggregator<OMNI_DECIMAL64, OMNI_DECIMAL64>;

template class SumAggregator<OMNI_DECIMAL64, OMNI_VARCHAR>;

template class SumAggregator<OMNI_DECIMAL64, OMNI_CONTAINER>;

template class SumAggregator<OMNI_VARCHAR, OMNI_SHORT>;

template class SumAggregator<OMNI_VARCHAR, OMNI_INT>;

template class SumAggregator<OMNI_VARCHAR, OMNI_LONG>;

template class SumAggregator<OMNI_VARCHAR, OMNI_DOUBLE>;

template class SumAggregator<OMNI_VARCHAR, OMNI_DECIMAL128>;

template class SumAggregator<OMNI_VARCHAR, OMNI_DECIMAL64>;

template class SumAggregator<OMNI_VARCHAR, OMNI_VARCHAR>;

template class SumAggregator<OMNI_VARCHAR, OMNI_CONTAINER>;

template class SumAggregator<OMNI_CONTAINER, OMNI_SHORT>;

template class SumAggregator<OMNI_CONTAINER, OMNI_INT>;

template class SumAggregator<OMNI_CONTAINER, OMNI_LONG>;

template class SumAggregator<OMNI_CONTAINER, OMNI_DOUBLE>;

template class SumAggregator<OMNI_CONTAINER, OMNI_DECIMAL128>;

template class SumAggregator<OMNI_CONTAINER, OMNI_DECIMAL64>;

template class SumAggregator<OMNI_CONTAINER, OMNI_VARCHAR>;

template class SumAggregator<OMNI_CONTAINER, OMNI_CONTAINER>;
}
}
