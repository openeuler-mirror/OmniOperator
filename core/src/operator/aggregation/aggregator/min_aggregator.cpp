/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2024. All rights reserved.
 * Description: Min aggregate
 */

#include "min_aggregator.h"
#include "operator/aggregation/neon_aggregation/simd_aggregation_external.h"

namespace omniruntime {
namespace op {
template <DataTypeId IN_ID, DataTypeId OUT_ID>
void MinAggregator<IN_ID, OUT_ID>::ExtractValues(const AggregateState &state, std::vector<BaseVector *> &vectors,
    int32_t rowIndex)
{
    auto v = static_cast<OutVector *>(vectors[0]);
    if (state.count == 0) {
        v->SetNull(rowIndex);
        return;
    }

    bool overflow = state.count < 0;
    auto result = CastWithOverflowEntry<ResultType, OutType>(state.val, overflow);
    v->SetValue(rowIndex, result);
    if (overflow) {
        this->SetNullOrThrowException(v, rowIndex, "min_aggregator overflow.");
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void MinAggregator<IN_ID, OUT_ID>::GetSpillType(std::vector<DataTypePtr> &spillTypes)
{
    if constexpr (IN_ID == OMNI_SHORT) {
        spillTypes.push_back(std::make_shared<DataType>(OMNI_INT));
    } else {
        spillTypes.push_back(std::make_shared<DataType>(IN_ID));
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void MinAggregator<IN_ID, OUT_ID>::ExtractSpillValues(const AggregateState &state, std::vector<BaseVector *> &vectors,
    int32_t rowIndex)
{
    auto spillValue = static_cast<Vector<ResultType> *>(vectors[0]);
    if (state.count == 0) {
        spillValue->SetNull(rowIndex);
        return;
    }
    if constexpr (std::is_same_v<ResultType, Decimal128> || std::is_floating_point_v<ResultType>) {
        spillValue->SetValue(rowIndex, *reinterpret_cast<ResultType *>(state.val));
    } else {
        spillValue->SetValue(rowIndex, static_cast<ResultType>(state.val));
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID> void MinAggregator<IN_ID, OUT_ID>::InitState(AggregateState &state)
{
    if constexpr (std::is_same_v<ResultType, Decimal128> || std::is_floating_point_v<ResultType>) {
        state.val = reinterpret_cast<int64_t>(arenaAllocator->Allocate(sizeof(ResultType)));
        *reinterpret_cast<ResultType *>(state.val) = GetMax<ResultType>();
    } else {
        state.val = GetMax<ResultType>();
    }

    state.count = 0;
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void MinAggregator<IN_ID, OUT_ID>::ProcessSingleInternal(AggregateState &state, BaseVector *vector,
    const int32_t rowOffset, const int32_t rowCount, const uint8_t *nullMap)
{
    if (vector->GetEncoding() != vec::OMNI_DICTIONARY) {
        auto *ptr = reinterpret_cast<InType *>(GetValuesFromVector<IN_ID>(vector));
        ptr += rowOffset;
        if (nullMap == nullptr) {
            if constexpr (simd::CheckTypesContainsDecimal128<InType, ResultType>::value) {
                Add<InType, ResultType, MinOp<InType, ResultType>>(reinterpret_cast<ResultType *>(state.val),
                    state.count, ptr, rowCount);
            } else if constexpr (std::is_floating_point_v<ResultType>) {
                simd::SIMDAdd<InType, ResultType, simd::BasicOp::Min>(reinterpret_cast<ResultType *>(state.val),
                    state.count, ptr, rowCount);
            } else {
                simd::SIMDAdd<InType, ResultType, simd::BasicOp::Min>(reinterpret_cast<ResultType *>(&state.val),
                    state.count, ptr, rowCount);
            }
        } else {
            if constexpr (simd::CheckTypesContainsDecimal128<InType, ResultType>::value) {
                AddConditional<InType, ResultType, MinConditionalOp<InType, ResultType, false>>(
                    reinterpret_cast<ResultType *>(state.val), state.count, ptr, rowCount, nullMap);
            } else if constexpr (std::is_floating_point_v<ResultType>) {
                simd::SIMDAddConditional<InType, ResultType, simd::BasicOp::Min>(
                    reinterpret_cast<ResultType *>(state.val), state.count, ptr, rowCount, nullMap);
            } else {
                simd::SIMDAddConditional<InType, ResultType, simd::BasicOp::Min>(
                    reinterpret_cast<ResultType *>(&state.val), state.count, ptr, rowCount, nullMap);
            }
        }
    } else {
        auto *ptr = reinterpret_cast<InType *>(GetValuesFromDict<IN_ID>(vector));
        auto *indexMap = GetIdsFromDict<IN_ID>(vector) + rowOffset;
        if (nullMap == nullptr) {
            if constexpr (simd::CheckTypesContainsDecimal128<InType, ResultType>::value) {
                AddDict<InType, ResultType, MinOp<InType, ResultType>>(reinterpret_cast<ResultType *>(state.val),
                    state.count, ptr, rowCount, indexMap);
            } else if constexpr (std::is_floating_point_v<ResultType>) {
                simd::SIMDAddDict<InType, ResultType, simd::BasicOp::Min>(reinterpret_cast<ResultType *>(state.val),
                    state.count, ptr, rowCount, indexMap);
            } else {
                simd::SIMDAddDict<InType, ResultType, simd::BasicOp::Min>(reinterpret_cast<ResultType *>(&state.val),
                    state.count, ptr, rowCount, indexMap);
            }
        } else {
            if constexpr (simd::CheckTypesContainsDecimal128<InType, ResultType>::value) {
                AddDictConditional<InType, ResultType, MinConditionalOp<InType, ResultType, false>>(
                    reinterpret_cast<ResultType *>(state.val), state.count, ptr, rowCount, nullMap, indexMap);
            } else if constexpr (std::is_floating_point_v<ResultType>) {
                simd::SIMDAddDictConditional<InType, ResultType, simd::BasicOp::Min>(
                    reinterpret_cast<ResultType *>(state.val), state.count, ptr, rowCount, nullMap, indexMap);
            } else {
                simd::SIMDAddDictConditional<InType, ResultType, simd::BasicOp::Min>(
                    reinterpret_cast<ResultType *>(&state.val), state.count, ptr, rowCount, nullMap, indexMap);
            }
        }
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void MinAggregator<IN_ID, OUT_ID>::ProcessGroupInternal(std::vector<AggregateState *> &rowStates, const size_t aggIdx,
    BaseVector *vector, const int32_t rowOffset, const uint8_t *nullMap)
{
    if (vector->GetEncoding() != vec::OMNI_DICTIONARY) {
        auto *ptr = reinterpret_cast<InType *>(GetValuesFromVector<IN_ID>(vector));
        ptr += rowOffset;
        if (nullMap == nullptr) {
            AddUseRowIndex<InType, ResultType, MinOp<InType, ResultType>>(rowStates, aggIdx, ptr);
        } else {
            AddConditionalUseRowIndex<InType, ResultType, MinConditionalOp<InType, ResultType, false>>(rowStates,
                aggIdx, ptr, nullMap);
        }
    } else {
        auto *ptr = reinterpret_cast<InType *>(GetValuesFromDict<IN_ID>(vector));
        auto *indexMap = GetIdsFromDict<IN_ID>(vector) + rowOffset;
        if (nullMap == nullptr) {
            AddDictUseRowIndex<InType, ResultType, MinOp<InType, ResultType>>(rowStates, aggIdx, ptr, indexMap);
        } else {
            AddDictConditionalUseRowIndex<InType, ResultType, MinConditionalOp<InType, ResultType, false>>(rowStates,
                aggIdx, ptr, nullMap, indexMap);
        }
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void MinAggregator<IN_ID, OUT_ID>::ProcessGroupAfterSpill(AggregateState &state, VectorBatch *vectorBatch,
    int32_t &vectorIndex, int32_t rowIdx)
{
    auto vectorPtr = vectorBatch->Get(vectorIndex++);
    if (!vectorPtr->IsNull(rowIdx)) {
        if constexpr (IN_ID == type::OMNI_SHORT) {
            auto *ptr = reinterpret_cast<ResultType *>(GetValuesFromVector<OMNI_INT>(vectorPtr));
            ptr = (ResultType *)__builtin_assume_aligned(ptr, ARRAY_ALIGNMENT);
            MinOp<ResultType, ResultType>(reinterpret_cast<ResultType *>(&state.val), state.count, ptr[rowIdx], 1LL);
        } else {
            auto *ptr = reinterpret_cast<ResultType *>(GetValuesFromVector<IN_ID>(vectorPtr));
            ptr = (ResultType *)__builtin_assume_aligned(ptr, ARRAY_ALIGNMENT);
            if constexpr (std::is_same_v<ResultType, Decimal128> || std::is_floating_point_v<ResultType>) {
                MinOp<ResultType, ResultType>(reinterpret_cast<ResultType *>(state.val), state.count, ptr[rowIdx], 1LL);
            } else {
                MinOp<ResultType, ResultType>(reinterpret_cast<ResultType *>(&state.val), state.count, ptr[rowIdx],
                    1LL);
            }
        }
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
MinAggregator<IN_ID, OUT_ID>::MinAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes,
    std::vector<int32_t> &channels, const bool inputRaw, const bool outputPartial, const bool isOverflowAsNull)
    : TypedAggregator(OMNI_AGGREGATION_TYPE_MIN, inputTypes, outputTypes, channels, inputRaw, outputPartial,
    isOverflowAsNull)
{}

// Explicit template instantiation
// Defining templated aggregators in header file consume a lot of memory during compilation
// since, compiler needs to generate each individual template instance wherever aggregator header is include
// to reduce time and memory usage during compilation moved templated aggregator implementation into .cpp files
// and used explicit template instantiation to generate template instances
template class MinAggregator<OMNI_BOOLEAN, OMNI_BOOLEAN>;

template class MinAggregator<OMNI_BOOLEAN, OMNI_SHORT>;

template class MinAggregator<OMNI_BOOLEAN, OMNI_INT>;

template class MinAggregator<OMNI_BOOLEAN, OMNI_LONG>;

template class MinAggregator<OMNI_BOOLEAN, OMNI_DOUBLE>;

template class MinAggregator<OMNI_BOOLEAN, OMNI_DECIMAL128>;

template class MinAggregator<OMNI_BOOLEAN, OMNI_DECIMAL64>;

template class MinAggregator<OMNI_SHORT, OMNI_BOOLEAN>;

template class MinAggregator<OMNI_SHORT, OMNI_SHORT>;

template class MinAggregator<OMNI_SHORT, OMNI_INT>;

template class MinAggregator<OMNI_SHORT, OMNI_LONG>;

template class MinAggregator<OMNI_SHORT, OMNI_DOUBLE>;

template class MinAggregator<OMNI_SHORT, OMNI_DECIMAL128>;

template class MinAggregator<OMNI_SHORT, OMNI_DECIMAL64>;

template class MinAggregator<OMNI_INT, OMNI_BOOLEAN>;

template class MinAggregator<OMNI_INT, OMNI_SHORT>;

template class MinAggregator<OMNI_INT, OMNI_INT>;

template class MinAggregator<OMNI_INT, OMNI_LONG>;

template class MinAggregator<OMNI_INT, OMNI_DOUBLE>;

template class MinAggregator<OMNI_INT, OMNI_DECIMAL128>;

template class MinAggregator<OMNI_INT, OMNI_DECIMAL64>;

template class MinAggregator<OMNI_LONG, OMNI_BOOLEAN>;

template class MinAggregator<OMNI_LONG, OMNI_SHORT>;

template class MinAggregator<OMNI_LONG, OMNI_INT>;

template class MinAggregator<OMNI_LONG, OMNI_LONG>;

template class MinAggregator<OMNI_LONG, OMNI_DOUBLE>;

template class MinAggregator<OMNI_LONG, OMNI_DECIMAL128>;

template class MinAggregator<OMNI_LONG, OMNI_DECIMAL64>;

template class MinAggregator<OMNI_DOUBLE, OMNI_BOOLEAN>;

template class MinAggregator<OMNI_DOUBLE, OMNI_SHORT>;

template class MinAggregator<OMNI_DOUBLE, OMNI_INT>;

template class MinAggregator<OMNI_DOUBLE, OMNI_LONG>;

template class MinAggregator<OMNI_DOUBLE, OMNI_DOUBLE>;

template class MinAggregator<OMNI_DOUBLE, OMNI_DECIMAL128>;

template class MinAggregator<OMNI_DOUBLE, OMNI_DECIMAL64>;

template class MinAggregator<OMNI_DECIMAL128, OMNI_BOOLEAN>;

template class MinAggregator<OMNI_DECIMAL128, OMNI_SHORT>;

template class MinAggregator<OMNI_DECIMAL128, OMNI_INT>;

template class MinAggregator<OMNI_DECIMAL128, OMNI_LONG>;

template class MinAggregator<OMNI_DECIMAL128, OMNI_DOUBLE>;

template class MinAggregator<OMNI_DECIMAL128, OMNI_DECIMAL128>;

template class MinAggregator<OMNI_DECIMAL128, OMNI_DECIMAL64>;

template class MinAggregator<OMNI_DECIMAL64, OMNI_BOOLEAN>;

template class MinAggregator<OMNI_DECIMAL64, OMNI_SHORT>;

template class MinAggregator<OMNI_DECIMAL64, OMNI_INT>;

template class MinAggregator<OMNI_DECIMAL64, OMNI_LONG>;

template class MinAggregator<OMNI_DECIMAL64, OMNI_DOUBLE>;

template class MinAggregator<OMNI_DECIMAL64, OMNI_DECIMAL128>;

template class MinAggregator<OMNI_DECIMAL64, OMNI_DECIMAL64>;
}
}
