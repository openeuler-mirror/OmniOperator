/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2024. All rights reserved.
 * Description: Max aggregate
 */

#include "max_aggregator.h"
#include "operator/aggregation/neon_aggregation/simd_aggregation_external.h"

namespace omniruntime {
namespace op {
template <DataTypeId IN_ID, DataTypeId OUT_ID>
void MaxAggregator<IN_ID, OUT_ID>::ExtractValues(const AggregateState &state, std::vector<BaseVector *> &vectors,
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
        this->SetNullOrThrowException(v, rowIndex, "max_aggregator overflow.");
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void MaxAggregator<IN_ID, OUT_ID>::ExtractValuesBatch(std::vector<AggregateState *> &groupStates, const size_t aggIdx,
    std::vector<BaseVector *> &vectors, int32_t rowOffset, int32_t rowCount)
{
    auto v = static_cast<OutVector *>(vectors[0]);
    for (int32_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        auto &state = groupStates[rowIndex][aggIdx];
        if (state.count == 0) {
            v->SetNull(rowIndex);
        } else if (state.count < 0) {
            this->SetNullOrThrowException(v, rowIndex, "max_aggregator overflow.");
        } else {
            OutType result;
            bool isOverflow = false;
            result = CastWithOverflowEntry<ResultType, OutType>(state.val, isOverflow);
            if (isOverflow) {
                this->SetNullOrThrowException(v, rowIndex, "max_aggregator overflow.");
            } else {
                v->SetValue(rowIndex, result);
            }
        }
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID> std::vector<DataTypePtr> MaxAggregator<IN_ID, OUT_ID>::GetSpillType()
{
    std::vector<DataTypePtr> spillTypes;
    if constexpr (IN_ID == OMNI_SHORT) {
        spillTypes.emplace_back(std::make_shared<DataType>(OMNI_INT));
    } else {
        spillTypes.emplace_back(outputTypes.GetType(0));
    }
    return spillTypes;
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void MaxAggregator<IN_ID, OUT_ID>::ExtractValuesForSpill(std::vector<AggregateState *> &groupStates,
    const size_t aggIdx, std::vector<BaseVector *> &vectors)
{
    auto spillValueVec = static_cast<Vector<ResultType> *>(vectors[0]);
    auto rowCount = static_cast<int32_t>(groupStates.size());
    for (int32_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        auto &state = groupStates[rowIndex][aggIdx];
        if (state.count == 0) {
            spillValueVec->SetNull(rowIndex);
        } else {
            if constexpr (std::is_same_v<ResultType, Decimal128> || std::is_floating_point_v<ResultType>) {
                spillValueVec->SetValue(rowIndex, *reinterpret_cast<ResultType *>(state.val));
            } else {
                spillValueVec->SetValue(rowIndex, static_cast<ResultType>(state.val));
            }
        }
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID> void MaxAggregator<IN_ID, OUT_ID>::InitState(AggregateState &state)
{
    if constexpr (std::is_same_v<ResultType, Decimal128> || std::is_floating_point_v<ResultType>) {
        state.val = reinterpret_cast<int64_t>(arenaAllocator->Allocate(sizeof(ResultType)));
        *reinterpret_cast<ResultType *>(state.val) = GetMin<ResultType>();
    } else {
        state.val = GetMin<ResultType>();
    }

    state.count = 0;
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void MaxAggregator<IN_ID, OUT_ID>::InitStates(std::vector<AggregateState *> groupStates, const size_t aggIdx)
{
    ResultType minVal = GetMin<ResultType>();
    for (auto groupState : groupStates) {
        auto &state = groupState[aggIdx];
        if constexpr (std::is_same_v<ResultType, Decimal128> || std::is_floating_point_v<ResultType>) {
            state.val = reinterpret_cast<int64_t>(arenaAllocator->Allocate(sizeof(ResultType)));
            *reinterpret_cast<ResultType *>(state.val) = minVal;
        } else {
            state.val = minVal;
        }

        state.count = 0;
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void MaxAggregator<IN_ID, OUT_ID>::ProcessSingleInternal(AggregateState &state, BaseVector *vector,
    const int32_t rowOffset, const int32_t rowCount, const uint8_t *nullMap)
{
    if (vector->GetEncoding() != vec::OMNI_DICTIONARY) {
        auto *ptr = reinterpret_cast<InType *>(GetValuesFromVector<IN_ID>(vector));
        ptr += rowOffset;
        if (nullMap == nullptr) {
            if constexpr (simd::CheckTypesContainsDecimal128<InType, ResultType>::value) {
                Add<InType, ResultType, MaxOp<InType, ResultType>>(reinterpret_cast<ResultType *>(state.val),
                    state.count, ptr, rowCount);
            } else if constexpr (std::is_floating_point_v<ResultType>) {
                simd::SIMDAdd<InType, ResultType, simd::BasicOp::Max>(reinterpret_cast<ResultType *>(state.val),
                    state.count, ptr, rowCount);
            } else {
                simd::SIMDAdd<InType, ResultType, simd::BasicOp::Max>(reinterpret_cast<ResultType *>(&state.val),
                    state.count, ptr, rowCount);
            }
        } else {
            if constexpr (simd::CheckTypesContainsDecimal128<InType, ResultType>::value) {
                AddConditional<InType, ResultType, MaxConditionalOp<InType, ResultType, false>>(
                    reinterpret_cast<ResultType *>(state.val), state.count, ptr, rowCount, nullMap);
            } else if constexpr (std::is_floating_point_v<ResultType>) {
                simd::SIMDAddConditional<InType, ResultType, simd::BasicOp::Max>(
                    reinterpret_cast<ResultType *>(state.val), state.count, ptr, rowCount, nullMap);
            } else {
                simd::SIMDAddConditional<InType, ResultType, simd::BasicOp::Max>(
                    reinterpret_cast<ResultType *>(&state.val), state.count, ptr, rowCount, nullMap);
            }
        }
    } else {
        auto *ptr = reinterpret_cast<InType *>(GetValuesFromDict<IN_ID>(vector));
        auto *indexMap = GetIdsFromDict<IN_ID>(vector) + rowOffset;
        if (nullMap == nullptr) {
            if constexpr (simd::CheckTypesContainsDecimal128<InType, ResultType>::value) {
                AddDict<InType, ResultType, MaxOp<InType, ResultType>>(reinterpret_cast<ResultType *>(state.val),
                    state.count, ptr, rowCount, indexMap);
            } else if constexpr (std::is_floating_point_v<ResultType>) {
                simd::SIMDAddDict<InType, ResultType, simd::BasicOp::Max>(reinterpret_cast<ResultType *>(state.val),
                    state.count, ptr, rowCount, indexMap);
            } else {
                simd::SIMDAddDict<InType, ResultType, simd::BasicOp::Max>(reinterpret_cast<ResultType *>(&state.val),
                    state.count, ptr, rowCount, indexMap);
            }
        } else {
            if constexpr (simd::CheckTypesContainsDecimal128<InType, ResultType>::value) {
                AddDictConditional<InType, ResultType, MaxConditionalOp<InType, ResultType, false>>(
                    reinterpret_cast<ResultType *>(state.val), state.count, ptr, rowCount, nullMap, indexMap);
            } else if constexpr (std::is_floating_point_v<ResultType>) {
                simd::SIMDAddDictConditional<InType, ResultType, simd::BasicOp::Max>(
                    reinterpret_cast<ResultType *>(state.val), state.count, ptr, rowCount, nullMap, indexMap);
            } else {
                simd::SIMDAddDictConditional<InType, ResultType, simd::BasicOp::Max>(
                    reinterpret_cast<ResultType *>(&state.val), state.count, ptr, rowCount, nullMap, indexMap);
            }
        }
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void MaxAggregator<IN_ID, OUT_ID>::ProcessGroupInternal(std::vector<AggregateState *> &rowStates, const size_t aggIdx,
    BaseVector *vector, const int32_t rowOffset, const uint8_t *nullMap)
{
    if (vector->GetEncoding() != vec::OMNI_DICTIONARY) {
        auto *ptr = reinterpret_cast<InType *>(GetValuesFromVector<IN_ID>(vector));
        ptr += rowOffset;
        if (nullMap == nullptr) {
            AddUseRowIndex<InType, ResultType, MaxOp<InType, ResultType>>(rowStates, aggIdx, ptr);
        } else {
            AddConditionalUseRowIndex<InType, ResultType, MaxConditionalOp<InType, ResultType, false>>(rowStates,
                aggIdx, ptr, nullMap);
        }
    } else {
        auto *ptr = reinterpret_cast<InType *>(GetValuesFromDict<IN_ID>(vector));
        auto *indexMap = GetIdsFromDict<IN_ID>(vector) + rowOffset;
        if (nullMap == nullptr) {
            AddDictUseRowIndex<InType, ResultType, MaxOp<InType, ResultType>>(rowStates, aggIdx, ptr, indexMap);
        } else {
            AddDictConditionalUseRowIndex<InType, ResultType, MaxConditionalOp<InType, ResultType, false>>(rowStates,
                aggIdx, ptr, nullMap, indexMap);
        }
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void MaxAggregator<IN_ID, OUT_ID>::ProcessGroupUnspill(std::vector<UnspillRowInfo> &unspillRows, int32_t rowCount,
    const size_t aggIdx, int32_t &vectorIndex)
{
    auto firstVecIdx = vectorIndex++;
    for (int32_t rowIdx = 0; rowIdx < rowCount; rowIdx++) {
        auto &row = unspillRows[rowIdx];
        auto batch = row.batch;
        auto index = row.rowIdx;
        auto vectorPtr = static_cast<Vector<ResultType> *>(batch->Get(firstVecIdx));
        if (!vectorPtr->IsNull(index)) {
            auto &state = row.state[aggIdx];
            auto value = vectorPtr->GetValue(index);
            if constexpr (std::is_same_v<ResultType, Decimal128> || std::is_floating_point_v<ResultType>) {
                MaxOp<ResultType, ResultType>(reinterpret_cast<ResultType *>(state.val), state.count, value, 1LL);
            } else {
                MaxOp<ResultType, ResultType>(reinterpret_cast<ResultType *>(&state.val), state.count, value, 1LL);
            }
        }
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void MaxAggregator<IN_ID, OUT_ID>::ProcessAlignAggSchema(VectorBatch *result, BaseVector *originVector) {
    int rowCount = originVector->GetSize();
    if constexpr (std::is_same_v<InType, OutType>) {
        auto maxVector = VectorHelper::SliceVector(originVector, 0, rowCount);
        result->Append(maxVector);
        return;
    }

    auto maxVector = reinterpret_cast<OutVector *>(VectorHelper::CreateFlatVector(OUT_ID, rowCount));
    if (originVector->GetEncoding() == OMNI_DICTIONARY) {
        auto vector = reinterpret_cast<Vector<DictionaryContainer<InType>> *>(originVector);
        for (int index = 0; index < rowCount; ++index) {
            if (vector->IsNull(index)) {
                maxVector->SetNull(index);
            } else {
                InType val = vector->GetValue(index);
                bool overflow = false;
                OutType out = this->template CastWithOverflow<InType, OutType>(static_cast<InType>(val), overflow);
                maxVector->SetValue(index, out);
            }
        }
    } else {
        auto vector = reinterpret_cast<Vector<InType> *>(originVector);
        for (int index = 0; index < rowCount; ++index) {
            if (vector->IsNull(index)) {
                maxVector->SetNull(index);
            } else {
                InType val = vector->GetValue(index);
                bool overflow = false;
                OutType out = this->template CastWithOverflow<InType, OutType>(static_cast<InType>(val), overflow);
                maxVector->SetValue(index, out);
            }
        }
    }
    result->Append(maxVector);
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
MaxAggregator<IN_ID, OUT_ID>::MaxAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes,
    std::vector<int32_t> &channels, const bool inputRaw, const bool outputPartial, const bool isOverflowAsNull)
    : TypedAggregator(OMNI_AGGREGATION_TYPE_MAX, inputTypes, outputTypes, channels, inputRaw, outputPartial,
    isOverflowAsNull)
{}

// Explicit template instantiation
// Defining templated aggregators in header file consume a lot of memory during compilation
// since, compiler needs to generate each individual template instance wherever aggregator header is include
// to reduce time and memory usage during compilation moved templated aggregator implementation into .cpp files
// and used explicit template instantiation to generate template instances
template class MaxAggregator<OMNI_BOOLEAN, OMNI_BOOLEAN>;

template class MaxAggregator<OMNI_BOOLEAN, OMNI_SHORT>;

template class MaxAggregator<OMNI_BOOLEAN, OMNI_INT>;

template class MaxAggregator<OMNI_BOOLEAN, OMNI_LONG>;

template class MaxAggregator<OMNI_BOOLEAN, OMNI_DOUBLE>;

template class MaxAggregator<OMNI_BOOLEAN, OMNI_DECIMAL128>;

template class MaxAggregator<OMNI_BOOLEAN, OMNI_DECIMAL64>;

template class MaxAggregator<OMNI_SHORT, OMNI_BOOLEAN>;

template class MaxAggregator<OMNI_SHORT, OMNI_SHORT>;

template class MaxAggregator<OMNI_SHORT, OMNI_INT>;

template class MaxAggregator<OMNI_SHORT, OMNI_LONG>;

template class MaxAggregator<OMNI_SHORT, OMNI_DOUBLE>;

template class MaxAggregator<OMNI_SHORT, OMNI_DECIMAL128>;

template class MaxAggregator<OMNI_SHORT, OMNI_DECIMAL64>;

template class MaxAggregator<OMNI_INT, OMNI_BOOLEAN>;

template class MaxAggregator<OMNI_INT, OMNI_SHORT>;

template class MaxAggregator<OMNI_INT, OMNI_INT>;

template class MaxAggregator<OMNI_INT, OMNI_LONG>;

template class MaxAggregator<OMNI_INT, OMNI_DOUBLE>;

template class MaxAggregator<OMNI_INT, OMNI_DECIMAL128>;

template class MaxAggregator<OMNI_INT, OMNI_DECIMAL64>;

template class MaxAggregator<OMNI_LONG, OMNI_BOOLEAN>;

template class MaxAggregator<OMNI_LONG, OMNI_SHORT>;

template class MaxAggregator<OMNI_LONG, OMNI_INT>;

template class MaxAggregator<OMNI_LONG, OMNI_LONG>;

template class MaxAggregator<OMNI_LONG, OMNI_DOUBLE>;

template class MaxAggregator<OMNI_LONG, OMNI_DECIMAL128>;

template class MaxAggregator<OMNI_LONG, OMNI_DECIMAL64>;

template class MaxAggregator<OMNI_DOUBLE, OMNI_BOOLEAN>;

template class MaxAggregator<OMNI_DOUBLE, OMNI_SHORT>;

template class MaxAggregator<OMNI_DOUBLE, OMNI_INT>;

template class MaxAggregator<OMNI_DOUBLE, OMNI_LONG>;

template class MaxAggregator<OMNI_DOUBLE, OMNI_DOUBLE>;

template class MaxAggregator<OMNI_DOUBLE, OMNI_DECIMAL128>;

template class MaxAggregator<OMNI_DOUBLE, OMNI_DECIMAL64>;

template class MaxAggregator<OMNI_DECIMAL128, OMNI_BOOLEAN>;

template class MaxAggregator<OMNI_DECIMAL128, OMNI_SHORT>;

template class MaxAggregator<OMNI_DECIMAL128, OMNI_INT>;

template class MaxAggregator<OMNI_DECIMAL128, OMNI_LONG>;

template class MaxAggregator<OMNI_DECIMAL128, OMNI_DOUBLE>;

template class MaxAggregator<OMNI_DECIMAL128, OMNI_DECIMAL128>;

template class MaxAggregator<OMNI_DECIMAL128, OMNI_DECIMAL64>;

template class MaxAggregator<OMNI_DECIMAL64, OMNI_BOOLEAN>;

template class MaxAggregator<OMNI_DECIMAL64, OMNI_SHORT>;

template class MaxAggregator<OMNI_DECIMAL64, OMNI_INT>;

template class MaxAggregator<OMNI_DECIMAL64, OMNI_LONG>;

template class MaxAggregator<OMNI_DECIMAL64, OMNI_DOUBLE>;

template class MaxAggregator<OMNI_DECIMAL64, OMNI_DECIMAL128>;

template class MaxAggregator<OMNI_DECIMAL64, OMNI_DECIMAL64>;
}
}
