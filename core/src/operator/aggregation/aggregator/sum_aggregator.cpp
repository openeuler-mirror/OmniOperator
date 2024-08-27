/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2024. All rights reserved.
 * Description: Sum aggregator
 */

#include "sum_aggregator.h"

namespace omniruntime {
namespace op {
template <DataTypeId IN_ID, DataTypeId OUT_ID>
void SumAggregator<IN_ID, OUT_ID>::ExtractValuesInternal(const AggregateState &state, OutVector *vector,
    int32_t rowIndex)
{
    bool isOverflow = state.count < 0;
    bool isEmpty = state.count == 0;
    if (isOverflow && !this->IsOverflowAsNull()) {
        throw OmniException("OPERATOR_RUNTIME_ERROR", "sum_aggregator overflow.");
    }

    OutType result {};
    if constexpr (OUT_ID == OMNI_VARCHAR) {
        if (isOverflow || isEmpty) {
            std::string_view decimal2Str(reinterpret_cast<char *>(&result), sizeof(OutType));
            vector->SetValue(rowIndex, decimal2Str);
        } else {
            result.sum =
                CastWithOverflow<Decimal128, Decimal128>(*reinterpret_cast<Decimal128 *>(state.val), isOverflow);
            if (isOverflow) {
                if (!this->IsOverflowAsNull()) {
                    throw OmniException("OPERATOR_RUNTIME_ERROR", "sum_aggregator overflow.");
                } else {
                    OutType overflowValue {};
                    std::string_view decimal2Str(reinterpret_cast<char *>(&overflowValue), sizeof(OutType));
                    vector->SetValue(rowIndex, decimal2Str);
                }
            } else {
                result.count = state.count;
                std::string_view decimal2Str(reinterpret_cast<char *>(&result), sizeof(OutType));
                vector->SetValue(rowIndex, decimal2Str);
            }
        }
    } else {
        if (isOverflow) {
            this->SetNullOrThrowException(vector, rowIndex, "sum_aggregator overflow.");
        } else if (isEmpty) {
            vector->SetNull(rowIndex);
        } else {
            result = CastWithOverflowEntry<ResultType, OutType>(state.val, isOverflow);
            if (isOverflow) {
                this->SetNullOrThrowException(vector, rowIndex, "sum_aggregator overflow.");
            } else {
                vector->SetValue(rowIndex, result);
            }
        }
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void SumAggregator<IN_ID, OUT_ID>::ExtractValues(const AggregateState &state, std::vector<BaseVector *> &vectors,
    int32_t rowIndex)
{
    ExtractValuesInternal(state, static_cast<OutVector *>(vectors[0]), rowIndex);
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void SumAggregator<IN_ID, OUT_ID>::ExtractValuesBatch(std::vector<AggregateState *> &groupStates, const size_t aggIdx,
    std::vector<BaseVector *> &vectors, int32_t rowOffset, int32_t rowCount)
{
    auto v = static_cast<OutVector *>(vectors[0]);
    for (int32_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        auto &state = groupStates[rowIndex][aggIdx];
        ExtractValuesInternal(state, v, rowIndex);
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID> std::vector<DataTypePtr> SumAggregator<IN_ID, OUT_ID>::GetSpillType()
{
    std::vector<DataTypePtr> spillTypes;
    if constexpr (IN_ID == OMNI_SHORT || IN_ID == OMNI_INT || IN_ID == OMNI_LONG) {
        spillTypes.emplace_back(std::make_shared<DataType>(OMNI_LONG));
    } else if constexpr (IN_ID == OMNI_DOUBLE || IN_ID == OMNI_CONTAINER) {
        spillTypes.emplace_back(std::make_shared<DataType>(OMNI_DOUBLE));
    } else {
        spillTypes.emplace_back(std::make_shared<DataType>(OMNI_DECIMAL128));
    }
    return spillTypes;
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void SumAggregator<IN_ID, OUT_ID>::ExtractValuesForSpill(std::vector<AggregateState *> &groupStates,
    const size_t aggIdx, std::vector<BaseVector *> &vectors)
{
    auto spillValueVec = static_cast<Vector<ResultType> *>(vectors[0]);

    auto rowCount = static_cast<int32_t>(groupStates.size());
    for (int32_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        auto &state = groupStates[rowIndex][aggIdx];
        auto count = state.count;
        bool isOverflow = count < 0;
        bool isEmpty = count == 0;

        // use null and value to distinguish empty group, overflow and other normal case
        if (isOverflow) {
            // set null and overflow value to mark overflow case
            this->SetNullOrThrowException(spillValueVec, rowIndex, "sum_aggregator overflow.");
            spillValueVec->SetValue(rowIndex, SPILL_OVERFLOW_VALUE);
        } else if (isEmpty) {
            // set null for empty group(all rows are NULL) when spill to ensure skip empty group when unspill
            spillValueVec->SetNull(rowIndex);
            spillValueVec->SetValue(rowIndex, SPILL_EMPTY_VALUE);
        } else {
            ResultType result;
            if constexpr (std::is_same_v<ResultType, Decimal128> || std::is_floating_point_v<ResultType>) {
                result = *reinterpret_cast<ResultType *>(state.val);
            } else {
                result = static_cast<ResultType>(state.val);
            }
            spillValueVec->SetValue(rowIndex, result);
        }
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID> void SumAggregator<IN_ID, OUT_ID>::InitState(AggregateState &state)
{
    if constexpr (std::is_same_v<ResultType, Decimal128> || std::is_floating_point_v<ResultType> ||
        std::is_same_v<ResultType, std::string_view>) {
        state.val = reinterpret_cast<int64_t>(arenaAllocator->Allocate(sizeof(ResultType)));
        *reinterpret_cast<ResultType *>(state.val) = ResultType {};
    } else {
        state.val = 0;
    }
    state.count = 0;
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void SumAggregator<IN_ID, OUT_ID>::InitStates(std::vector<AggregateState *> groupStates, const size_t aggIdx)
{
    for (auto groupState : groupStates) {
        auto &state = groupState[aggIdx];
        InitState(state);
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void SumAggregator<IN_ID, OUT_ID>::ProcessSingleInternal(AggregateState &state, BaseVector *vector,
    const int32_t rowOffset, const int32_t rowCount, const uint8_t *nullMap)
{
    if (vector->GetEncoding() != vec::OMNI_DICTIONARY) {
        auto *ptr = reinterpret_cast<InType *>(GetValuesFromVector<IN_ID>(vector));
        ptr += rowOffset;
        if (nullMap == nullptr) {
            if constexpr (std::is_same_v<ResultType, Decimal128> || std::is_floating_point_v<ResultType>) {
                Add<InType, ResultType, SumOp<InType, ResultType>>(reinterpret_cast<ResultType *>(state.val),
                    state.count, ptr, rowCount);
            } else {
                Add<InType, ResultType, SumOp<InType, ResultType>>(reinterpret_cast<ResultType *>(&state.val),
                    state.count, ptr, rowCount);
            }
        } else {
            if constexpr (std::is_floating_point_v<InType>) {
                if constexpr (std::is_same_v<ResultType, Decimal128> || std::is_floating_point_v<ResultType>) {
                    SumConditionalFloat<InType, ResultType, false>(reinterpret_cast<ResultType *>(state.val),
                        state.count, ptr, rowCount, nullMap);
                } else {
                    SumConditionalFloat<InType, ResultType, false>(reinterpret_cast<ResultType *>(&state.val),
                        state.count, ptr, rowCount, nullMap);
                }
            } else {
                if constexpr (std::is_same_v<ResultType, Decimal128> || std::is_floating_point_v<ResultType>) {
                    AddConditional<InType, ResultType, SumConditionalOp<InType, ResultType, false>>(
                        reinterpret_cast<ResultType *>(state.val), state.count, ptr, rowCount, nullMap);
                } else {
                    AddConditional<InType, ResultType, SumConditionalOp<InType, ResultType, false>>(
                        reinterpret_cast<ResultType *>(&state.val), state.count, ptr, rowCount, nullMap);
                }
            }
        }
    } else {
        auto *ptr = reinterpret_cast<InType *>(GetValuesFromDict<IN_ID>(vector));
        auto *indexMap = GetIdsFromDict<IN_ID>(vector) + rowOffset;
        if (nullMap == nullptr) {
            if constexpr (std::is_same_v<ResultType, Decimal128> || std::is_floating_point_v<ResultType>) {
                AddDict<InType, ResultType, SumOp<InType, ResultType>>(reinterpret_cast<ResultType *>(state.val),
                    state.count, ptr, rowCount, indexMap);
            } else {
                AddDict<InType, ResultType, SumOp<InType, ResultType>>(reinterpret_cast<ResultType *>(&state.val),
                    state.count, ptr, rowCount, indexMap);
            }
        } else {
            if constexpr (std::is_same_v<ResultType, Decimal128> || std::is_floating_point_v<ResultType>) {
                AddDictConditional<InType, ResultType, SumConditionalOp<InType, ResultType, false>>(
                    reinterpret_cast<ResultType *>(state.val), state.count, ptr, rowCount, nullMap, indexMap);
            } else {
                AddDictConditional<InType, ResultType, SumConditionalOp<InType, ResultType, false>>(
                    reinterpret_cast<ResultType *>(&state.val), state.count, ptr, rowCount, nullMap, indexMap);
            }
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
void SumAggregator<IN_ID, OUT_ID>::ProcessGroupUnspill(std::vector<UnspillRowInfo> &unspillRows, int32_t rowCount,
    const size_t aggIdx, int32_t &vectorIndex)
{
    auto firstVecIdx = vectorIndex++;
    for (int32_t rowIdx = 0; rowIdx < rowCount; rowIdx++) {
        auto &row = unspillRows[rowIdx];
        auto batch = row.batch;
        auto index = row.rowIdx;
        auto &state = row.state[aggIdx];
        auto sumVector = static_cast<Vector<ResultType> *>(batch->Get(firstVecIdx));
        auto value = sumVector->GetValue(index);
        if (!sumVector->IsNull(index)) {
            if constexpr (std::is_same_v<ResultType, Decimal128> || std::is_floating_point_v<ResultType>) {
                SumOp<ResultType, ResultType>(reinterpret_cast<ResultType *>(state.val), state.count, value, 1LL);
            } else {
                SumOp<ResultType, ResultType>(reinterpret_cast<ResultType *>(&state.val), state.count, value, 1LL);
            }
        } else {
            // empty group or overflow case
            // if it is overflow we set -1
            // if it is empty group we skipped
            if (value == SPILL_OVERFLOW_VALUE) {
                state.count = -1;
            }
        }
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void SumAggregator<IN_ID, OUT_ID>::ProcessAlignAggSchema(VectorBatch *result, BaseVector *originVector) {
    int rowCount = originVector->GetSize();
    // opt branch
    if (std::is_same_v<InType, OutType>) {
        auto sumVector = VectorHelper::SliceVector(originVector, 0, rowCount);
        result->Append(sumVector);
        return;
    }

    auto sumVector = reinterpret_cast<OutVector *>(VectorHelper::CreateFlatVector(OUT_ID, rowCount));
    if constexpr (IN_ID == OMNI_VARCHAR) {
        // olk final agg pause, so throw an exception
        throw OmniException("OPERATOR_RUNTIME_ERROR", "this interface only be called in partial agg pause.");
    } else if constexpr (OUT_ID == OMNI_VARCHAR) {
        // olk branch, decimal128 is stored in DecimalPartialResult object, and then be converted to std::string_view type.
        if (originVector->GetEncoding() == OMNI_DICTIONARY) {
            auto vector = reinterpret_cast<Vector<DictionaryContainer<Decimal128>> *>(originVector);
            for (int index = 0; index < rowCount; ++index) {
                if (vector->IsNull(index)) {
                    sumVector->SetNull(index);
                } else {
                    OutType out;
                    out.sum = vector->GetValue(index);
                    out.count = 1;
                    std::string_view decimal2Str(reinterpret_cast<char *>(&out), sizeof(OutType));
                    sumVector->SetValue(index, decimal2Str);
                }
            }
        } else {
            auto vector = reinterpret_cast<Vector<Decimal128> *>(originVector);
            for (int index = 0; index < rowCount; ++index) {
                if (vector->IsNull(index)) {
                    sumVector->SetNull(index);
                } else {
                    OutType out;
                    out.sum = vector->GetValue(index);
                    out.count = 1;
                    std::string_view decimal2Str(reinterpret_cast<char *>(&result), sizeof(OutType));
                    sumVector->SetValue(index, decimal2Str);
                }
            }
        }
    } else {
        // hive or spark branch
        if (originVector->GetEncoding() == OMNI_DICTIONARY) {
            auto vector = reinterpret_cast<Vector<DictionaryContainer<InType>> *>(originVector);
            for (int index = 0; index < rowCount; ++index) {
                if (vector->IsNull(index)) {
                    sumVector->SetNull(index);
                } else {
                    InType val = vector->GetValue(index);
                    bool overflow = false;
                    OutType out = this->template CastWithOverflow<InType, OutType>(static_cast<InType>(val), overflow);
                    sumVector->SetValue(index, out);
                }
            }
        } else {
            // The varchar type is converted to the double type in advance.
            auto vector = reinterpret_cast<Vector<InType> *>(originVector);
            for (int index = 0; index < rowCount; ++index) {
                if (vector->IsNull(index)) {
                    sumVector->SetNull(index);
                } else {
                    InType val = vector->GetValue(index);
                    bool overflow = false;
                    OutType out = this->template CastWithOverflow<InType, OutType>(static_cast<InType>(val), overflow);
                    sumVector->SetValue(index, out);
                }
            }
        }
    }
    result->Append(sumVector);
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
