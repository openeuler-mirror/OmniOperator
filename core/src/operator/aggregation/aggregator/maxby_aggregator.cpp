/*
* Copyright (c) Huawei Technologies Co., Ltd. 2021-2024. All rights reserved.
 * Description: Max by aggregate
 */

#include "maxby_aggregator.h"
#include "simd/func/reduce.h"

namespace omniruntime {
namespace op {

template <DataTypeId COL1_ID, DataTypeId COL2_ID>
void MaxByAggregator<COL1_ID, COL2_ID>::ExtractValues(const AggregateState *state, std::vector<BaseVector *> &vectors,
    int32_t rowIndex)
{
    if (this->outputPartial) {
        auto *maxByState = MaxByState<targetValueType, sortKeyType>::ConstCastState(state + aggStateOffset);

        if (maxByState->isEmpty) {
            // no partial result
            return;
        }

        auto targetValueVector = static_cast<targetValueTypeVec *>(vectors[0]);
        auto sortKeyVector = static_cast<sortKeyTypeVec *>(vectors[1]);

        targetValueVector->SetValue(rowIndex, maxByState->targetValue);
        sortKeyVector->SetValue(rowIndex, maxByState->sortKey);
    } else {
        // final output, only one col
        auto *maxByState = MaxByState<targetValueType, sortKeyType>::ConstCastState(state + aggStateOffset);
        auto targetValueVector = static_cast<targetValueTypeVec *>(vectors[0]);
        if (maxByState->isEmpty) {
            // max_by empty table，final result is null
            targetValueVector->SetNull(rowIndex);
            return;
        }
        targetValueVector->SetValue(rowIndex, maxByState->targetValue);
    }
}

template <DataTypeId COL1_ID, DataTypeId COL2_ID>
void MaxByAggregator<COL1_ID, COL2_ID>::ExtractValuesBatch(std::vector<AggregateState *> &groupStates,
    std::vector<BaseVector *> &vectors, int32_t rowOffset, int32_t rowCount)
{
    if (this->outputPartial) {
        auto targetValueVector = static_cast<targetValueTypeVec *>(vectors[0]);
        auto sortKeyVector = static_cast<sortKeyTypeVec *>(vectors[1]);
        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            auto *maxByState = MaxByState<targetValueType, sortKeyType>::ConstCastState(groupStates[rowIndex] + aggStateOffset);
            if (maxByState->isEmpty) {
                // no partial result
                continue;
            }
            targetValueVector->SetValue(rowIndex, maxByState->targetValue);
            sortKeyVector->SetValue(rowIndex, maxByState->sortKey);
        }
    } else {
        // output final result, only one col
        auto targetValueVector = static_cast<targetValueTypeVec *>(vectors[0]);
        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            auto *maxByState = MaxByState<targetValueType, sortKeyType>::ConstCastState(groupStates[rowIndex] + aggStateOffset);
            if (maxByState->isEmpty) {
                // max_by empty table，final result is null
                targetValueVector->SetNull(rowIndex);
                continue;
            }
            targetValueVector->SetValue(rowIndex, maxByState->targetValue);
        }
    }
}

template <DataTypeId COL1_ID, DataTypeId COL2_ID> std::vector<DataTypePtr> MaxByAggregator<COL1_ID, COL2_ID>::GetSpillType()
{
    std::vector<DataTypePtr> spillTypes;
    spillTypes.emplace_back(std::make_shared<DataType>(COL1_ID));
    spillTypes.emplace_back(std::make_shared<DataType>(COL2_ID));
    return spillTypes;
}


template <DataTypeId COL1_ID, DataTypeId COL2_ID>
void MaxByAggregator<COL1_ID, COL2_ID>::ExtractValuesForSpill(std::vector<AggregateState *> &groupStates,
    std::vector<BaseVector *> &vectors)
{
    auto targetValueVector = static_cast<targetValueTypeVec *>(vectors[0]);
    auto sortKeyVector = static_cast<sortKeyTypeVec *>(vectors[1]);

    auto rowCount = static_cast<int32_t>(groupStates.size());
    for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        auto *maxByState = MaxByState<targetValueType, sortKeyType>::CastState(groupStates[rowIndex] + aggStateOffset);
        if (maxByState->isEmpty) {
            // dont need spill any states
			sortKeyVector->SetNull(rowIndex);
			targetValueVector->SetNull(rowIndex);
            continue;
        }
        targetValueVector->SetValue(rowIndex, maxByState->targetValue);
        sortKeyVector->SetValue(rowIndex, maxByState->sortKey);
    }
}

template <DataTypeId COL1_ID, DataTypeId COL2_ID> void MaxByAggregator<COL1_ID, COL2_ID>::InitState(AggregateState *state)
{
    auto *maxByState = MaxByState<targetValueType, sortKeyType>::CastState(state + aggStateOffset);
    maxByState->isEmpty = true;
    maxByState->sortKey = GetSortKeyMin<sortKeyType>();
}

template <DataTypeId COL1_ID, DataTypeId COL2_ID>
void MaxByAggregator<COL1_ID, COL2_ID>::InitStates(std::vector<AggregateState *> &groupStates)
{
    for (auto groupState : groupStates) {
        auto *maxByState = MaxByState<targetValueType, sortKeyType>::CastState(groupState + aggStateOffset);
        maxByState->isEmpty = true;
        maxByState->sortKey = GetSortKeyMin<sortKeyType>();
    }
}

template <DataTypeId COL1_ID, DataTypeId COL2_ID>
void MaxByAggregator<COL1_ID, COL2_ID>::ProcessSingleInternal(AggregateState *state, BaseVector *vector,
    const int32_t rowOffset, const int32_t rowCount, const std::shared_ptr<NullsHelper> nullMap)
{
    auto *maxByState = MaxByState<targetValueType, sortKeyType>::CastState(state);

    auto *col1Vector = this->curVectorBatch->Get(this->channels[0]);
    auto *col2Vector = this->curVectorBatch->Get(this->channels[1]);

    auto *col1ptr = reinterpret_cast<targetValueType *>(GetValuesFromVector<COL1_ID>(col1Vector));
    auto *col2ptr = reinterpret_cast<sortKeyType *>(GetValuesFromVector<COL2_ID>(col2Vector));

    col1ptr += rowOffset;
    col2ptr += rowOffset;

    int col1Size = col1Vector->GetSize();
    int col2Size = col2Vector->GetSize();

    if (col1Size != col2Size) {
        std::string omniExceptionInfo = "col1Size != col2Size: " + std::to_string(col1Size) + " != " + std::to_string(col2Size);
        throw omniruntime::exception::OmniException("Error in MaxByAggregator::ProcessSingleInternal():", omniExceptionInfo);
    }

    for (int i = 0; i < col2Size; i++) {
        if (col2ptr[i] >= maxByState->sortKey) {
            maxByState->isEmpty = false;
            maxByState->sortKey = col2ptr[i];
            maxByState->targetValue = col1ptr[i];
        }
    }
}

template <DataTypeId COL1_ID, DataTypeId COL2_ID>
void MaxByAggregator<COL1_ID, COL2_ID>::ProcessGroupInternal(std::vector<AggregateState *> &rowStates, BaseVector *vector,
    const int32_t rowOffset, const std::shared_ptr<NullsHelper> nullMap)
{
    // input vector must be <targetValueType, sortKeyType>
    auto *col1Vector = this->curVectorBatch->Get(this->channels[0]);
    auto *col2Vector = this->curVectorBatch->Get(this->channels[1]);

    auto *col1ptr = reinterpret_cast<targetValueType *>(GetValuesFromVector<COL1_ID>(col1Vector));
    auto *col2ptr = reinterpret_cast<sortKeyType *>(GetValuesFromVector<COL2_ID>(col2Vector));

    col1ptr += rowOffset;
    col2ptr += rowOffset;

    const size_t rowCount = rowStates.size();
    if (rowCount != col1Vector->GetSize() || rowCount != col2Vector->GetSize()) {
        std::string omniExceptionInfo = "rowStates count must be equal to base vec size";
        throw omniruntime::exception::OmniException("Error in MaxByAggregator::ProcessGroupInternal():", omniExceptionInfo);
    }

    if (rowCount <= 0) {
        // empty table, no result
        return;
    }

    for (size_t i = 0; i < rowCount; i++) {
        auto *maxByState = MaxByState<targetValueType, sortKeyType>::CastState(rowStates[i] + aggStateOffset);
        if (col2ptr[i] >= maxByState->sortKey) {
            maxByState->isEmpty = false;
            maxByState->sortKey = col2ptr[i];
            maxByState->targetValue = col1ptr[i];
        }
    }
}

template <DataTypeId COL1_ID, DataTypeId COL2_ID>
void MaxByAggregator<COL1_ID, COL2_ID>::ProcessGroupUnspill(std::vector<UnspillRowInfo> &unspillRows, int32_t rowCount,
    int32_t &vectorIndex)
{
    auto targetValueVecIdx = vectorIndex++;
    auto sortKeyVecIdx = vectorIndex++;
    for (int32_t rowIdx = 0; rowIdx < rowCount; rowIdx++) {
        auto &row = unspillRows[rowIdx];
        auto batch = row.batch;
        auto index = row.rowIdx;
        auto targetValueVector = static_cast<targetValueTypeVec *>(batch->Get(targetValueVecIdx));
        if (!targetValueVector->IsNull(index)) {
            // take value from state
            auto targetValue = targetValueVector->GetValue(index);
            auto sortKeyVector = static_cast<sortKeyTypeVec *>(batch->Get(sortKeyVecIdx));

            // take key from state
            auto sortKey = sortKeyVector->GetValue(index);

            auto *state = MaxByState<targetValueType, sortKeyType>::CastState(row.state + aggStateOffset);

            // cunstruct state
            if (sortKey >= state->sortKey) {
                state->isEmpty = false;
                state->sortKey = sortKey;
                state->targetValue = targetValue;
            }
        }
    }
}


template <DataTypeId COL1_ID, DataTypeId COL2_ID>
void MaxByAggregator<COL1_ID, COL2_ID>::ProcessAlignAggSchema(VectorBatch *result, BaseVector *originVector,
    const std::shared_ptr<NullsHelper> nullMap, const bool aggFilter)
{
    std::string omniExceptionInfo = "currently not support skipping partial agg";
    throw omniruntime::exception::OmniException("Error in MaxByAggregator::ProcessAlignAggSchema:", omniExceptionInfo);
}

template <DataTypeId COL1_ID, DataTypeId COL2_ID>
template<typename T>
void MaxByAggregator<COL1_ID, COL2_ID>::ProcessAlignAggSchemaInternal(VectorBatch *result, BaseVector *originVector,
    const std::shared_ptr<NullsHelper> nullMap)
{
    std::string omniExceptionInfo = "currently not support skipping partial agg";
    throw omniruntime::exception::OmniException("Error in MaxByAggregator::ProcessAlignAggSchemaInternal:", omniExceptionInfo);
}

template <DataTypeId COL1_ID, DataTypeId COL2_ID>
MaxByAggregator<COL1_ID, COL2_ID>::MaxByAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes,
    std::vector<int32_t> &channels, const bool inputRaw, const bool outputPartial, const bool isOverflowAsNull)
    : TypedAggregator(OMNI_AGGREGATION_TYPE_MAX_BY, inputTypes, outputTypes, channels, inputRaw, outputPartial,
    isOverflowAsNull)
{}

// Explicit template instantiation
// Defining templated aggregators in header file consume a lot of memory during compilation
// since, compiler needs to generate each individual template instance wherever aggregator header is include
// to reduce time and memory usage during compilation moved templated aggregator implementation into .cpp files
// and used explicit template instantiation to generate template instances
template class MaxByAggregator<OMNI_BOOLEAN, OMNI_BOOLEAN>;

template class MaxByAggregator<OMNI_BOOLEAN, OMNI_SHORT>;

template class MaxByAggregator<OMNI_BOOLEAN, OMNI_INT>;

template class MaxByAggregator<OMNI_BOOLEAN, OMNI_LONG>;

template class MaxByAggregator<OMNI_BOOLEAN, OMNI_DOUBLE>;

template class MaxByAggregator<OMNI_BOOLEAN, OMNI_DECIMAL128>;

template class MaxByAggregator<OMNI_BOOLEAN, OMNI_DECIMAL64>;

template class MaxByAggregator<OMNI_SHORT, OMNI_BOOLEAN>;

template class MaxByAggregator<OMNI_SHORT, OMNI_SHORT>;

template class MaxByAggregator<OMNI_SHORT, OMNI_INT>;

template class MaxByAggregator<OMNI_SHORT, OMNI_LONG>;

template class MaxByAggregator<OMNI_SHORT, OMNI_DOUBLE>;

template class MaxByAggregator<OMNI_SHORT, OMNI_DECIMAL128>;

template class MaxByAggregator<OMNI_SHORT, OMNI_DECIMAL64>;

template class MaxByAggregator<OMNI_INT, OMNI_BOOLEAN>;

template class MaxByAggregator<OMNI_INT, OMNI_SHORT>;

template class MaxByAggregator<OMNI_INT, OMNI_INT>;

template class MaxByAggregator<OMNI_INT, OMNI_LONG>;

template class MaxByAggregator<OMNI_INT, OMNI_DOUBLE>;

template class MaxByAggregator<OMNI_INT, OMNI_DECIMAL128>;

template class MaxByAggregator<OMNI_INT, OMNI_DECIMAL64>;

template class MaxByAggregator<OMNI_LONG, OMNI_BOOLEAN>;

template class MaxByAggregator<OMNI_LONG, OMNI_SHORT>;

template class MaxByAggregator<OMNI_LONG, OMNI_INT>;

template class MaxByAggregator<OMNI_LONG, OMNI_LONG>;

template class MaxByAggregator<OMNI_LONG, OMNI_DOUBLE>;

template class MaxByAggregator<OMNI_LONG, OMNI_DECIMAL128>;

template class MaxByAggregator<OMNI_LONG, OMNI_DECIMAL64>;

template class MaxByAggregator<OMNI_DOUBLE, OMNI_BOOLEAN>;

template class MaxByAggregator<OMNI_DOUBLE, OMNI_SHORT>;

template class MaxByAggregator<OMNI_DOUBLE, OMNI_INT>;

template class MaxByAggregator<OMNI_DOUBLE, OMNI_LONG>;

template class MaxByAggregator<OMNI_DOUBLE, OMNI_DOUBLE>;

template class MaxByAggregator<OMNI_DOUBLE, OMNI_DECIMAL128>;

template class MaxByAggregator<OMNI_DOUBLE, OMNI_DECIMAL64>;

template class MaxByAggregator<OMNI_DECIMAL128, OMNI_BOOLEAN>;

template class MaxByAggregator<OMNI_DECIMAL128, OMNI_SHORT>;

template class MaxByAggregator<OMNI_DECIMAL128, OMNI_INT>;

template class MaxByAggregator<OMNI_DECIMAL128, OMNI_LONG>;

template class MaxByAggregator<OMNI_DECIMAL128, OMNI_DOUBLE>;

template class MaxByAggregator<OMNI_DECIMAL128, OMNI_DECIMAL128>;

template class MaxByAggregator<OMNI_DECIMAL128, OMNI_DECIMAL64>;

template class MaxByAggregator<OMNI_DECIMAL64, OMNI_BOOLEAN>;

template class MaxByAggregator<OMNI_DECIMAL64, OMNI_SHORT>;

template class MaxByAggregator<OMNI_DECIMAL64, OMNI_INT>;

template class MaxByAggregator<OMNI_DECIMAL64, OMNI_LONG>;

template class MaxByAggregator<OMNI_DECIMAL64, OMNI_DOUBLE>;

template class MaxByAggregator<OMNI_DECIMAL64, OMNI_DECIMAL128>;

template class MaxByAggregator<OMNI_DECIMAL64, OMNI_DECIMAL64>;

} // namespace op
} // namespace omniruntime