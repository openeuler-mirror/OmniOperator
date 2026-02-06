/*
* Copyright (c) Huawei Technologies Co., Ltd. 2021-2024. All rights reserved.
 * Description: Min_by aggregate
 */

#include "minby_varchar_aggregator.h"
#include "simd/func/reduce.h"

namespace omniruntime {
namespace op {

template <DataTypeId COL1_ID, DataTypeId COL2_ID>
void MinByVarcharAggregator<COL1_ID, COL2_ID>::ExtractValues(const AggregateState *state, std::vector<BaseVector *> &vectors,
    int32_t rowIndex)
{
    auto *minByVarcharState = MinByVarcharState<targetValueType>::ConstCastState(state + aggStateOffset);

    if (this->outputPartial) {
        if (minByVarcharState->GetStrKeyAddress() == 0) {
            // no partial result
            return;
        }

        auto targetValueVector = static_cast<targetValueTypeVec *>(vectors[0]);
        auto sortKeyVector = static_cast<Vector<LargeStringContainer<std::string_view>> *>(vectors[1]);

        std::string_view val(reinterpret_cast<char *>(minByVarcharState->GetStrKeyAddress()), minByVarcharState->GetStrKeyLen());
        sortKeyVector->SetValue(rowIndex, val);

        targetValueVector->SetValue(rowIndex, minByVarcharState->targetValue);

        // release data after extracting
        minByVarcharState->ReleaseSortKey();

    } else {
        auto targetValueVector = static_cast<targetValueTypeVec *>(vectors[0]);
        if (minByVarcharState->GetStrKeyAddress() == 0) {
            // min_by empty table, final result is null
            targetValueVector->SetNull(rowIndex);
            return;
        }
        targetValueVector->SetValue(rowIndex, minByVarcharState->targetValue);

        // release data after extracting
        minByVarcharState->ReleaseSortKey();
    }
}

template <DataTypeId COL1_ID, DataTypeId COL2_ID>
void MinByVarcharAggregator<COL1_ID, COL2_ID>::ExtractValuesBatch(std::vector<AggregateState *> &groupStates,
    std::vector<BaseVector *> &vectors, int32_t rowOffset, int32_t rowCount)
{
    if (this->outputPartial) {
        auto targetValueVector = static_cast<targetValueTypeVec *>(vectors[0]);
        auto sortKeyVector = static_cast<Vector<LargeStringContainer<std::string_view>> *>(vectors[1]);

        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            auto *minByVarcharState = MinByVarcharState<targetValueType>::ConstCastState(groupStates[rowIndex] + aggStateOffset);
            if (minByVarcharState->GetStrKeyAddress() == 0) {
                // no partial result
                continue;
            }

            std::string_view val(reinterpret_cast<char *>(minByVarcharState->GetStrKeyAddress()), minByVarcharState->GetStrKeyLen());
            sortKeyVector->SetValue(rowIndex, val);
            targetValueVector->SetValue(rowIndex, minByVarcharState->targetValue);

            // release data after extracting
            minByVarcharState->ReleaseSortKey();
        }

    } else {
        // output final resule，only one col
        auto targetValueVector = static_cast<targetValueTypeVec *>(vectors[0]);
        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            auto *minByVarcharState = MinByVarcharState<targetValueType>::ConstCastState(groupStates[rowIndex] + aggStateOffset);
            if (minByVarcharState->GetStrKeyAddress() == 0) {
                // min_by empty table, final result is null
                targetValueVector->SetNull(rowIndex);
                continue;
            }
            targetValueVector->SetValue(rowIndex, minByVarcharState->targetValue);

            // release data after extracting
            minByVarcharState->ReleaseSortKey();
        }
    }
}

template <DataTypeId COL1_ID, DataTypeId COL2_ID> std::vector<DataTypePtr> MinByVarcharAggregator<COL1_ID, COL2_ID>::GetSpillType()
{
    std::vector<DataTypePtr> spillTypes;
    spillTypes.emplace_back(std::make_shared<DataType>(COL1_ID));
    spillTypes.emplace_back(std::make_shared<DataType>(OMNI_VARCHAR));
    return spillTypes;
}


template <DataTypeId COL1_ID, DataTypeId COL2_ID>
void MinByVarcharAggregator<COL1_ID, COL2_ID>::ExtractValuesForSpill(std::vector<AggregateState *> &groupStates,
    std::vector<BaseVector *> &vectors)
{
    auto targetValueVector = static_cast<targetValueTypeVec *>(vectors[0]);
    auto sortKeyVector = static_cast<Vector<LargeStringContainer<std::string_view>> *>(vectors[1]);

    auto rowCount = static_cast<int32_t>(groupStates.size());
    for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        auto *minByVarcharState = MinByVarcharState<targetValueType>::CastState(groupStates[rowIndex] + aggStateOffset);
        if (minByVarcharState->GetStrKeyAddress() == 0) {
            // dont need spill any states
            sortKeyVector->SetNull(rowIndex);
            targetValueVector->SetNull(rowIndex);
            continue;
        }
        targetValueVector->SetValue(rowIndex, minByVarcharState->targetValue);

        std::string_view val(reinterpret_cast<char *>(minByVarcharState->GetStrKeyAddress()), minByVarcharState->GetStrKeyLen());
        sortKeyVector->SetValue(rowIndex, val);
    }
}

template <DataTypeId COL1_ID, DataTypeId COL2_ID> void MinByVarcharAggregator<COL1_ID, COL2_ID>::InitState(AggregateState *state)
{
    auto *minByVarcharState = MinByVarcharState<targetValueType>::CastState(state + aggStateOffset);
    minByVarcharState->SetStrKey(0, 0);
}

template <DataTypeId COL1_ID, DataTypeId COL2_ID>
void MinByVarcharAggregator<COL1_ID, COL2_ID>::InitStates(std::vector<AggregateState *> &groupStates)
{
    for (auto *state : groupStates) {
        InitState(state);
    }
}

template <DataTypeId COL1_ID, DataTypeId COL2_ID>
void MinByVarcharAggregator<COL1_ID, COL2_ID>::ProcessSingleInternal(AggregateState *state, BaseVector *vector,
    const int32_t rowOffset, const int32_t rowCount, const std::shared_ptr<NullsHelper> nullMap)
{
    auto *minByVarcharState = MinByVarcharState<targetValueType>::CastState(state);

    // data vector
    auto *col1Vector = this->curVectorBatch->Get(this->channels[0]);
    auto *col2Vector = this->curVectorBatch->Get(this->channels[1]);

    auto *col1ptr = reinterpret_cast<targetValueType *>(GetValuesFromVector<COL1_ID>(col1Vector));
    auto *col2ptr = reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(col2Vector);

    for (int32_t i = rowOffset; i < rowCount; i++) {
        if (nullMap != nullptr && (*nullMap)[i]) {
            continue;
        }
        auto strView = col2ptr->GetValue(i);
        if (minByVarcharState->GetStrKeyAddress() == 0) {

            // set sort key (varchar)
            minByVarcharState->SetStrKey(reinterpret_cast<int64_t>(strView.data()), strView.size());

            // set target value
            minByVarcharState->targetValue = col1ptr[i];

        } else {
            const auto *curVal = strView.data();
            int32_t curLen = strView.size();
            auto result = memcmp((const char *)minByVarcharState->GetStrKeyAddress(), curVal, std::min(minByVarcharState->GetStrKeyLen(), curLen));

            if (result > 0 || (result == 0 && minByVarcharState->GetStrKeyLen() > curLen)) {
                // current key is smaller
                minByVarcharState->SetStrKey(reinterpret_cast<int64_t>(strView.data()), curLen);

                // set target value
                minByVarcharState->targetValue = col1ptr[i];

            }
        }
    }

    minByVarcharState->SaveSortKey();
}

template <DataTypeId COL1_ID, DataTypeId COL2_ID>
void MinByVarcharAggregator<COL1_ID, COL2_ID>::ProcessGroupInternal(std::vector<AggregateState *> &rowStates, BaseVector *vector,
    const int32_t rowOffset, const std::shared_ptr<NullsHelper> nullMap)
{
    // input vector must be <targetValueType, varchar>
    auto *col1Vector = this->curVectorBatch->Get(this->channels[0]);
    auto *col2Vector = this->curVectorBatch->Get(this->channels[1]);

    auto *col1ptr = reinterpret_cast<targetValueType *>(GetValuesFromVector<COL1_ID>(col1Vector));
    auto *col2ptr = reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(col2Vector);

    col1ptr += rowOffset;
    col2ptr += rowOffset;

    const size_t rowCount = rowStates.size();
    if (rowCount != col1Vector->GetSize() || rowCount != col2Vector->GetSize()) {
        std::string omniExceptionInfo = "rowStates count must be equal to base vec size";
        throw omniruntime::exception::OmniException("Error in MinByVarcharAggregator::ProcessGroupInternal(): ", omniExceptionInfo);
    }

    if (rowCount <= 0) {
        // empty table, no result
        return;
    }

    for (size_t i = 0; i < rowCount; i++) {
        if (nullMap != nullptr && (*nullMap)[i]) {
            continue;
        }
        auto *minByVarcharState = MinByVarcharState<targetValueType>::CastState(rowStates[i] + aggStateOffset);
        auto strView = col2ptr->GetValue(i);
        if (minByVarcharState->GetStrKeyAddress() == 0) {
            // set sort key (varchar)
            minByVarcharState->SetStrKey(reinterpret_cast<int64_t>(strView.data()), strView.size());

            // set target value
            minByVarcharState->targetValue = col1ptr[i];
        } else {
            const auto *curVal = strView.data();
            int32_t curLen = strView.size();
            auto result = memcmp((const char *)minByVarcharState->GetStrKeyAddress(), curVal, std::min(minByVarcharState->GetStrKeyLen(), curLen));

            if (result > 0 || (result == 0 && minByVarcharState->GetStrKeyLen() > curLen)) {
                // current key is smaller
                minByVarcharState->SetStrKey(reinterpret_cast<int64_t>(strView.data()), curLen);

                // set target value
                minByVarcharState->targetValue = col1ptr[i];
            }
        }
    }

    // copy the smallest key in case it will be released
    for (size_t i = 0; i < rowCount; i++) {
        auto *minByVarcharState = MinByVarcharState<targetValueType>::CastState(rowStates[i] + aggStateOffset);
        minByVarcharState->SaveSortKey();
    }
}

template <DataTypeId COL1_ID, DataTypeId COL2_ID>
void MinByVarcharAggregator<COL1_ID, COL2_ID>::ProcessGroupUnspill(std::vector<UnspillRowInfo> &unspillRows, int32_t rowCount,
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
            auto sortKeyVector = reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(batch->Get(sortKeyVecIdx));

            auto sortKey = sortKeyVector->GetValue(index);  //string_view

            auto *state = MinByVarcharState<targetValueType>::CastState(row.state + aggStateOffset);
            if (state->GetStrKeyAddress() == 0) {
                // set sort kay
                state->SetStrKey(reinterpret_cast<int64_t>(sortKey.data()), sortKey.size());
                // set target value
                state->targetValue = targetValue;
                // save stage
                state->SaveSortKey();
            } else {
                const auto *curVal = sortKey.data();
                int32_t curLen = sortKey.size();
                auto result = memcmp((const char *)state->GetStrKeyAddress(), curVal, std::min(state->GetStrKeyLen(), curLen));

                if (result > 0 || (result == 0 && state->GetStrKeyLen() > curLen)) {
                    // current key is smaller
                    // set sort kay
                    state->SetStrKey(reinterpret_cast<int64_t>(sortKey.data()), curLen);
                    // set target value
                    state->targetValue = targetValue;
                    // save stage
                    state->SaveSortKey();
                }
            }
        }
    }
}

template <DataTypeId COL1_ID, DataTypeId COL2_ID>
void MinByVarcharAggregator<COL1_ID, COL2_ID>::ProcessAlignAggSchema(VectorBatch *result, BaseVector *originVector,
    const std::shared_ptr<NullsHelper> nullMap, const bool aggFilter)
{
    std::string omniExceptionInfo = "currently not support skipping partial agg";
    throw omniruntime::exception::OmniException("Error in MinByVarcharAggregator::ProcessAlignAggSchema: ", omniExceptionInfo);
}

template <DataTypeId COL1_ID, DataTypeId COL2_ID>
template<typename T>
void MinByVarcharAggregator<COL1_ID, COL2_ID>::ProcessAlignAggSchemaInternal(VectorBatch *result, BaseVector *originVector,
    const std::shared_ptr<NullsHelper> nullMap)
{
    std::string omniExceptionInfo = "currently not support skipping partial agg";
    throw omniruntime::exception::OmniException("EError in MinByVarcharAggregator::ProcessAlignAggSchemaInternal: ", omniExceptionInfo);
}

template <DataTypeId COL1_ID, DataTypeId COL2_ID>
MinByVarcharAggregator<COL1_ID, COL2_ID>::MinByVarcharAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes,
    std::vector<int32_t> &channels, const bool inputRaw, const bool outputPartial, const bool isOverflowAsNull)
    : TypedAggregator(OMNI_AGGREGATION_TYPE_MIN_BY, inputTypes, outputTypes, channels, inputRaw, outputPartial,
    isOverflowAsNull)
{}

// Explicit template instantiation
// Defining templated aggregators in header file consume a lot of memory during compilation
// since, compiler needs to generate each individual template instance wherever aggregator header is include
// to reduce time and memory usage during compilation moved templated aggregator implementation into .cpp files
// and used explicit template instantiation to generate template instances
template class MinByVarcharAggregator<OMNI_BOOLEAN, OMNI_VARCHAR>;

template class MinByVarcharAggregator<OMNI_SHORT, OMNI_VARCHAR>;

template class MinByVarcharAggregator<OMNI_INT, OMNI_VARCHAR>;

template class MinByVarcharAggregator<OMNI_LONG, OMNI_VARCHAR>;

template class MinByVarcharAggregator<OMNI_DOUBLE, OMNI_VARCHAR>;

template class MinByVarcharAggregator<OMNI_FLOAT, OMNI_VARCHAR>;

template class MinByVarcharAggregator<OMNI_DECIMAL128, OMNI_VARCHAR>;

template class MinByVarcharAggregator<OMNI_DECIMAL64, OMNI_VARCHAR>;

template class MinByVarcharAggregator<OMNI_BOOLEAN, OMNI_CHAR>;

template class MinByVarcharAggregator<OMNI_SHORT, OMNI_CHAR>;

template class MinByVarcharAggregator<OMNI_INT, OMNI_CHAR>;

template class MinByVarcharAggregator<OMNI_LONG, OMNI_CHAR>;

template class MinByVarcharAggregator<OMNI_DOUBLE, OMNI_CHAR>;

template class MinByVarcharAggregator<OMNI_FLOAT, OMNI_CHAR>;

template class MinByVarcharAggregator<OMNI_DECIMAL128, OMNI_CHAR>;

template class MinByVarcharAggregator<OMNI_DECIMAL64, OMNI_CHAR>;

} // namespace op
} // namespace omniruntime