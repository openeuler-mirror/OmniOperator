/*
* Copyright (c) Huawei Technologies Co., Ltd. 2021-2024. All rights reserved.
 * Description: Max_by varchar aggregate
 */

#include "maxby_varchar_aggregator.h"
#include "simd/func/reduce.h"

namespace omniruntime {
namespace op {

template <DataTypeId COL1_ID, DataTypeId COL2_ID>
void MaxByVarcharAggregator<COL1_ID, COL2_ID>::ExtractValues(const AggregateState *state, std::vector<BaseVector *> &vectors,
    int32_t rowIndex)
{
    auto *maxByVarcharState = MaxByVarcharState<targetValueType>::ConstCastState(state + aggStateOffset);

    if (this->outputPartial) {
        if (maxByVarcharState->GetStrKeyAddress() == 0) {
            return;
        }
        auto targetValueVector = static_cast<targetValueTypeVec *>(vectors[0]);
        auto sortKeyVector = static_cast<Vector<LargeStringContainer<std::string_view>> *>(vectors[1]);
        std::string_view val(reinterpret_cast<char *>(maxByVarcharState->GetStrKeyAddress()), maxByVarcharState->GetStrKeyLen());
        sortKeyVector->SetValue(rowIndex, val);
        targetValueVector->SetValue(rowIndex, maxByVarcharState->targetValue);
        maxByVarcharState->ReleaseSortKey();
    } else {
        auto targetValueVector = static_cast<targetValueTypeVec *>(vectors[0]);
        if (maxByVarcharState->GetStrKeyAddress() == 0) {
            targetValueVector->SetNull(rowIndex);
            return;
        }
        targetValueVector->SetValue(rowIndex, maxByVarcharState->targetValue);
        maxByVarcharState->ReleaseSortKey();
    }
}

template <DataTypeId COL1_ID, DataTypeId COL2_ID>
void MaxByVarcharAggregator<COL1_ID, COL2_ID>::ExtractValuesBatch(std::vector<AggregateState *> &groupStates,
    std::vector<BaseVector *> &vectors, int32_t rowOffset, int32_t rowCount)
{
    if (this->outputPartial) {
        auto targetValueVector = static_cast<targetValueTypeVec *>(vectors[0]);
        auto sortKeyVector = static_cast<Vector<LargeStringContainer<std::string_view>> *>(vectors[1]);
        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            auto *maxByVarcharState = MaxByVarcharState<targetValueType>::ConstCastState(groupStates[rowIndex] + aggStateOffset);
            if (maxByVarcharState->GetStrKeyAddress() == 0) {
                continue;
            }
            std::string_view val(reinterpret_cast<char *>(maxByVarcharState->GetStrKeyAddress()), maxByVarcharState->GetStrKeyLen());
            sortKeyVector->SetValue(rowIndex, val);
            targetValueVector->SetValue(rowIndex, maxByVarcharState->targetValue);
            maxByVarcharState->ReleaseSortKey();
        }
    } else {
        auto targetValueVector = static_cast<targetValueTypeVec *>(vectors[0]);
        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            auto *maxByVarcharState = MaxByVarcharState<targetValueType>::ConstCastState(groupStates[rowIndex] + aggStateOffset);
            if (maxByVarcharState->GetStrKeyAddress() == 0) {
                targetValueVector->SetNull(rowIndex);
                continue;
            }
            targetValueVector->SetValue(rowIndex, maxByVarcharState->targetValue);
            maxByVarcharState->ReleaseSortKey();
        }
    }
}

template <DataTypeId COL1_ID, DataTypeId COL2_ID> std::vector<DataTypePtr> MaxByVarcharAggregator<COL1_ID, COL2_ID>::GetSpillType()
{
    std::vector<DataTypePtr> spillTypes;
    spillTypes.emplace_back(std::make_shared<DataType>(COL1_ID));
    spillTypes.emplace_back(std::make_shared<DataType>(OMNI_VARCHAR));
    return spillTypes;
}

template <DataTypeId COL1_ID, DataTypeId COL2_ID>
void MaxByVarcharAggregator<COL1_ID, COL2_ID>::ExtractValuesForSpill(std::vector<AggregateState *> &groupStates,
    std::vector<BaseVector *> &vectors)
{
    auto targetValueVector = static_cast<targetValueTypeVec *>(vectors[0]);
    auto sortKeyVector = static_cast<Vector<LargeStringContainer<std::string_view>> *>(vectors[1]);
    auto rowCount = static_cast<int32_t>(groupStates.size());
    for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        auto *maxByVarcharState = MaxByVarcharState<targetValueType>::CastState(groupStates[rowIndex] + aggStateOffset);
        if (maxByVarcharState->GetStrKeyAddress() == 0) {
            sortKeyVector->SetNull(rowIndex);
            targetValueVector->SetNull(rowIndex);
            continue;
        }
        targetValueVector->SetValue(rowIndex, maxByVarcharState->targetValue);
        std::string_view val(reinterpret_cast<char *>(maxByVarcharState->GetStrKeyAddress()), maxByVarcharState->GetStrKeyLen());
        sortKeyVector->SetValue(rowIndex, val);
    }
}

template <DataTypeId COL1_ID, DataTypeId COL2_ID> void MaxByVarcharAggregator<COL1_ID, COL2_ID>::InitState(AggregateState *state)
{
    auto *maxByVarcharState = MaxByVarcharState<targetValueType>::CastState(state + aggStateOffset);
    maxByVarcharState->SetStrKey(0, 0);
}

template <DataTypeId COL1_ID, DataTypeId COL2_ID>
void MaxByVarcharAggregator<COL1_ID, COL2_ID>::InitStates(std::vector<AggregateState *> &groupStates)
{
    for (auto *state : groupStates) {
        InitState(state);
    }
}

template <DataTypeId COL1_ID, DataTypeId COL2_ID>
void MaxByVarcharAggregator<COL1_ID, COL2_ID>::ProcessSingleInternal(AggregateState *state, BaseVector *vector,
    const int32_t rowOffset, const int32_t rowCount, const std::shared_ptr<NullsHelper> nullMap)
{
    auto *maxByVarcharState = MaxByVarcharState<targetValueType>::CastState(state);
    auto *col1Vector = this->curVectorBatch->Get(this->channels[0]);
    auto *col2Vector = this->curVectorBatch->Get(this->channels[1]);
    auto *col1ptr = reinterpret_cast<targetValueType *>(GetValuesFromVector<COL1_ID>(col1Vector));
    auto *col2ptr = reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(col2Vector);

    for (int32_t i = rowOffset; i < rowCount; i++) {
        if (nullMap != nullptr && (*nullMap)[i]) {
            continue;
        }
        auto strView = col2ptr->GetValue(i);
        if (maxByVarcharState->GetStrKeyAddress() == 0) {
            maxByVarcharState->SetStrKey(reinterpret_cast<int64_t>(strView.data()), strView.size());
            maxByVarcharState->targetValue = col1ptr[i];
        } else {
            const auto *curVal = strView.data();
            int32_t curLen = strView.size();
            auto result = memcmp((const char *)maxByVarcharState->GetStrKeyAddress(), curVal, std::min(maxByVarcharState->GetStrKeyLen(), curLen));
            if (result < 0 || (result == 0 && maxByVarcharState->GetStrKeyLen() < curLen)) {
                maxByVarcharState->SetStrKey(reinterpret_cast<int64_t>(strView.data()), curLen);
                maxByVarcharState->targetValue = col1ptr[i];
            }
        }
    }
    maxByVarcharState->SaveSortKey();
}

template <DataTypeId COL1_ID, DataTypeId COL2_ID>
void MaxByVarcharAggregator<COL1_ID, COL2_ID>::ProcessGroupInternal(std::vector<AggregateState *> &rowStates, BaseVector *vector,
    const int32_t rowOffset, const std::shared_ptr<NullsHelper> nullMap)
{
    auto *col1Vector = this->curVectorBatch->Get(this->channels[0]);
    auto *col2Vector = this->curVectorBatch->Get(this->channels[1]);
    auto *col1ptr = reinterpret_cast<targetValueType *>(GetValuesFromVector<COL1_ID>(col1Vector));
    auto *col2ptr = reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(col2Vector);
    col1ptr += rowOffset;
    col2ptr += rowOffset;

    const size_t rowCount = rowStates.size();
    if (rowCount != col1Vector->GetSize() || rowCount != col2Vector->GetSize()) {
        std::string omniExceptionInfo = "rowStates count must be equal to base vec size";
        throw omniruntime::exception::OmniException("Error in MaxByVarcharAggregator::ProcessGroupInternal(): ", omniExceptionInfo);
    }
    if (rowCount <= 0) {
        return;
    }

    for (size_t i = 0; i < rowCount; i++) {
        if (nullMap != nullptr && (*nullMap)[i]) {
            continue;
        }
        auto *maxByVarcharState = MaxByVarcharState<targetValueType>::CastState(rowStates[i] + aggStateOffset);
        auto strView = col2ptr->GetValue(i);
        if (maxByVarcharState->GetStrKeyAddress() == 0) {
            maxByVarcharState->SetStrKey(reinterpret_cast<int64_t>(strView.data()), strView.size());
            maxByVarcharState->targetValue = col1ptr[i];
        } else {
            const auto *curVal = strView.data();
            int32_t curLen = strView.size();
            auto result = memcmp((const char *)maxByVarcharState->GetStrKeyAddress(), curVal, std::min(maxByVarcharState->GetStrKeyLen(), curLen));
            if (result < 0 || (result == 0 && maxByVarcharState->GetStrKeyLen() < curLen)) {
                maxByVarcharState->SetStrKey(reinterpret_cast<int64_t>(strView.data()), curLen);
                maxByVarcharState->targetValue = col1ptr[i];
            }
        }
    }

    for (size_t i = 0; i < rowCount; i++) {
        auto *maxByVarcharState = MaxByVarcharState<targetValueType>::CastState(rowStates[i] + aggStateOffset);
        maxByVarcharState->SaveSortKey();
    }
}

template <DataTypeId COL1_ID, DataTypeId COL2_ID>
void MaxByVarcharAggregator<COL1_ID, COL2_ID>::ProcessGroupUnspill(std::vector<UnspillRowInfo> &unspillRows, int32_t rowCount,
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
            auto targetValue = targetValueVector->GetValue(index);
            auto sortKeyVector = reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(batch->Get(sortKeyVecIdx));
            auto sortKey = sortKeyVector->GetValue(index);
            auto *state = MaxByVarcharState<targetValueType>::CastState(row.state + aggStateOffset);
            if (state->GetStrKeyAddress() == 0) {
                state->SetStrKey(reinterpret_cast<int64_t>(sortKey.data()), sortKey.size());
                state->targetValue = targetValue;
                state->SaveSortKey();
            } else {
                const auto *curVal = sortKey.data();
                int32_t curLen = sortKey.size();
                auto result = memcmp((const char *)state->GetStrKeyAddress(), curVal, std::min(state->GetStrKeyLen(), curLen));
                if (result < 0 || (result == 0 && state->GetStrKeyLen() < curLen)) {
                    state->SetStrKey(reinterpret_cast<int64_t>(sortKey.data()), curLen);
                    state->targetValue = targetValue;
                    state->SaveSortKey();
                }
            }
        }
    }
}

template <DataTypeId COL1_ID, DataTypeId COL2_ID>
void MaxByVarcharAggregator<COL1_ID, COL2_ID>::ProcessAlignAggSchema(VectorBatch *result, BaseVector *originVector,
    const std::shared_ptr<NullsHelper> nullMap, const bool aggFilter)
{
    std::string omniExceptionInfo = "currently not support skipping partial agg";
    throw omniruntime::exception::OmniException("Error in MaxByVarcharAggregator::ProcessAlignAggSchema: ", omniExceptionInfo);
}

template <DataTypeId COL1_ID, DataTypeId COL2_ID>
template<typename T>
void MaxByVarcharAggregator<COL1_ID, COL2_ID>::ProcessAlignAggSchemaInternal(VectorBatch *result, BaseVector *originVector,
    const std::shared_ptr<NullsHelper> nullMap)
{
    std::string omniExceptionInfo = "currently not support skipping partial agg";
    throw omniruntime::exception::OmniException("Error in MaxByVarcharAggregator::ProcessAlignAggSchemaInternal: ", omniExceptionInfo);
}

template <DataTypeId COL1_ID, DataTypeId COL2_ID>
MaxByVarcharAggregator<COL1_ID, COL2_ID>::MaxByVarcharAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes,
    std::vector<int32_t> &channels, const bool inputRaw, const bool outputPartial, const bool isOverflowAsNull)
    : TypedAggregator(OMNI_AGGREGATION_TYPE_MAX_BY, inputTypes, outputTypes, channels, inputRaw, outputPartial,
    isOverflowAsNull)
{}

template class MaxByVarcharAggregator<OMNI_BOOLEAN, OMNI_VARCHAR>;
template class MaxByVarcharAggregator<OMNI_SHORT, OMNI_VARCHAR>;
template class MaxByVarcharAggregator<OMNI_INT, OMNI_VARCHAR>;
template class MaxByVarcharAggregator<OMNI_LONG, OMNI_VARCHAR>;
template class MaxByVarcharAggregator<OMNI_FLOAT, OMNI_VARCHAR>;
template class MaxByVarcharAggregator<OMNI_DOUBLE, OMNI_VARCHAR>;
template class MaxByVarcharAggregator<OMNI_DECIMAL128, OMNI_VARCHAR>;
template class MaxByVarcharAggregator<OMNI_DECIMAL64, OMNI_VARCHAR>;

template class MaxByVarcharAggregator<OMNI_BOOLEAN, OMNI_CHAR>;
template class MaxByVarcharAggregator<OMNI_SHORT, OMNI_CHAR>;
template class MaxByVarcharAggregator<OMNI_INT, OMNI_CHAR>;
template class MaxByVarcharAggregator<OMNI_LONG, OMNI_CHAR>;
template class MaxByVarcharAggregator<OMNI_FLOAT, OMNI_CHAR>;
template class MaxByVarcharAggregator<OMNI_DOUBLE, OMNI_CHAR>;
template class MaxByVarcharAggregator<OMNI_DECIMAL128, OMNI_CHAR>;
template class MaxByVarcharAggregator<OMNI_DECIMAL64, OMNI_CHAR>;

} // namespace op
} // namespace omniruntime
