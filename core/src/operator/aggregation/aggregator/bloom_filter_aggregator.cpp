/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.
 * Description: BloomFilter aggregate
 */

#include "bloom_filter_aggregator.h"

namespace omniruntime {
namespace op {
/**
 * Invoked by GetOutput, it extracts the serialized data of BloomFilter from AggregateState and stores it into the specified BaseVector.
 *
 * @param state: Pointer to AggregateState from which the serialized data of BloomFilter is extracted.
 * @param vectors: A vector containing BaseVector pointers, used to store the extracted BloomFilter serialized data.
 * @param rowIndex: Row index in BaseVector, used to store the extracted BloomFilter serialized data.
 */
template <DataTypeId IN_ID, DataTypeId OUT_ID>
void BloomFilterAggregator<IN_ID, OUT_ID>::ExtractValues(const AggregateState *state, std::vector<BaseVector *> &vectors,
    int32_t rowIndex)
{
    auto largeStringContainer = static_cast<Vector<LargeStringContainer<std::string_view>> *>(vectors[0]);
    const auto *bloomFilterAggState = BloomFilterAggState::ConstCastState(state + aggStateOffset);

    // Get the length of the serialized string
    auto len = bloomFilterAggState->len;

    if (len == 0) {
        largeStringContainer->SetNull(rowIndex);
    } else {
        char *value = reinterpret_cast<char *>(bloomFilterAggState->serializePtr);
        std::string_view val(value, len);
        largeStringContainer->SetValue(rowIndex, val);
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void BloomFilterAggregator<IN_ID, OUT_ID>::ExtractValuesBatch(std::vector<AggregateState *> &groupStates,
    std::vector<BaseVector *> &vectors, int32_t rowOffset, int32_t rowCount)
{
    throw omniruntime::exception::OmniException("not implement", "BloomFilterAggregator not support ExtractValuesBatch");
}

template <DataTypeId IN_ID, DataTypeId OUT_ID> std::vector<DataTypePtr> BloomFilterAggregator<IN_ID, OUT_ID>::GetSpillType()
{
    throw omniruntime::exception::OmniException("not implement", "BloomFilterAggregator not support GetSpillType");
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void BloomFilterAggregator<IN_ID, OUT_ID>::ExtractValuesForSpill(std::vector<AggregateState *> &groupStates,
    std::vector<BaseVector *> &vectors)
{
    throw omniruntime::exception::OmniException("not implement", "BloomFilterAggregator not support ExtractValuesForSpill");
}

/**
 * Initialize a single AggregateState, create and serialize a BloomFilter object, and store the serialized data into the AggregateState.
 *
 * @param state: Pointer to AggregateState, used to save the serialized data of BloomFilter.
 */
template <DataTypeId IN_ID, DataTypeId OUT_ID> void BloomFilterAggregator<IN_ID, OUT_ID>::InitState(AggregateState *state)
{
    auto *bloomFilterAggState = BloomFilterAggState::CastState(state + aggStateOffset);

    // todo: The length is tentatively set at 100000, pending performance optimization
    int32_t size = 100000;
    int32_t version = 1;
    auto bloomFilter = std::make_shared<omniruntime::op::BloomFilter>(size, version);
    // Get the serialized length
    size_t serializedSize = bloomFilter->GetSerializedSize();
    char* serializeChar = new char[serializedSize];
    // Serializing bloomFilter
    bloomFilter->Serialize(serializeChar);
    // Update bloomFilterAggState
    int64_t serializeCharPtr = reinterpret_cast<int64_t>(serializeChar);
    bloomFilterAggState->serializePtr = serializeCharPtr;
    stateSerializeStringPtrs.push_back(serializeCharPtr);
    bloomFilterAggState->len = static_cast<int32_t>(serializedSize);
}

/**
 * Initialize a Multiple AggregateState.
 *
 * @param state: A vector containing multiple AggregateState pointers, used to save the serialized data of BloomFilter.
 */
template <DataTypeId IN_ID, DataTypeId OUT_ID>
void BloomFilterAggregator<IN_ID, OUT_ID>::InitStates(std::vector<AggregateState *> &groupStates)
{
    for (auto groupState : groupStates) {
        InitState(groupState);
    }
}

/**
 * Invoked by AddInput, it processes a single input vector and decides whether to execute the Partial phase or the Final phase of the BloomFilter based on IsInputRaw().
 *
 * @param state: Pointer to AggregateState, used to save the serialized data of BloomFilter.
 * @param vectors: A vector containing BaseVector pointers, used to store the extracted BloomFilter serialized data.
 * @param rowOffset: Start row offset in the input vector.
 * @param rowCount: Number of rows in the input vector.
 * @param nullMap: Shared pointer to NullsHelper, used to determine whether the values in the input vector are null.
 */
template <DataTypeId IN_ID, DataTypeId OUT_ID>
void BloomFilterAggregator<IN_ID, OUT_ID>::ProcessSingleInternal(AggregateState *state, BaseVector *vector,
    const int32_t rowOffset, const int32_t rowCount, const std::shared_ptr<NullsHelper> nullMap)
{
    if (IsInputRaw()) {
        if (vector->GetEncoding() == vec::OMNI_ENCODING_CONST) {
            auto constVector = static_cast<ConstVector<int64_t> *>(vector);
            if (constVector->IsNull(0)) {
                return;
            }
            int64_t value = constVector->GetConstValue();
            auto *bloomFilterAggState = BloomFilterAggState::CastState(state);
            char *serializeChar = reinterpret_cast<char *>(bloomFilterAggState->serializePtr);
            auto bloomFilter = std::make_shared<omniruntime::op::BloomFilter>(serializeChar);
            for (int32_t i = 0; i < rowCount; i++) {
                if (nullMap == nullptr || !(*nullMap)[i]) {
                    bloomFilter->PutLong(value);
                }
            }
            bloomFilter->Serialize(serializeChar);
            bloomFilterAggState->serializePtr = reinterpret_cast<int64_t>(serializeChar);
        } else {
            BloomFilterPartialOp(state, vector, rowCount, nullMap, (vector->GetEncoding() == vec::OMNI_DICTIONARY));
        }
    } else {
        if (vector->GetEncoding() == vec::OMNI_ENCODING_CONST) {
            auto constVector = static_cast<ConstVector<std::string_view> *>(vector);
            if (constVector->IsNull(0)) {
                return;
            }
            std::string_view value = constVector->GetConstValue();
            auto *bloomFilterAggState = BloomFilterAggState::CastState(state);
            char *serializeChar = reinterpret_cast<char *>(bloomFilterAggState->serializePtr);
            auto bloomFilter = std::make_shared<omniruntime::op::BloomFilter>(serializeChar);
            for (int32_t i = 0; i < rowCount; i++) {
                if (nullMap == nullptr || !(*nullMap)[i]) {
                    char *newSerializeChar = const_cast<char *>(value.data());
                    bloomFilter->Merge(newSerializeChar);
                }
            }
            bloomFilter->Serialize(serializeChar);
            bloomFilterAggState->serializePtr = reinterpret_cast<int64_t>(serializeChar);
        } else {
            BloomFilterFinalOp(state, vector, rowCount, nullMap);
        }
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void BloomFilterAggregator<IN_ID, OUT_ID>::ProcessGroupInternal(std::vector<AggregateState *> &rowStates, BaseVector *vector,
    const int32_t rowOffset, const std::shared_ptr<NullsHelper> nullMap)
{
    throw omniruntime::exception::OmniException("not implement", "BloomFilterAggregator not support ProcessGroupInternal");
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void BloomFilterAggregator<IN_ID, OUT_ID>::ProcessGroupUnspill(std::vector<UnspillRowInfo> &unspillRows, int32_t rowCount,
    int32_t &vectorIndex)
{
    throw omniruntime::exception::OmniException("not implement", "BloomFilterAggregator not support ProcessGroupUnspill");
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void BloomFilterAggregator<IN_ID, OUT_ID>::ProcessAlignAggSchema(VectorBatch *result, BaseVector *originVector,
    const std::shared_ptr<NullsHelper> nullMap, const bool aggFilter)
{
    throw omniruntime::exception::OmniException("not implement", "BloomFilterAggregator not support ProcessAlignAggSchema");
}

template<DataTypeId IN_ID, DataTypeId OUT_ID>
template<typename T>
void BloomFilterAggregator<IN_ID, OUT_ID>::ProcessAlignAggSchemaInternal(VectorBatch *result, BaseVector *originVector,
    const std::shared_ptr<NullsHelper> nullMap)
{
    throw omniruntime::exception::OmniException("not implement", "BloomFilterAggregator not support ProcessAlignAggSchemaInternal");
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
BloomFilterAggregator<IN_ID, OUT_ID>::BloomFilterAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes,
    std::vector<int32_t> &channels, const bool inputRaw, const bool outputPartial, const bool isOverflowAsNull)
    : TypedAggregator(OMNI_AGGREGATION_TYPE_BLOOM_FILTER, inputTypes, outputTypes, channels, inputRaw, outputPartial,
    isOverflowAsNull)
{}

// Explicit template instantiation
// Defining templated aggregators in header file consume a lot of memory during compilation
// since, compiler needs to generate each individual template instance wherever aggregator header is include
// to reduce time and memory usage during compilation moved templated aggregator implementation into .cpp files
// and used explicit template instantiation to generate template instances
template class BloomFilterAggregator<OMNI_CONTAINER, OMNI_VARBINARY>;
template class BloomFilterAggregator<OMNI_VARCHAR, OMNI_VARBINARY>;
template class BloomFilterAggregator<OMNI_DECIMAL64, OMNI_VARBINARY>;
template class BloomFilterAggregator<OMNI_DOUBLE, OMNI_VARBINARY>;
template class BloomFilterAggregator<OMNI_CHAR, OMNI_VARBINARY>;
template class BloomFilterAggregator<OMNI_DECIMAL128, OMNI_VARBINARY>;
template class BloomFilterAggregator<OMNI_INT, OMNI_VARBINARY>;
template class BloomFilterAggregator<OMNI_NONE, OMNI_VARBINARY>;
template class BloomFilterAggregator<OMNI_BYTE, OMNI_VARBINARY>;
template class BloomFilterAggregator<OMNI_SHORT, OMNI_VARBINARY>;
template class BloomFilterAggregator<OMNI_BOOLEAN, OMNI_VARBINARY>;
template class BloomFilterAggregator<OMNI_LONG, OMNI_VARBINARY>;
template class BloomFilterAggregator<OMNI_VARBINARY, OMNI_VARBINARY>;
}
}