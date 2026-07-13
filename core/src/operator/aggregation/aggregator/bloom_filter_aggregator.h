/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.
 * Description: BloomFilter aggregate
 */
#ifndef OMNI_RUNTIME_BLOOM_FILTER_AGGREGATOR_H
#define OMNI_RUNTIME_BLOOM_FILTER_AGGREGATOR_H

#include <cstdint>
#include <cfloat>

#include "typed_aggregator.h"
#include "codegen/bloom_filter.h"

namespace omniruntime {
namespace op {

#pragma pack(push, 1)
    struct BloomFilterAggState {
        // Pointer to the serialized data, save the serialized state of BloomFilter. todo pointer of BloomFilter
        int64_t serializePtr;
        // Length of serialized data
        int32_t len;

        static const BloomFilterAggState *ConstCastState(const AggregateState *state)
        {
            return reinterpret_cast<const BloomFilterAggState *>(state);
        }

        static BloomFilterAggState *CastState(AggregateState *state)
        {
            return reinterpret_cast<BloomFilterAggState *>(state);
        }
    };
#pragma pack(pop)

/**
 * In the Partial phase of BloomFilter, values are extracted from the input BaseVector and inserted into the BloomFilter.
   Finally, the state of the BloomFilter is serialized and updated into the AggregateState.
 *
 * @param state: Pointer to AggregateState, used to save the serialized data of BloomFilter.
 * @param vectors: A vector containing BaseVector pointers, used to store the extracted BloomFilter serialized data.
 * @param rowCount: Number of rows in the input vector.
 * @param nullMap: Shared pointer to NullsHelper, used to determine whether the values in the input vector are null.
 * @param isDictionary: Boolean value indication whether the input BaseVector is a dictionary-encoded BaseVector.
 */
VECTORIZE_LOOP inline void BloomFilterPartialOp(
        AggregateState *state, BaseVector *vector, const int32_t rowCount, const std::shared_ptr<NullsHelper> nullMap, bool isDictionary)
{
    if (rowCount == 0) {
        return;
    }

    // Retrieve values from BaseVector and convert them;the values in the Partial phase are of type int64_t.
    int64_t *valuePtr;
    if (isDictionary) {
        auto *dictVector = reinterpret_cast<Vector<DictionaryContainer<int64_t>> *>(vector);
        valuePtr = unsafe::UnsafeDictionaryVector::GetDictionary(dictVector);
    } else {
        auto rawVector = reinterpret_cast<Vector<int64_t> *>(vector);
        valuePtr = unsafe::UnsafeVector::GetRawValues<int64_t>(rawVector);
    }

    auto *bloomFilterAggState = BloomFilterAggState::CastState(state);
    // Build a BloomFilter utility class based on bloomFilterAggState.
    char *serializeChar = reinterpret_cast<char *>(bloomFilterAggState->serializePtr);
    auto bloomFilter = std::make_shared<omniruntime::op::BloomFilter>(serializeChar);

    for (int32_t i = 0; i < rowCount; i++) {
        // If nullMap is empty, or if there is a value marked in nullMap indicating that this position is empty, then iterate over bloomFilter.
        if (nullMap == nullptr || !(*nullMap)[i]) {
            auto value = valuePtr[i];
            bloomFilter->PutLong(reinterpret_cast<int64_t>(value));
        }
    }
    // Update the bit data in bloomFilter into serializePtr.
    bloomFilter->Serialize(serializeChar);
    // Update bloomFilterAggState
    bloomFilterAggState->serializePtr = reinterpret_cast<int64_t>(serializeChar);
}


/**
 * In the Final phase of BloomFilter, the serialized BloomFilter state is extracted from the input BaseVector,
   merged with the current BloomFilter state, and finally the merged BloomFilter state is serialized and updated into the AggregateState.
 *
 * @param state: Pointer to AggregateState, used to save the serialized data of BloomFilter.
 * @param vectors: A vector containing BaseVector pointers, used to store the extracted BloomFilter serialized data.
 * @param rowCount: Number of rows in the input vector.
 * @param nullMap: Shared pointer to NullsHelper, used to determine whether the values in the input vector are null.
 */
VECTORIZE_LOOP inline void BloomFilterFinalOp(
        AggregateState *state, BaseVector *vector, const int32_t rowCount, const std::shared_ptr<NullsHelper> nullMap)
{
    if (rowCount == 0) {
        return;
    }

    // Retrieve values from BaseVector and convert them;the values in the Partial phase are of type int64_t.
    auto largeStringVector = reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(vector);

    auto *bloomFilterAggState = BloomFilterAggState::CastState(state);
    // Build a BloomFilter utility class based on bloomFilterAggState.
    char *serializeChar = reinterpret_cast<char *>(bloomFilterAggState->serializePtr);
    auto bloomFilter = std::make_shared<omniruntime::op::BloomFilter>(serializeChar);

    for (int32_t i = 0; i < rowCount; i++) {
        // If nullMap is empty, or if there is a value marked in nullMap indicating that this position is empty, then iterate over bloomFilter.
        if (nullMap == nullptr || !(*nullMap)[i]) {
            auto value = largeStringVector->GetValue(i);
            char *newSerializeChar = const_cast<char *>(value.data());
            // Merge two bloom filters
            bloomFilter->Merge(newSerializeChar);
        }
    }
    // Update the bit data in bloomFilter into serializePtr.
    bloomFilter->Serialize(serializeChar);
    // Update bloomFilterAggState
    bloomFilterAggState->serializePtr = reinterpret_cast<int64_t>(serializeChar);
}

template <DataTypeId IN_ID, DataTypeId OUT_ID> class BloomFilterAggregator : public TypedAggregator {
    using InVector = typename AggNativeAndVectorType<IN_ID>::vector;
    using InType = typename AggNativeAndVectorType<IN_ID>::type;
    using OutVector = typename AggNativeAndVectorType<OUT_ID>::vector;
    using OutType = typename AggNativeAndVectorType<OUT_ID>::type;
    using ResultType = std::conditional_t<IN_ID == OMNI_VARBINARY, std::string_view, InType>;

public:
    // todo release BloomFilterAggStete serializePtr, now will coredump
    ~BloomFilterAggregator()
    {
        for (auto stringPtr : stateSerializeStringPtrs) {
            if (stringPtr == 0) {
                continue;
            }

            uintptr_t rawPtr = static_cast<uintptr_t>(stringPtr);
            char* data = reinterpret_cast<char *>(rawPtr);

            delete[] data;
        }

        stateSerializeStringPtrs.clear();
    }

    void ExtractValues(const AggregateState *state, std::vector<BaseVector *> &vectors, int32_t rowIndex) override;
    void ExtractValuesBatch(std::vector<AggregateState *> &groupStates, std::vector<BaseVector *> &vectors,
        int32_t rowOffset, int32_t rowCount) override;
    void ExtractValuesForSpill(std::vector<AggregateState *> &groupStates, std::vector<BaseVector *> &vectors) override;
    void InitState(AggregateState *state) override;
    void InitStates(std::vector<AggregateState *> &groupStates) override;
    std::vector<DataTypePtr> GetSpillType() override;

    size_t GetStateSize() override
    {
        return sizeof(BloomFilterAggState);
    }

    static std::unique_ptr<Aggregator> Create(const DataTypes &inputTypes, const DataTypes &outputTypes,
        std::vector<int32_t> &channels, bool rawIn, bool partialOut, bool isOverflowAsNull)
    {
        if constexpr (!(IN_ID == OMNI_LONG || IN_ID == OMNI_VARBINARY)) {
            LogError("Error in bloomFilter aggregator: Unsupported input type %s", TypeUtil::TypeToStringLog(IN_ID).c_str());
            return nullptr;
        } else if constexpr (!(OUT_ID == OMNI_VARBINARY)) {
            LogError("Error in bloomFilter aggregator: Unsupported output type %s", TypeUtil::TypeToStringLog(OUT_ID).c_str());
            return nullptr;
        } else {
            if (!TypedAggregator::CheckTypes("bloomFilter", inputTypes, outputTypes, IN_ID, OUT_ID)) {
                return nullptr;
            }

            return std::unique_ptr<BloomFilterAggregator<IN_ID, OUT_ID>>(new BloomFilterAggregator<IN_ID, OUT_ID>(inputTypes,
                outputTypes, channels, rawIn, partialOut, isOverflowAsNull));
        }
    }

    void ProcessGroupUnspill(std::vector<UnspillRowInfo> &unspillRows, int32_t rowCount, int32_t &vectorIndex) override;

    void ProcessAlignAggSchema(VectorBatch *result, BaseVector *originVector,
        const std::shared_ptr<NullsHelper> nullMap, const bool aggFilter) override;

protected:
    BloomFilterAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels,
        const bool inputRaw, const bool outputPartial, const bool isOverflowAsNull,
        int32_t bloomFilterVersion = BloomFilter::VERSION, FunctionType aggregationType = OMNI_AGGREGATION_TYPE_BLOOM_FILTER);

    void ProcessSingleInternal(AggregateState *state, BaseVector *vector, const int32_t rowOffset,
        const int32_t rowCount, const std::shared_ptr<NullsHelper> nullMap) override;

    void ProcessGroupInternal(std::vector<AggregateState *> &rowStates, BaseVector *vector, const int32_t rowOffset,
                              const std::shared_ptr<NullsHelper> nullMap) override;

    template <typename T>
    void ProcessAlignAggSchemaInternal(VectorBatch *result, BaseVector *originVector,
        const std::shared_ptr<NullsHelper> nullMap);

private:
    // store all ptr of char* from BloomFilterAggState when initState / initStates
    std::vector<int64_t> stateSerializeStringPtrs;
    int32_t bloomFilterVersion;
};
}
}
#endif // OMNI_RUNTIME_BLOOM_FILTER_AGGREGATOR_H
