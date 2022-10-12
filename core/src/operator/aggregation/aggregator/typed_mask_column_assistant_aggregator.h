#pragma once

/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Description: Count aggregate
 */

#include "typed_aggregator.h"

namespace omniruntime {
namespace op {
// mask not null, agg vec not null
inline uint8_t addMask(uint8_t * __restrict nullMap, const size_t length, const uint8_t * __restrict maskPtr)
{
    uint8_t nonZero = 0;
    for (size_t i = 0; i < length; ++i) {
        const auto v = maskPtr[i];
        nullMap[i] = !v;
        nonZero |= v;
    }
    return nonZero;
}

inline uint8_t addDictMask(uint8_t * __restrict nullMap, const size_t length,
    const uint8_t * __restrict maskPtr, const int32_t * __restrict indexMap)
{
    uint8_t nonZero = 0;
    for (size_t i = 0; i < length; ++i) {
        const auto v = maskPtr[indexMap[i]];
        nullMap[i] = !v;
        nonZero |= v;
    }
    return nonZero;
}

// mask nullable, agg vec not null OR
// mask not null, agg vec nullable: in this case maskNullMap is actually agg vec nullMap
inline uint8_t addMask(uint8_t * __restrict nullMap, const size_t length,
    const uint8_t * __restrict maskPtr, const uint8_t * __restrict maskNullMap)
{
    uint8_t nonZero = 0;
    for (size_t i = 0; i < length; ++i) {
        const auto v = !maskNullMap[i] && maskPtr[i];
        nullMap[i] = !v;
        nonZero |= v;
    }
    return nonZero;
}

inline uint8_t addDictMask(uint8_t * __restrict nullMap, const size_t length,
    const uint8_t * __restrict maskPtr, const uint8_t * __restrict maskNullMap, const int32_t * __restrict indexMap)
{
    uint8_t nonZero = 0;
    for (size_t i = 0; i < length; ++i) {
        const auto v = !maskNullMap[i] && maskPtr[indexMap[i]];
        nullMap[i] = !v;
        nonZero |= v;
    }
    return nonZero;
}

// mask nullable, agg vec nullable
inline uint8_t addMask(uint8_t * __restrict nullMap, const size_t length,
    const uint8_t * __restrict maskPtr, const uint8_t * __restrict maskNullMap, const uint8_t * __restrict vecNullMap)
{
    uint8_t nonZero = 0;
    for (size_t i = 0; i < length; ++i) {
        const auto v = !maskNullMap[i] && !vecNullMap[i] && maskPtr[i];
        nullMap[i] = !v;
        nonZero |= v;
    }
    return nonZero;
}

inline uint8_t addDictMask(uint8_t * __restrict nullMap, const size_t length,
    const uint8_t * __restrict maskPtr, const uint8_t * __restrict maskNullMap, const uint8_t * __restrict vecNullMap,
    const int32_t * __restrict indexMap)
{
    uint8_t nonZero = 0;
    for (size_t i = 0; i < length; ++i) {
        const auto v = !maskNullMap[i] && !vecNullMap[i] && maskPtr[indexMap[i]];
        nullMap[i] = !v;
        nonZero |= v;
    }
    return nonZero;
}

template <bool RAW_IN, bool PARTIAL_OUT, bool NULL_OVERFLOW>
class TypedMaskColAggregator : public TypedAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW> {
public:
    TypedMaskColAggregator(int32_t maskColumnId, std::unique_ptr<Aggregator> realAggregator)
        : TypedAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW>(
            realAggregator->GetType(), realAggregator->GetInputTypes(), realAggregator->GetOutputTypes(),
            realAggregator->GetInputChannels()),
        maskColumnId(maskColumnId)
    {
        this->realAggregator = std::unique_ptr<TypedAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW>>(
            static_cast<TypedAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW> *>(realAggregator.release()));
    }

    ~TypedMaskColAggregator() override = default;

#ifdef ENABLE_HMPP
    void ProcessGroupWithHMPP(AggregateState &state, VectorBatch *vectorBatch) override
    {
        this->ProcessGroup(state, vectorBatch, 0, vectorBatch->GetRowCount());
    }

    bool CanProcessWithHMPP(AggregateState &state, VectorBatch *vectorBatch) override
    {
        return realAggregator->CanProcessWithHMPP(state, vectorBatch);
    }
#endif

    void ProcessGroup(AggregateState &state, VectorBatch *vectorBatch,
        const int32_t rowOffset, const int32_t rowCount) override
    {
        AggregatorBuffer<uint8_t> nullMap;
        GenerateNullMap(nullMap, vectorBatch, rowOffset, rowCount);
        if (nullMap.data == nullptr) {
            return;
        }

        AggregatorBuffer<int32_t> indexMap;
        Vector *vector = nullptr;
        auto aggChannels = realAggregator->GetInputChannels();
        if (aggChannels.size() > 0 && aggChannels[0] >= 0) {
            vector = vectorBatch->GetVector(aggChannels[0]);
            if (vector->GetEncoding() == OMNI_VEC_ENCODING_DICTIONARY) {
                indexMap.Create(this->allocator, rowCount, false);
                vector = static_cast<DictionaryVector *>(vector)->ExtractDictionaryAndIds(
                    rowOffset, rowCount, indexMap.data);
            }
        }

        if constexpr (RAW_IN) {
            realAggregator->ProcessRawInput(state, vector, rowOffset, rowCount, nullMap.data, indexMap.data);
        } else {
            realAggregator->ProcessPartialInput(state, vector, rowOffset, rowCount, nullMap.data, indexMap.data);
        }
    }

    void ProcessGroup(std::vector<AggregateState *> &rowStates, const size_t aggIdx,
        VectorBatch *vectorBatch, const int32_t rowOffset) override
    {
        const size_t rowCount = rowStates.size();
        AggregatorBuffer<uint8_t> nullMap;
        GenerateNullMap(nullMap, vectorBatch, rowOffset, rowCount);
        if (nullMap.data == nullptr) {
            return;
        }

        AggregatorBuffer<int32_t> indexMap;
        Vector *vector = nullptr;
        auto aggChannels = realAggregator->GetInputChannels();
        if (aggChannels.size() > 0 && aggChannels[0] >= 0) {
            vector = vectorBatch->GetVector(aggChannels[0]);
            if (vector->GetEncoding() == OMNI_VEC_ENCODING_DICTIONARY) {
                indexMap.Create(this->allocator, rowCount, false);
                vector = static_cast<DictionaryVector *>(vector)->ExtractDictionaryAndIds(
                    rowOffset, rowCount, indexMap.data);
            }
        }

        // ProcessGroupUseIndex
        if constexpr (RAW_IN) {
            realAggregator->ProcessGroupRawInput(rowStates, aggIdx, vector, rowOffset, nullMap.data, indexMap.data);
        } else {
            realAggregator->ProcessGroupPartialInput(rowStates, aggIdx, vector, rowOffset, nullMap.data, indexMap.data);
        }
    }

    void InitState(AggregateState &state) override
    {
        realAggregator->InitState(state);
    }

    void ExtractValues(const AggregateState &state, std::vector<Vector *> &vectors, int32_t rowIndex) override
    {
        realAggregator->ExtractValues(state, vectors, rowIndex);
    }

    bool IsInputRaw() const override
    {
        return realAggregator->IsInputRaw();
    }

    bool IsOutputPartial() const override
    {
        return realAggregator->IsOutputPartial();
    }

    bool IsOverflowAsNull() const override
    {
        return realAggregator->IsOverflowAsNull();
    }

    FunctionType GetType() const override
    {
        return realAggregator->GetType();
    }

    const DataTypesPtr &GetInputTypes() const override
    {
        return realAggregator->GetInputTypes();
    }

    const DataTypesPtr &GetOutputTypes() const override
    {
        return realAggregator->GetOutputTypes();
    }

    const std::vector<int32_t> &GetInputChannels() const override
    {
        return realAggregator->GetInputChannels();
    }

protected:
    ALWAYS_INLINE void ProcessRawInput(
        AggregateState &state, Vector *vector, const int32_t rowOffset, const int32_t rowCount,
        const uint8_t *nullMap, const int32_t *indexMap)
    {}

    ALWAYS_INLINE void ProcessGroupRawInput(
        std::vector<AggregateState *> &rowStates, const size_t aggIdx, Vector *vector,
        const int32_t rowOffset, const uint8_t *nullMap, const int32_t *indexMap)
    {}

private:
    void GenerateNullMap(
        AggregatorBuffer<uint8_t> &nullMap, VectorBatch *vectorBatch, const int32_t rowOffset, const size_t rowCount)
    {
        if (maskColumnId < 0 || maskColumnId >= vectorBatch->GetVectorCount()) {
            throw OmniException("Illegal Arguement",
                "Aggregator maskColumnId " + std::to_string(maskColumnId) + " out of range [0, "
                    + std::to_string(vectorBatch->GetVectorCount()) + ") for masked aggregator");
        }
        auto maskVector = vectorBatch->GetVector(maskColumnId);
        uint8_t *maskNullMap = maskVector->MayHaveNull()
                ? reinterpret_cast<uint8_t *>(maskVector->GetValueNulls())
                : nullptr;
        if (maskNullMap != nullptr) {
            maskNullMap += maskVector->GetPositionOffset() + rowOffset;
        }

        uint8_t *aggNullMap = nullptr;
        auto aggChannels = realAggregator->GetInputChannels();
        if (aggChannels.size() > 0) {
            auto aggColumnId = aggChannels[0];
            if (aggColumnId >= vectorBatch->GetVectorCount()) {
                throw OmniException("Illegal Arguement",
                    "Aggregator columnId " + std::to_string(aggColumnId) + " out of range [0, "
                        + std::to_string(vectorBatch->GetVectorCount()) + ") for masked aggregator");
            } else if (aggColumnId >= 0) {
                auto aggVector = vectorBatch->GetVector(aggColumnId);
                aggNullMap = aggVector->MayHaveNull()
                        ? reinterpret_cast<uint8_t *>(aggVector->GetValueNulls())
                        : nullptr;
                if (aggNullMap != nullptr) {
                    aggNullMap += aggVector->GetPositionOffset() + rowOffset;
                }
            }
        }

        nullMap.Create(this->allocator, rowCount, false);

        bool hasValidRows;
        if (maskVector->GetEncoding() == OMNI_VEC_ENCODING_DICTIONARY) {
            hasValidRows = GenerateNullMapDict(nullMap, rowOffset, rowCount, maskVector, maskNullMap, aggNullMap);
        } else {
            hasValidRows = GenerateNullMapFlat(nullMap, rowOffset, rowCount, maskVector, maskNullMap, aggNullMap);
        }

        if (!hasValidRows) {
            nullMap.Release();
        }
    }

    bool GenerateNullMapDict(
        AggregatorBuffer<uint8_t> &nullMap, const int32_t rowOffset, const size_t rowCount,
        Vector *maskVector, const uint8_t *maskNullMap, const uint8_t *aggNullMap)
    {
        uint8_t hasValidRows;
        AggregatorBuffer<int32_t> indexMap(this->allocator, rowCount, false);
        BooleanVector *orgVector = static_cast<BooleanVector *>(
            static_cast<DictionaryVector *>(maskVector)->ExtractDictionaryAndIds(rowOffset, rowCount, indexMap.data)
        );
        uint8_t *maskPtr = reinterpret_cast<uint8_t *>(orgVector->GetValues());
        maskPtr += orgVector->GetPositionOffset();

        if (maskNullMap == nullptr) {
            if (aggNullMap == nullptr) {
                hasValidRows = addDictMask(nullMap.data, rowCount, maskPtr, indexMap.data);
            } else {
                hasValidRows = addDictMask(nullMap.data, rowCount, maskPtr, aggNullMap, indexMap.data);
            }
        } else {
            if (aggNullMap == nullptr) {
                hasValidRows = addDictMask(nullMap.data, rowCount, maskPtr, maskNullMap, indexMap.data);
            } else {
                hasValidRows = addDictMask(nullMap.data, rowCount, maskPtr, maskNullMap, aggNullMap, indexMap.data);
            }
        }

        return hasValidRows != 0;
    }

    bool GenerateNullMapFlat(
        AggregatorBuffer<uint8_t> &nullMap, const int32_t rowOffset, const size_t rowCount,
        Vector *maskVector, const uint8_t *maskNullMap, const uint8_t *aggNullMap)
    {
        uint8_t hasValidRows;
        uint8_t *maskPtr = reinterpret_cast<uint8_t *>(static_cast<BooleanVector *>(maskVector)->GetValues());
        maskPtr += maskVector->GetPositionOffset() + rowOffset;

        if (maskNullMap == nullptr) {
            if (aggNullMap == nullptr) {
                hasValidRows = addMask(nullMap.data, rowCount, maskPtr);
            } else {
                hasValidRows = addMask(nullMap.data, rowCount, maskPtr, aggNullMap);
            }
        } else {
            if (aggNullMap == nullptr) {
                hasValidRows = addMask(nullMap.data, rowCount, maskPtr, maskNullMap);
            } else {
                hasValidRows = addMask(nullMap.data, rowCount, maskPtr, maskNullMap, aggNullMap);
            }
        }

        return hasValidRows != 0;
    }

    int32_t maskColumnId;
    std::unique_ptr<TypedAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW>> realAggregator;
};
}
}
