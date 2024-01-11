/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2023. All rights reserved.
 * Description: Count aggregate
 */
#pragma once
#include "typed_aggregator.h"

namespace omniruntime {
namespace op {
// mask not null, agg vec not null
inline uint8_t AddMask(uint8_t *__restrict nullMap, const size_t length, const uint8_t *__restrict maskPtr)
{
    uint8_t nonZero = 0;
    for (size_t i = 0; i < length; ++i) {
        const auto v = maskPtr[i];
        nullMap[i] = !v;
        nonZero |= v;
    }
    return nonZero;
}

inline uint8_t AddDictMask(uint8_t *__restrict nullMap, const size_t length, const uint8_t *__restrict maskPtr,
    const int32_t *__restrict indexMap)
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
inline uint8_t AddMask(uint8_t *__restrict nullMap, const size_t length, const uint8_t *__restrict maskPtr,
    const uint8_t *__restrict maskNullMap)
{
    uint8_t nonZero = 0;
    for (size_t i = 0; i < length; ++i) {
        const auto v = !maskNullMap[i] && maskPtr[i];
        nullMap[i] = !v;
        nonZero |= v;
    }
    return nonZero;
}

inline uint8_t AddDictMask(uint8_t *__restrict nullMap, const size_t length, const uint8_t *__restrict maskPtr,
    const uint8_t *__restrict maskNullMap, const int32_t *__restrict indexMap)
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
inline uint8_t AddMask(uint8_t *__restrict nullMap, const size_t length, const uint8_t *__restrict maskPtr,
    const uint8_t *__restrict maskNullMap, const uint8_t *__restrict vecNullMap)
{
    uint8_t nonZero = 0;
    for (size_t i = 0; i < length; ++i) {
        const auto v = !maskNullMap[i] && !vecNullMap[i] && maskPtr[i];
        nullMap[i] = !v;
        nonZero |= v;
    }
    return nonZero;
}

inline uint8_t AddDictMask(uint8_t *__restrict nullMap, const size_t length, const uint8_t *__restrict maskPtr,
    const uint8_t *__restrict maskNullMap, const uint8_t *__restrict vecNullMap, const int32_t *__restrict indexMap)
{
    uint8_t nonZero = 0;
    for (size_t i = 0; i < length; ++i) {
        const auto v = !maskNullMap[i] && !vecNullMap[i] && maskPtr[indexMap[i]];
        nullMap[i] = !v;
        nonZero |= v;
    }
    return nonZero;
}

class TypedMaskColAggregator : public TypedAggregator {
public:
    ~TypedMaskColAggregator() override = default;

    void ProcessGroup(AggregateState &state, VectorBatch *vectorBatch, const int32_t rowOffset,
        const int32_t rowCount) override
    {
        const uint8_t *nullMap = GenerateNullMap(vectorBatch, rowOffset, rowCount);
        if (nullMap == nullptr) {
            return;
        }

        BaseVector *vector = nullptr;
        auto aggChannels = realAggregator->GetInputChannels();
        if (aggChannels.size() > 0 && aggChannels[0] >= 0) {
            vector = vectorBatch->Get(aggChannels[0]);
        }

        realAggregator->ProcessSingleInternal(state, vector, rowOffset, rowCount, nullMap);
    }

    void ProcessGroupFilter(AggregateState &state, VectorBatch *vectorBatch, const int32_t rowOffset,
        const int32_t filterIndex) override
    {
        int32_t rowCount = vectorBatch->GetRowCount();
        uint8_t *nullMap = GenerateNullMap(vectorBatch, rowOffset, rowCount);
        if (nullMap == nullptr) {
            return;
        }

        BaseVector *vector = nullptr;
        auto aggChannels = realAggregator->GetInputChannels();
        if (aggChannels.size() > 0 && aggChannels[0] >= 0) {
            vector = vectorBatch->Get(aggChannels[0]);
        }

        Vector<bool> *booleanVector = static_cast<Vector<bool> *>(vectorBatch->Get(filterIndex));

        bool needFilterJude = false;
        for (int32_t start = 0, end = rowCount - 1; start <= end; ++start, --end) {
            if (!booleanVector->GetValue(start) || !booleanVector->GetValue(end)) {
                needFilterJude = true;
                break;
            }
        }

        auto *filterPtr = unsafe::UnsafeVector::GetRawValues(booleanVector);
        filterPtr += rowOffset;
        if (needFilterJude) {
            // nullmapPtr can filter row which no need to aggregate
            // the nullMap: true means null
            // booleanVector: false means one row has been filtered
            auto *nullmapPtr = nullMap;

            for (int i = 0; i < rowCount; ++i) {
                nullmapPtr[i] |= not filterPtr[i];
            }
        }

        realAggregator->ProcessSingleInternal(state, vector, rowOffset, rowCount, nullMap);
    }

    void ProcessGroup(std::vector<AggregateState *> &rowStates, const size_t aggIdx, VectorBatch *vectorBatch,
        const int32_t rowOffset) override
    {
        const size_t rowCount = rowStates.size();
        const uint8_t *nullMap = GenerateNullMap(vectorBatch, rowOffset, rowCount);
        if (nullMap == nullptr) {
            return;
        }

        BaseVector *vector = nullptr;
        auto aggChannels = realAggregator->GetInputChannels();
        if (aggChannels.size() > 0 && aggChannels[0] >= 0) {
            vector = vectorBatch->Get(aggChannels[0]);
        }

        realAggregator->ProcessGroupInternal(rowStates, aggIdx, vector, rowOffset, nullMap);
    }

    void ProcessGroupFilter(std::vector<AggregateState *> &rowStates, const size_t aggIdx, VectorBatch *vectorBatch,
        const int32_t filterStart, const int32_t rowOffset) override
    {
        const size_t rowCount = rowStates.size();
        uint8_t *nullMap = GenerateNullMap(vectorBatch, rowOffset, rowCount);
        if (nullMap == nullptr) {
            return;
        }

        BaseVector *vector = nullptr;
        auto aggChannels = realAggregator->GetInputChannels();
        if (aggChannels.size() > 0 && aggChannels[0] >= 0) {
            vector = vectorBatch->Get(aggChannels[0]);
        }
        auto booleanVector = static_cast<Vector<bool> *>(vectorBatch->Get(filterStart + aggIdx));

        bool needFilterJude = false;
        for (int32_t start = 0, end = rowCount - 1; start <= end; ++start, --end) {
            if (!booleanVector->GetValue(start) || !booleanVector->GetValue(end)) {
                needFilterJude = true;
                break;
            }
        }

        auto *filterPtr = unsafe::UnsafeVector::GetRawValues(booleanVector);
        filterPtr += rowOffset;
        if (needFilterJude) {
            // nullMap can filter row which no need to aggregate
            // the nullMap: true means need filter
            // booleanVector: false means one row has been filtered
            auto *nullmapPtr = nullMap;

            for (int i = 0; i < rowCount; ++i) {
                nullmapPtr[i] |= not filterPtr[i];
            }
        }

        realAggregator->ProcessGroupInternal(rowStates, aggIdx, vector, rowOffset, nullMap);
    }

    void InitState(AggregateState &state) override
    {
        realAggregator->InitState(state);
    }

    void ExtractValues(const AggregateState &state, std::vector<BaseVector *> &vectors, int32_t rowIndex) override
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

    const DataTypes &GetInputTypes() const override
    {
        return realAggregator->GetInputTypes();
    }

    const DataTypes &GetOutputTypes() const override
    {
        return realAggregator->GetOutputTypes();
    }

    const std::vector<int32_t> &GetInputChannels() const override
    {
        return realAggregator->GetInputChannels();
    }

    static std::unique_ptr<Aggregator> Create(int32_t maskColumnId, std::unique_ptr<Aggregator> realAggregator)
    {
        return std::unique_ptr<TypedMaskColAggregator>(
            new TypedMaskColAggregator(maskColumnId, std::move(realAggregator)));
    }

protected:
    TypedMaskColAggregator(int32_t maskColumnId, std::unique_ptr<Aggregator> realAggregator)
        : TypedAggregator(realAggregator->GetType(), realAggregator->GetInputTypes(), realAggregator->GetOutputTypes(),
        realAggregator->GetInputChannels(), realAggregator->IsInputRaw(), realAggregator->IsOutputPartial(),
        realAggregator->IsOverflowAsNull()),
          maskColumnId(maskColumnId)
    {
        this->realAggregator =
            std::unique_ptr<TypedAggregator>(static_cast<TypedAggregator *>(realAggregator.release()));
    }

    void ProcessSingleInternal(AggregateState &state, BaseVector *vector, const int32_t rowOffset,
        const int32_t rowCount, const uint8_t *nullMap)
    {}

    void ProcessGroupInternal(std::vector<AggregateState *> &rowStates, const size_t aggIdx, BaseVector *vector,
        const int32_t rowOffset, const uint8_t *nullMap)
    {}

private:
    uint8_t *GenerateNullMap(VectorBatch *vectorBatch, const int32_t rowOffset, const size_t rowCount)
    {
        if (maskColumnId < 0 || maskColumnId >= vectorBatch->GetVectorCount()) {
            throw OmniException("Illegal Arguement", "Aggregator maskColumnId " + std::to_string(maskColumnId) +
                " out of range [0, " + std::to_string(vectorBatch->GetVectorCount()) + ") for masked aggregator");
        }
        auto maskVector = vectorBatch->Get(maskColumnId);
        uint8_t *maskNullMap = maskVector->HasNull() ?
            reinterpret_cast<uint8_t *>(unsafe::UnsafeBaseVector::GetNulls(maskVector)) :
            nullptr;
        if (maskNullMap != nullptr) {
            maskNullMap += rowOffset;
        }

        uint8_t *aggNullMap = nullptr;
        auto aggChannels = realAggregator->GetInputChannels();
        if (aggChannels.size() > 0) {
            auto aggColumnId = aggChannels[0];
            if (aggColumnId >= vectorBatch->GetVectorCount()) {
                throw OmniException("Illegal Arguement", "Aggregator columnId " + std::to_string(aggColumnId) +
                    " out of range [0, " + std::to_string(vectorBatch->GetVectorCount()) + ") for masked aggregator");
            } else if (aggColumnId >= 0) {
                auto aggVector = vectorBatch->Get(aggColumnId);
                aggNullMap = aggVector->HasNull() ?
                    reinterpret_cast<uint8_t *>(unsafe::UnsafeBaseVector::GetNulls(aggVector)) :
                    nullptr;
                if (aggNullMap != nullptr) {
                    aggNullMap += rowOffset;
                }
            }
        }

        uint8_t *nullMap = nullMapBuffer.AllocateReuse(rowCount, false);
        bool hasValidRows;
        if (maskVector->GetEncoding() == OMNI_DICTIONARY) {
            hasValidRows = GenerateNullMapDict(nullMap, rowOffset, rowCount, maskVector, maskNullMap, aggNullMap);
        } else {
            hasValidRows = GenerateNullMapFlat(nullMap, rowOffset, rowCount, maskVector, maskNullMap, aggNullMap);
        }

        return hasValidRows ? nullMap : nullptr;
    }

    bool GenerateNullMapDict(uint8_t *nullMap, const int32_t rowOffset, const size_t rowCount, BaseVector *maskVector,
        const uint8_t *maskNullMap, const uint8_t *aggNullMap)
    {
        uint8_t hasValidRows;
        const int32_t *indexMap = GetIdsFromDict<OMNI_BOOLEAN>(maskVector) + rowOffset;
        uint8_t *maskPtr = reinterpret_cast<uint8_t *>(GetValuesFromDict<OMNI_BOOLEAN>(maskVector));

        if (maskNullMap == nullptr) {
            if (aggNullMap == nullptr) {
                hasValidRows = AddDictMask(nullMap, rowCount, maskPtr, indexMap);
            } else {
                hasValidRows = AddDictMask(nullMap, rowCount, maskPtr, aggNullMap, indexMap);
            }
        } else {
            if (aggNullMap == nullptr) {
                hasValidRows = AddDictMask(nullMap, rowCount, maskPtr, maskNullMap, indexMap);
            } else {
                hasValidRows = AddDictMask(nullMap, rowCount, maskPtr, maskNullMap, aggNullMap, indexMap);
            }
        }

        return hasValidRows != 0;
    }

    bool GenerateNullMapFlat(uint8_t *nullMap, const int32_t rowOffset, const size_t rowCount, BaseVector *maskVector,
        const uint8_t *maskNullMap, const uint8_t *aggNullMap)
    {
        uint8_t hasValidRows;
        uint8_t *maskPtr = reinterpret_cast<uint8_t *>(GetValuesFromVector<OMNI_BOOLEAN>(maskVector));
        maskPtr += rowOffset;

        if (maskNullMap == nullptr) {
            if (aggNullMap == nullptr) {
                hasValidRows = AddMask(nullMap, rowCount, maskPtr);
            } else {
                hasValidRows = AddMask(nullMap, rowCount, maskPtr, aggNullMap);
            }
        } else {
            if (aggNullMap == nullptr) {
                hasValidRows = AddMask(nullMap, rowCount, maskPtr, maskNullMap);
            } else {
                hasValidRows = AddMask(nullMap, rowCount, maskPtr, maskNullMap, aggNullMap);
            }
        }

        return hasValidRows != 0;
    }

    int32_t maskColumnId;
    std::unique_ptr<TypedAggregator> realAggregator;
    // define nullmap buffer as member variable tor reduce number of memory allocaitons
    AlignedBuffer<uint8_t> nullMapBuffer;
};
}
}
