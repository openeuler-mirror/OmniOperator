/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.
 * Description: CollectSet aggregation (collect_set_aggregator header).
 */

#ifndef OMNI_RUNTIME_COLLECT_SET_AGGREGATOR_H
#define OMNI_RUNTIME_COLLECT_SET_AGGREGATOR_H

#include <fmt/format.h>
#include <vector>
#include "typed_aggregator.h"
#include "operator/hashmap/base_hash_map.h"

namespace omniruntime::op {
#pragma pack(push, 1)
    template <typename T>
    struct SetState {
        // store address for keep the state length to be fixed.
        int64_t uniqueValuesAddr;

        static const SetState<T> *ConstCastState(const AggregateState *state)
        {
            return reinterpret_cast<const SetState<T> *>(state);
        }

        static SetState<T> *CastState(AggregateState *state)
        {
            return reinterpret_cast<SetState<T> *>(state);
        }

        void UpdatePartialState(BaseVector *vector, const int32_t rowCount, const std::shared_ptr<NullsHelper> nullMap, const bool isDictionary)
        {
            T *valuePtr;
            if (isDictionary) {
                // todo: DictionaryContainer support complex type?
                auto *dictVector = reinterpret_cast<Vector<DictionaryContainer<T>> *>(vector);
                valuePtr = unsafe::UnsafeDictionaryVector::GetDictionary(dictVector);
            } else {
                auto flatVector = reinterpret_cast<Vector<T> *>(vector);
                valuePtr = unsafe::UnsafeVector::GetRawValues<T>(flatVector);
            }

            DefaultHashMap<T, int8_t>* uniqueValues = reinterpret_cast<DefaultHashMap<T, int8_t> *>(uniqueValuesAddr);
            for (uint32_t i = 0; i < rowCount; i++) {
                // nullMap: true means null (per framework). Only add when no nullMap or row is not null.
                if (nullMap == nullptr || !(*nullMap)[i]) {
                    uniqueValues->Emplace(valuePtr[i]);
                }
            }
        }

        void UpdateFinalState(ArrayVector *arrayVector, const int32_t rowCount, const std::shared_ptr<NullsHelper> nullMap, const bool isDictionary)
        {
            DefaultHashMap<T, int8_t>* uniqueValues = reinterpret_cast<DefaultHashMap<T, int8_t> *>(uniqueValuesAddr);

            for (uint32_t i = 0; i < rowCount; i++) {
                // nullMap: true means null. Only merge when no nullMap or row is not null.
                if (nullMap == nullptr || !(*nullMap)[i]) {
                    auto elementVector = arrayVector->GetArrayAt(i, false);
                    auto elementSize = elementVector->GetSize();

                    T *valuePtr;
                    if (isDictionary) {
                        auto *dictVector = reinterpret_cast<Vector<DictionaryContainer<T>> *>(elementVector.get());
                        valuePtr = unsafe::UnsafeDictionaryVector::GetDictionary(dictVector);
                    } else {
                        auto flatVector = reinterpret_cast<Vector<T> *>(elementVector.get());
                        valuePtr = unsafe::UnsafeVector::GetRawValues<T>(flatVector);
                    }

                    // merge all ElementVectors from input ArrayVectors
                    for (uint32_t j = 0; j < elementSize; j++) {
                        uniqueValues->Emplace(valuePtr[j]);
                    }
                }
            }
        }

        /**
         * Updates the set state with a single value when the filter condition is satisfied.
         * Used by the aggregation framework when an aggregate filter is present (e.g. FILTER (WHERE cond)).
         * @param state Aggregator state (caller passes state + aggStateOffset).
         * @param value Input value to add to the set.
         * @param condition Filter flag: only when condition == addIf is the value added.
         */
        template <bool addIf>
        static void UpdateStateWithCondition(AggregateState *state, const T &value, const uint8_t &condition)
        {
            if (condition == addIf) {
                SetState<T> *setState = SetState<T>::CastState(state);
                DefaultHashMap<T, int8_t> *uniqueValues =
                    reinterpret_cast<DefaultHashMap<T, int8_t> *>(setState->uniqueValuesAddr);
                uniqueValues->Emplace(value);
            }
        }
    };
#pragma pack(pop)

template<DataTypeId IN_ID, DataTypeId OUT_ID>
class CollectSetAggregator : public TypedAggregator {
    using InVector = typename AggNativeAndVectorType<IN_ID>::vector;
    using OutVector = typename AggNativeAndVectorType<OUT_ID>::vector;
    using InType = typename AggNativeAndVectorType<IN_ID>::type;
    using OutType = typename AggNativeAndVectorType<OUT_ID>::type;

public:
    CollectSetAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes, const std::vector<int32_t> &channels, const bool inputRaw, const bool outputPartial, const bool isOverflowAsNull);
    ~CollectSetAggregator() override;
    void ExtractValues(const AggregateState *state, std::vector<BaseVector *> &vectors, int32_t rowIndex) override;
    void ExtractValuesBatch(std::vector<AggregateState *> &groupStates, std::vector<BaseVector *> &vectors, int32_t rowOffset, int32_t rowCount) override;
    void ExtractValuesForSpill(std::vector<AggregateState *> &groupStates, std::vector<BaseVector *> &vectors) override;
    void InitState(AggregateState *state) override;
    void InitStates(std::vector<AggregateState *> &groupStates) override;
    // Frees the DefaultHashMap in state. Call before freeing state buffer to avoid leak.
    void DestroyState(AggregateState *state);
    std::vector<DataTypePtr> GetSpillType() override;

    size_t GetStateSize() override
    {
        return sizeof(SetState<InType>);
    }

    // Factory always creates CollectSetAggregator<T, T>: partial input T -> T, final input Array<T> -> element T.
    // Supports: basic types (BOOLEAN, SHORT, INT, LONG, FLOAT, DOUBLE, DECIMAL64, DECIMAL128), string/binary (CHAR, VARCHAR, VARBINARY), nested array (OMNI_ARRAY).
    static std::unique_ptr<Aggregator> Create(const DataTypes &inputTypes, const DataTypes &outputTypes,
        std::vector<int32_t> &channels, bool rawIn, bool partialOut, bool isOverflowAsNull)
    {
        if constexpr (IN_ID != OUT_ID) {
            LogError("CollectSetAggregator expects IN_ID == OUT_ID (element type T), got IN_ID=%s OUT_ID=%s",
                TypeUtil::TypeToStringLog(IN_ID).c_str(), TypeUtil::TypeToStringLog(OUT_ID).c_str());
            return nullptr;
        }
        // Basic types
        if constexpr (IN_ID == OMNI_BOOLEAN || IN_ID == OMNI_SHORT || IN_ID == OMNI_INT || IN_ID == OMNI_LONG ||
            IN_ID == OMNI_FLOAT || IN_ID == OMNI_DOUBLE || IN_ID == OMNI_DECIMAL64) {
            LogInfo("Create CollectSetAggregator<%s, %s>.", TypeUtil::TypeToStringLog(IN_ID).c_str(),
                TypeUtil::TypeToStringLog(OUT_ID).c_str());
            return std::make_unique<CollectSetAggregator<IN_ID, OUT_ID>>(inputTypes, outputTypes, channels, rawIn, partialOut, isOverflowAsNull);
        }
        LogError("CollectSetAggregator::Create: Unsupported input type %s", TypeUtil::TypeToStringLog(IN_ID).c_str());
        return nullptr;
    }

    void ProcessGroupUnspill(std::vector<UnspillRowInfo> &unspillRows, int32_t rowCount, int32_t &vectorIndex) override;

    void ProcessAlignAggSchema(VectorBatch *result, BaseVector *originVector, const std::shared_ptr<NullsHelper> nullMap, const bool aggFilter) override;

protected:
    /** Addresses of DefaultHashMap allocated in InitState; freed in DestroyState or in destructor. */
    std::vector<int64_t> allocatedUniqueValuesAddrs_;

    void ProcessSingleInternal(AggregateState *state, BaseVector *vector, const int32_t rowOffset, const int32_t rowCount, const std::shared_ptr<NullsHelper> nullMap) override;

    void ProcessGroupInternal(std::vector<AggregateState *> &rowStates, BaseVector *vector, const int32_t rowOffset, const std::shared_ptr<NullsHelper> nullMap) override;

    template<typename T>
    void ProcessAlignAggSchemaInteranal(VectorBatch *result, BaseVector *originVector, const std::shared_ptr<NullsHelper> nullMap);
};

}// omniruntime::op

#endif //OMNI_RUNTIME_COLLECT_SET_AGGREGATOR_H

