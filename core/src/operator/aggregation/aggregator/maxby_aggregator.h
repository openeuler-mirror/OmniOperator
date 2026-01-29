/*
* Copyright (c) Huawei Technologies Co., Ltd. 2021-2024. All rights reserved.
 * Description: MAX_BY aggregate
 */
#ifndef OMNI_RUNTIME_MAXBY_AGGREGATOR_H
#define OMNI_RUNTIME_MAXBY_AGGREGATOR_H

#include <cstdint>
#include <cfloat>
#include "typed_aggregator.h"

namespace omniruntime {
namespace op {
template <typename T> T GetSortKeyMin()
{
    if constexpr (std::is_same_v<T, int8_t>) {
        return std::numeric_limits<int8_t>::min();
    } else if constexpr (std::is_same_v<T, int16_t>) {
        return std::numeric_limits<int16_t>::min();
    } else if constexpr (std::is_same_v<T, int32_t>) {
        return std::numeric_limits<int32_t>::min();
    } else if constexpr (std::is_same_v<T, int64_t>) {
        return std::numeric_limits<int64_t>::min();
    } else if constexpr (std::is_same_v<T, float>) {
        return std::numeric_limits<float>::min();
    } else if constexpr (std::is_same_v<T, double>) {
        return std::numeric_limits<double>::min();
    } else if constexpr (std::is_same_v<T, int128_t>) {
        return std::numeric_limits<int128_t>::min();
    } else if constexpr (std::is_same_v<T, omniruntime::type::Decimal128>) {
        return Decimal128(type::DECIMAL128_MIN_VALUE);
    } else {
        throw OmniException("LogicalError", "Unsupoorted data type");
    }
};

template <DataTypeId COL1_ID, DataTypeId COL2_ID> class MaxByAggregator : public TypedAggregator {
    using targetValueType = typename AggNativeAndVectorType<COL1_ID>::type;
    using targetValueTypeVec = typename AggNativeAndVectorType<COL1_ID>::vector;
    using sortKeyType = typename AggNativeAndVectorType<COL2_ID>::type;
    using sortKeyTypeVec = typename AggNativeAndVectorType<COL2_ID>::vector;

    // inner class for aggregate state, the member depends on targetValueType, sortKeyType of Aggregator
#pragma pack(push, 1)
    template <typename targetValueType, typename sortKeyType>
    struct MaxByState {
        targetValueType targetValue;
        sortKeyType sortKey;
        bool isEmpty = true;
        static const MaxByAggregator<COL1_ID, COL2_ID>::MaxByState<targetValueType, sortKeyType> *ConstCastState(const AggregateState *state)
        {
            return reinterpret_cast<const MaxByAggregator<COL1_ID, COL2_ID>::MaxByState<targetValueType, sortKeyType> *>(state);
        }

        static MaxByAggregator<COL1_ID, COL2_ID>::MaxByState<targetValueType, sortKeyType> *CastState(AggregateState *state)
        {
            return reinterpret_cast<MaxByAggregator<COL1_ID, COL2_ID>::MaxByState<targetValueType, sortKeyType> *>(state);
        }
    };
#pragma pack(pop)

public:
    ~MaxByAggregator() override = default;
    void ExtractValues(const AggregateState *state, std::vector<BaseVector *> &vectors, int32_t rowIndex) override;
    void ExtractValuesBatch(std::vector<AggregateState *> &groupStates, std::vector<BaseVector *> &vectors, int32_t rowOffset, int32_t rowCount) override;
    void ExtractValuesForSpill(std::vector<AggregateState *> &groupStates, std::vector<BaseVector *> &vectors) override;
    void InitState(AggregateState *state) override;
    void InitStates(std::vector<AggregateState *> &groupStates) override;
    std::vector<DataTypePtr> GetSpillType() override;
    size_t GetStateSize() override
    {
        return sizeof(MaxByState<targetValueType, sortKeyType>);
    }
    static std::unique_ptr<Aggregator> Create(const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels, bool rawIn, bool partialOut, bool isOverflowAsNull)
    {
        if (inputTypes.GetType(0)->GetId() != outputTypes.GetType(0)->GetId()) {
            throw std::runtime_error("Error in maxby aggregator: output col type not match input");
        }

        if constexpr (!(COL1_ID == OMNI_SHORT || COL1_ID == OMNI_INT || COL1_ID == OMNI_LONG || COL1_ID == OMNI_DOUBLE || COL1_ID == OMNI_DECIMAL128 || COL1_ID == OMNI_DECIMAL64 || COL1_ID == OMNI_BOOLEAN)) {
            throw std::runtime_error("Error in maxby aggregator: unsupported target value type " + TypeUtil::TypeToStringLog(COL1_ID));
        } else if constexpr (!(COL2_ID == OMNI_SHORT || COL2_ID == OMNI_INT || COL2_ID == OMNI_LONG || COL2_ID == OMNI_DOUBLE || COL2_ID == OMNI_DECIMAL128 || COL2_ID == OMNI_DECIMAL64 || COL2_ID == OMNI_BOOLEAN)) {
            throw std::runtime_error("Error in maxby aggregator: unsupported sort key type " + TypeUtil::TypeToStringLog(COL2_ID));
        } else {
            return std::unique_ptr<MaxByAggregator<COL1_ID, COL2_ID>>(new MaxByAggregator<COL1_ID, COL2_ID>(inputTypes, outputTypes, channels, rawIn, partialOut, isOverflowAsNull));
        }
    }

    void ProcessGroupUnspill(std::vector<UnspillRowInfo> &unspillRows, int32_t rowCount, int32_t &vectorIndex) override;

    void ProcessAlignAggSchema(VectorBatch *result, BaseVector *originVector,
        const std::shared_ptr<NullsHelper> nullMap, const bool aggFilter) override;

protected:
    MaxByAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels,
        const bool inputRaw, const bool outputPartial, const bool isOverflowAsNull);

    void ProcessSingleInternal(AggregateState *state, BaseVector *vector, const int32_t rowOffset,
        const int32_t rowCount, const std::shared_ptr<NullsHelper> nullMap) override;

    void ProcessGroupInternal(std::vector<AggregateState *> &rowStates, BaseVector *vector, const int32_t rowOffset,
                              const std::shared_ptr<NullsHelper> nullMap) override;

    template <typename T>
    void ProcessAlignAggSchemaInternal(VectorBatch *result, BaseVector *originVector,
        const std::shared_ptr<NullsHelper> nullMap);
};
} // namespace op
} // namespace omniruntime



#endif // OMNI_RUNTIME_MAXBY_AGGREGATOR_H