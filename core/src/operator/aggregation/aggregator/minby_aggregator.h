/*
* Copyright (c) Huawei Technologies Co., Ltd. 2021-2024. All rights reserved.
 * Description: Min_by aggregate
 */
#ifndef OMNI_RUNTIME_MINBY_AGGREGATOR_H
#define OMNI_RUNTIME_MINBY_AGGREGATOR_H

#include <cstdint>
#include <cfloat>
#include "typed_aggregator.h"
#include "minby_varchar_aggregator.h"

namespace omniruntime {
namespace op {
template <typename T> T GetSortKeyMax()
{
    if constexpr (std::is_same_v<T, int8_t>) {
        return std::numeric_limits<int8_t>::max();
    } else if constexpr (std::is_same_v<T, int16_t>) {
        return std::numeric_limits<int16_t>::max();
    } else if constexpr (std::is_same_v<T, int32_t>) {
        return std::numeric_limits<int32_t>::max();
    } else if constexpr (std::is_same_v<T, int64_t>) {
        return std::numeric_limits<int64_t>::max();
    } else if constexpr (std::is_same_v<T, float>) {
        return std::numeric_limits<float>::max();
    } else if constexpr (std::is_same_v<T, double>) {
        return std::numeric_limits<double>::max();
    } else if constexpr (std::is_same_v<T, int128_t>) {
        return std::numeric_limits<int128_t>::max();
    } else if constexpr (std::is_same_v<T, omniruntime::type::Decimal128>) {
        return Decimal128(type::DECIMAL128_MAX_VALUE);
    } else {
        throw OmniException("LogicalError", "Unsupoorted data type");
    }
};

template <DataTypeId COL1_ID, DataTypeId COL2_ID> class MinByAggregator : public TypedAggregator {
    using targetValueType = typename AggNativeAndVectorType<COL1_ID>::type;
    using targetValueTypeVec = typename AggNativeAndVectorType<COL1_ID>::vector;
    using sortKeyType = typename AggNativeAndVectorType<COL2_ID>::type;
    using sortKeyTypeVec = typename AggNativeAndVectorType<COL2_ID>::vector;

    // inner class for aggregate state, the member depends on targetValueType, sortKeyType of Aggregator
#pragma pack(push, 1)
    template <typename targetValueType, typename sortKeyType>
    struct MinByState {
        targetValueType targetValue;
        sortKeyType sortKey;
        bool isEmpty = true;
        static const MinByAggregator<COL1_ID, COL2_ID>::MinByState<targetValueType, sortKeyType> *ConstCastState(const AggregateState *state)
        {
            return reinterpret_cast<const MinByAggregator<COL1_ID, COL2_ID>::MinByState<targetValueType, sortKeyType> *>(state);
        }

        static MinByAggregator<COL1_ID, COL2_ID>::MinByState<targetValueType, sortKeyType> *CastState(AggregateState *state)
        {
            return reinterpret_cast<MinByAggregator<COL1_ID, COL2_ID>::MinByState<targetValueType, sortKeyType> *>(state);
        }
    };
#pragma pack(pop)

public:
    ~MinByAggregator() override = default;
    void ExtractValues(const AggregateState *state, std::vector<BaseVector *> &vectors, int32_t rowIndex) override;
    void ExtractValuesBatch(std::vector<AggregateState *> &groupStates, std::vector<BaseVector *> &vectors, int32_t rowOffset, int32_t rowCount) override;
    void ExtractValuesForSpill(std::vector<AggregateState *> &groupStates, std::vector<BaseVector *> &vectors) override;
    void InitState(AggregateState *state) override;
    void InitStates(std::vector<AggregateState *> &groupStates) override;
    std::vector<DataTypePtr> GetSpillType() override;
    size_t GetStateSize() override
    {
        return sizeof(MinByState<targetValueType, sortKeyType>);
    }

    static constexpr bool IsSupportedBasicMinByType(DataTypeId type_id)
    {
        switch (type_id) {
            case OMNI_SHORT:
            case OMNI_INT:
            case OMNI_LONG:
            case OMNI_FLOAT:
            case OMNI_DOUBLE:
            case OMNI_DECIMAL128:
            case OMNI_DECIMAL64:
            case OMNI_BOOLEAN:
                return true;
            default:
                return false;
        }
    }

    static std::unique_ptr<Aggregator> Create(const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels, bool rawIn, bool partialOut, bool isOverflowAsNull)
    {
        if (inputTypes.GetType(0)->GetId() != outputTypes.GetType(0)->GetId()) {
            std::string omniExceptionInfo = "output col type not match input";
            throw omniruntime::exception::OmniException("Error in minby aggregator: ", omniExceptionInfo);
        }

        if constexpr (COL2_ID == OMNI_VARCHAR || COL2_ID == OMNI_CHAR) {
            return MinByVarcharAggregator<COL1_ID, COL2_ID>::Create(inputTypes, outputTypes, channels, rawIn, partialOut,
                isOverflowAsNull);
        }

        if constexpr (!IsSupportedBasicMinByType(COL1_ID)) {
            std::string omniExceptionInfo = "unsupported target value type " + TypeUtil::TypeToStringLog(COL1_ID);
            throw omniruntime::exception::OmniException("Error in minby aggregator: : ", omniExceptionInfo);
        } else if constexpr (!IsSupportedBasicMinByType(COL2_ID)) {
            std::string omniExceptionInfo = "unsupported target value type " + TypeUtil::TypeToStringLog(COL2_ID);
            throw omniruntime::exception::OmniException("Error in minby aggregator: : ", omniExceptionInfo);
        } else {
            return std::unique_ptr<MinByAggregator<COL1_ID, COL2_ID>>(new MinByAggregator<COL1_ID, COL2_ID>(inputTypes, outputTypes, channels, rawIn, partialOut, isOverflowAsNull));
        }
    }

    void ProcessGroupUnspill(std::vector<UnspillRowInfo> &unspillRows, int32_t rowCount, int32_t &vectorIndex) override;

    void ProcessAlignAggSchema(VectorBatch *result, BaseVector *originVector,
        const std::shared_ptr<NullsHelper> nullMap, const bool aggFilter) override;

protected:
    MinByAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels,
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



#endif // OMNI_RUNTIME_MINBY_AGGREGATOR_H