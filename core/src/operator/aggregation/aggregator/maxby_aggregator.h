/*
* Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: MAX_BY aggregate
 */
#ifndef OMNI_RUNTIME_MAXBY_AGGREGATOR_H
#define OMNI_RUNTIME_MAXBY_AGGREGATOR_H

#include <cstdint>
#include <cfloat>
#include <limits>
#include "typed_aggregator.h"
#include "maxby_varchar_aggregator.h"

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
        // std::numeric_limits<float>::min() is the smallest *positive* normalized float (FLT_MIN), not -inf.
        // Using it breaks max_by for any negative ordering keys (Spark compares against -inf semantics).
        return -std::numeric_limits<float>::infinity();
    } else if constexpr (std::is_same_v<T, double>) {
        return -std::numeric_limits<double>::infinity();
    } else if constexpr (std::is_same_v<T, int128_t>) {
        return std::numeric_limits<int128_t>::min();
    } else if constexpr (std::is_same_v<T, omniruntime::type::Decimal128>) {
        return Decimal128(type::DECIMAL128_MIN_VALUE);
    } else {
        throw OmniException("LogicalError", "Unsupoorted data type");
    }
};

template <DataTypeId COL1_ID, DataTypeId COL2_ID> class MaxByAggregator : public TypedAggregator {
    using targetValueType = std::conditional_t<COL1_ID == OMNI_VARCHAR || COL1_ID == OMNI_CHAR || COL1_ID == OMNI_VARBINARY,
        std::string_view, typename AggNativeAndVectorType<COL1_ID>::type>;
    using targetValueTypeVec = std::conditional_t<COL1_ID == OMNI_VARCHAR || COL1_ID == OMNI_CHAR || COL1_ID == OMNI_VARBINARY,
        Vector<LargeStringContainer<std::string_view>>, typename AggNativeAndVectorType<COL1_ID>::vector>;
    using sortKeyType = typename AggNativeAndVectorType<COL2_ID>::type;
    using sortKeyTypeVec = typename AggNativeAndVectorType<COL2_ID>::vector;

    // inner class for aggregate state, the member depends on targetValueType, sortKeyType of Aggregator
#pragma pack(push, 1)
    template <typename targetValueType, typename sortKeyType>
    struct MaxByState {
        targetValueType targetValue;
        sortKeyType sortKey;
        bool isEmpty = true;
        bool targetIsNull = false;  // true when the winning row has null target (Spark semantics)
        bool targetValueOwned = false;
        static const MaxByAggregator<COL1_ID, COL2_ID>::MaxByState<targetValueType, sortKeyType> *ConstCastState(const AggregateState *state)
        {
            return reinterpret_cast<const MaxByAggregator<COL1_ID, COL2_ID>::MaxByState<targetValueType, sortKeyType> *>(state);
        }

        static MaxByAggregator<COL1_ID, COL2_ID>::MaxByState<targetValueType, sortKeyType> *CastState(AggregateState *state)
        {
            return reinterpret_cast<MaxByAggregator<COL1_ID, COL2_ID>::MaxByState<targetValueType, sortKeyType> *>(state);
        }

        /** Reset target value fields without releasing (for InitState when col1 is varchar/char). */
        void ClearTargetValue()
        {
            if constexpr (std::is_same_v<targetValueType, std::string_view>) {
                targetValue = std::string_view();
                targetValueOwned = false;
            }
        }

        void ReleaseTargetValueIfOwned()
        {
            if constexpr (std::is_same_v<targetValueType, std::string_view>) {
                if (!targetValueOwned) {
                    return;
                }
                if (targetValue.data() == nullptr) {
                    targetValueOwned = false;
                    return;
                }
                delete[] const_cast<char *>(targetValue.data());
                targetValue = std::string_view();
                targetValueOwned = false;
            }
        }

        void SetTargetValueOwned(bool owned) { targetValueOwned = owned; }
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

    static constexpr bool IsSupportedBasicMaxByType(DataTypeId type_id)
    {
        switch (type_id) {
            case OMNI_BYTE:
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

    static constexpr bool IsSupportedStringMaxByType(DataTypeId type_id)
    {
        switch (type_id) {
        case OMNI_VARCHAR:
        case OMNI_CHAR:
        case OMNI_VARBINARY:
            return true;
        default:
            return false;
        }
    }

    // Output type support: only scalar types. ARRAY/MAP/ROW are handled by MaxByComplexAggregator (factory routes there).
    static constexpr bool IsSupportedOutputType(DataTypeId type_id)
    {
        return IsSupportedBasicMaxByType(type_id) || IsSupportedStringMaxByType(type_id);
    }

    static std::unique_ptr<Aggregator> Create(const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels, bool rawIn, bool partialOut, bool isOverflowAsNull)
    {
        if (inputTypes.GetType(0)->GetId() != outputTypes.GetType(0)->GetId()) {
            std::string omniExceptionInfo = "output col type not match input";
            throw omniruntime::exception::OmniException("Error in maxby aggregator:", omniExceptionInfo);
        }

        if constexpr (IsSupportedStringMaxByType(COL2_ID)) {
            return MaxByVarcharAggregator<COL1_ID, COL2_ID>::Create(inputTypes, outputTypes, channels, rawIn, partialOut,
                isOverflowAsNull);
        }

        if constexpr (!IsSupportedOutputType(COL1_ID)) {
            std::string omniExceptionInfo = "unsupported target value type " + TypeUtil::TypeToStringLog(COL1_ID);
            throw omniruntime::exception::OmniException("Error in maxby aggregator:", omniExceptionInfo);
        } else if constexpr (!IsSupportedBasicMaxByType(COL2_ID)) {
            std::string omniExceptionInfo = "unsupported sort key type " + TypeUtil::TypeToStringLog(COL2_ID);
            throw omniruntime::exception::OmniException("Error in maxby aggregator: ", omniExceptionInfo);
        } else {
            return std::unique_ptr<MaxByAggregator<COL1_ID, COL2_ID>>(new MaxByAggregator<COL1_ID, COL2_ID>(inputTypes, outputTypes, channels, rawIn, partialOut, isOverflowAsNull));
        }
    }

    void ProcessGroupUnspill(std::vector<UnspillRowInfo> &unspillRows, int32_t rowCount, int32_t &vectorIndex) override;

    void AlignAggSchema(VectorBatch *result, VectorBatch *inputVecBatch) override;

    void AlignAggSchemaWithFilter(VectorBatch *result, VectorBatch *inputVecBatch,
        const int32_t filterIndex) override;

    void ProcessAlignAggSchema(VectorBatch *result, BaseVector *originVector,
        const std::shared_ptr<NullsHelper> nullMap, const bool aggFilter) override;

protected:
    MaxByAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels,
        const bool inputRaw, const bool outputPartial, const bool isOverflowAsNull);

    void ProcessSingleInternal(AggregateState *state, BaseVector *vector, const int32_t rowOffset,
        const int32_t rowCount, const std::shared_ptr<NullsHelper> nullMap) override;

    void ProcessGroupInternal(std::vector<AggregateState *> &rowStates, BaseVector *vector, const int32_t rowOffset,
                              const std::shared_ptr<NullsHelper> nullMap) override;
};
} // namespace op
} // namespace omniruntime



#endif // OMNI_RUNTIME_MAXBY_AGGREGATOR_H