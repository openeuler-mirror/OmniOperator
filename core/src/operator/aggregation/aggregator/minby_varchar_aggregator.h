/*
* Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: Min_by varchar aggregate
 */
#ifndef OMNI_RUNTIME_MINBY_VARCHAR_AGGREGATOR_H
#define OMNI_RUNTIME_MINBY_VARCHAR_AGGREGATOR_H

#include <cstdint>
#include <cfloat>
#include <string_view>
#include <type_traits>
#include "typed_aggregator.h"

namespace omniruntime {
namespace op {
// When target col is VARCHAR/CHAR, state holds std::string_view (pointing to arena); otherwise use AggNativeAndVectorType.
template <DataTypeId COL1_ID, DataTypeId COL2_ID> class MinByVarcharAggregator : public TypedAggregator {
    using targetValueType = std::conditional_t<COL1_ID == OMNI_VARCHAR || COL1_ID == OMNI_CHAR,
        std::string_view, typename AggNativeAndVectorType<COL1_ID>::type>;
    using targetValueTypeVec = std::conditional_t<COL1_ID == OMNI_VARCHAR || COL1_ID == OMNI_CHAR,
        Vector<LargeStringContainer<std::string_view>>, typename AggNativeAndVectorType<COL1_ID>::vector>;

    // inner class for aggregate state, the member depends on targetValueType, sortKeyType of Aggregator
#pragma pack(push, 1)
    template <typename targetValueType>
    struct MinByVarcharState {
        // the final output type
        targetValueType targetValue;
        bool targetIsNull = false;  // true when the winning row has null target (Spark semantics)

    private:
        // sortkey: string
        int64_t strKeyAddress;
        int32_t strKeyLen;

        // judge if the string key is copied
        bool saved = false;
        bool targetValueOwned = false;

    public:
        static const MinByVarcharAggregator<COL1_ID, COL2_ID>::MinByVarcharState<targetValueType> *ConstCastState(const AggregateState *state)
        {
            return reinterpret_cast<const MinByVarcharAggregator<COL1_ID, COL2_ID>::MinByVarcharState<targetValueType> *>(state);
        }

        static MinByVarcharAggregator<COL1_ID, COL2_ID>::MinByVarcharState<targetValueType> *CastState(AggregateState *state)
        {
            return reinterpret_cast<MinByVarcharAggregator<COL1_ID, COL2_ID>::MinByVarcharState<targetValueType> *>(state);
        }

        void SetStrKey(int64_t address, int32_t len)
        {
            ReleaseSortKey();
            strKeyAddress = address;
            strKeyLen = len;
        }

        /** Reset key fields without releasing (for InitState; avoids delete on uninitialized state). */
        void ClearStrKey()
        {
            strKeyAddress = 0;
            strKeyLen = 0;
            saved = false;
        }

        /** Reset target value fields without releasing (for InitState when col1 is varchar/char). */
        void ClearTargetValue()
        {
            if constexpr (std::is_same_v<targetValueType, std::string_view>) {
                targetValue = std::string_view();
                targetValueOwned = false;
            }
        }

        bool IsSaved()
        {
            return saved;
        }

        int64_t GetStrKeyAddress() const
        {
            return strKeyAddress;
        }

        int32_t GetStrKeyLen() const
        {
            return strKeyLen;
        }

        // this method used to save the varchar sort key otherwise it will be released
        void SaveSortKey()
        {
            if (saved || strKeyAddress == 0) {
                // already copied varchar key, don't need to copy again
                return;
            }

            char* copied_data = new char[strKeyLen + 1];
            std::memcpy(copied_data, reinterpret_cast<const char*>(strKeyAddress), strKeyLen);
            copied_data[strKeyLen] = '\0';

            // reassign the address of the copied key to state
            strKeyAddress = reinterpret_cast<int64_t>(copied_data);

            saved = true;
        }

        void ReleaseSortKey()
        {
            if (!saved) {
                return;
            }
            if (strKeyAddress == 0) {
                saved = false;
                return;
            }
            char* strKeyToFree = reinterpret_cast<char*>(strKeyAddress);
            if (strKeyLen > 0) {
                delete[] strKeyToFree;
            }
            strKeyAddress = 0;
            strKeyLen = 0;
            saved = false;
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

        ~MinByVarcharState()
        {
            ReleaseSortKey();
            ReleaseTargetValueIfOwned();
        }
    };
#pragma pack(pop)

public:
    ~MinByVarcharAggregator() override = default;
    void ExtractValues(const AggregateState *state, std::vector<BaseVector *> &vectors, int32_t rowIndex) override;
    void ExtractValuesBatch(std::vector<AggregateState *> &groupStates, std::vector<BaseVector *> &vectors, int32_t rowOffset, int32_t rowCount) override;
    void ExtractValuesForSpill(std::vector<AggregateState *> &groupStates, std::vector<BaseVector *> &vectors) override;
    void InitState(AggregateState *state) override;
    void InitStates(std::vector<AggregateState *> &groupStates) override;
    std::vector<DataTypePtr> GetSpillType() override;
    size_t GetStateSize() override
    {
        return sizeof(MinByVarcharState<targetValueType>);
    }

    static constexpr bool IsSupportedBasicMinByType(DataTypeId type_id)
    {
        switch (type_id) {
            case OMNI_BYTE:
            case OMNI_SHORT:
            case OMNI_INT:
            case OMNI_LONG:
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
            throw omniruntime::exception::OmniException("Error in minby varchar aggregator: ", omniExceptionInfo);
        }

        if constexpr (!IsSupportedBasicMinByType(COL1_ID) && COL1_ID != OMNI_VARCHAR && COL1_ID != OMNI_CHAR) {
            std::string omniExceptionInfo = "unsupported target value type " + TypeUtil::TypeToStringLog(COL1_ID);
            throw omniruntime::exception::OmniException("Error in minby varchar aggregator: : ", omniExceptionInfo);
        } else if constexpr (COL2_ID != OMNI_VARCHAR && COL2_ID != OMNI_CHAR) {
            std::string omniExceptionInfo = "sort col type must be varchar or char";
            throw omniruntime::exception::OmniException("Error in minby varchar aggregator: : ", omniExceptionInfo);
        } else {
            return std::unique_ptr<MinByVarcharAggregator<COL1_ID, COL2_ID>>(new MinByVarcharAggregator<COL1_ID, COL2_ID>(inputTypes, outputTypes, channels, rawIn, partialOut, isOverflowAsNull));
        }
    }

    void ProcessGroupUnspill(std::vector<UnspillRowInfo> &unspillRows, int32_t rowCount, int32_t &vectorIndex) override;

    void ProcessAlignAggSchema(VectorBatch *result, BaseVector *originVector,
        const std::shared_ptr<NullsHelper> nullMap, const bool aggFilter) override;

protected:
    MinByVarcharAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels,
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



#endif // OMNI_RUNTIME_MINBY_VARCHAR_AGGREGATOR_H