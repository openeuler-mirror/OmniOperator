/*
* Copyright (c) Huawei Technologies Co., Ltd. 2021-2024. All rights reserved.
 * Description: Max_by varchar aggregate
 */
#ifndef OMNI_RUNTIME_MAXBY_VARCHAR_AGGREGATOR_H
#define OMNI_RUNTIME_MAXBY_VARCHAR_AGGREGATOR_H

#include <cstdint>
#include <cfloat>
#include "typed_aggregator.h"

namespace omniruntime {
namespace op {
template <DataTypeId COL1_ID, DataTypeId COL2_ID> class MaxByVarcharAggregator : public TypedAggregator {
    using targetValueType = typename AggNativeAndVectorType<COL1_ID>::type;
    using targetValueTypeVec = typename AggNativeAndVectorType<COL1_ID>::vector;

#pragma pack(push, 1)
    template <typename targetValueType>
    struct MaxByVarcharState {
        targetValueType targetValue;

    private:
        int64_t strKeyAddress;
        int32_t strKeyLen;
        bool saved = false;

    public:
        static const MaxByVarcharAggregator<COL1_ID, COL2_ID>::MaxByVarcharState<targetValueType> *ConstCastState(const AggregateState *state)
        {
            return reinterpret_cast<const MaxByVarcharAggregator<COL1_ID, COL2_ID>::MaxByVarcharState<targetValueType> *>(state);
        }

        static MaxByVarcharAggregator<COL1_ID, COL2_ID>::MaxByVarcharState<targetValueType> *CastState(AggregateState *state)
        {
            return reinterpret_cast<MaxByVarcharAggregator<COL1_ID, COL2_ID>::MaxByVarcharState<targetValueType> *>(state);
        }

        void SetStrKey(int64_t address, int32_t len)
        {
            ReleaseSortKey();
            strKeyAddress = address;
            strKeyLen = len;
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

        void SaveSortKey()
        {
            if (saved || strKeyAddress == 0) {
                return;
            }
            char* copied_data = new char[strKeyLen + 1];
            std::memcpy(copied_data, strKeyAddress, strKeyLen);
            copied_data[strKeyLen] = '\0';
            strKeyAddress = reinterpret_cast<int64_t>(copied_data);
            saved = true;
        }

        void ReleaseSortKey()
        {
            if (!saved) {
                return;
            }
            char* strKeyToFree = reinterpret_cast<char*>(strKeyAddress);
            if (strKeyToFree != nullptr && strKeyLen > 0) {
                delete[] strKeyToFree;
                strKeyToFree = nullptr;
            }
            strKeyAddress = 0;
            saved = false;
        }

        ~MaxByVarcharState()
        {
            ReleaseSortKey();
        }
    };
#pragma pack(pop)

public:
    ~MaxByVarcharAggregator() override = default;
    void ExtractValues(const AggregateState *state, std::vector<BaseVector *> &vectors, int32_t rowIndex) override;
    void ExtractValuesBatch(std::vector<AggregateState *> &groupStates, std::vector<BaseVector *> &vectors, int32_t rowOffset, int32_t rowCount) override;
    void ExtractValuesForSpill(std::vector<AggregateState *> &groupStates, std::vector<BaseVector *> &vectors) override;
    void InitState(AggregateState *state) override;
    void InitStates(std::vector<AggregateState *> &groupStates) override;
    std::vector<DataTypePtr> GetSpillType() override;
    size_t GetStateSize() override
    {
        return sizeof(MaxByVarcharState<targetValueType>);
    }

    static constexpr bool IsSupportedBasicMaxByType(DataTypeId type_id)
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
            throw omniruntime::exception::OmniException("Error in maxby varchar aggregator: ", omniExceptionInfo);
        }

        if constexpr (!IsSupportedBasicMaxByType(COL1_ID)) {
            std::string omniExceptionInfo = "unsupported target value type " + TypeUtil::TypeToStringLog(COL1_ID);
            throw omniruntime::exception::OmniException("Error in maxby varchar aggregator: ", omniExceptionInfo);
        } else if constexpr (COL2_ID != OMNI_VARCHAR && COL2_ID != OMNI_CHAR) {
            std::string omniExceptionInfo = "sort col type must be varchar or char";
            throw omniruntime::exception::OmniException("Error in maxby varchar aggregator: ", omniExceptionInfo);
        } else {
            return std::unique_ptr<MaxByVarcharAggregator<COL1_ID, COL2_ID>>(new MaxByVarcharAggregator<COL1_ID, COL2_ID>(inputTypes, outputTypes, channels, rawIn, partialOut, isOverflowAsNull));
        }
    }

    void ProcessGroupUnspill(std::vector<UnspillRowInfo> &unspillRows, int32_t rowCount, int32_t &vectorIndex) override;

    void ProcessAlignAggSchema(VectorBatch *result, BaseVector *originVector,
        const std::shared_ptr<NullsHelper> nullMap, const bool aggFilter) override;

protected:
    MaxByVarcharAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels,
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

#endif // OMNI_RUNTIME_MAXBY_VARCHAR_AGGREGATOR_H
