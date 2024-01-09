/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2023. All rights reserved.
 * Description: Hash Aggregation Header
 */
#ifndef GROUP_AGGREGATION_H
#define GROUP_AGGREGATION_H

#include "definitions.h"
#include "aggregation.h"
#include "type/data_types.h"
#include "operator/hash_util.h"
#include "operator/execution_context.h"
#include "operator/aggregation/aggregator/only_aggregator_factory.h"
#include "operator/util/operator_util.h"
#include "operator/hashmap/column_marshaller.h"
#include "operator/aggregation/aggregator/aggregator_factory.h"
#include "operator/config/operator_config.h"
#include "operator/filter/filter_and_project.h"

namespace omniruntime {
namespace op {
using namespace vec;
class HashAggregationOperatorFactory;
class HashAggregationOperator;

using HashFunc = void (*)(BaseVector *vector, const uint32_t r, const int32_t *ri, uint64_t *hashVal);
using HashFuncVect = void (*)(BaseVector *vector, const uint32_t s, const uint32_t r, uint64_t *hashVal);
using DuplicateKeyValue = void (*)(AggregateState &state, BaseVector *vector, const uint32_t offset,
    ExecutionContext *context);
using IsSameNodeFunc = void (*)(BaseVector *vector, const uint32_t offset, const AggregateState &slot, bool &isSame);
using SetVector = void (*)(VectorBatch *vecBatch, int32_t rowCount);
using FillValue = void (*)(BaseVector *vector, int32_t rowIndex, const AggregateState &state);

using FunctionByDataType = struct FunctionByDataType {
    DataTypeId dataTypeId;
    HashFunc hashFunc;
    HashFuncVect hashFuncVect;
    IsSameNodeFunc isSameNode;
    DuplicateKeyValue duplicateKey;
    SetVector setVector;
    FillValue fillValue;
};

template <typename V, typename D>
void HashFuncImpl(BaseVector *vector, const uint32_t rowCount, const int32_t *rowIndexes, uint64_t *combinedHash)
{
    uint64_t hash;
    std::hash<D> hasher;
    for (uint32_t i = 0; i < rowCount; ++i) {
        auto idx = rowIndexes[i];
        hash = !vector->IsNull(idx) * hasher(static_cast<V *>(vector)->GetValue(idx));
        combinedHash[i] = HashUtil::CombineHash(static_cast<int64_t>(combinedHash[i]), static_cast<int64_t>(hash));
    }
}

template <typename V = Vector<LargeStringContainer<std::string_view>>>
void HashVarcharFuncImpl(BaseVector *vector, const uint32_t rowCount, const int32_t *rowIndexes, uint64_t *combinedHash)
{
    for (uint32_t i = 0; i < rowCount; ++i) {
        auto idx = rowIndexes[i];
        std::string_view str = static_cast<V *>(vector)->GetValue(idx);
        auto valLen = str.size();
        auto val = HashUtil::HashValue(reinterpret_cast<int8_t *>(const_cast<char *>(str.data())), valLen);
        combinedHash[i] = HashUtil::CombineHash(static_cast<int64_t>(combinedHash[i]), !vector->IsNull(idx) * val);
    }
}

template <typename V = Vector<Decimal128>>
void HashDecimalFunc(BaseVector *vector, const uint32_t rowCount, const int32_t *rowIndexes, uint64_t *combinedHash)
{
    for (uint32_t i = 0; i < rowCount; ++i) {
        int32_t idx = rowIndexes[i];
        Decimal128 val = static_cast<V *>(vector)->GetValue(idx);
        auto hash = HashUtil::CombineHash(combinedHash[i],
            !vector->IsNull(idx) * HashUtil::HashValue(val.LowBits(), val.HighBits()));
        combinedHash[i] = static_cast<uint64_t>(hash);
    }
}

template <typename V, typename D>
void HashFuncVectImpl(BaseVector *vector, const uint32_t start, const uint32_t rowCount, uint64_t *combinedHash)
{
    uint64_t hash;
    std::hash<D> hasher;
    for (uint32_t i = 0; i < rowCount; ++i) {
        auto idx = i + start;
        hash = !vector->IsNull(idx) * hasher(static_cast<V *>(vector)->GetValue(idx));
        combinedHash[i] = HashUtil::CombineHash(static_cast<int64_t>(combinedHash[i]), static_cast<int64_t>(hash));
    }
}

template <typename V>
void HashVarcharVectFuncImpl(BaseVector *vector, const uint32_t start, const uint32_t rowCount, uint64_t *combinedHash)
{
    for (uint32_t i = 0; i < rowCount; ++i) {
        auto idx = i + start;
        std::string_view str = static_cast<V *>(vector)->GetValue(idx);
        auto valLen = str.size();
        auto val = HashUtil::HashValue(reinterpret_cast<int8_t *>(const_cast<char *>(str.data())), valLen);
        auto hash = HashUtil::CombineHash(static_cast<int64_t>(combinedHash[i]), !vector->IsNull(idx) * val);
        combinedHash[i] = static_cast<uint64_t>(hash);
    }
}

template <typename V>
void HashDecimalVectFunc(BaseVector *vector, const uint32_t start, const uint32_t rowCount, uint64_t *combinedHash)
{
    for (uint32_t i = 0; i < rowCount; ++i) {
        auto idx = i + start;
        Decimal128 val = static_cast<V *>(vector)->GetValue(idx);
        auto hash = HashUtil::CombineHash(static_cast<int64_t>(combinedHash[i]),
            !vector->IsNull(idx) * HashUtil::HashValue(val.LowBits(), val.HighBits()));
        combinedHash[i] = static_cast<uint64_t>(hash);
    }
}

template <typename V, typename D>
void HashFuncVectImplProxy(BaseVector *vector, uint32_t start, uint32_t rowCount, uint64_t *combinedHash)
{
    HashFuncVectImpl<V, D>(vector, start, rowCount, combinedHash);
}

template <typename V = Vector<LargeStringContainer<std::string_view>>>
void HashVarcharVectFuncImplProxy(BaseVector *vector, const uint32_t start, const uint32_t rowCount,
    uint64_t *combinedHash)
{
    HashVarcharVectFuncImpl<V>(vector, start, rowCount, combinedHash);
}

template <typename V = Vector<Decimal128>>
void HashDecimalVectFuncProxy(BaseVector *vector, const uint32_t start, const uint32_t rowCount, uint64_t *combinedHash)
{
    HashDecimalVectFunc<V>(vector, start, rowCount, combinedHash);
}

template <typename V, typename D>
void IsSameNodeFuncImpl(BaseVector *vector, const uint32_t offset, const AggregateState &slot, bool &isSame)
{
    bool isIntermediateNull = static_cast<D *>(slot.val) == nullptr;
    bool isInputNull = vector->IsNull(offset);
    if (!isInputNull && !isIntermediateNull) {
        auto intTmp = static_cast<V *>(vector)->GetValue(offset);
        isSame = intTmp == *static_cast<D *>(slot.val);
        return;
    }
    if (isInputNull != isIntermediateNull) {
        isSame = false;
        return;
    }
    isSame = true;
    return;
}

template <typename V = Vector<LargeStringContainer<std::string_view>>>
void IsSameNodeFuncVarcharImpl(BaseVector *vector, const uint32_t offset, const AggregateState &slot, bool &isSame)
{
    bool isIntermediateNull = slot.val == nullptr;
    bool isInputNull = vector->IsNull(offset);
    if (!isInputNull && !isIntermediateNull) {
        std::string_view str = static_cast<V *>(vector)->GetValue(offset);
        auto valLen = str.size();
        auto *data = reinterpret_cast<uint8_t *>(const_cast<char *>(str.data()));
        isSame = (static_cast<int64_t>(valLen) == slot.count) && (memcmp(data, slot.val, valLen) == 0);
        return;
    }
    if (isInputNull != isIntermediateNull) {
        isSame = false;
        return;
    }
    isSame = true;
    return;
}

template <typename V, typename D>
void DuplicateKeyValueImpl(AggregateState &state, BaseVector *vector, const uint32_t offset, ExecutionContext *context)
{
    if (vector->IsNull(offset)) {
        return;
    }
    auto len = sizeof(D);
    uint8_t *ptr = context->GetArena()->Allocate(len);
    D data = static_cast<V *>(vector)->GetValue(offset);
    memcpy_s(ptr, len, &data, len);
    state.val = ptr;
}

template <typename V = Vector<LargeStringContainer<std::string_view>>>
void DuplicateVarcharKeyValue(AggregateState &state, BaseVector *vector, const uint32_t offset,
    ExecutionContext *context)
{
    if (vector->IsNull(offset)) {
        return;
    }

    std::string_view str = static_cast<V *>(vector)->GetValue(offset);
    int32_t valLen = str.size();
    uint8_t *tmp = reinterpret_cast<uint8_t *>(const_cast<char *>(str.data()));
    uint8_t *data = context->GetArena()->Allocate(valLen);
    memcpy_s(data, valLen, tmp, static_cast<size_t>(valLen));
    state.val = data;
    state.count = valLen;
}

template <typename V> void SetVectorImpl(VectorBatch *vecBatch, int32_t rowCount)
{
    vecBatch->Append(new V(rowCount));
}

void SetVarcharVector(VectorBatch *vecBatch, int32_t rowCount);
void SetContainerVector(VectorBatch *vecBatch, int32_t rowCount);

template <typename V, typename D> void FillValueImpl(BaseVector *v, int32_t rowIndex, const AggregateState &state)
{
    if (state.val == nullptr) {
        static_cast<V *>(v)->SetNull(rowIndex);
    } else {
        static_cast<V *>(v)->SetValue(rowIndex, *static_cast<D *>(state.val));
    }
}
void FillVarcharValue(BaseVector *vector, int32_t rowIndex, const AggregateState &state);

class HashAggregationOperator : public AggregationCommonOperator {
public:
    HashAggregationOperator(std::vector<ColumnIndex> &groupByCols, std::vector<std::vector<int32_t>> &aggInputCols,
        uint32_t aggInputColsSize, std::vector<DataTypes> &aggInputTypes, std::vector<DataTypes> &aggOutputTypes,
        std::vector<std::unique_ptr<Aggregator>> &&aggs, std::vector<bool> &inputRaws,
        std::vector<bool> &outputPartials)
        : AggregationCommonOperator(std::move(aggs), inputRaws, outputPartials),
          groupByCols(groupByCols),
          aggInputCols(aggInputCols),
          aggInputColsSize(aggInputColsSize),
          aggInputTypes(aggInputTypes),
          aggOutputTypes(aggOutputTypes)
    {}

    ~HashAggregationOperator() override = default;

    int32_t AddInput(VectorBatch *data) override;

    int32_t GetOutput(VectorBatch **outputVecBatch) override;

    OmniStatus Init() override;

    OmniStatus Close() override;

    template <typename Serialize>
    void Emplace(Serialize &emplaceKey, VectorBatch *vecBatch, BaseVector **groupVectors, int32_t groupColNum);

private:
    int32_t InitMaxRowCountAndOutputTypes();

    void SetVectors(VectorBatch *output, const std::vector<DataTypePtr> &types, int32_t rowCount);

    template <typename Deserialize> int32_t Output(Deserialize &deserializeHashmap, VectorBatch **outputVecBatch);
    void SetGroupByColumnsHandleType(HandleType t);

    friend class HashAggregationOperatorFactory;
    template <typename V, typename D>
    friend void FillValueImpl(BaseVector *vector, int32_t rowIndex, const AggregateState &state);
    friend void FillVarcharValue(BaseVector *vector, int32_t rowIndex, const AggregateState &state);

    std::vector<ColumnIndex> groupByCols;
    std::vector<std::vector<int32_t>> aggInputCols;
    uint32_t aggInputColsSize;
    std::vector<DataTypes> aggInputTypes;
    std::vector<DataTypes> aggOutputTypes;
    std::vector<type::DataTypePtr> outputTypes;
    std::unique_ptr<ExecutionContext> executionContext;
    HandleType groupByColumnsHandleType = HandleType::serialize;
    std::unique_ptr<ColumnSerializeHandler<DefaultHashMap<StringRef, AggregateState *>>> serialize = nullptr;
    bool isInited = false;

    OutputState outputState;

    template <typename Deserialize>
    void TraverseHashmapToGetOneResult(Deserialize &deserializeHashmap, VectorBatch *output);

    int32_t rowsPerBatch;
};

class HashAggregationOperatorFactory : public AggregationCommonOperatorFactory {
public:
    Operator *CreateOperator() override;
    /*
     * @param groupByCol      the col index which is used as group column in VectorBatch
     * @param groupInputTypes all the group types
     * @param aggsCols        aggsCols contains all aggregators 's all agg col index
     * @param aggInputTypes   input types of all aggregators
     * @param aggOutputTypes  output types of all aggregators
     * @param aggFuncTypes    func types of aggregators
     * @param maskColsVector  mask col index in VectorBatch
     * @param inputRaws       whether the input VectorBatch is raw, the input raw is true in the first stage
     * @param outputPartials  whether the output VectorBatch is paritial result
     * @param overflowAsNull  determine throw exception or set null when catch overflow result
     */
    HashAggregationOperatorFactory(std::vector<uint32_t> &groupByCol, const DataTypes &groupInputTypes,
        std::vector<std::vector<uint32_t>> &aggsCols, std::vector<DataTypes> &aggInputTypes,
        std::vector<DataTypes> &aggOutputTypes, std::vector<uint32_t> &aggFuncTypes,
        std::vector<uint32_t> &maskColsVector, std::vector<bool> inputRaws, std::vector<bool> outputPartials,
        bool overflowAsNull = false)
        : AggregationCommonOperatorFactory(inputRaws, outputPartials, maskColsVector, overflowAsNull),
          groupByColsVector(groupByCol),
          groupByTypes(groupInputTypes),
          aggsInputColsVector(aggsCols),
          aggInputTypes(aggInputTypes),
          aggOutputTypes(aggOutputTypes),
          aggFuncTypesVector(aggFuncTypes)
    {}

    ~HashAggregationOperatorFactory() override {}
    OmniStatus Init() override;
    OmniStatus Close() override;

private:
    std::vector<uint32_t> groupByColsVector;
    std::vector<int32_t> groupByColIdx;
    DataTypes groupByTypes;
    std::vector<std::vector<uint32_t>> aggsInputColsVector;
    std::vector<std::vector<int32_t>> aggsInputCols;
    std::vector<std::vector<uint32_t>> aggOutputColsVector;
    std::vector<DataTypes> aggInputTypes;
    std::vector<DataTypes> aggOutputTypes;
    std::vector<uint32_t> aggFuncTypesVector;
    std::vector<std::unique_ptr<AggregatorFactory>> aggregatorFactories;
    HandleType handleType;
    void ChooseGroupByType();
};
} // end of namespace op
} // end of namespace omniruntimef
#endif