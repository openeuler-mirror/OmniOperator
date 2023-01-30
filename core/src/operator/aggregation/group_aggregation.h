/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Description: Hash Aggregation Header
 */
#ifndef GROUP_AGGREGATION_H
#define GROUP_AGGREGATION_H

#include "definitions.h"
#include "aggregation.h"
#include "type/data_types.h"
#include "operator/hash_util.h"
#include "operator/execution_context.h"
#include "operator/aggregation/aggregator/aggregator_factory.h"
#include "operator/util/operator_util.h"
#include "group_column_marshaller.h"

namespace omniruntime {
namespace op {
using namespace vec;
class HashAggregationOperatorFactory;
class HashAggregationOperator;

using HashFunc = void (*)(Vector *vector, const uint32_t r, const int32_t *ri, uint64_t *hashVal);
using HashFuncVect = void (*)(Vector *vector, const uint32_t s, const uint32_t r, uint64_t *hashVal);
using DuplicateKeyValue = void (*)(AggregateState &state, Vector *vector, const uint32_t offset,
    ExecutionContext *context);
using IsSameNodeFunc = void (*)(Vector *vector, const uint32_t offset, const AggregateState &slot, bool &isSame);
using SetVector = void (*)(VectorBatch *vecBatch, DataType &type, int32_t columnIndex, VectorAllocator *vecAllocator,
    int32_t rowCount);
using FillValue = void (*)(Vector *vector, int32_t rowIndex, const AggregateState &state);

using FunctionByDataType = struct FunctionByDataType {
    DataTypeId dataTypeId;
    HashFunc hashFunc;
    HashFuncVect hashFuncVect;
    IsSameNodeFunc isSameNode;
    DuplicateKeyValue duplicateKey;
    SetVector setVector;
    FillValue fillValue;
};

using HashAggModule = HashAggregationOperator *(*)(HashAggregationOperatorFactory *);

// HMPP function
#ifdef ENABLE_HMPP
template <typename V, typename D>
void HashFuncVectImplHMPP(Vector *vector, const uint32_t start, const uint32_t rowCount, uint64_t *combinedHash);
void HashVarcharVectFuncImplHMPP(Vector *vector, const uint32_t start, const uint32_t rowCount, uint64_t *combinedHash);
void HashDecimalVectFuncHMPP(Vector *vector, const uint32_t start, const uint32_t rowCount, uint64_t *combinedHash);
#endif

template <typename V, typename D>
void HashFuncImpl(Vector *vector, const uint32_t rowCount, const int32_t *rowIndexes, uint64_t *combinedHash);
void HashVarcharFuncImpl(Vector *vector, const uint32_t rowCount, const int32_t *rowIndexes, uint64_t *combinedHash);
void HashDecimalFunc(Vector *vector, const uint32_t rowCount, const int32_t *rowIndexes, uint64_t *combinedHash);

template <typename V, typename D>
void HashFuncVectImpl(Vector *vector, const uint32_t start, const uint32_t rowCount, uint64_t *combinedHash);
void HashVarcharVectFuncImpl(Vector *vector, const uint32_t start, const uint32_t rowCount, uint64_t *combinedHash);
void HashDecimalVectFunc(Vector *vector, const uint32_t start, const uint32_t rowCount, uint64_t *combinedHash);


template <typename V, typename D>
void HashFuncVectImplProxy(Vector *vector, const uint32_t start, const uint32_t rowCount, uint64_t *combinedHash);
void HashVarcharVectFuncImplProxy(Vector *vector, const uint32_t start, const uint32_t rowCount,
    uint64_t *combinedHash);
void HashDecimalVectFuncProxy(Vector *vector, const uint32_t start, const uint32_t rowCount, uint64_t *combinedHash);

template <typename V, typename D>
void IsSameNodeFuncImpl(Vector *vector, const uint32_t offset, const AggregateState &slot, bool &isSame);
void IsSameNodeFuncVarcharImpl(Vector *vector, const uint32_t offset, const AggregateState &slot, bool &isSame);

template <typename V, typename D>
void DuplicateKeyValueImpl(AggregateState &state, Vector *vector, const uint32_t offset, ExecutionContext *context);
void DuplicateVarcharKeyValue(AggregateState &state, Vector *vector, const uint32_t offset, ExecutionContext *context);

template <typename V>
void SetVectorImpl(VectorBatch *vecBatch, DataType &type, int32_t columnIndex, VectorAllocator *vecAllocator,
    int32_t rowCount);
void SetVarcharVector(VectorBatch *vecBatch, DataType &type, int32_t columnIndex, VectorAllocator *vecAllocator,
    int32_t rowCount);
void SetContainerVector(VectorBatch *vecBatch, DataType &type, int32_t columnIndex, VectorAllocator *vecAllocator,
    int32_t rowCount);

template <typename V, typename D>
void FillValueImpl(Vector *vector, int32_t rowIndex, const AggregateState &state);

void FillVarcharValue(Vector *vector, int32_t rowIndex, const AggregateState &state);

class HashAggregationOperator : public AggregationCommonOperator {
public:
    HashAggregationOperator(std::vector<ColumnIndex> &groupByCols, std::vector<std::vector<int32_t>> &aggInputCols,
        uint32_t aggInputColsSize, std::vector<DataTypes> &aggInputTypes, std::vector<DataTypes> &aggOutputTypes,
        std::vector<std::unique_ptr<Aggregator>> &&aggs, std::vector<bool> &inputRaws, std::vector<bool> &outputPartials)
        : AggregationCommonOperator(std::move(aggs), inputRaws, outputPartials),
          groupByCols(groupByCols),
          aggInputCols(aggInputCols),
          aggInputColsSize(aggInputColsSize),
          aggInputTypes(aggInputTypes),
          aggOutputTypes(aggOutputTypes)
    {}

    ~HashAggregationOperator() override {}

    int32_t AddInput(VectorBatch *data) override;

    int32_t GetOutput(std::vector<VectorBatch *> &data) override;

    OmniStatus Init() override;

    OmniStatus Close() override;

    template <typename Serialize> void Emplace(Serialize &emplaceKey, VectorBatch *vecBatch, VectorBatch &groupVectors);

private:
    int32_t GetRowSizeAndOutputTypes(std::vector<DataTypePtr> &types);

    void SetVectors(VectorAllocator *vecAllocator, VectorBatch *vectorBatch, const std::vector<DataTypePtr> &types,
        int32_t rowCount);

    template <typename Deserialize> int32_t Output(Deserialize &deserializeHashmap, std::vector<VectorBatch *> &result);
    void SetGroupByColumnsHandleType(GroupByFieldHandleType t);

    friend class HashAggregationOperatorFactory;
    template <typename V, typename D>
    friend void FillValueImpl(Vector *vector, int32_t rowIndex, const AggregateState &state);
    friend void FillVarcharValue(Vector *vector, int32_t rowIndex, const AggregateState &state);

    std::vector<ColumnIndex> groupByCols;
    std::vector<std::vector<int32_t>> aggInputCols;
    uint32_t aggInputColsSize;
    std::vector<DataTypes> aggInputTypes;
    std::vector<DataTypes> aggOutputTypes;
    std::unique_ptr<ExecutionContext> executionContext;
    GroupByFieldHandleType groupByColumnsHandleType = GroupByFieldHandleType::serialize;
    std::unique_ptr<GroupbyColumnSerializeHandler<DefaultHashMap<StringRef, AggregateState *>>> serialize = nullptr;
    bool isInited = false;

    void FillOutputResultVectors(const int32_t totalRowCount, std::vector<VectorBatch *> &result);

    template <typename Deserialize>
    void TraverseHashmapToGetResults(
        Deserialize &deserializeHashmap, const int32_t groupByColSize, std::vector<VectorBatch *> &result);
};

class HashAggregationOperatorFactory : public AggregationCommonOperatorFactory {
public:
    Operator *CreateOperator() override;

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
    GroupByFieldHandleType handleType;
    void ChooseGroupByType();
};
} // end of namespace op
} // end of namespace omniruntimef
#endif