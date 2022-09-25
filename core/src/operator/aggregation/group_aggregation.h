/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
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

namespace omniruntime {
namespace op {
using namespace vec;
using BucketIterator = std::unordered_map<uint64_t, std::vector<std::vector<AggregateState>>, HashUtil>::iterator;
using ChainIterator = std::vector<std::vector<AggregateState>>::iterator;

class HashAggregationOperatorFactory;
class HashAggregationOperator;

using HashFunc = void (*)(Vector *vector, const uint32_t r, const int32_t *ri, uint64_t *hashVal);
using HashFuncVect = void (*)(Vector *vector, const uint32_t s, const uint32_t r, uint64_t *hashVal);
using DuplicateKeyValue = void (*)(AggregateState &state, Vector *vector, const uint32_t offset,
    ExecutionContext *context);
using IsSameNodeFunc = void (*)(Vector *vector, const uint32_t offset, AggregateState &slot, bool &isSame);
using SetVector = void (*)(VectorBatch *vecBatch, DataType &type, int32_t columnIndex, VectorAllocator *vecAllocator,
    int32_t rowCount);
using FillValue = void (*)(VectorBatch *vecBatch, int32_t rowIndex, ChainIterator &tempRowIterator, int colIndex);

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

template <typename V, typename D>
void HashFuncImpl(Vector *vector, const uint32_t rowCount, const int32_t *rowIndexes, uint64_t *combinedHash);
void HashVarcharFuncImpl(Vector *vector, const uint32_t rowCount, const int32_t *rowIndexes, uint64_t *combinedHash);
void HashDecimalFunc(Vector *vector, const uint32_t rowCount, const int32_t *rowIndexes, uint64_t *combinedHash);

template <typename V, typename D>
void HashFuncVectImpl(Vector *vector, const uint32_t start, const uint32_t rowCount, uint64_t *combinedHash);
void HashVarcharVectFuncImpl(Vector *vector, const uint32_t start, const uint32_t rowCount, uint64_t *combinedHash);
void HashDecimalVectFunc(Vector *vector, const uint32_t start, const uint32_t rowCount, uint64_t *combinedHash);

template <typename V, typename D>
void IsSameNodeFuncImpl(Vector *vector, const uint32_t offset, AggregateState &slot, bool &isSame);
void IsSameNodeFuncVarcharImpl(Vector *vector, const uint32_t offset, AggregateState &slot, bool &isSame);

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
void FillValueImpl(VectorBatch *vecBatch, int32_t rowIndex, ChainIterator &tempRowIterator, int colIndex);

void FillVarcharValue(VectorBatch *vecBatch, int32_t rowIndex, ChainIterator &tempRowIterator, int colIndex);

class HashAggregationOperator : public AggregationCommonOperator {
public:
    HashAggregationOperator(std::vector<ColumnIndex> &groupByCols, std::vector<std::vector<int32_t>> &aggInputCols,
        uint32_t aggInputColsSize, std::vector<DataTypes> &aggInputTypes, std::vector<DataTypes> &aggOutputTypes,
        std::vector<std::unique_ptr<Aggregator>> aggs, std::vector<bool> &inputRaws, std::vector<bool> &outputPartials)
        : AggregationCommonOperator(std::move(aggs), inputRaws, outputPartials),
          groupByCols(groupByCols),
          aggInputCols(aggInputCols),
          aggInputColsSize(aggInputColsSize),
          aggInputTypes(aggInputTypes),
          aggOutputTypes(aggOutputTypes)
    {
        groupedRows.reserve(DEFAULT_HASHTABLE_SIZE);
    }

    ~HashAggregationOperator() override {}

    int32_t AddInput(VectorBatch *data) override;

    int32_t GetOutput(std::vector<VectorBatch *> &data) override;

    OmniStatus Init() override;

    OmniStatus Close() override;
    void PreLoop(VectorBatch *vecBatch);
    void InLoop(VectorBatch *vecBatch, uint32_t offset, const int32_t *groupByColIdx, int32_t groupByColNum,
        int32_t aggNum);
    void PostLoop(VectorBatch *vecBatch) const;
    std::unordered_map<uint64_t, std::vector<std::vector<AggregateState>>, HashUtil> &GetStates()
    {
        return groupedRows;
    }

private:
    std::vector<BucketIterator> FindBuckets(uint64_t *hash, int32_t blockSize);
    int32_t GetRowSizeAndOutputTypes(std::vector<DataTypePtr> &types, int32_t columnCount);

    void FillGroupByVectors(VectorBatch *vecBatch, int startIndex, int endIndex, ChainIterator &rowIterator,
        int32_t rowIndex);

    void FillAggVectors(VectorBatch *vecBatch, int startIndex, int endIndex, ChainIterator &rowIterator,
        int32_t rowCount);

private:
    friend class HashAggregationOperatorFactory;
    template <typename V, typename D>
    friend void FillValueImpl(VectorBatch *vecBatch, int32_t rowIndex, ChainIterator &tempRowIterator, int colIndex);
    friend void FillVarcharValue(VectorBatch *vecBatch, int32_t rowIndex, ChainIterator &tempRowIterator, int colIndex);
    std::unordered_map<uint64_t, std::vector<std::vector<AggregateState>>, HashUtil> groupedRows;
    std::vector<ColumnIndex> groupByCols;
    std::vector<std::vector<int32_t>> aggInputCols;
    uint32_t aggInputColsSize;
    std::vector<DataTypes> aggInputTypes;
    std::vector<DataTypes> aggOutputTypes;
    std::unique_ptr<ExecutionContext> executionContext;

    void FillOutputVecBatch(std::vector<VectorBatch *> &result, uint32_t groupByColSize, uint32_t colCount,
                            int32_t rowsPerBatch, std::vector<ChainIterator> &allGroups);
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
};
} // end of namespace op
} // end of namespace omniruntimef
#endif