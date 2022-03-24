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

#ifdef DEBUG_OPERATOR
#define VERIFY_INPUT_TYPES(vector_batch, group_by_idx, group_by_num, agg_idx, agg_num, operator_types, agg_func_types) \
    do {                                                                                                               \
        for (int32_t i = 0; i < group_by_num; ++i) {                                                                   \
            auto vector = vector_batch->GetVector(group_by_idx[i]);                                                    \
            auto typeId = vector->GetTypeId();                                                                         \
            if (vector->GetEncoding() == OMNI_VEC_ENCODING_DICTIONARY) {                                               \
                typeId = static_cast<DictionaryVector *>(vector)->ExtractDictionaryTypeId();                           \
            }                                                                                                          \
            if (typeId != operator_types[group_by_idx[i]]) {                                                           \
                LogWarn("Group by vector type %d != operator column type %d!", typeId,                                 \
                    operator_types[group_by_idx[i]]);                                                                  \
            }                                                                                                          \
        }                                                                                                              \
        uint32_t aggInputIndex = 0;                                                                                    \
        for (int32_t i = 0; i < agg_num; ++i) {                                                                        \
            uint32_t aggregateType = agg_func_types[i];                                                                \
            if (aggregateType != OMNI_AGGREGATION_TYPE_COUNT_ALL) {                                                    \
                auto vector = vector_batch->GetVector(agg_idx[aggInputIndex]);                                         \
                auto typeId = vector->GetTypeId();                                                                     \
                auto operatorType = operator_types[agg_idx[aggInputIndex]];                                            \
                aggInputIndex++;                                                                                       \
                if (vector->GetEncoding() == OMNI_VEC_ENCODING_DICTIONARY) {                                           \
                    typeId = static_cast<DictionaryVector *>(vector)->ExtractDictionaryTypeId();                       \
                }                                                                                                      \
                if (typeId != operatorType) {                                                                          \
                    LogWarn("Aggregate vector type %d != operator column type %d!", typeId, operatorType);             \
                }                                                                                                      \
            }                                                                                                          \
        }                                                                                                              \
    } while (0)
#else
#define VERIFY_INPUT_TYPES(vector_batch, group_by_idx, group_by_num, agg_idx, agg_num, operator_types, agg_func_types)
#endif

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
    HashAggregationOperator(std::vector<ColumnIndex> groupByCols, std::vector<int32_t> &aggInputCols,
        omniruntime::type::DataTypes &aggInputTypes, omniruntime::type::DataTypes &aggOutputTypes,
        std::vector<std::unique_ptr<Aggregator>> aggs, bool inputRaw, bool outputPartial)
        : groupByCols(groupByCols),
          aggInputCols(aggInputCols),
          aggInputTypes(aggInputTypes),
          aggOutputTypes(aggOutputTypes),
          AggregationCommonOperator(std::move(aggs), inputRaw, outputPartial)
    {
        groupedRows.reserve(DEFAULT_HASHTABLE_SIZE);
    }

    ~HashAggregationOperator() override {}

    int32_t AddInput(VectorBatch *data) override;

    int32_t GetOutput(std::vector<VectorBatch *> &data) override;

    explicit HashAggregationOperator(std::vector<std::unique_ptr<Aggregator>> aggregators,
        omniruntime::type::DataTypes aggInputTypes, omniruntime::type::DataTypes aggOutputTypes)
        : AggregationCommonOperator(std::move(aggregators), true, false),
          aggInputTypes(aggInputTypes),
          aggOutputTypes(aggOutputTypes)
    {}

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
    int32_t GetRowSizeAndOutputTypes(std::vector<DataType> &types, int32_t columnCount);

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
    std::vector<int32_t> aggInputCols;
    omniruntime::type::DataTypes aggInputTypes;
    omniruntime::type::DataTypes aggOutputTypes;
    std::unique_ptr<ExecutionContext> executionContext;
};

class HashAggregationOperatorFactory : public AggregationCommonOperatorFactory {
public:
    Operator *CreateOperator() override;

    HashAggregationOperatorFactory(PrepareContext groupByCol, DataTypes groupInputTypes, PrepareContext aggCol,
        DataTypes aggInputTypes, DataTypes aggOutputTypes, PrepareContext aggFuncTypes, bool inputRaw,
        bool outputPartial)
        : groupByColsContext(groupByCol),
          groupByTypes(groupInputTypes),
          aggInputColsContext(aggCol),
          aggInputTypes(aggInputTypes),
          aggOutputTypes(aggOutputTypes),
          aggFuncTypesContext(aggFuncTypes),
          AggregationCommonOperatorFactory(inputRaw, outputPartial)
    {}

    ~HashAggregationOperatorFactory() override {}
    OmniStatus Init() override;
    OmniStatus Close() override;

private:
    PrepareContext groupByColsContext;
    std::vector<int32_t> groupByColIdx;
    DataTypes groupByTypes;
    PrepareContext aggInputColsContext;
    std::vector<int32_t> aggInputCols;
    DataTypes aggInputTypes;
    DataTypes aggOutputTypes;
    PrepareContext aggFuncTypesContext;
    std::vector<std::unique_ptr<AggregatorFactory>> aggregatorFactories;
};
} // end of namespace op
} // end of namespace omniruntimef
#endif