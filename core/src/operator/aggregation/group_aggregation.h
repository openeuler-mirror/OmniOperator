/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Hash Aggregation Header
 */
#ifndef GROUP_AGGREGATION_H
#define GROUP_AGGREGATION_H

#include "definitions.h"
#include "aggregation.h"
#include "../../vector/vector_types.h"
#include "../hash_util.h"
#include "../execution_context.h"

#ifdef DEBUG_OPERATOR
#define VERIFY_INPUT_TYPES(vector_batch, group_by_idx, group_by_num, agg_idx, agg_num, operator_types)               \
    do {                                                                                                             \
        for (int32_t i = 0; i < group_by_num; ++i) {                                                                 \
            auto vector = vector_batch->GetVector(group_by_idx[i]);                                                  \
            auto typeId = vector->GetTypeId();                                                                       \
            if (typeId == OMNI_VEC_TYPE_DICTIONARY) {                                                                \
                typeId = static_cast<DictionaryVector *>(vector)->ExtractDictionaryTypeId();                         \
            }                                                                                                        \
            if (typeId != operator_types[group_by_idx[i]]) {                                                         \
                LogWarn("Group by vector type %d != operator column type %d!", typeId,                               \
                    operator_types[group_by_idx[i]]);                                                                \
            }                                                                                                        \
        }                                                                                                            \
        for (int32_t i = 0; i < agg_num; ++i) {                                                                      \
            auto vector = vector_batch->GetVector(agg_idx[i]);                                                       \
            auto typeId = vector->GetTypeId();                                                                       \
            if (typeId == OMNI_VEC_TYPE_DICTIONARY) {                                                                \
                typeId = static_cast<DictionaryVector *>(vector)->ExtractDictionaryTypeId();                         \
            }                                                                                                        \
            if (typeId != operator_types[agg_idx[i]]) {                                                              \
                LogWarn("Aggregate vector type %d != operator column type %d!", typeId, operator_types[agg_idx[i]]); \
            }                                                                                                        \
        }                                                                                                            \
    } while (0)
#else
#define VERIFY_INPUT_TYPES(vector_batch, group_by_idx, group_by_num, agg_idx, agg_num, operator_types)
#endif

namespace omniruntime {
namespace op {
using namespace vec;
using BucketIterator = std::unordered_map<uint64_t, std::vector<std::vector<GroupBySlot>>, HashUtil>::iterator;
using ChainIterator = std::vector<std::vector<GroupBySlot>>::iterator;

class HashAggregationOperatorFactory;
class HashAggregationOperator;

using HashFunc = void (*)(Vector *vector, const uint32_t r, const int32_t *ri, uint64_t *hashVal);
using HashFuncVect = void (*)(Vector *vector, const uint32_t s, const uint32_t r, uint64_t *hashVal);
using DuplicateKeyValue = void (*)(GroupBySlot &groupBySlot, Vector *vector, const uint32_t offset, ExecutionContext *context);
using IsSameNodeFunc = void(*)(Vector* vector, const uint32_t offset, GroupBySlot &slot, bool &isSame);
using SetVector = void (*)(VectorBatch *vecBatch, VecType &type, int32_t columnIndex, VectorAllocator *vecAllocator,
    int32_t rowCount);
using FillValue = void (*)(VectorBatch *vecBatch, int32_t rowIndex, ChainIterator &tempRowIterator, int colIndex);
using ReleaseMemory = void (*)(GroupBySlot &rowIterator, int32_t columnIndex, VecType &type);

using FunctionByDataType = struct {
    VecTypeId vecTypeId;
    HashFunc hashFunc;
    HashFuncVect hashFuncVect;
    IsSameNodeFunc isSameNode;
    DuplicateKeyValue duplicateKey;
    SetVector setVector;
    FillValue fillValue;
    ReleaseMemory releaseMemory;
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

template<typename V, typename D>
void IsSameNodeFuncImpl(Vector* vector, const uint32_t offset, GroupBySlot &slot, bool &isSame);
void IsSameNodeFuncVarcharImpl(Vector* vector, const uint32_t offset, GroupBySlot &slot, bool &isSame);

template <typename V, typename D> void DuplicateKeyValueImpl(GroupBySlot &groupBySlot, Vector *vector, const uint32_t offset, ExecutionContext *context);
void DuplicateVarcharKeyValue(GroupBySlot &groupBySlot, Vector *vector, const uint32_t offset, ExecutionContext *context);

template <typename V>
void SetVectorImpl(VectorBatch *vecBatch, VecType &type, int32_t columnIndex, VectorAllocator *vecAllocator,
    int32_t rowCount);
void SetVarcharVector(VectorBatch *vecBatch, VecType &type, int32_t columnIndex, VectorAllocator *vecAllocator,
    int32_t rowCount);
void SetContainerVector(VectorBatch *vecBatch, VecType &type, int32_t columnIndex, VectorAllocator *vecAllocator,
    int32_t rowCount);

template <typename V, typename D>
void FillValueImpl(VectorBatch *vecBatch, int32_t rowIndex,
                   ChainIterator &tempRowIterator, int colIndex);

void FillVarcharValue(VectorBatch *vecBatch, int32_t rowIndex,
                      ChainIterator &tempRowIterator, int colIndex);

template <typename T> void ReleaseMemoryImpl(GroupBySlot& rowIterator, int32_t columnIndex, VecType& type);
void ReleaseMemoryVarcharImpl(GroupBySlot &columnVal, int32_t columnIndex, VecType &type);

class HashAggregationOperator : public AggregationCommonOperator {
public:
    HashAggregationOperator(std::vector<ColumnIndex> groupByCol, std::vector<ColumnIndex> aggCol,
        std::vector<std::unique_ptr<Aggregator>> aggs, bool inputRaw, bool outputPartial)
        : groupByCols(groupByCol), aggCols(aggCol), AggregationCommonOperator(std::move(aggs), inputRaw, outputPartial)
    {
        groupedRows.reserve(DEFAULT_HASHTABLE_SIZE);
    }

    ~HashAggregationOperator() override {}

    int32_t AddInput(VectorBatch *data) override;

    int32_t GetOutput(std::vector<VectorBatch *> &data) override;

    explicit HashAggregationOperator(std::vector<std::unique_ptr<Aggregator>> aggregators)
        : AggregationCommonOperator(std::move(aggregators), true, false)
    {}

    OmniStatus Init() override;

    OmniStatus Close() override;
    void PreLoop(VectorBatch *vecBatch);
    void InLoop(Vector **vectors, uint32_t offset, const int32_t *types, int32_t colNum, const int32_t *groupByColIdx,
        int32_t groupByColNum, const int32_t *aggColIdx, int32_t aggColNum, const int32_t *aggFuncTypes);
    void PostLoop(VectorBatch *vecBatch) const;
    std::unordered_map<uint64_t, std::vector<std::vector<GroupBySlot>>, HashUtil>& GetStates()
    {
        return groupedRows;
    }

    static constexpr FunctionByDataType FUNCTIONS[VEC_TYPE_MAX_COUNT] = {
        {OMNI_VEC_TYPE_NONE, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr},
        {
            OMNI_VEC_TYPE_INT, HashFuncImpl<IntVector, int32_t>, HashFuncVectImpl<IntVector, int32_t>, IsSameNodeFuncImpl<IntVector, int32_t>,
            DuplicateKeyValueImpl<IntVector, int32_t>, SetVectorImpl<IntVector>, FillValueImpl<IntVector, int32_t>,
            ReleaseMemoryImpl<int32_t>
        },
        {
            OMNI_VEC_TYPE_LONG, HashFuncImpl<LongVector, int64_t>, HashFuncVectImpl<LongVector, int64_t>, IsSameNodeFuncImpl<LongVector, int64_t>,
            DuplicateKeyValueImpl<LongVector, int64_t>, SetVectorImpl<LongVector>, FillValueImpl<LongVector, int64_t>,
            ReleaseMemoryImpl<int64_t>
        },
        {
            OMNI_VEC_TYPE_DOUBLE, HashFuncImpl<DoubleVector, double>, HashFuncVectImpl<DoubleVector, double>, IsSameNodeFuncImpl<DoubleVector, double>, DuplicateKeyValueImpl<DoubleVector, double>,
            SetVectorImpl<DoubleVector>, FillValueImpl<DoubleVector, double>, ReleaseMemoryImpl<double>
        },
        {OMNI_VEC_TYPE_BOOLEAN, nullptr, nullptr, nullptr, nullptr, nullptr},
        {OMNI_VEC_TYPE_SHORT, nullptr, nullptr, nullptr, nullptr, nullptr},
        {
            OMNI_VEC_TYPE_DECIMAL64, HashFuncImpl<LongVector, int64_t>, HashFuncVectImpl<LongVector, int64_t>, IsSameNodeFuncImpl<LongVector, int64_t>, DuplicateKeyValueImpl<LongVector, int64_t>,
            SetVectorImpl<LongVector>, FillValueImpl<LongVector, int64_t>, ReleaseMemoryImpl<int64_t>
        },
        {
            OMNI_VEC_TYPE_DECIMAL128, HashDecimalFunc, HashDecimalVectFunc, IsSameNodeFuncImpl<Decimal128Vector, Decimal128>, DuplicateKeyValueImpl<Decimal128Vector, Decimal128>,
            SetVectorImpl<Decimal128Vector>, FillValueImpl<Decimal128Vector, Decimal128>, ReleaseMemoryImpl<Decimal128>
        },
        {
            OMNI_VEC_TYPE_DATE32, HashFuncImpl<IntVector, int32_t>, HashFuncVectImpl<IntVector, int32_t>, IsSameNodeFuncImpl<IntVector, int32_t>, DuplicateKeyValueImpl<IntVector, int32_t>,
            SetVectorImpl<IntVector>, FillValueImpl<IntVector, int32_t>, ReleaseMemoryImpl<int32_t>
        },
        {OMNI_VEC_TYPE_DATE64, nullptr, nullptr, nullptr, nullptr, nullptr},
        {OMNI_VEC_TYPE_TIME32, nullptr, nullptr, nullptr, nullptr, nullptr},
        {OMNI_VEC_TYPE_TIME64, nullptr, nullptr, nullptr, nullptr, nullptr},
        {OMNI_VEC_TYPE_TIMESTAMP, nullptr, nullptr, nullptr, nullptr, nullptr},
        {OMNI_VEC_TYPE_INTERVAL_MONTHS, nullptr, nullptr, nullptr, nullptr, nullptr},
        {OMNI_VEC_TYPE_INTERVAL_DAY_TIME, nullptr, nullptr, nullptr, nullptr, nullptr},
        {
            OMNI_VEC_TYPE_VARCHAR, HashVarcharFuncImpl, HashVarcharVectFuncImpl, IsSameNodeFuncVarcharImpl, DuplicateVarcharKeyValue, SetVarcharVector, FillVarcharValue,
            ReleaseMemoryVarcharImpl
        },
        {OMNI_VEC_TYPE_DICTIONARY, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr},
        {OMNI_VEC_TYPE_CONTAINER, nullptr, nullptr, nullptr, nullptr, SetContainerVector, nullptr, nullptr},
    };

private:
    std::vector<BucketIterator> FindBuckets(uint64_t* hash, int32_t blockSize);
    int32_t GetRowSize(std::vector<VecType> &types, int32_t columnCount);

    void FillGroupByVectors(VectorBatch *vecBatch, int startIndex, int endIndex,
                            ChainIterator &rowIterator, int32_t rowIndex);

    void FillAggVectors(VectorBatch *vecBatch, int startIndex, int endIndex, ChainIterator &rowIterator,
                        int32_t rowCount);

    void FillNormalAgg(VectorBatch *vecBatch, int32_t aggIndex, int32_t colIndex, int32_t rowCount,
                       BucketIterator &rowIterator);

    void FillAvgAgg(VectorBatch *vecBatch, int32_t aggIndex, int32_t colIndex, ChainIterator &rowIterator,
                    int32_t rowIndex);

private:
    friend class HashAggregationOperatorFactory;
    template <typename V, typename D>
    friend void FillValueImpl(VectorBatch *vecBatch, int32_t rowIndex,
                              ChainIterator &tempRowIterator, int colIndex);
    friend void FillVarcharValue(VectorBatch *vecBatch, int32_t rowIndex,
                                 ChainIterator &tempRowIterator, int colIndex);
    std::unordered_map<uint64_t, std::vector<std::vector<GroupBySlot>>, HashUtil> groupedRows;
    std::vector<ColumnIndex> groupByCols;
    std::vector<ColumnIndex> aggCols;
    std::unique_ptr<ExecutionContext> executionContext;
};

class HashAggregationOperatorFactory : public AggregationCommonOperatorFactory {
public:
    Operator *CreateOperator() override;

    HashAggregationOperatorFactory(PrepareContext groupByCol, VecTypes groupInputTypes, PrepareContext aggCol,
        VecTypes aggInputTypes, VecTypes aggOutputTypes, PrepareContext aggFuncTypes, bool inputRaw, bool outputPartial)
        : groupByColContext(groupByCol),
          groupByTypes(groupInputTypes),
          aggColContext(aggCol),
          aggInputTypes(aggInputTypes),
          aggOutputTypes(aggOutputTypes),
          aggFuncTypeContext(aggFuncTypes),
          AggregationCommonOperatorFactory(inputRaw, outputPartial)
    {}

    ~HashAggregationOperatorFactory() override {}
    OmniStatus Init() override;
    OmniStatus Close() override;

private:
    PrepareContext groupByColContext;
    std::vector<uint32_t> groupByColIdx;
    VecTypes groupByTypes;
    PrepareContext aggColContext;
    std::vector<uint32_t> aggColIdx;
    VecTypes aggInputTypes;
    VecTypes aggOutputTypes;
    PrepareContext aggFuncTypeContext;
    std::vector<std::unique_ptr<AggregatorFactory>> aggregatorFactories;
};
} // end of namespace op
} // end of namespace omniruntimef
#endif