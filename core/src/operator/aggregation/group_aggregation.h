/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Hash Aggregation Header
 */
#ifndef GROUP_AGGREGATION_H
#define GROUP_AGGREGATION_H

#include "aggregation.h"
#include "../../vector/vector_types.h"
#include "../hash_util.h"

const int32_t MAX_TABLE_SIZE_IN_BYTES = 1024 * 1024;

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
using RowIterator = std::unordered_map<uint64_t, std::vector<GroupBySlot>, HashUtil>::iterator;

class HashAggregationOperatorFactory;
class HashAggregationOperator;

using HashFunc = void (*)(Vector *vector, const uint32_t r, const int32_t *ri, int64_t *hashVal);
using DuplicateKeyValue = void *(*)(Vector *vector, const uint32_t offset);
using SetVector = void (*)(VectorBatch *vecBatch, VecType &type, int32_t columnIndex, VectorAllocator *vecAllocator,
    int32_t rowCount);
using FillValue = void (*)(HashAggregationOperator &hashOperator, VectorBatch *vecBatch, int32_t rowCount,
    RowIterator &tempRowIterator, int colIndex);
using ReleaseMemory = void (*)(RowIterator &rowIterator, int32_t columnIndex, VecType &type);

using FunctionByDataType = struct {
    VecTypeId vecTypeId;
    HashFunc hashFunc;
    DuplicateKeyValue duplicateKey;
    SetVector setVector;
    FillValue fillValue;
    ReleaseMemory releaseMemory;
};

using HashAggModule = HashAggregationOperator *(*)(HashAggregationOperatorFactory *);

template <typename V, typename D>
void HashFuncImpl(Vector *vector, const uint32_t rowCount, const int32_t *rowIndexes, int64_t *combinedHash);
void HashVarcharFuncImpl(Vector *vector, const uint32_t rowCount, const int32_t *rowIndexes, int64_t *combinedHash);
void HashDecimalFunc(Vector *vector, const uint32_t rowCount, const int32_t *rowIndexes, int64_t *combinedHash);

template <typename V, typename D> void *DuplicateKeyValueImpl(Vector *vector, const uint32_t offset);
void *DuplicateVarcharKeyValue(Vector *vector, const uint32_t offset);

template <typename V>
void SetVectorImpl(VectorBatch *vecBatch, VecType &type, int32_t columnIndex, VectorAllocator *vecAllocator,
    int32_t rowCount);
void SetVarcharVector(VectorBatch *vecBatch, VecType &type, int32_t columnIndex, VectorAllocator *vecAllocator,
    int32_t rowCount);
void SetContainerVector(VectorBatch *vecBatch, VecType &type, int32_t columnIndex, VectorAllocator *vecAllocator,
    int32_t rowCount);

template <typename V, typename D>
void FillValueImpl(HashAggregationOperator &hashOperator, VectorBatch *vecBatch, int32_t rowCount,
    RowIterator &tempRowIterator, int colIndex);

void FillVarcharValue(HashAggregationOperator &hashOperator, VectorBatch *vecBatch, int32_t rowCount,
    RowIterator &tempRowIterator, int colIndex);

template <typename T> void ReleaseMemoryImpl(RowIterator &rowIterator, int32_t columnIndex, VecType &type);

class HashAggregationOperator : public AggregationCommonOperator {
public:
    HashAggregationOperator(std::vector<ColumnIndex> groupByCol, std::vector<ColumnIndex> aggCol,
        std::vector<std::unique_ptr<Aggregator>> aggs, bool inputRaw, bool outputPartial)
        : groupByCols(groupByCol), aggCols(aggCol), AggregationCommonOperator(std::move(aggs), inputRaw, outputPartial)
    {}

    ~HashAggregationOperator() override {}

    int32_t AddInput(VectorBatch *data) override;

    int32_t GetOutput(std::vector<VectorBatch *> &data) override;

    explicit HashAggregationOperator(std::vector<std::unique_ptr<Aggregator>> aggregators)
        : AggregationCommonOperator(std::move(aggregators), true, false)
    {}

    OmniStatus Init() override;

    OmniStatus Close() override;
    OmniStatus CloseGroupBy();
    OmniStatus CloseAgg();
    void PreLoop(VectorBatch *vecBatch);
    void InLoop(Vector **vectors, uint32_t offset, const int32_t *types, int32_t colNum, const int32_t *groupByColIdx,
        int32_t groupByColNum, const int32_t *aggColIdx, int32_t aggColNum, const int32_t *aggFuncTypes);
    void PostLoop(VectorBatch *vecBatch) const;
    ;
    static constexpr FunctionByDataType FUNCTIONS[VEC_TYPE_MAX_COUNT] = {
        {OMNI_VEC_TYPE_NONE, nullptr, nullptr, nullptr, nullptr, nullptr},
        {
            OMNI_VEC_TYPE_INT, HashFuncImpl<IntVector, int32_t>, DuplicateKeyValueImpl<IntVector, int32_t>,
            SetVectorImpl<IntVector>, FillValueImpl<IntVector, int32_t>, ReleaseMemoryImpl<int32_t>
        },
        {
            OMNI_VEC_TYPE_LONG, HashFuncImpl<LongVector, int64_t>, DuplicateKeyValueImpl<LongVector, int64_t>,
            SetVectorImpl<LongVector>, FillValueImpl<LongVector, int64_t>, ReleaseMemoryImpl<int64_t>
        },
        {
            OMNI_VEC_TYPE_DOUBLE, HashFuncImpl<DoubleVector, double>, DuplicateKeyValueImpl<DoubleVector, double>,
            SetVectorImpl<DoubleVector>, FillValueImpl<DoubleVector, double>, ReleaseMemoryImpl<double>
        },
        {OMNI_VEC_TYPE_BOOLEAN, nullptr, nullptr, nullptr, nullptr, nullptr},
        {OMNI_VEC_TYPE_SHORT, nullptr, nullptr, nullptr, nullptr, nullptr},
        {
            OMNI_VEC_TYPE_DECIMAL64, HashFuncImpl<LongVector, int64_t>, DuplicateKeyValueImpl<LongVector, int64_t>,
            SetVectorImpl<LongVector>, FillValueImpl<LongVector, int64_t>, ReleaseMemoryImpl<int64_t>
        },
        {
            OMNI_VEC_TYPE_DECIMAL128, HashDecimalFunc, DuplicateKeyValueImpl<Decimal128Vector, Decimal128>,
            SetVectorImpl<Decimal128Vector>, FillValueImpl<Decimal128Vector, Decimal128>, ReleaseMemoryImpl<Decimal128>
        },
        {
            OMNI_VEC_TYPE_DATE32, HashFuncImpl<IntVector, int32_t>, DuplicateKeyValueImpl<IntVector, int32_t>,
            SetVectorImpl<IntVector>, FillValueImpl<IntVector, int32_t>, ReleaseMemoryImpl<int32_t>
        },
        {OMNI_VEC_TYPE_DATE64, nullptr, nullptr, nullptr, nullptr, nullptr},
        {OMNI_VEC_TYPE_TIME32, nullptr, nullptr, nullptr, nullptr, nullptr},
        {OMNI_VEC_TYPE_TIME64, nullptr, nullptr, nullptr, nullptr, nullptr},
        {OMNI_VEC_TYPE_TIMESTAMP, nullptr, nullptr, nullptr, nullptr, nullptr},
        {OMNI_VEC_TYPE_INTERVAL_MONTHS, nullptr, nullptr, nullptr, nullptr, nullptr},
        {OMNI_VEC_TYPE_INTERVAL_DAY_TIME, nullptr, nullptr, nullptr, nullptr, nullptr},
        {
            OMNI_VEC_TYPE_VARCHAR, HashVarcharFuncImpl, DuplicateVarcharKeyValue, SetVarcharVector, FillVarcharValue,
            ReleaseMemoryImpl<std::string>
        },
        {OMNI_VEC_TYPE_DICTIONARY, nullptr, nullptr, nullptr, nullptr, nullptr},
        {OMNI_VEC_TYPE_CONTAINER, nullptr, nullptr, SetContainerVector, nullptr, nullptr},
    };

private:
    int32_t GetRowSize(std::vector<VecType> &types, int32_t columnCount);

    void FillGroupByVectors(VectorBatch *vecBatch, int startIndex, int endIndex, RowIterator &rowIterator,
        int32_t rowCount);

    void FillAggVectors(VectorBatch *vecBatch, int startIndex, int endIndex, RowIterator &rowIterator,
        int32_t rowCount);

    void FillNormalAgg(VectorBatch *vecBatch, int32_t aggIndex, int32_t colIndex, int32_t rowCount,
        RowIterator &rowIterator);

    void FillAvgAgg(VectorBatch *vecBatch, int32_t aggIndex, int32_t colIndex, int32_t rowCount,
        RowIterator &rowIterator);

private:
    friend class HashAggregationOperatorFactory;
    template <typename V, typename D>
    friend void FillValueImpl(HashAggregationOperator &hashOperator, VectorBatch *vecBatch, int32_t rowCount,
        RowIterator &tempRowIterator, int colIndex);
    friend void FillVarcharValue(HashAggregationOperator &hashOperator, VectorBatch *vecBatch, int32_t rowCount,
        RowIterator &tempRowIterator, int colIndex);
    std::unordered_map<uint64_t, std::vector<GroupBySlot>, HashUtil> groupedRows;
    std::vector<ColumnIndex> groupByCols;
    std::vector<ColumnIndex> aggCols;
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