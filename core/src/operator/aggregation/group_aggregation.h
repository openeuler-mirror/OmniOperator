/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Hash Aggregation Header
 */
#ifndef GROUP_AGGREGATION_H
#define GROUP_AGGREGATION_H

#include "aggregation.h"
#include "../hash_util.h"
#include "../../vector/int_vector.h"
#include "../../vector/long_vector.h"
#include "../../vector/double_vector.h"
#include "../../vector/decimal128_vector.h"
#include "../../vector/varchar_vector.h"

const int32_t MAX_TABLE_SIZE_IN_BYTES = 1024 * 1024;
namespace omniruntime {
namespace op {
using RowIterator = std::unordered_map<uint64_t, std::vector<GroupBySlot>, HashUtil>::iterator;

class HashAggregationOperatorFactory;

class HashAggregationOperator : public AggregationCommonOperator {
public:
    HashAggregationOperator(std::vector<ColumnIndex> groupByCol, std::vector<ColumnIndex> aggCol,
        std::vector<std::unique_ptr<Aggregator>> aggs, bool inputRaw, bool outputPartial)
        : groupByCols(groupByCol), aggCols(aggCol), AggregationCommonOperator(std::move(aggs), inputRaw, outputPartial)
    {}

    ~HashAggregationOperator() override {}

    int32_t AddInput(omniruntime::vec::VectorBatch *data) override;

    int32_t GetOutput(std::vector<omniruntime::vec::VectorBatch *> &data) override;

    explicit HashAggregationOperator(std::vector<std::unique_ptr<Aggregator>> aggregators)
        : AggregationCommonOperator(std::move(aggregators), true, false)
    {}

    OmniStatus Init() override;

    OmniStatus Close() override;
    OmniStatus CloseGroupBy();
    OmniStatus CloseAgg();

    void PreLoop(omniruntime::vec::VectorBatch *vecBatch);

    void InLoop(omniruntime::vec::Vector **vectors, uint32_t offset, const int32_t *types, int32_t colNum,
        const int32_t *groupByColIdx, int32_t groupByColNum, const int32_t *aggColIdx, int32_t aggColNum,
        const int32_t *aggFuncTypes);

    void PostLoop(omniruntime::vec::VectorBatch *vecBatch) const;

private:
    int32_t GetRowSize(std::vector<omniruntime::vec::VecType> &types, int32_t columnCount);

    void FillGroupByVectors(omniruntime::vec::VectorBatch *vecBatch, int startIndex, int endIndex,
        RowIterator &rowIterator, int32_t rowCount);

    void FillAggVectors(omniruntime::vec::VectorBatch *vecBatch, int startIndex, int endIndex, RowIterator &rowIterator,
        int32_t rowCount);

    void FillNormalAgg(omniruntime::vec::VectorBatch * vecBatch, int32_t aggIndex, int32_t colIndex, int32_t rowCount,
         RowIterator &rowIterator);

    void FillAvgAgg(omniruntime::vec::VectorBatch* vecBatch,
        int32_t aggIndex, int32_t colIndex, int32_t rowCount, RowIterator &rowIterator);

private:
    friend class HashAggregationOperatorFactory;
    std::unordered_map<uint64_t, std::vector<GroupBySlot>, HashUtil> groupedRows;
    std::vector<ColumnIndex> groupByCols;
    std::vector<ColumnIndex> aggCols;
    void FillVectorVal(vec::VectorBatch *vecBatch, int32_t rowCount, RowIterator &tempRowIterator, int colIndex,
                       int groupByIndex);
    void FillAggDateValue(int32_t colIndex, int32_t rowCount, RowIterator &rowIterator,
                          omniruntime::vec::IntVector *vector);

    void FillAggDecimal64Value(int32_t colIndex, int32_t rowCount, RowIterator &rowIterator,
                               omniruntime::vec::LongVector *vector);

    void FillAggDoubleValue(int32_t colIndex, int32_t rowCount, RowIterator &rowIterator,
                            omniruntime::vec::DoubleVector *vector);

    void FillAggDecimal128Value(int32_t colIndex, int32_t rowCount, RowIterator &rowIterator,
                                omniruntime::vec::Decimal128Vector *vector);

    void FillAggVarCharValue(int32_t colIndex, int32_t rowCount, RowIterator &rowIterator,
                             omniruntime::vec::VarcharVector *vector);

    void FillGroupByDate32(int32_t rowCount, RowIterator &tempRowIterator, int colIndex,
                           vec::IntVector *vector);

    void FillGroupByDecimal64(int32_t rowCount, RowIterator &tempRowIterator, int colIndex,
                              vec::LongVector *vector);

    void FillGroupByDouble(int32_t rowCount, RowIterator &tempRowIterator, int colIndex,
                           vec::DoubleVector *vector);

    void FillGroupByVarChar(int32_t rowCount, RowIterator &tempRowIterator, int colIndex,
                            vec::VarcharVector *vector);

    void FillGroupByDecimal128(int32_t rowCount, RowIterator &tempRowIterator, int colIndex,
                               vec::Decimal128Vector *vector);
};

class HashAggregationOperatorFactory : public AggregationCommonOperatorFactory {
public:
    Operator *CreateOperator() override;

    HashAggregationOperatorFactory(PrepareContext groupByCol, PrepareContext groupByType, PrepareContext aggCol,
        PrepareContext aggType, PrepareContext aggFuncType, bool inputRaw, bool outputPartial)
        : groupByColContext(groupByCol),
          groupByTypeContext(groupByType),
          aggColContext(aggCol),
          aggTypeContext(aggType),
          aggFuncTypeContext(aggFuncType),
          AggregationCommonOperatorFactory(inputRaw, outputPartial)
    {}

    ~HashAggregationOperatorFactory() override {}
    OmniStatus Init() override;
    OmniStatus Close() override;

private:
    PrepareContext groupByColContext;
    std::vector<uint32_t> groupByColIdx;
    PrepareContext groupByTypeContext;
    std::vector<uint32_t> groupByTypes;
    PrepareContext aggColContext;
    std::vector<uint32_t> aggColIdx;
    PrepareContext aggTypeContext;
    std::vector<uint32_t> aggTypes;
    PrepareContext aggFuncTypeContext;
    std::vector<std::unique_ptr<AggregatorFactory>> aggregatorFactories;
};

using HashAggModule = omniruntime::op::HashAggregationOperator *(*)(HashAggregationOperatorFactory *);
} // end of namespace op
} // end of namespace omniruntimef
#endif