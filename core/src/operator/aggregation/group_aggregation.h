/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Hash Aggregation Header
 */
#ifndef GROUP_AGGREGATION_H
#define GROUP_AGGREGATION_H

#include "aggregation.h"
#include "../hash_util.h"

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

    int32_t AddInput(omniruntime::vec::VectorBatch *data) override;

    int32_t GetOutput(std::vector<omniruntime::vec::VectorBatch *> &data) override;

    explicit HashAggregationOperator(std::vector<std::unique_ptr<Aggregator>> aggregators)
        : AggregationCommonOperator(std::move(aggregators), true, false)
    {}

    OmniStatus Close() override;

    ~HashAggregationOperator() override {}

    void PreLoop(omniruntime::vec::VectorBatch *vecBatch);

    void InLoop(omniruntime::vec::Vector **vectors, uint32_t offset, const int32_t *types, int32_t colNum,
        const int32_t *groupByColIdx, int32_t groupByColNum, const int32_t *aggColIdx, int32_t aggColNum,
        const int32_t *aggFuncTypes);

    void PostLoop(omniruntime::vec::VectorBatch *vecBatch) const;

private:
    int32_t GetRowSize(std::vector<int32_t> &types, int32_t columnCount);

    void FillGroupByVectors(omniruntime::vec::VectorBatch *vecBatch, int startIndex, int endIndex,
        RowIterator &rowIterator, int32_t rowCount);

    void FillAggVectors(omniruntime::vec::VectorBatch *vecBatch, int startIndex, int endIndex, RowIterator &rowIterator,
        int32_t rowCount);

private:
    friend class HashAggregationOperatorFactory;
    std::unordered_map<uint64_t, std::vector<GroupBySlot>, HashUtil> groupedRows;
    std::vector<ColumnIndex> groupByCols;
    std::vector<ColumnIndex> aggCols;
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