/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Aggregation Base Class
 * Author: Songling Liu
 * Create: 2021-07-01
 * Notes: None
 */
#ifndef GROUP_AGGREGATION_H
#define GROUP_AGGREGATION_H

#include "aggregation.h"

const int32_t MAX_TABLE_SIZE_IN_BYTES = 1024 * 1024;
namespace omniruntime {
namespace op {
using RowIterator = std::unordered_map<uint64_t, std::vector<GroupBySlot>>::iterator;

class MultiChannelHash {
public:
    MultiChannelHash() : result(0) {}
    virtual ~MultiChannelHash() {}
    uint64_t CombineHash(uint64_t result, uint64_t value) const
    {
        return (31 * result + value);
    }

private:
    uint64_t result;
};

using HashPosition = struct HashPosition {
    uint64_t hashVal;
    uint32_t offset;
};

class HashAggregationOperatorFactory;

class HashAggregationOperator : public AggregationCommonOperator {
public:
    HashAggregationOperator(std::vector<ColumnIndex> groupByCol, std::vector<ColumnIndex> aggCol,
        std::vector<Aggregator *> aggs, bool inputRaw, bool outputPartial)
        : groupByCols(groupByCol), aggCols(aggCol), AggregationCommonOperator(aggs, inputRaw, outputPartial)
    {}

    int32_t AddInput(VectorBatch *data) override;

    int32_t GetOutput(std::vector<VectorBatch *> &data) override;

    HashAggregationOperator(std::vector<Aggregator *> aggregators) : AggregationCommonOperator(aggregators, true, false)
    {}

    ~HashAggregationOperator()
    {
        // delete map
        for (auto &item : groupedRows) {
            for (int32_t idx = 0; idx < item.second.size(); ++idx) {
                switch (groupByCols[idx].type) {
                    case OMNI_VEC_TYPE_INT: {
                        delete reinterpret_cast<int32_t *>(item.second[idx].val);
                        break;
                    }
                    case OMNI_VEC_TYPE_LONG: {
                        delete reinterpret_cast<int64_t *>(item.second[idx].val);
                        break;
                    }
                    case OMNI_VEC_TYPE_DOUBLE: {
                        delete reinterpret_cast<double *>(item.second[idx].val);
                        break;
                    }
                    default:
                        break;
                }
            }
        }
        groupedRows.clear();
        // delete all aggregators
        for (auto &agg : aggregators) {
            delete agg;
        }
    }
    void preLoop(VectorBatch *vecBatch);

    void inLoop(Vector **vectors, uint32_t offset, int32_t *types, int32_t colNum, int32_t *groupByColIdx,
        int32_t groupByColNum, int32_t *aggColIdx, int32_t aggColNum, int32_t *aggFuncTypes);

    void postLoop(VectorBatch *vecBatch);

private:
    int32_t getRowSize(int32_t *types, int32_t columnCount);

    void fillGroupByVectors(VectorBatch *vecBatch, int startIndex, int endIndex, RowIterator &rowIterator,
        int32_t rowCount);

    void fillAggVectors(VectorBatch *vecBatch, int startIndex, int endIndex, int32_t rowCount);

private:
    friend class HashAggregationOperatorFactory;
    std::unordered_map<uint64_t, std::vector<GroupBySlot>> groupedRows;
    std::vector<ColumnIndex> groupByCols;
    std::vector<ColumnIndex> aggCols;
    uint32_t *inputColTypes;
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

private:
    PrepareContext groupByColContext;
    PrepareContext groupByTypeContext;
    PrepareContext aggColContext;
    PrepareContext aggTypeContext;
    PrepareContext aggFuncTypeContext;
};

using hashagg_module = omniruntime::op::HashAggregationOperator *(*)(HashAggregationOperatorFactory *);
} // end of namespace op
} // end of namespace omniruntimef
#endif