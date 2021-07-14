/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Aggregation Base Class
 * Author: Songling Liu
 * Create: 2021-07-01
 * Notes: None
 */
#ifndef NON_GROUP_AGGREGATION_H
#define NON_GROUP_AGGREGATION_H

#include "aggregation.h"

namespace omniruntime {
namespace op {
class AggregationOperator : public AggregationCommonOperator {
public:
    AggregationOperator(std::vector<ColumnIndex> aggCol,
                        std::vector<unique_ptr<Aggregator>> aggs,
                        bool inputRaw,
                        bool outputPartial)
        : aggCols(aggCol), AggregationCommonOperator(aggs, inputRaw, outputPartial)
    {}

    ~AggregationOperator() final {}
    int32_t AddInput(VectorBatch *vecBatch) override;
    int32_t GetOutput(std::vector<VectorBatch *> &data) override;
    inline void InLoop(Vector **vectors, uint32_t offset, int32_t colNum, int32_t *aggDataType, int32_t *aggFuncType);
    inline void PreLoop(VectorBatch *vecBatch)
    {
        sourceTypes = new int32_t[aggCols.size()];
        int32_t idx = 0;
        for (auto &c : aggCols) {
            sourceTypes[idx++] = (int32_t)c.type;
        }
    }
    inline void PostLoop(VectorBatch *vecBatch) {}

    void FillResultVectors(VectorBatch *vecBatch);

private:
    std::vector<ColumnIndex> aggCols;
};

class AggregationOperatorFactory : public AggregationCommonOperatorFactory {
public:
    Operator *CreateOperator() override;

    AggregationOperatorFactory(PrepareContext aggType, PrepareContext aggFuncType, bool inputRaw, bool outputPartial)
        : aggTypeContext(aggType),
          aggFuncTypeContext(aggFuncType),
          AggregationCommonOperatorFactory(inputRaw, outputPartial)
    {}

    ~AggregationOperatorFactory() override {}

private:
    PrepareContext aggTypeContext;
    PrepareContext aggFuncTypeContext;
};
} // end op
} // edn omniruntime

#endif