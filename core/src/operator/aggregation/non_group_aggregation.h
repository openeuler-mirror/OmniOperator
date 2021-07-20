/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Aggregation Header
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
        : aggCols(aggCol), AggregationCommonOperator(std::move(aggs), inputRaw, outputPartial)
    {}

    ~AggregationOperator() override {}
    int32_t AddInput(VectorBatch *vecBatch) override;
    int32_t GetOutput(std::vector<VectorBatch *> &data) override;
    inline void InLoop(Vector **vectors, uint32_t offset, int32_t colNum, const int32_t *aggDataType, const int32_t *aggFuncType);
    inline void PreLoop(VectorBatch *vecBatch)
    {
        sourceTypes = new int32_t[aggCols.size()];
        int32_t idx = 0;
        for (auto &c : aggCols) {
            sourceTypes[idx++] = static_cast<int32_t>(c.type);
        }
    }
    inline void PostLoop(VectorBatch *vecBatch) const {}

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
    OmniStatus Init() override;
    OmniStatus Close() override;
private:
    PrepareContext aggTypeContext;
    std::vector<uint32_t> aggTypes;
    PrepareContext aggFuncTypeContext;
    std::vector<std::unique_ptr<AggregatorFactory>> aggregatorFactories;
};
} // end op
} // edn omniruntime

#endif