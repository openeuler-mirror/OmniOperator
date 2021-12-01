/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Aggregation Header
 */
#ifndef NON_GROUP_AGGREGATION_H
#define NON_GROUP_AGGREGATION_H

#include "aggregation.h"
#include "vector/vector_types.h"

namespace omniruntime {
namespace op {
class AggregationOperator : public AggregationCommonOperator {
public:
    AggregationOperator(std::vector<ColumnIndex> aggCol, std::vector<std::unique_ptr<Aggregator>> aggs, bool inputRaw,
        bool outputPartial)
        : aggCols(aggCol), AggregationCommonOperator(std::move(aggs), inputRaw, outputPartial)
    {}

    ~AggregationOperator() override {}
    int32_t AddInput(omniruntime::vec::VectorBatch *vecBatch) override;
    int32_t GetOutput(std::vector<omniruntime::vec::VectorBatch *> &data) override;
    void InLoop(omniruntime::vec::Vector **vectors, uint32_t offset, int32_t colNum, const int32_t *aggDataType,
        const int32_t *aggFuncType);
    inline void PreLoop(omniruntime::vec::VectorBatch *vecBatch)
    {
        sourceTypes = new int32_t[aggCols.size()];
        int32_t idx = 0;
        for (auto &c : aggCols) {
            sourceTypes[idx++] = static_cast<int32_t>(c.input.GetId());
        }
    }
    inline void PostLoop(omniruntime::vec::VectorBatch *vecBatch) const {}

    void FillResultVectors(omniruntime::vec::VectorBatch *vecBatch);

private:
    std::vector<ColumnIndex> aggCols;
};

class AggregationOperatorFactory : public AggregationCommonOperatorFactory {
public:
    Operator *CreateOperator() override;

    AggregationOperatorFactory(vec::VecTypes &aggInput, vec::VecTypes &aggOutput, PrepareContext aggFuncType,
        bool inputRaw, bool outputPartial)
        : aggInputTypes(aggInput),
          aggOutputTypes(aggOutput),
          aggFuncTypeContext(aggFuncType),
          AggregationCommonOperatorFactory(inputRaw, outputPartial)
    {}

    ~AggregationOperatorFactory() override {}
    OmniStatus Init() override;
    OmniStatus Close() override;

private:
    vec::VecTypes aggInputTypes;
    vec::VecTypes aggOutputTypes;
    PrepareContext aggFuncTypeContext;
    std::vector<std::unique_ptr<AggregatorFactory>> aggregatorFactories;
};
} // end op
} // edn omniruntime

#endif