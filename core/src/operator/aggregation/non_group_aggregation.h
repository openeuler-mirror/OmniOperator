#ifndef __NON_GROUP_AGGREGATION_H__
#define __NON_GROUP_AGGREGATION_H__

#include "aggregation.h"

namespace omniruntime {
namespace op {
class AggregationOperator : public AggregationCommonOperator {
public:
    AggregationOperator(std::vector<ColumnIndex> aggCol, std::vector<Aggregator*> aggs) 
    : aggCols(aggCol), AggregationCommonOperator(aggs)
    {
        sourceTypes = new int32_t[aggCol.size()];
        int32_t idx = 0;
        for (auto& c : aggCol) {
            sourceTypes[idx++] = (int32_t)c.type;
        }
    }

    virtual ~AggregationOperator() {}
    int32_t addInput(VectorBatch *vecBatch) override;
    int32_t getOutput(std::vector<VectorBatch*>& data) override;
    inline void inLoop(Vector **vectors, uint32_t offset, int32_t colNum, int32_t* aggDataType, int32_t* aggFuncType);
    inline void preLoop(VectorBatch *vecBatch) {}
    inline void postLoop(VectorBatch *vecBatch) {}

    void fillResultVectors(VectorBatch *vecBatch);
private:
    std::vector<ColumnIndex> aggCols;
};

class AggregationOperatorFactory : public OperatorFactory
{
public:
    Operator* createOperator() override;

    AggregationOperatorFactory
    (PrepareContext aggType, PrepareContext aggFuncType)
    : aggTypeContext(aggType), aggFuncTypeContext(aggFuncType)
    { }

    ~AggregationOperatorFactory() override
    {}
private:
    PrepareContext aggTypeContext;
    PrepareContext aggFuncTypeContext;
};
} // end op
} // edn omniruntime

#endif