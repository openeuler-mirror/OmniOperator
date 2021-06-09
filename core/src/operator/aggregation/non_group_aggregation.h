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

    }

    virtual ~AggregationOperator() {}
    int32_t addInput(Table* data, int32_t rowCount) override;
    int32_t getOutput(std::vector<Table*>& data) override;
    int32_t addInput(Table** data, int32_t* rowCount, int32_t pageCount) override;
    inline void inLoop(char** vecPtrs, uint32_t offset, int32_t colNum, int32_t* aggDataType, int32_t* aggFuncType);
    inline void preLoop(Table* data) {}
    inline void postLoop(Table* data) {}
    void constructColumn(Table* table, int32_t* types, uint32_t aggColSize);
private:
    std::vector<ColumnIndex> aggCols;
};

class AggregationOperatorFactory : public OperatorFactory
{
public:
    Operator* createOperator() override;

    AggregationOperatorFactory
    (PrepareContext aggCol, PrepareContext aggType, PrepareContext aggFuncType)
    : aggColContext(aggCol), aggTypeContext(aggType), aggFuncTypeContext(aggFuncType)
    { }

    ~AggregationOperatorFactory() override
    {}
private:
    PrepareContext aggColContext;
    PrepareContext aggTypeContext;
    PrepareContext aggFuncTypeContext;
};
} // end op
} // edn omniruntime

#endif