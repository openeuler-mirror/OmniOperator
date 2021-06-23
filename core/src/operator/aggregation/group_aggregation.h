#ifndef __GROUP_AGGREGATION_H__
#define __GROUP_AGGREGATION_H__

#include "aggregation.h"

const int32_t MAX_TABLE_SIZE_IN_BYTES = 1024 * 1024;
namespace omniruntime {
namespace op {

class MultiChannelHash {
public:
    MultiChannelHash() : result(0){}
    uint64_t combineHash(uint64_t result, uint64_t value) {
        return (31 * result + value);
    }
private:
    uint64_t result;
};

typedef struct HashPosition
{
    uint64_t hashVal;
    uint32_t offset;
} HashPosition;

class HashAggregationOperatorFactory;

class HashAggregationOperator : public AggregationCommonOperator
{
public:
    HashAggregationOperator(std::vector<ColumnIndex> groupByCol, std::vector<ColumnIndex> aggCol, std::vector<Aggregator*> aggs)
    : groupByCols(groupByCol), aggCols(aggCol), AggregationCommonOperator(aggs)
    {
        int32_t colSize = groupByCol.size() + aggCol.size();
        sourceTypes = new int32_t[colSize];
        int32_t idx = 0;
        for (auto& c : groupByCol) {
            sourceTypes[idx++] = (int32_t)c.type;
        }
        for (auto& c : aggCol) {
            sourceTypes[idx++] = (int32_t)c.type;
        }
    }

    int32_t addInput(Table* data, int32_t rowCount) override;

    int32_t getOutput(std::vector<Table*>& data) override;

    int32_t addInput(Table** data, int32_t* rowCount, int32_t pageCount) override;

    HashAggregationOperator(std::vector<Aggregator*> aggregators)
    : AggregationCommonOperator(aggregators)
    { }

    ~HashAggregationOperator()
    {
        // delete map
        for (auto& item : groupedRows) {
            for (int32_t idx = 0; idx < item.second.size(); ++idx) {
                switch (groupByCols[idx].type)
                {
                    case INT32: {
                        delete reinterpret_cast<int32_t*>(item.second[idx].val);
                        break;
                    }
                    case INT64: {
                        delete reinterpret_cast<int64_t*>(item.second[idx].val);
                        break;
                    }
                    case DOUBLE: {
                        delete reinterpret_cast<double*>(item.second[idx].val);
                        break;
                    }
                    default:
                        break;
                }
            }
        }
        groupedRows.clear();
        // delete all aggregators
        for (auto& agg : aggregators) {
            delete agg;
        }
    }
    void preLoop(Table* table);
    void inLoop(char** head, uint32_t offset, int32_t* types, int32_t colNum, int32_t* groupByColIdx, int32_t groupByColNum, int32_t* aggColIdx, int32_t aggColNum, int32_t* aggFuncTypes); 
    void postLoop(Table* table);
    void constructHashColumn(Table* table, int32_t* types, uint32_t groupByColSize, int32_t tableRowSize);
    void constructAggColumn(Table* table, int32_t* types, uint32_t aggColSize, int32_t tableRowSize);
    uint32_t* groupByColumnIndexes();
    uint32_t* aggColumnIndexes();

private:
    friend class HashAggregationOperatorFactory;
    std::unordered_map<uint64_t, std::vector<GroupBySlot>> groupedRows;
    std::vector<ColumnIndex> groupByCols;
    std::vector<ColumnIndex> aggCols;
    uint32_t* inputColTypes;
};

class HashAggregationOperatorFactory : public AggregationCommonOperatorFactory
{
public:
    Operator* createOperator() override;

    HashAggregationOperatorFactory
    (PrepareContext groupByCol, PrepareContext groupByType, PrepareContext aggCol, PrepareContext aggType, PrepareContext aggFuncType, bool inputRaw, bool outputPartial) :
    groupByColContext(groupByCol),
    groupByTypeContext(groupByType),
    aggColContext(aggCol),
    aggTypeContext(aggType),
    aggFuncTypeContext(aggFuncType),
    AggregationCommonOperatorFactory(inputRaw, outputPartial)
    {

    }

    ~HashAggregationOperatorFactory() override
    {}
private:
    PrepareContext groupByColContext; 
    PrepareContext groupByTypeContext;
    PrepareContext aggColContext;
    PrepareContext aggTypeContext;
    PrepareContext aggFuncTypeContext;
};

typedef omniruntime::op::HashAggregationOperator *(*hashagg_module)(HashAggregationOperatorFactory*);
} // end of namespace op
} // end of namespace omniruntimef
#endif