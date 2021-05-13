#ifndef __HASH_GROUPBY_H__
#define __HASH_GROUPBY_H__

#include "../native_base.h"
#include "aggregator.h"
#include "../../util/debug.h"

#include <vector>
#include <stdint.h>
#include <thread>

const int32_t MAX_TABLE_SIZE_IN_BYTES = 1024 * 1024;

typedef struct Iterator
{
    std::unordered_map<uint64_t, std::vector<GroupByColumn>>::iterator groupIterator;
    std::vector<std::unordered_map<uint64_t, std::vector<GroupByColumn>>::iterator> aggIterators;
} HashGroupByIterator;

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

class NativeOmniHashAggregationOperatorFactory;

class NativeOmniHashAggregationOperator : public NativeOmniOperator
{
public:
    NativeOmniHashAggregationOperator(std::vector<ColumnIndex> groupByCol, std::vector<ColumnIndex> aggCol, std::vector<Aggregator*> aggs)
    : groupByCols(groupByCol), aggCols(aggCol), aggregators(aggs)
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

    int32_t addInput(Table** data, int32_t* rowCount, int32_t pageCount) override
    {
        return 0;
    }

    NativeOmniHashAggregationOperator(std::vector<Aggregator*> aggregators)
    : aggregators(aggregators)
    { }

    ~NativeOmniHashAggregationOperator()
    {
        // delete map
        for (auto& item : groupedRows) {
            for (auto& gc : item.second) {
                switch (gc.type)
                {
                    case INT32: {
                        delete reinterpret_cast<int32_t*>(gc.val);
                        break;
                    }
                    case INT64: {
                        delete reinterpret_cast<int64_t*>(gc.val);
                        break;
                    }
                    case DOUBLE: {
                        delete reinterpret_cast<double*>(gc.val);
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
    void preloop(Table* table);
    void inloop(Table* table, uint32_t rowIdx);
    void inloop(char** head, uint32_t offset, int32_t* types, int32_t colNum, int32_t* groupByColIdx, int32_t groupByColNum, int32_t* aggColIdx, int32_t aggColNum, int32_t* aggFuncTypes); 
    void postloop(Table* table);
    void constructColumn(Table* table, uint32_t type, int32_t columnIdx, uint32_t outputColType);
    void constructColumn(Table* table, int32_t* types, uint32_t groupByColSize, uint32_t aggColSize, int32_t tableRowSize, HashGroupByIterator& iterator);
    uint32_t* groupByColumnIndexes();
    uint32_t* aggColumnIndexes();
    int32_t* getSourceTypes()
    {
        return sourceTypes;
    }

private:
    friend class NativeOmniHashAggregationOperatorFactory;
    std::vector<Aggregator*> aggregators;
    std::unordered_map<uint64_t, std::vector<GroupByColumn>> groupedRows;
    std::vector<ColumnIndex> groupByCols;
    std::vector<ColumnIndex> aggCols;
    uint32_t* inputColTypes;
    int32_t* sourceTypes;
};

class NativeOmniHashAggregationOperatorFactory : public NativeOmniOperatorFactory
{
public:
    NativeOmniOperator* createOmniOperator() override;

    NativeOmniHashAggregationOperatorFactory
    (PrepareContext groupByCol, PrepareContext groupByType, PrepareContext aggCol, PrepareContext aggType, PrepareContext aggFuncType)
    : groupByColContext(groupByCol), groupByTypeContext(groupByType), aggColContext(aggCol), aggTypeContext(aggType), aggFuncTypeContext(aggFuncType)
    { }

    ~NativeOmniHashAggregationOperatorFactory() override
    {}
private:
    PrepareContext groupByColContext; 
    PrepareContext groupByTypeContext;
    PrepareContext aggColContext;
    PrepareContext aggTypeContext;
    PrepareContext aggFuncTypeContext;
};

typedef void (*jit_module)(NativeOmniHashAggregationOperator*, Table*);

#endif