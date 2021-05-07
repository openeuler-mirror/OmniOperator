#ifndef __HASH_GROUPBY_H__
#define __HASH_GROUPBY_H__

#include "aggregator.h"
#include "../op_template.h"
#include "../../util/op_template_cache.h"
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

class HashGroupBy : public OpTemplate {
public:
    HashGroupBy(std::vector<ColumnIndex> groupByCols, std::vector<ColumnIndex> aggCols, std::vector<Aggregator*> aggregators)
    : groupByCols(groupByCols), aggCols(aggCols), aggregators(aggregators)
    { }
    ~HashGroupBy()
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
        delete[] inputColTypes;
    }
    void preloop(Table* table) override;
    void inloop(Table* table, uint32_t rowIdx) override;
    void inloop(char** head, 
                uint32_t offset, 
                int32_t* types, 
                int32_t colNum,  
                int32_t* groupByColIdx,
                int32_t groupByColNum,
                int32_t* aggColIdx,
                int32_t aggColNum,
                int32_t* aggFuncTypes); 
    void postloop(Table* table) override;
    void process(Table*, uint32_t) override;
    void constructColumn(Table* table, uint32_t type, int32_t columnIdx, uint32_t outputColType);
    void constructColumn(Table* table, int32_t* types, uint32_t groupByColSize, uint32_t aggColSize, int32_t tableRowSize, HashGroupByIterator& iterator);
    // Table* getResult() override;
    int32_t getResult(std::vector<Table*>&);
    Table* getResult() {} 
    uint32_t* groupByColumnIndexes();
    uint32_t* aggColumnIndexes();
private:
    std::vector<ColumnIndex> groupByCols;
    std::vector<ColumnIndex> aggCols;
    std::vector<Aggregator*> aggregators;
    std::unordered_map<uint64_t, std::vector<GroupByColumn>> groupedRows;
    uint32_t* inputColTypes;
};
HashGroupBy* createHashGroupBy(std::vector<ColumnIndex>& groupByIndex, 
                                    std::vector<ColumnIndex>& aggIndex, 
                                    std::vector<Aggregator*>& aggs);
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

#endif