#ifndef __HASH_GROUPBY_H__
#define __HASH_GROUPBY_H__

#include "op_template.h"
#include "aggregator.h"
#include "../util/op_template_cache.h"
#include <vector>
#include <stdint.h>
#include <thread>

class HashGroupBy : public OpTemplate {
public:
    HashGroupBy(std::vector<ColumnIndex> groupByCols, std::vector<ColumnIndex> aggCols, std::vector<Aggregator*> aggregators)
    : groupByCols(groupByCols), aggCols(aggCols), aggregators(aggregators)
    {}
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
                int32_t* type, 
                int32_t colNum, 
                int32_t* groupColIdx, 
                int32_t groupColNum, 
                int32_t* aggColIdx, 
                int32_t aggColNum,
                int32_t* aggDataTypes);
    void postloop(Table* table) override;
    void process(Table*, uint32_t) override;
    void constructColumn(Table* table, uint32_t type, int32_t columnIdx, uint32_t outputColType);
    Table* getResult() override;
    uint32_t* groupByColumnIndexes();
    uint32_t* aggColumnIndexes();
private:
    std::vector<ColumnIndex> groupByCols;
    std::vector<ColumnIndex> aggCols;
    std::vector<Aggregator*> aggregators;
    ska::unordered_map<uint64_t, std::vector<GroupByColumn>> groupedRows;
    uint32_t* inputColTypes;
};
HashGroupBy* createHashGroupBy(std::vector<ColumnIndex>& groupByIndex, 
                                    std::vector<ColumnIndex>& aggIndex, 
                                    std::vector<Aggregator*>& aggs,
                                    OpTemplateCache<HashGroupBy*>& operatorCache,
                                    std::string& opId);
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