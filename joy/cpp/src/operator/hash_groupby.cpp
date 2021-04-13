#include "hash_groupby.h"
#include "../util/type_infer.h"
#include <iostream>
#include <chrono>
#include "../memory_pool/memory_pool.h"
#include <mutex>
#include <thread>

#if defined(DEBUG_LEVEL_LOW) || defined(DEBUG_LEVEL_HIGH)
#include <sstream>
#endif


void HashGroupBy::preloop(Table* table) 
{
    this->inputColTypes = new uint32_t[groupByCols.size() + aggCols.size()];
}

void HashGroupBy::inloop(Table* table, uint32_t rowIndex) 
{
    // caculate hash value on group by column(s)
    uint64_t combinedHash = 0;
    MultiChannelHash hashFunc;
    std::vector<GroupByColumn> groupByTuple;
    for (auto c : this->groupByCols) {    
        ColumnType type = table->getColumn(c.idx)->getType();
        void* rowPtr = table->getColumn(c.idx)->getValue(rowIndex);
        GroupByColumn groupCol = {type, rowPtr};
        groupByTuple.push_back(groupCol);
        uint64_t hash = 0;
        switch (type)
        {
            case INT32: {
                int32_t* rowVal = TypeUtil<int32_t>::cast(rowPtr);
                int32_t* copyVal = new int32_t;
                *copyVal = *rowVal;
                rowPtr = copyVal;
                std::hash<int32_t> hashInt32;
                hash = hashInt32(*rowVal);
                break;
            }
            case INT64: {
                int64_t* rowVal = TypeUtil<int64_t>::cast(rowPtr);
                int64_t* copyVal = new int64_t;
                *copyVal = *rowVal;
                rowPtr = copyVal;
                std::hash<int64_t> hashInt64;
                hash = hashInt64(*rowVal);
                break;
            }
            case DOUBLE: {
                double* rowVal = TypeUtil<double>::cast(rowPtr);
                double* copyVal = new double;
                *copyVal = *rowVal;
                rowPtr = copyVal;
                std::hash<double> hashDouble;
                hash = hashDouble(*rowVal);
                break;
            }
            default:
                break;
        }
        combinedHash = hashFunc.combineHash(combinedHash, hash);
    }

    if (groupedRows.find(combinedHash) == groupedRows.end()) {
        groupedRows.insert({combinedHash, groupByTuple});
    }

    // pass hash value to each aggregator
    for (int32_t idx = 0; idx < this->aggCols.size(); ++idx) {
        ColumnType type = table->getColumn(idx + this->groupByCols.size())->getType();
        void* rowPtr = table->getColumn(idx + this->groupByCols.size())->getValue(rowIndex);
        this->aggregators[idx]->process(combinedHash, rowPtr, type);
    }
}

void HashGroupBy::postloop(Table* table) 
{

}

extern "C" void processAgg(uint64_t key, 
                            std::vector<Aggregator*>& aggs, 
                            int32_t* aggTypes,
                            int32_t aggNum, 
                            int32_t* types, 
                            int32_t* aggIdx, 
                            void** head, 
                            uint32_t offset)
{
    for (int32_t i = 0; i < aggNum; ++i) {
        int32_t idx = aggIdx[i];
        int32_t type = types[idx];
        void* colPtr = head[idx];
        switch (aggTypes[i])
        {
            case 0: { // sum
                switch (type)
                {
                    case 1: {
                        /* code */
                        sumProcessInt32(reinterpret_cast<SumAggregator*>(aggs[i]), key, colPtr, offset);
                        break;
                    }
                    case 2: {
                        sumProcessInt64(reinterpret_cast<SumAggregator*>(aggs[i]), key, colPtr, offset);
                        break;
                    }
                    case 3: {                        
                        sumProcessDouble(reinterpret_cast<SumAggregator*>(aggs[i]), key, colPtr, offset);
                        break;
                    }
                    default: {
                        DebugError("No such type %d", type);
                        break;
                    }
                }
                break;
            }
            
            default: {
                DebugError("No such aggregator %d", aggTypes[i]);
                break;
            }
        }
    }
}

void HashGroupBy::inloop(char** head, 
                        uint32_t offset, 
                        int32_t* types, 
                        int32_t colNum,  
                        int32_t* groupByColIdx,
                        int32_t groupByColNum,
                        int32_t* aggColIdx,
                        int32_t aggColNum,
                        int32_t* aggFuncTypes) 
{    
    HashPosition combinedHash = {0, 0};
    MultiChannelHash hashFunc;
    for (int32_t i = 0; i < groupByColNum; ++i) {    
        uint64_t hash = 0;
        void* rowPtr = nullptr;
        uint32_t idx = groupByColIdx[i];
        switch (types[idx])
        {
            case 1: {
                int32_t* rowVal = reinterpret_cast<int32_t*>(head[idx]) + offset;
                std::hash<int32_t> hashInt32;
                hash = hashInt32(*rowVal);
                rowPtr = reinterpret_cast<void*>(rowVal);
                break;
            }
            case 2: {
                int64_t* rowVal = reinterpret_cast<int64_t*>(head[idx]) + offset;
                std::hash<int64_t> hashInt64;
                hash = hashInt64(*rowVal);
                rowPtr = reinterpret_cast<void*>(rowVal);
                break;
            }
            case 3: {
                double* rowVal = reinterpret_cast<double*>(head[idx]) + offset;
                std::hash<double> hashDouble;
                hash = hashDouble(*rowVal);
                rowPtr = reinterpret_cast<void*>(rowVal);
                break;
            }
            default:
                break;
        }
        combinedHash.hashVal = hashFunc.combineHash(combinedHash.hashVal, hash);
        combinedHash.offset = offset;
    }

    if (groupedRows.find(combinedHash.hashVal) == groupedRows.end()) {
        std::vector<GroupByColumn> groupByTuple;
        for (int32_t i = 0; i < groupByColNum; ++i) {
            // copy col value to map
            void* rowPtr = nullptr;
            uint32_t idx = groupByColIdx[i];
            switch (types[idx])
            {
                case 1: {
                    int32_t* rowVal = reinterpret_cast<int32_t*>(head[idx]) + combinedHash.offset;
                    // int32_t* copyVal = new int32_t;
                    int32_t* copyVal = (int32_t*)omni_allocate(sizeof(int32_t));
                    *copyVal = *rowVal;
                    rowPtr = reinterpret_cast<void*>(copyVal);
                    break;
                }
                case 2: {
                    int64_t* rowVal = reinterpret_cast<int64_t*>(head[idx]) + combinedHash.offset;
                    // int64_t* copyVal = new int64_t;
                    int64_t* copyVal = (int64_t*)omni_allocate(sizeof(int64_t));
                    *copyVal = *rowVal;
                    rowPtr = reinterpret_cast<void*>(copyVal);
                    break;
                }
                case 3: {
                    double* rowVal = reinterpret_cast<double*>(head[idx]) + combinedHash.offset;
                    // double* copyVal = new double;
                    double* copyVal = (double*)omni_allocate(sizeof(double));
                    *copyVal = *rowVal;
                    rowPtr = reinterpret_cast<void*>(copyVal);
                    break;
                }
                default:
                    break;
            }
            GroupByColumn groupCol = {(ColumnType)types[idx], rowPtr};
            groupByTuple.push_back(groupCol);
        }
        groupedRows.insert({combinedHash.hashVal, groupByTuple});
    }
    
    // processAgg(combinedHash.hashVal, aggregators, aggDataTypes, aggColIdx, (void**)head, offset);
    processAgg(combinedHash.hashVal, aggregators, aggFuncTypes, aggColNum, types, aggColIdx, (void**)head, offset);
}

void HashGroupBy::process(Table* table, uint32_t rowCount) 
{
#ifdef DEBUG_LEVEL_HIGH
    DebugFuncEntry;
#endif
    this->preloop(table);
    char** heads = table->getHeads();
    int32_t* ct = reinterpret_cast<int32_t*>(table->getColumnTypes());
    int32_t n = table->getColumnNumber();
    int32_t groupColNum = this->groupByCols.size();
    int32_t* groupByColIdx = new int32_t[groupColNum];
    int32_t aggColNum = this->aggCols.size();
    int32_t* aggColIdx = new int32_t[aggColNum];
    int32_t* aggFuncTypes = new int32_t[aggColNum];
    
    for (int32_t i = 0; i < groupColNum; ++i) {
        groupByColIdx[i] = this->groupByCols[i].idx;
        this->inputColTypes[this->groupByCols[i].idx] = 0; // 0 represents groupby
    }
    for (int32_t i = 0; i < aggColNum; ++i) {
        aggColIdx[i] = this->aggCols[i].idx;
        this->inputColTypes[this->aggCols[i].idx] = 1; // 1 represents agg
        aggFuncTypes[i] = aggregators[i]->getType();
    }
   
    for (int32_t i = 0; i < rowCount; ++i) {
        this->inloop(heads, i, ct, n, groupByColIdx, groupColNum, aggColIdx, aggColNum, aggFuncTypes);
    }
    this->postloop(table);
    delete[] groupByColIdx;
    delete[] aggColIdx;
    delete[] aggFuncTypes;
#ifdef DEBUG_LEVEL_HIGH
    DebugFuncExit;
#endif
}
typedef std::chrono::high_resolution_clock Time;
typedef std::chrono::milliseconds ms;
typedef std::chrono::duration<float> fsec;

fsec g_total_execute_time;

extern "C" void JIT_hashGroupByExecute(HashGroupBy* op, Table* table) 
{
    fsec local_execute_time;
    auto t0 = Time::now();
    op->process(table, table->getPositionCount());
    auto t1 = Time::now();
    local_execute_time = (t1 - t0);
    
    ms dd = std::chrono::duration_cast<ms>(local_execute_time);
    g_total_execute_time += (t1 - t0);
    ms d = std::chrono::duration_cast<ms>(g_total_execute_time);
}

void HashGroupBy::constructColumn(Table* table, int32_t* types, uint32_t groupByColSize, uint32_t aggColSize)
{
    for (int32_t i = 0; i < groupByColSize; ++i) {
        uint32_t rowCount = this->groupedRows.size();
        switch (types[i])
        {
            case 1: {
                int32_t* c = reinterpret_cast<int32_t*>(omni_allocate(rowCount * sizeof(int32_t)));
                int32_t rIdx = 0;
                for (auto& row : this->groupedRows) {
                    c[rIdx++] = *TypeUtil<int32_t>::cast(row.second[i].val);
                }
                table->setColumn(new Column(c, INT32, rowCount), INT32);
                break;
            }
            case 2: {
                int64_t* c = reinterpret_cast<int64_t*>(omni_allocate(rowCount * sizeof(int64_t)));
                int32_t rIdx = 0;
                for (auto& row : this->groupedRows) {
                    c[rIdx++] = *TypeUtil<int64_t>::cast(row.second[i].val);
                }
                table->setColumn(new Column(c, INT64, rowCount), INT64);
                break;
            }
            case 3: {
                double* c = reinterpret_cast<double*>(omni_allocate(rowCount * sizeof(double)));
                int32_t rIdx = 0;
                for (auto& row : this->groupedRows) {
                    c[rIdx++] = *TypeUtil<double>::cast(row.second[i].val);
                }
                table->setColumn(new Column(c, DOUBLE, rowCount), DOUBLE);
                break;
            }
            default:
                break;
        }
    }
            
    
    for (int32_t i = 0; i < aggColSize; ++i){
        std::unordered_map<uint64_t, std::vector<GroupByColumn>> rows = this->aggregators[i]->getState();
        uint32_t rowCount = this->aggregators[i]->getState().size();
        switch (types[groupByColSize + i])
        {
            case 1: {
                int32_t* c = reinterpret_cast<int32_t*>(omni_allocate(rowCount * sizeof(int32_t)));
                int32_t rIdx = 0;
                for (auto& row : rows) {
                    c[rIdx++] = *TypeUtil<int32_t>::cast(row.second[0].val);
                }
                table->setColumn(new Column(c, INT32, rowCount), INT32);
                break;
            }
            case 2: {
                int64_t* c = reinterpret_cast<int64_t*>(omni_allocate(rowCount * sizeof(int64_t)));
                int32_t rIdx = 0;
                for (auto& row : rows) {
                    c[rIdx++] = *TypeUtil<int64_t>::cast(row.second[0].val);
                }
                table->setColumn(new Column(c, INT64, rowCount), INT64);
                break;
            }
            case 3: {
                double* c = reinterpret_cast<double*>(omni_allocate(rowCount * sizeof(double)));
                int32_t rIdx = 0;
                for (auto& row : rows) {
                    c[rIdx++] = *TypeUtil<double>::cast(row.second[0].val);
                }
                table->setColumn(new Column(c, DOUBLE, rowCount), DOUBLE);
                break;
            }
            default:
                break;
        }
    }    
}

Table* HashGroupBy::getResult() 
{
    uint32_t gbSize = groupByCols.size();
    uint32_t aggSize =  + aggCols.size();
    uint32_t colSize = gbSize + aggSize;
    // Table* result = new Table(groupedRows.size(), colSize);
    // uint32_t groupByColCnt = 0;
    // uint32_t aggColCnt = 0;
    // for (int32_t i = 0; i < colSize; ++i) {
    //     if (this->inputColTypes[i] == 0) {
    //         constructColumn(result, groupByCols[groupByColCnt].type, groupByColCnt, 0);
    //         groupByColCnt++;
    //     }else if(this->inputColTypes[i] == 1) {
    //         constructColumn(result, aggCols[aggColCnt].type, aggColCnt, 1);
    //         aggColCnt++;
    //     }else {
    //         DebugPrint("Error column type %d", this->inputColTypes[i]);
    //     }
    // }
    int32_t* types = (int32_t*)omni_allocate(colSize * sizeof(uint32_t));    
    int32_t idx = 0;
    for (auto& i : groupByCols) {
        types[idx++] = i.type;
    }
    for (auto& i : aggCols) {
        types[idx++] = i.type;
    }
    Table* result = new Table(groupedRows.size(), colSize);
    constructColumn(result, types, gbSize, aggSize);
    omni_release((uint64_t)types);
#ifdef DEBUG_LEVEL_LOW
    std::stringstream os;
    os << std::this_thread::get_id();
    DebugPrint("Thread %s: end of getResult.", os.str().c_str());
#endif
    return result;
}

void HashGroupBy::constructColumn(Table* table,
                                    uint32_t type,
                                    int32_t columnIdx,
                                    uint32_t outputColType) 
{
    if (outputColType == 0) {
        uint32_t rowCount = this->groupedRows.size();
        switch (type)
        {
            case INT32: {
                int32_t* c = reinterpret_cast<int32_t*>(omni_allocate(rowCount * sizeof(int32_t)));
                int32_t rIdx = 0;
                for (auto& row : this->groupedRows) {
                    c[rIdx++] = *TypeUtil<int32_t>::cast(row.second[columnIdx].val);
                }
                table->setColumn(new Column(c, INT32, rowCount), INT32);
                break;
            }
            case INT64: {
                int64_t* c = reinterpret_cast<int64_t*>(omni_allocate(rowCount * sizeof(int64_t)));
                int32_t rIdx = 0;
                for (auto& row : this->groupedRows) {
                    c[rIdx++] = *TypeUtil<int64_t>::cast(row.second[columnIdx].val);
                }
                table->setColumn(new Column(c, INT64, rowCount), INT64);
                break;
            }
            case DOUBLE: {
                double* c = reinterpret_cast<double*>(omni_allocate(rowCount * sizeof(double)));
                int32_t rIdx = 0;
                for (auto& row : this->groupedRows) {
                    c[rIdx++] = *TypeUtil<double>::cast(row.second[columnIdx].val);
                }
                table->setColumn(new Column(c, DOUBLE, rowCount), DOUBLE);
                break;
            }
            default:
                break;
        }
    }else if (outputColType == 1) {
        std::unordered_map<uint64_t, std::vector<GroupByColumn>> rows = this->aggregators[columnIdx]->getState();
        uint32_t rowCount = this->aggregators[columnIdx]->getState().size();
        switch (type)
        {
            case INT32: {
                int32_t* c = reinterpret_cast<int32_t*>(omni_allocate(rowCount * sizeof(int32_t)));
                int32_t rIdx = 0;
                for (auto& row : rows) {
                    c[rIdx++] = *TypeUtil<int32_t>::cast(row.second[0].val);
                }
                table->setColumn(new Column(c, INT32, rowCount), INT32);
                break;
            }
            case INT64: {
                int64_t* c = reinterpret_cast<int64_t*>(omni_allocate(rowCount * sizeof(int64_t)));
                int32_t rIdx = 0;
                for (auto& row : rows) {
                    c[rIdx++] = *TypeUtil<int64_t>::cast(row.second[0].val);
                }
                table->setColumn(new Column(c, INT64, rowCount), INT64);
                break;
            }
            case DOUBLE: {
                double* c = reinterpret_cast<double*>(omni_allocate(rowCount * sizeof(double)));
                int32_t rIdx = 0;
                for (auto& row : rows) {
                    c[rIdx++] = *TypeUtil<double>::cast(row.second[0].val);
                }
                table->setColumn(new Column(c, DOUBLE, rowCount), DOUBLE);
                break;
            }
            default:
                break;
        }
    }

}
HashGroupBy* createHashGroupBy(std::vector<ColumnIndex>& groupByIndex, 
                                    std::vector<ColumnIndex>& aggIndex, 
                                    std::vector<Aggregator*>& aggs)
{
#ifdef DEBUG_LEVEL_HIGH
    DebugFuncEntry;
#endif
#ifdef DEBUG_LEVEL_HIGH
        DebugPrint("Creating groupby id: %ld", 1L);
#endif
    HashGroupBy* groupBy = new HashGroupBy(groupByIndex, aggIndex, aggs);
    return groupBy;
}