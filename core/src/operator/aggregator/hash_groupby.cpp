#include "hash_groupby.h"
#include "../../util/type_infer.h"
#include <iostream>
#include <chrono>
#include "../../memory/memory_pool.h"
#include <mutex>
#include <thread>
#include <math.h>

#if defined(DEBUG_LEVEL_LOW) || defined(DEBUG_LEVEL_HIGH)
#include <sstream>
#endif

omni::Operator * HashAggregationOperatorFactory::createOperator()
{
    std::vector<ColumnIndex> groupByIndex;
    std::vector<ColumnIndex> aggIndex;
    std::vector<Aggregator*> aggs;
    
    for (int32_t i = 0; i < this->groupByColContext.len; ++i) {
        ColumnIndex c = {this->groupByColContext.context[i], (ColumnType)this->groupByTypeContext.context[i]};
        groupByIndex.push_back(c);
    }
    for (int32_t i = 0; i < this->aggColContext.len; ++i) {
        ColumnIndex c = {this->aggColContext.context[i], (ColumnType)this->aggTypeContext.context[i]};
        aggIndex.push_back(c);

        if ((AggregateType)this->aggFuncTypeContext.context[i] == SUM) {
            switch (this->aggTypeContext.context[i])
            {
                case INT32: {
                    SumAggregator* agg = new SumAggregator(1);
                    aggs.push_back(agg);
                    break;
                }
                case INT64: {
                    SumAggregator* agg = new SumAggregator(2);
                    aggs.push_back(agg);
                    break;
                }
                case DOUBLE: {
                    SumAggregator* agg = new SumAggregator(3);
                    aggs.push_back(agg);
                    break;
                }
                default: {
                    DebugError("No such type %d", this->aggTypeContext.context[i]);
                    break;
                }
            }
        }
    }

    HashAggregationOperator* groupBy = new HashAggregationOperator(groupByIndex, aggIndex, aggs);
    return groupBy;
}

void HashAggregationOperator::preloop(Table* table)
{
    this->inputColTypes = new uint32_t[groupByCols.size() + aggCols.size()];
}

void HashAggregationOperator::inloop(Table* table, uint32_t rowIndex)
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

void HashAggregationOperator::postloop(Table* table)
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

void HashAggregationOperator::inloop(char** head,
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
        uint32_t idx = groupByColIdx[i];
        switch (types[idx])
        {
            case 1: {
                int32_t* rowVal = reinterpret_cast<int32_t*>(head[idx]) + offset;
                std::hash<int32_t> hashInt32;
                hash = hashInt32(*rowVal);
                break;
            }
            case 2: {
                int64_t* rowVal = reinterpret_cast<int64_t*>(head[idx]) + offset;
                std::hash<int64_t> hashInt64;
                hash = hashInt64(*rowVal);
                break;
            }
            case 3: {
                double* rowVal = reinterpret_cast<double*>(head[idx]) + offset;
                std::hash<double> hashDouble;
                hash = hashDouble(*rowVal);
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
                    int32_t* copyVal = new int32_t;
                    *copyVal = *rowVal;
                    rowPtr = reinterpret_cast<void*>(copyVal);
                    break;
                }
                case 2: {
                    int64_t* rowVal = reinterpret_cast<int64_t*>(head[idx]) + combinedHash.offset;
                    int64_t* copyVal = new int64_t;
                    *copyVal = *rowVal;
                    rowPtr = reinterpret_cast<void*>(copyVal);
                    break;
                }
                case 3: {
                    double* rowVal = reinterpret_cast<double*>(head[idx]) + combinedHash.offset;
                    double* copyVal = new double;
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

int32_t HashAggregationOperator::addInput(Table* table, int32_t rowCount)
{
#ifdef DEBUG_LEVEL_HIGH
    DebugFuncEntry;
#endif
    this->preloop(table);
    char** heads = table->getHeads();
    int32_t* columnTypes = reinterpret_cast<int32_t*>(table->getColumnTypes());

    int32_t rowNum = table->getColumnNumber();
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
        aggFuncTypes[i] = this->aggregators[i]->getType();
    }
    for (int32_t i = 0; i < rowCount; ++i) {
        this->inloop(heads, i, columnTypes, rowNum, groupByColIdx, groupColNum, aggColIdx, aggColNum, aggFuncTypes);
    }
    this->postloop(table);
    delete[] groupByColIdx;
    delete[] aggColIdx;
    delete[] aggFuncTypes;
    return 0;
}

int32_t HashAggregationOperator::addInput(Table** tables, int32_t* rowCount, int32_t pageCount)
{
    for (int32_t tableIdx = 0; tableIdx < pageCount; ++tableIdx) {
        this->addInput(tables[tableIdx], rowCount[tableIdx]);
    }
    return 0;
}

typedef std::chrono::high_resolution_clock Time;
typedef std::chrono::milliseconds ms;
typedef std::chrono::duration<float> fsec;

fsec g_total_execute_time;

void HashAggregationOperator::constructColumn(Table* table,
                                                        int32_t* types, 
                                                        uint32_t groupByColSize, 
                                                        uint32_t aggColSize, 
                                                        int32_t tableRowSize, 
                                                        HashGroupByIterator& iterator)
                {
#ifdef DEBUG_LEVEL_HIGH
    DebugFuncEntry;
#endif
    // allocate all column memory first
    for (int32_t i = 0; i < groupByColSize; ++i) {
        switch (types[i])
        {
            case 1: {
                int32_t* c = reinterpret_cast<int32_t*>(omni_allocate(tableRowSize * sizeof(int32_t)));
                table->setColumn(new Column(c, INT32, tableRowSize), INT32);
                break;
            }
           case 2: {
                int64_t* c = reinterpret_cast<int64_t*>(omni_allocate(tableRowSize * sizeof(int64_t)));
                table->setColumn(new Column(c, INT64, tableRowSize), INT64);
                break;
            }
            case 3: {
                double* c = reinterpret_cast<double*>(omni_allocate(tableRowSize * sizeof(double)));
                table->setColumn(new Column(c, DOUBLE, tableRowSize), DOUBLE);
                break;
            }
            default: {
                DebugError("Type %d doesn't support", types[i]);
                break;
            }
        }
    }
    // set value row by row
    int32_t rIdx = 0;
    for (; rIdx < tableRowSize && iterator.groupIterator != groupedRows.end();) {
        for (int32_t i = 0; i < groupByColSize; ++i) {
            auto column = table->getColumn(i);
            switch (column->getType())
            {
                case 1: {
                    int32_t* c = (int32_t*)column->getData();
                    c[rIdx] = *(int32_t*)(iterator.groupIterator->second[i].val);
                     /* code */
                    break;
                }
                case 2: {
                    int64_t* c = (int64_t*)column->getData();
                    c[rIdx] = *(int64_t*)(iterator.groupIterator->second[i].val);
                     /* code */
                    break;
                }
                case 3: {
                    double* c = (double*)column->getData();
                    c[rIdx] = *(double*)(iterator.groupIterator->second[i].val);
                     /* code */
                    break;
                }
                default: {
                    DebugError("Type %d doesn't support", types[i]);
                    break;
                }
            }
        }
        rIdx++;
        iterator.groupIterator++;
    }
    
    for (int32_t i = 0; i < aggColSize; ++i){
        switch (types[groupByColSize + i])
        {
            case 1: {
                int32_t* c = reinterpret_cast<int32_t*>(omni_allocate(tableRowSize * sizeof(int32_t)));
                int32_t rIdx = 0;
                for (; rIdx < tableRowSize && iterator.aggIterators[i] != aggregators[i]->getState().end(); ) {
                    c[rIdx++] = *TypeUtil<int32_t>::cast(iterator.aggIterators[i]->second[0].val);
                    iterator.aggIterators[i]++;
                }
                table->setColumn(new Column(c, INT32, tableRowSize), INT32);
                break;
            }
            case 2: {
                int64_t* c = reinterpret_cast<int64_t*>(omni_allocate(tableRowSize * sizeof(int64_t)));
                int32_t rIdx = 0;
                for (; rIdx < tableRowSize && iterator.aggIterators[i] != aggregators[i]->getState().end(); ) {
                    c[rIdx++] = *TypeUtil<int64_t>::cast(iterator.aggIterators[i]->second[0].val);
                    iterator.aggIterators[i]++;
                }
                table->setColumn(new Column(c, INT64, tableRowSize), INT64);
                break;
            }
            case 3: {
                double* c = reinterpret_cast<double*>(omni_allocate(tableRowSize * sizeof(double)));
                int32_t rIdx = 0;
                for (; rIdx < tableRowSize && iterator.aggIterators[i] != aggregators[i]->getState().end(); ) {
                    c[rIdx++] = *TypeUtil<double>::cast(iterator.aggIterators[i]->second[0].val);
                    iterator.aggIterators[i]++;
                }
                table->setColumn(new Column(c, DOUBLE, tableRowSize), DOUBLE);
                break;
            }
            default:
                break;
        }
    }   
#ifdef DEBUG_LEVEL_HIGH
    DebugFuncExit;
#endif 
}

int32_t HashAggregationOperator::getOutput(std::vector<Table*>& result)
{
    uint32_t gbSize = groupByCols.size();
    uint32_t aggSize = aggCols.size();
    uint32_t colSize = gbSize + aggSize;
    
    // int32_t* types = (int32_t*)omni_allocate(colSize * sizeof(uint32_t));    
    int32_t* types = new int32_t[colSize];    
    int32_t idx = 0;
    int32_t rowSize = 0;

    for (auto& i : groupByCols) {
        types[idx++] = i.type;
        
        switch (i.type)
        {
            case INT32: {
                rowSize += sizeof(int32_t);
                /* code */
                break;
            }
            case INT64: {
                rowSize += sizeof(int64_t);
                /* code */
                break;
            }
            case DOUBLE: {
                rowSize += sizeof(double);
                /* code */
                break;
            }
            default:
                break;
        }
    }
    for (auto& i : aggCols) {
        types[idx++] = i.type;

        switch (i.type)
        {
            case INT32: {
                rowSize += sizeof(int32_t);
                /* code */
                break;
            }
            case INT64: {
                rowSize += sizeof(int64_t);
                /* code */
                break;
            }
            case DOUBLE: {
                rowSize += sizeof(double);
                /* code */
                break;
            }
            default:
                break;
        }
    }
    int32_t maxRowNum = MAX_TABLE_SIZE_IN_BYTES / rowSize;
    int32_t pageCount = std::ceil((double)this->groupedRows.size() / (double)maxRowNum);
    int32_t currentPosition = 0;
    // Table** finalResult = new Table*[pageCount];
    Iterator iterator;
    iterator.groupIterator = groupedRows.begin();
    for (auto& agg : aggregators)
    {
        iterator.aggIterators.push_back(agg->getState().begin());
    } 

    for (int32_t i = 0; i < pageCount; ++i) {
        int32_t tableSize = std::min(maxRowNum, (int32_t)(this->groupedRows.size() - currentPosition));
        Table* table = new Table(tableSize, colSize);
        constructColumn(table, types, gbSize, aggSize, tableSize, iterator);
        result.push_back(table);
        currentPosition += maxRowNum;
    }
    delete[] types;
#ifdef DEBUG_LEVEL_LOW
    std::stringstream os;
    os << std::this_thread::get_id();
    DebugPrint("Thread %s: end of getResult.", os.str().c_str());
#endif
    // set finished.
    status = 2;
    return pageCount;
}

void HashAggregationOperator::constructColumn(Table* table,
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