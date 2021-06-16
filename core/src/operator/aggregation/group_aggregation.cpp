#include "group_aggregation.h"
#include <math.h>

#if defined(DEBUG_LEVEL_LOW) || defined(DEBUG_LEVEL_HIGH)
#include <sstream>
#endif
namespace omniruntime {
namespace op {

Operator * HashAggregationOperatorFactory::createOperator()
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
                    aggs.push_back(new SumAggregator(1));
                    break;
                }
                case INT64: {
                    aggs.push_back(new SumAggregator(2));
                    break;
                }
                case DOUBLE: {
                    aggs.push_back(new SumAggregator(3));
                    break;
                }
                default: {
                    DebugError("No such type %d", this->aggTypeContext.context[i]);
                    break;
                }
            }
        } else if ((AggregateType)this->aggFuncTypeContext.context[i] == AVG)
        {
            switch (this->aggTypeContext.context[i])
            {
                case INT32: {
                    aggs.push_back(new AverageAggregator(1));
                    break;
                }
                case INT64: {
                    aggs.push_back(new AverageAggregator(2));
                    break;
                }
                case DOUBLE: {
                    aggs.push_back(new AverageAggregator(3));
                    break;
                }
                default: {
                    DebugError("No such type %d", this->aggTypeContext.context[i]);
                    break;
                }
            }
        } else if ((AggregateType)this->aggFuncTypeContext.context[i] == MAX) 
        {
            switch (this->aggTypeContext.context[i])
            {
                case INT32: {
                    aggs.push_back(new MaxAggregator(1));
                    break;
                }
                case INT64: {
                    aggs.push_back(new MaxAggregator(2));
                    break;
                }
                case DOUBLE: {
                    aggs.push_back(new MaxAggregator(3));
                    break;
                }
                default: {
                    DebugError("No such type %d", this->aggTypeContext.context[i]);
                    break;
                }
            }
        } else if ((AggregateType)this->aggFuncTypeContext.context[i] == MIN)
        {
            switch (this->aggTypeContext.context[i])
            {
                case INT32: {
                    aggs.push_back(new MinAggregator(1));
                    break;
                }
                case INT64: {
                    aggs.push_back(new MinAggregator(2));
                    break;
                }
                case DOUBLE: {
                    aggs.push_back(new MinAggregator(3));
                    break;
                }
                default: {
                    DebugError("No such type %d", this->aggTypeContext.context[i]);
                    break;
                }
            }
        } else if ((AggregateType)this->aggFuncTypeContext.context[i] == COUNT)
        {
            switch (this->aggTypeContext.context[i])
            {
                case INT32: {
                    aggs.push_back(new CountAggregator(1));
                    break;
                }
                case INT64: {
                    aggs.push_back(new CountAggregator(2));
                    break;
                }
                case DOUBLE: {
                    aggs.push_back(new CountAggregator(3));
                    break;
                }
                default: {
                    DebugError("No such type %d", this->aggTypeContext.context[i]);
                    break;
                }
            }
        }  else {
            // UDAF
        }
    }

    HashAggregationOperator* groupBy = new HashAggregationOperator(groupByIndex, aggIndex, aggs);
    return groupBy;
}

void HashAggregationOperator::preLoop(Table* table)
{
    this->inputColTypes = new uint32_t[groupByCols.size() + aggCols.size()];
}

void HashAggregationOperator::postLoop(Table* table)
{

}

extern "C" void processAgg(uint64_t key, 
                            std::vector<Aggregator*>& aggs, 
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
        auto groupIter = aggs[i]->getGroupState().find(key);
        if (groupIter != aggs[i]->getGroupState().end()) {
            aggs[i]->processGroup(groupIter->second, colPtr, type, offset);      
        }else { // insert a new GroupBySlot as a state
            aggs[i]->insert(key, colPtr, type, offset);
        }
    }
}

void HashAggregationOperator::inLoop(char** head, 
                                    uint32_t offset, 
                                    int32_t* types, 
                                    int32_t colNum,  
                                    int32_t* groupByColIdx,
                                    int32_t groupByColNum,
                                    int32_t* aggColIdx,
                                    int32_t aggColNum,
                                    int32_t* aggFuncTypes) 
{    
    int64_t combineHashVal = 0;
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
            default: {
                DebugError("No such data type %d", types[idx]);
                break;
            }
        }
        combineHashVal = hashFunc.combineHash(combineHashVal, hash);
    }

    if (groupedRows.find(combineHashVal) == groupedRows.end()) {
        std::vector<GroupBySlot> groupByTuple;
        for (int32_t i = 0; i < groupByColNum; ++i) {
            // copy col value to map
            void* rowPtr = nullptr;
            uint32_t idx = groupByColIdx[i];
            switch (types[idx])
            {
                case 1: {
                    int32_t* rowVal = reinterpret_cast<int32_t*>(head[idx]) + offset;
                    int32_t* copyVal = new int32_t;
                    *copyVal = *rowVal;
                    rowPtr = reinterpret_cast<void*>(copyVal);
                    break;
                }
                case 2: {
                    int64_t* rowVal = reinterpret_cast<int64_t*>(head[idx]) + offset;
                    int64_t* copyVal = new int64_t;
                    *copyVal = *rowVal;
                    rowPtr = reinterpret_cast<void*>(copyVal);
                    break;
                }
                case 3: {
                    double* rowVal = reinterpret_cast<double*>(head[idx]) + offset;
                    double* copyVal = new double;
                    *copyVal = *rowVal;
                    rowPtr = reinterpret_cast<void*>(copyVal);
                    break;
                }
                default: {
                    DebugError("No such data type %d", types[idx]);
                    break;
                }
            }
            GroupBySlot groupCol = {rowPtr};
            groupByTuple.push_back(groupCol);
        }
        groupedRows.insert({combineHashVal, groupByTuple});
    }
    
    // processAgg(combinedHash.hashVal, aggregators, aggDataTypes, aggColIdx, (void**)head, offset);
    processAgg(combineHashVal, aggregators, aggColNum, types, aggColIdx, (void**)head, offset);
}

int32_t HashAggregationOperator::addInput(Table* table, int32_t rowCount)
{
#ifdef DEBUG_LEVEL_HIGH
    DebugFuncEntry;
#endif
    this->preLoop(table);
    char** heads = table->getHeads();
    int32_t* columnTypes = reinterpret_cast<int32_t*>(table->getColumnTypes());

    int32_t columnNum = table->getColumnNumber();
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
        this->inLoop(heads, i, columnTypes, columnNum, groupByColIdx, groupColNum, aggColIdx, aggColNum, aggFuncTypes);
    }
    this->postLoop(table);
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

// TODO currently we need to traverse ColumnNum * RowNum times to build the output.
// The overhead need to be optimized.
void HashAggregationOperator::constructHashColumn(Table* table, 
                                                int32_t* types, 
                                                uint32_t groupByColSize, 
                                                int32_t tableRowSize)
{
#ifdef DEBUG_LEVEL_HIGH
    DebugFuncEntry;
#endif
    // allocate all column memory first
    allocateVec(table, types, 0, false, groupByColSize, tableRowSize);
    
    // set value row by row
    int32_t rIdx = 0;
    auto it = groupedRows.begin();
    for (; rIdx < tableRowSize && it != groupedRows.end();) {
        for (int32_t i = 0; i < groupByColSize; ++i) {
            auto column = table->getColumn(i);
            switch (column->getType())
            {
                case 1: {
                    int32_t* c = (int32_t*)column->getData();
                    c[rIdx] = *(int32_t*)(it->second[i].val);
                    break;
                }
                case 2: {
                    int64_t* c = (int64_t*)column->getData();
                    c[rIdx] = *(int64_t*)(it->second[i].val);
                    break;
                }
                case 3: {
                    double* c = (double*)column->getData();
                    c[rIdx] = *(double*)(it->second[i].val);
                    break;
                }
                default: {
                    DebugError("Type %d doesn't support", types[i]);
                    break;
                }
            }
        }
        rIdx++;
        it++;
    }
#ifdef DEBUG_LEVEL_HIGH
    DebugFuncExit;
#endif 
}

// TODO currently we need to traverse ColumnNum * RowNum times to build the output.
// The overhead need to be optimized.
void HashAggregationOperator::constructAggColumn(Table* table, 
                                                int32_t* types, 
                                                uint32_t aggColSize, 
                                                int32_t tableRowSize)
{
#ifdef DEBUG_LEVEL_HIGH
    DebugFuncEntry;
#endif
    const int32_t groupByColSize = this->groupByCols.size();
    allocateVec(table, types, groupByColSize, true, aggColSize, tableRowSize);

    for (int32_t i = 0; i < aggColSize; ++i){
        int32_t rIdx = 0;
        auto resultIterator = this->aggregators[i]->getGroupState().begin();
        AggregateType aggType = this->aggregators[i]->getType();
        auto col = table->getColumn(groupByColSize + i);
        switch (aggType)
        {
            case SUM:
            case COUNT:
            case MIN:
            case MAX: {
                for (; rIdx < tableRowSize && resultIterator != aggregators[i]->getGroupState().end(); ) {
                    if (aggType == COUNT) {
                        reinterpret_cast<int64_t*>(col->getData())[rIdx++] = resultIterator->second.count + 1;
                    }else {
                        switch (types[groupByColSize + i])
                        {
                            case 1:{
                                reinterpret_cast<int32_t*>(col->getData())[rIdx++] = *static_cast<int32_t*>(resultIterator->second.val);
                                break;
                            }
                            case 2:{
                                reinterpret_cast<int64_t*>(col->getData())[rIdx++] = *static_cast<int64_t*>(resultIterator->second.val);
                                break;
                            }
                            case 3:{
                                reinterpret_cast<double*>(col->getData())[rIdx++] = *static_cast<double*>(resultIterator->second.val);
                                break;
                            }
                            default:
                                break;
                        }
                    }
                    resultIterator++;
                }
                break;
            }
            case AVG: {
                for (; rIdx < tableRowSize && resultIterator != aggregators[i]->getGroupState().end(); ) {
                    if (resultIterator->second.count == 0) {
                        DebugError("Divisor is zero! key = %ld", resultIterator->first);
                    }
                    reinterpret_cast<double*>(col->getData())[rIdx++] = *static_cast<double*>(resultIterator->second.avgVal);
                    resultIterator++;
                }
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

    for (int32_t i = 0; i < pageCount; ++i) {
        int32_t tableSize = std::min(maxRowNum, (int32_t)(this->groupedRows.size() - currentPosition));
        Table* table = new Table(tableSize, colSize);
        constructHashColumn(table, types, gbSize, tableSize);
        constructAggColumn(table, types, aggSize, tableSize);
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

} // end of namespace op
} // end of namespace omniruntime
