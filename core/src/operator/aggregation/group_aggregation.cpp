#include "group_aggregation.h"
#include "../../vector/vector_common.h"
#include "../status.h"
#include "../../jit/annotation.h"
#include "../optimization.h"
#include "../../vector/container_vector.h"
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
        ColumnIndex c = {this->groupByColContext.context[i], (VecType)this->groupByTypeContext.context[i]};
        groupByIndex.push_back(c);
    }
    for (int32_t i = 0; i < this->aggColContext.len; ++i) {
        ColumnIndex c = {this->aggColContext.context[i], (VecType)this->aggTypeContext.context[i]};
        aggIndex.push_back(c);

        if ((AggregateType)this->aggFuncTypeContext.context[i] == OMNI_AGGREGATION_TYPE_SUM) {
            switch (this->aggTypeContext.context[i])
            {
                case OMNI_VEC_TYPE_INT: {
                    aggs.push_back(new SumAggregator(1, inputRaw, outputPartial));
                    break;
                }
                case OMNI_VEC_TYPE_LONG: {
                    aggs.push_back(new SumAggregator(2, inputRaw, outputPartial));
                    break;
                }
                case OMNI_VEC_TYPE_DOUBLE: {
                    aggs.push_back(new SumAggregator(3, inputRaw, outputPartial));
                    break;
                }
                default: {
                    DebugError("No such type %d", this->aggTypeContext.context[i]);
                    break;
                }
            }
        } else if ((AggregateType)this->aggFuncTypeContext.context[i] == OMNI_AGGREGATION_TYPE_AVG)
        {
            switch (this->aggTypeContext.context[i])
            {
                case OMNI_VEC_TYPE_INT: {
                    aggs.push_back(new AverageAggregator(1, inputRaw, outputPartial));
                    break;
                }
                case OMNI_VEC_TYPE_LONG: {
                    aggs.push_back(new AverageAggregator(2, inputRaw, outputPartial));
                    break;
                }
                case OMNI_VEC_TYPE_DOUBLE: {
                    aggs.push_back(new AverageAggregator(3, inputRaw, outputPartial));
                    break;
                }
                default: {
                    DebugError("No such type %d", this->aggTypeContext.context[i]);
                    break;
                }
            }
        } else if ((AggregateType)this->aggFuncTypeContext.context[i] == OMNI_AGGREGATION_TYPE_MAX)
        {
            switch (this->aggTypeContext.context[i])
            {
                case OMNI_VEC_TYPE_INT: {
                    aggs.push_back(new MaxAggregator(1, inputRaw, outputPartial));
                    break;
                }
                case OMNI_VEC_TYPE_LONG: {
                    aggs.push_back(new MaxAggregator(2, inputRaw, outputPartial));
                    break;
                }
                case OMNI_VEC_TYPE_DOUBLE: {
                    aggs.push_back(new MaxAggregator(3, inputRaw, outputPartial));
                    break;
                }
                default: {
                    DebugError("No such type %d", this->aggTypeContext.context[i]);
                    break;
                }
            }
        } else if ((AggregateType)this->aggFuncTypeContext.context[i] == OMNI_AGGREGATION_TYPE_MIN)
        {
            switch (this->aggTypeContext.context[i])
            {
                case OMNI_VEC_TYPE_INT: {
                    aggs.push_back(new MinAggregator(1, inputRaw, outputPartial));
                    break;
                }
                case OMNI_VEC_TYPE_LONG: {
                    aggs.push_back(new MinAggregator(2, inputRaw, outputPartial));
                    break;
                }
                case OMNI_VEC_TYPE_DOUBLE: {
                    aggs.push_back(new MinAggregator(3, inputRaw, outputPartial));
                    break;
                }
                default: {
                    DebugError("No such type %d", this->aggTypeContext.context[i]);
                    break;
                }
            }
        } else if ((AggregateType)this->aggFuncTypeContext.context[i] == OMNI_AGGREGATION_TYPE_COUNT)
        {
            switch (this->aggTypeContext.context[i])
            {
                case OMNI_VEC_TYPE_INT: {
                    aggs.push_back(new CountAggregator(1, inputRaw, outputPartial));
                    break;
                }
                case OMNI_VEC_TYPE_LONG: {
                    aggs.push_back(new CountAggregator(2, inputRaw, outputPartial));
                    break;
                }
                case OMNI_VEC_TYPE_DOUBLE: {
                    aggs.push_back(new CountAggregator(3, inputRaw, outputPartial));
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

void HashAggregationOperator::preLoop(VectorBatch *vecBatch)
{
    this->inputColTypes = new uint32_t[groupByCols.size() + aggCols.size()];
}

void HashAggregationOperator::postLoop(VectorBatch *vecBatch)
{

}

SPECIALIZE(OMNIJIT_HASH_GROUPBY_PROCESS_AGG)
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

SPECIALIZE(OMNIJIT_HASH_GROUPBY_INLOOP)
void HashAggregationOperator::inLoop(Vector **vectors,
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
                std::hash<int32_t> hashInt32;
                hash = hashInt32(((IntVector *) vectors[idx])->getValue(offset));
                break;
            }
            case 2: {
                std::hash<int64_t> hashInt64;
                hash = hashInt64(((LongVector *) vectors[idx])->getValue(offset));
                break;
            }
            case 3: {
                std::hash<double> hashDouble;
                hash = hashDouble(((DoubleVector *) vectors[idx])->getValue(offset));
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
            switch (types[idx]) {
                case 1: {
                    int32_t *copyVal = new int32_t;
                    *copyVal = ((IntVector *) vectors[idx])->getValue(offset);
                    rowPtr = reinterpret_cast<void *>(copyVal);
                    break;
                }
                case 2: {
                    int64_t *copyVal = new int64_t;
                    *copyVal = ((LongVector *) vectors[idx])->getValue(offset);
                    rowPtr = reinterpret_cast<void *>(copyVal);
                    break;
                }
                case 3: {
                    double *copyVal = new double;
                    *copyVal = ((DoubleVector *) vectors[idx])->getValue(offset);
                    rowPtr = reinterpret_cast<void *>(copyVal);
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
    processAgg(combineHashVal, aggregators, aggColNum, types, aggColIdx, (void**)vectors, offset);
}

int32_t HashAggregationOperator::addInput(VectorBatch *vecBatch)
{
#ifdef DEBUG_LEVEL_HIGH
    DebugFuncEntry;
#endif
    this->preLoop(vecBatch);
    int32_t *vectorTypes = (int32_t *)vecBatch->getVectorTypes();
    int32_t vectorCount = vecBatch->getVectorCount();
    int32_t groupColNum = this->groupByCols.size();
    int32_t *groupByColIdx = new int32_t[groupColNum];
    int32_t aggColNum = this->aggCols.size();
    int32_t *aggColIdx = new int32_t[aggColNum];
    int32_t *aggFuncTypes = new int32_t[aggColNum];

    for (int32_t i = 0; i < groupColNum; ++i) {
        groupByColIdx[i] = this->groupByCols[i].idx;
        this->inputColTypes[this->groupByCols[i].idx] = 0; // 0 represents groupby
    }
    for (int32_t i = 0; i < aggColNum; ++i) {
        aggColIdx[i] = this->aggCols[i].idx;
        this->inputColTypes[this->aggCols[i].idx] = 1; // 1 represents agg
        aggFuncTypes[i] = this->aggregators[i]->getType();
    }
    int32_t rowCount = vecBatch->getRowCount();
    Vector **vectors = vecBatch->getVectors();
    for (int32_t i = 0; i < rowCount; ++i) {
        this->inLoop(vectors, i, vectorTypes, vectorCount, groupByColIdx, groupColNum, aggColIdx, aggColNum,
                     aggFuncTypes);
    }
    this->postLoop(vecBatch);
    delete[] groupByColIdx;
    delete[] aggColIdx;
    delete[] aggFuncTypes;
    return 0;
}

int32_t HashAggregationOperator::getRowSize(int32_t *types) {
    int32_t rowSize = 0;
    int32_t typeIndex = 0;
    for (auto &i : groupByCols) {
        types[typeIndex++] = i.type;
        switch (i.type) {
            case OMNI_VEC_TYPE_INT: {
                rowSize += sizeof(int32_t);
                /* code */
                break;
            }
            case OMNI_VEC_TYPE_LONG: {
                rowSize += sizeof(int64_t);
                /* code */
                break;
            }
            case OMNI_VEC_TYPE_DOUBLE: {
                rowSize += sizeof(double);
                /* code */
                break;
            }
            default:
                break;
        }
    }
    for (int32_t i = 0; i < aggCols.size(); ++i) {
        if (aggregators[i]->getType() == OMNI_AGGREGATION_TYPE_COUNT) {
            types[typeIndex++] = OMNI_VEC_TYPE_LONG;
            rowSize += sizeof(int64_t);
            continue;
        }
        if (aggregators[i]->getType() == OMNI_AGGREGATION_TYPE_AVG) {
            if (aggregators[i]->isOutputPartial()) {
                types[typeIndex++] = OMNI_VEC_TYPE_CONTAINER;
                rowSize += sizeof(int64_t);
            } else {
                types[typeIndex++] = OMNI_VEC_TYPE_DOUBLE;
            }
            rowSize += sizeof(double);
            continue;
        }
        types[typeIndex++] = aggCols[i].type;
        switch (aggCols[i].type) {
            case OMNI_VEC_TYPE_INT: {
                rowSize += sizeof(int32_t);
                /* code */
                break;
            }
            case OMNI_VEC_TYPE_LONG: {
                rowSize += sizeof(int64_t);
                /* code */
                break;
            }
            case OMNI_VEC_TYPE_DOUBLE: {
                rowSize += sizeof(double);
                /* code */
                break;
            }
            default:
                break;
        }
    }
    return rowSize;
}

SPECIALIZE(OMNIJIT_HASH_GROUPBY_HASH_COLUMN)
void HashAggregationOperator::fillGroupByVectors(VectorBatch *vecBatch, int startIndex, int endIndex,
                                                 RowIterator &rowIterator, int32_t rowCount) {
    RowIterator tempRowIterator = rowIterator;
    for (int colIndex = startIndex, groupByIndex = 0; colIndex < endIndex; ++colIndex, ++groupByIndex) {
        tempRowIterator = rowIterator;
        switch (groupByCols[groupByIndex].type) {
            case 1: {
                IntVector *vector = ((IntVector *) vecBatch->getVector(colIndex));
                for (int rowIndex = 0;
                     rowIndex < rowCount && tempRowIterator != groupedRows.end(); ++rowIndex, ++tempRowIterator) {
                    vector->setValue(rowIndex, *(int32_t *) tempRowIterator->second[colIndex].val);
                }
                break;
            }
            case 2: {
                LongVector *vector = ((LongVector *) vecBatch->getVector(colIndex));
                for (int rowIndex = 0;
                     rowIndex < rowCount && tempRowIterator != groupedRows.end(); ++rowIndex, ++tempRowIterator) {
                    vector->setValue(rowIndex, *(int64_t *) tempRowIterator->second[colIndex].val);
                }
                break;
            }
            case 3: {
                DoubleVector *vector = ((DoubleVector *) vecBatch->getVector(colIndex));
                for (int rowIndex = 0;
                     rowIndex < rowCount && tempRowIterator != groupedRows.end(); ++rowIndex, ++tempRowIterator) {
                    vector->setValue(rowIndex, *(double *) tempRowIterator->second[colIndex].val);
                }
                break;
            }
            default: {
                DebugError("Type %d doesn't support", groupByCols[groupByIndex].type);
                break;
            }
        }
    }
    rowIterator = tempRowIterator;
}

// TODO currently we need to traverse ColumnNum * RowNum times to build the output.
// The overhead need to be optimized.
SPECIALIZE(OMNIJIT_HASH_GROUPBY_AGG_COLUMN)
void HashAggregationOperator::fillAggVectors(VectorBatch *vecBatch, int startIndex, int endIndex, int32_t rowCount)
{
    for (int32_t aggIndex = 0, colIndex = startIndex; colIndex < endIndex; ++aggIndex, ++colIndex){
        auto resultIterator = this->aggregators[aggIndex]->getGroupState().begin();
        AggregateType aggType = this->aggregators[aggIndex]->getType();
        switch (aggType)
        {
            case OMNI_AGGREGATION_TYPE_SUM:
            case OMNI_AGGREGATION_TYPE_MIN:
            case OMNI_AGGREGATION_TYPE_MAX: {
                switch (aggCols[aggIndex].type)
                {
                    case 1:{
                        IntVector* vector = (IntVector*) vecBatch->getVector(colIndex);
                        for (int32_t rIdx = 0; rIdx < rowCount && resultIterator != aggregators[aggIndex]->getGroupState().end(); ++rIdx, resultIterator++) {
                            vector->setValue(rIdx, *reinterpret_cast<int32_t*>(resultIterator->second.val));
                        }
                        break;
                    }
                    case 2:{
                        LongVector* vector = (LongVector*) vecBatch->getVector(colIndex);
                        for (int32_t rIdx = 0; rIdx < rowCount && resultIterator != aggregators[aggIndex]->getGroupState().end(); ++rIdx, resultIterator++) {
                            vector->setValue(rIdx, *reinterpret_cast<int64_t*>(resultIterator->second.val));
                        }
                        break;
                    }
                    case 3:{
                        DoubleVector* vector = (DoubleVector*) vecBatch->getVector(colIndex);
                        for (int32_t rIdx = 0; rIdx < rowCount && resultIterator != aggregators[aggIndex]->getGroupState().end(); ++rIdx, resultIterator++) {
                            vector->setValue(rIdx, *reinterpret_cast<double*>(resultIterator->second.val));
                        }
                        break;
                    }
                    default:
                        break;
                }
                break;
            }
            case OMNI_AGGREGATION_TYPE_COUNT: {
                LongVector* vector = (LongVector*) vecBatch->getVector(colIndex);
                for (int32_t rIdx = 0; rIdx < rowCount && resultIterator != aggregators[aggIndex]->getGroupState().end(); ++rIdx, resultIterator++) {
                    vector->setValue(rIdx, reinterpret_cast<int64_t>(resultIterator->second.count));
                }
                break;
            }
            case OMNI_AGGREGATION_TYPE_AVG: { // TODO process intermediate vectors
                // generate double or row type vector according to the step. Row type if outputPartial == 1 otherwise double vector.
                if (outputPartial) {
                    ContainerVector* vector = (ContainerVector*)vecBatch->getVector(colIndex);
                    for (int32_t rIdx = 0; rIdx < rowCount && resultIterator != aggregators[aggIndex]->getGroupState().end(); ++rIdx, resultIterator++) {
                        if (resultIterator->second.avgCnt == 0) {
                            DebugError("Divisor is zero! key = %ld", resultIterator->first);
                        }
                        DoubleVector* doubleVector = reinterpret_cast<DoubleVector*>(vector->getValue(0));
                        std::cout << "fillAgg vec address: " << doubleVector << std::endl;
                        doubleVector->setValue(rIdx, *(reinterpret_cast<double*>(resultIterator->second.avgVal)));
                        LongVector* longVector = reinterpret_cast<LongVector*>(vector->getValue(1));
                        longVector->setValue(rIdx, resultIterator->second.avgCnt);
                    }
                } else {
                    DoubleVector* vector = (DoubleVector*)vecBatch->getVector(colIndex);
                    for (int32_t rIdx = 0; rIdx < rowCount && resultIterator != aggregators[aggIndex]->getGroupState().end(); ++rIdx, resultIterator++) {
                        if (resultIterator->second.avgCnt == 0) {
                            DebugError("Divisor is zero! key = %ld", resultIterator->first);
                        }
                        vector->setValue(rIdx, *(reinterpret_cast<double*>(resultIterator->second.avgVal)));
                    }
                }
                break;
            }
            default: {
                DebugError("No such aggregate type %d\n", aggType);
                break;
            }
        }
    }
#ifdef DEBUG_LEVEL_HIGH
    DebugFuncExit;
#endif
}

void setVectors(VectorBatch* vectorBatch, int32_t* types, int32_t rowCount)
{
    for (int colIndex = 0; colIndex < vectorBatch->getVectorCount(); ++colIndex) {
        switch (types[colIndex]) {
            case OMNI_VEC_TYPE_INT: {
                vectorBatch->setVector(colIndex, new IntVector(nullptr, rowCount));
                break;
            }
            case OMNI_VEC_TYPE_LONG: {
                vectorBatch->setVector(colIndex, new LongVector(nullptr, rowCount));
                break;
            }
            case OMNI_VEC_TYPE_DOUBLE: {
                vectorBatch->setVector(colIndex, new DoubleVector(nullptr, rowCount));
                break;
            }
            case OMNI_VEC_TYPE_CONTAINER: {
                DoubleVector* doubleVector = new DoubleVector(nullptr, rowCount);
                LongVector* longVector = new LongVector(nullptr, rowCount);
                Vector** vectorAddresses = new Vector*[2];
                vectorAddresses[0] = doubleVector;
                vectorAddresses[1] = longVector;
                VecType* vecTypes = new VecType[2];
                vecTypes[0] = OMNI_VEC_TYPE_DOUBLE;
                vecTypes[1] = OMNI_VEC_TYPE_LONG;
                std::cout << "Double vector addr " << doubleVector << std::endl;
                ContainerVector* containerVector = new ContainerVector(nullptr, rowCount, vectorAddresses, 2, vecTypes);
                vectorBatch->setVector(colIndex, containerVector);
                break;
            }
                // TODO: support other types!!!
            default: {
                break;
            }
        }
    }
}

int32_t HashAggregationOperator::getOutput(std::vector<VectorBatch*>& result)
{
    uint32_t groupByColSize = groupByCols.size();
    uint32_t aggColSize = aggCols.size();
    uint32_t colCount = groupByColSize + aggColSize;
    int32_t* types = new int32_t[colCount];
    int32_t rowSize = getRowSize(types);

    int32_t maxRowNum = MAX_TABLE_SIZE_IN_BYTES / rowSize;
    int32_t vecBatchCount = std::ceil((double)this->groupedRows.size() / (double)maxRowNum);
    int32_t currentPosition = 0;

    RowIterator rowIterator = groupedRows.begin();
    for (int32_t i = 0; i < vecBatchCount; ++i) {
        int32_t rowCount = std::min(maxRowNum, (int32_t)(this->groupedRows.size() - currentPosition));
        VectorBatch* vecBatch = new VectorBatch(colCount);
        setVectors(vecBatch, types, rowCount);
        fillGroupByVectors(vecBatch, 0, groupByColSize, rowIterator, rowCount);
        fillAggVectors(vecBatch, groupByColSize, colCount, rowCount);
        result.push_back(vecBatch);
        currentPosition += maxRowNum;
    }
    delete[] types;
    // set finished.
    setStatus(OMNI_STATUS_FINISHED);
    return vecBatchCount;
}

} // end of namespace op
} // end of namespace omniruntime
