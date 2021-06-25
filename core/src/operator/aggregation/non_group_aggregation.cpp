#include "non_group_aggregation.h"
#include "../../jit/annotation.h"
#include "../optimization.h"

namespace omniruntime {
namespace op {

Operator* AggregationOperatorFactory::createOperator()
{
    std::vector<ColumnIndex> aggIndex;
    std::vector<Aggregator*> aggs;
    
    for (int32_t i = 0; i < this->aggTypeContext.len; ++i) {
        ColumnIndex c = {static_cast<uint32_t>(i), (ColumnType)this->aggTypeContext.context[i]};
        aggIndex.push_back(c);

        if ((AggregateType)this->aggFuncTypeContext.context[i] == SUM) {
            switch (this->aggTypeContext.context[i])
            {
                case INT32: {
                    aggs.push_back(new SumAggregator(1, inputRaw, outputPartial));
                    break;
                }
                case INT64: {
                    aggs.push_back(new SumAggregator(2, inputRaw, outputPartial));
                    break;
                }
                case DOUBLE: {
                    aggs.push_back(new SumAggregator(3, inputRaw, outputPartial));
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
                    aggs.push_back(new AverageAggregator(1, inputRaw, outputPartial));
                    break;
                }
                case INT64: {
                    aggs.push_back(new AverageAggregator(2, inputRaw, outputPartial));
                    break;
                }
                case DOUBLE: {
                    aggs.push_back(new AverageAggregator(3, inputRaw, outputPartial));
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
                    aggs.push_back(new MaxAggregator(1, inputRaw, outputPartial));
                    break;
                }
                case INT64: {
                    aggs.push_back(new MaxAggregator(2, inputRaw, outputPartial));
                    break;
                }
                case DOUBLE: {
                    aggs.push_back(new MaxAggregator(3, inputRaw, outputPartial));
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
                    aggs.push_back(new MinAggregator(1, inputRaw, outputPartial));
                    break;
                }
                case INT64: {
                    aggs.push_back(new MinAggregator(2, inputRaw, outputPartial));
                    break;
                }
                case DOUBLE: {
                    aggs.push_back(new MinAggregator(3, inputRaw, outputPartial));
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
                    aggs.push_back(new CountAggregator(1, inputRaw, outputPartial));
                    break;
                }
                case INT64: {
                    aggs.push_back(new CountAggregator(2, inputRaw, outputPartial));
                    break;
                }
                case DOUBLE: {
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

    AggregationOperator* aggOp = new AggregationOperator(aggIndex, aggs);
    return aggOp;
}

int32_t AggregationOperator::addInput(Table* table, int32_t rowCount)
{
#ifdef DEBUG_LEVEL_HIGH
    DebugFuncEntry;
#endif
    this->preLoop(table);
    char** vecPtrs = table->getHeads();
    int32_t* columnTypes = reinterpret_cast<int32_t*>(table->getColumnTypes());

    int32_t columnNum = table->getColumnNumber();
    int32_t aggColNum = this->aggCols.size();
    if (columnNum != aggColNum) {
        DebugError("Doing pure aggregation needs column number to equal with aggregate column number, but columnNum = %d aggColNum =%d", \
        columnNum, aggColNum);
    }
    int32_t* aggFuncTypes = new int32_t[aggColNum];
    
    for (int32_t i = 0; i < aggColNum; ++i) {
        aggFuncTypes[i] = this->aggregators[i]->getType();
    }
    for (int32_t rowOffst = 0; rowOffst < rowCount; ++rowOffst) {
        this->inLoop(vecPtrs, rowOffst, columnNum, columnTypes, aggFuncTypes);
    }
    this->postLoop(table);
    delete[] aggFuncTypes;
#ifdef DEBUG_LEVEL_HIGH
    DebugFuncExit;
#endif
    return 0;    
}

int32_t AggregationOperator::addInput(Table** tables, int32_t* rowCount, int32_t pageCount)
{
    for (int32_t tableIdx = 0; tableIdx < pageCount; ++tableIdx) {
        this->addInput(tables[tableIdx], rowCount[tableIdx]);
    }
    return 0;
}

SPECIALIZE(OMNIJIT_NON_GROUP_INLOOP)
void AggregationOperator::inLoop(char** vecPtrs, uint32_t offset, int32_t colNum, int32_t* aggDataType, int32_t* aggFuncType)
{
    for (int32_t aggIdx = 0; aggIdx < colNum; ++aggIdx) {
        int32_t type = aggDataType[aggIdx];
        void* colPtr = vecPtrs[aggIdx];
        aggregators[aggIdx]->processNonGroup(colPtr, type, offset);
    }
}

void AggregationOperator::constructColumn(Table* table, int32_t* types, uint32_t aggColSize)
{
    // allocate result memory
    allocateVec(table, types, 0, true, aggColSize, 1);

    // set result value
    for (int32_t colIdx = 0; colIdx < aggColSize; ++colIdx) {
        AggregateType aggType = this->aggregators[colIdx]->getType();
        auto state = this->aggregators[colIdx]->getNonGroupState();
        auto col = table->getColumn(colIdx);
        switch(aggType) {
            case SUM:
            case MIN:
            case MAX: {
                switch (types[colIdx])
                {
                    case 1:{
                        reinterpret_cast<int32_t*>(col->getData())[0] = *static_cast<int32_t*>(state.val);
                        break;
                    }
                    case 2:{
                        reinterpret_cast<int64_t*>(col->getData())[0] = *static_cast<int64_t*>(state.val);
                        break;
                    }
                    case 3:{
                        reinterpret_cast<double*>(col->getData())[0] = *static_cast<double*>(state.val);
                        break;
                    }
                    default:
                        break;
                }
                break;
            }
            case COUNT: {
                reinterpret_cast<int64_t*>(col->getData())[0] = state.count;
                break;
            }
            case AVG: { // TODO process intermediate vectors
                if (state.count == 0) {
                    DebugError("Divisor is zero! column index = %d", colIdx);
                }
                switch (outputPartial) {
                    case 0: {
                        reinterpret_cast<double*>(col->getData())[0] = *static_cast<double*>(state.avgVal);
                        break;
                    }
                    case 1: {
                        // construct row type vector
                        break;
                    }
                    default:
                        break;
                }
                break;
            }
            default: {
                DebugError("Not support %d aggregate type!", aggType);
                break;
            }
        }
    }
}

// always output one row
int AggregationOperator::getOutput(std::vector<Table*>& result)
{
    uint32_t colSize = aggCols.size();
    
    // int32_t* types = (int32_t*)omni_allocate(colSize * sizeof(uint32_t));    
    int32_t* types = new int32_t[colSize]; 
    int32_t idx = 0;   
    for (auto& i : aggCols) {
        types[idx++] = i.type;
    }

    Table* table = new Table(1, colSize);
    // allocate column memory
    constructColumn(table, types, colSize);
    result.push_back(table);
    delete[] types;
#ifdef DEBUG_LEVEL_LOW
    std::stringstream os;
    os << std::this_thread::get_id();
    DebugPrint("Thread %s: end of getResult.", os.str().c_str());
#endif
    // set finished.
    status = 2;
    return status;
}

}
}