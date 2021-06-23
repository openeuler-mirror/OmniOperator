#include "non_group_aggregation.h"
#include "../../vector/vector_common.h"
#include "../status.h"
#include "../../jit/annotation.h"
#include "../optimization.h"

namespace omniruntime {
namespace op {

Operator* AggregationOperatorFactory::createOperator()
{
    std::vector<ColumnIndex> aggIndex;
    std::vector<Aggregator*> aggs;

    for (int32_t i = 0; i < this->aggTypeContext.len; ++i) {
        ColumnIndex c = {static_cast<uint32_t>(i), (VecType)this->aggTypeContext.context[i]};
        aggIndex.push_back(c);

        if ((AggregateType)this->aggFuncTypeContext.context[i] == OMNI_AGGREGATION_TYPE_SUM) {
            switch (this->aggTypeContext.context[i])
            {
                case OMNI_VEC_TYPE_INT: {
                    aggs.push_back(new SumAggregator(1));
                    break;
                }
                case OMNI_VEC_TYPE_LONG: {
                    aggs.push_back(new SumAggregator(2));
                    break;
                }
                case OMNI_VEC_TYPE_DOUBLE: {
                    aggs.push_back(new SumAggregator(3));
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
                    aggs.push_back(new AverageAggregator(1));
                    break;
                }
                case OMNI_VEC_TYPE_LONG: {
                    aggs.push_back(new AverageAggregator(2));
                    break;
                }
                case OMNI_VEC_TYPE_DOUBLE: {
                    aggs.push_back(new AverageAggregator(3));
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
                    aggs.push_back(new MaxAggregator(1));
                    break;
                }
                case OMNI_VEC_TYPE_LONG: {
                    aggs.push_back(new MaxAggregator(2));
                    break;
                }
                case OMNI_VEC_TYPE_DOUBLE: {
                    aggs.push_back(new MaxAggregator(3));
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
                    aggs.push_back(new MinAggregator(1));
                    break;
                }
                case OMNI_VEC_TYPE_LONG: {
                    aggs.push_back(new MinAggregator(2));
                    break;
                }
                case OMNI_VEC_TYPE_DOUBLE: {
                    aggs.push_back(new MinAggregator(3));
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
                    aggs.push_back(new CountAggregator(1));
                    break;
                }
                case OMNI_VEC_TYPE_LONG: {
                    aggs.push_back(new CountAggregator(2));
                    break;
                }
                case OMNI_VEC_TYPE_DOUBLE: {
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

    AggregationOperator* aggOp = new AggregationOperator(aggIndex, aggs);
    return aggOp;
}

int32_t AggregationOperator::addInput(VectorBatch* vecBatch)
{
#ifdef DEBUG_LEVEL_HIGH
    DebugFuncEntry;
#endif
    this->preLoop(vecBatch);

    int32_t vectorCount = vecBatch->getVectorCount();
    int32_t aggColNum = this->aggCols.size();
    if (vectorCount != aggColNum) {
        DebugError("Doing pure aggregation needs column number to equal with aggregate column number, but vectorCount = %d aggColNum =%d", \
        vectorCount, aggColNum);
    }

    int32_t *vectorTypes = (int32_t *)vecBatch->getVectorTypes();

    int32_t* aggFuncTypes = new int32_t[aggColNum];
    for (int32_t i = 0; i < aggColNum; ++i) {
        aggFuncTypes[i] = this->aggregators[i]->getType();
    }

    int32_t rowCount = vecBatch->getRowCount();
    for (int32_t rowOffst = 0; rowOffst < rowCount; ++rowOffst) {
        this->inLoop(vecBatch->getVectors(), rowOffst, vectorCount, vectorTypes, aggFuncTypes);
    }

    this->postLoop(vecBatch);
    delete[] aggFuncTypes;
#ifdef DEBUG_LEVEL_HIGH
    DebugFuncExit;
#endif
    return 0;
}

SPECIALIZE(OMNIJIT_NON_GROUP_INLOOP)
void AggregationOperator::inLoop(Vector** vectors, uint32_t offset, int32_t colNum, int32_t* aggDataType, int32_t* aggFuncType)
{
    for (int32_t aggIdx = 0; aggIdx < colNum; ++aggIdx) {
        int32_t type = aggDataType[aggIdx];
        void* colPtr = vectors[aggIdx];
        aggregators[aggIdx]->processNonGroup(colPtr, type, offset);
    }
}

void AggregationOperator::fillResultVectors(VectorBatch* vecBatch)
{
    int32_t vectorCount = vecBatch->getVectorCount();
    for (int32_t colIdx = 0; colIdx < vectorCount; ++colIdx) {
        AggregateType aggType = this->aggregators[colIdx]->getType();
        auto state = this->aggregators[colIdx]->getNonGroupState();
        auto col = vecBatch->getVector(colIdx);
        switch(aggType) {
            case OMNI_AGGREGATION_TYPE_SUM:
            case OMNI_AGGREGATION_TYPE_COUNT:
            case OMNI_AGGREGATION_TYPE_MIN:
            case OMNI_AGGREGATION_TYPE_MAX: {
                if (aggType == OMNI_AGGREGATION_TYPE_COUNT) {
                    ((LongVector *)col)->setValue(0, state.count);
                }else {
                    switch (col->getType())
                    {
                        case 1:{
                            ((IntVector *)col)->setValue(0, *static_cast<int32_t*>(state.val));
                            break;
                        }
                        case 2:{
                            ((LongVector *)col)->setValue(0, *static_cast<int64_t*>(state.val));
                            break;
                        }
                        case 3:{
                            ((DoubleVector *)col)->setValue(0, *static_cast<double*>(state.val));
                            break;
                        }
                        default:
                            break;
                    }
                }
                break;
            }
            case OMNI_AGGREGATION_TYPE_AVG: {
                if (state.count == 0) {
                    DebugError("Divisor is zero! column index = %d", colIdx);
                }
                ((DoubleVector *)col)->setValue(0, *static_cast<double*>(state.avgVal));
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
int AggregationOperator::getOutput(std::vector<VectorBatch*>& result)
{
    uint32_t colSize = aggCols.size();

    int32_t* types = new int32_t[colSize];
    int32_t idx = 0;
    for (auto& i : aggCols) {
        types[idx++] = i.type;
    }

    VectorBatch* vecBatch = new VectorBatch(types, colSize, 1);
    fillResultVectors(vecBatch);
    result.push_back(vecBatch);
    delete[] types;
#ifdef DEBUG_LEVEL_LOW
    std::stringstream os;
    os << std::this_thread::get_id();
    DebugPrint("Thread %s: end of getResult.", os.str().c_str());
#endif
    // set finished.

    setStatus(OMNI_STATUS_FINISHED);
    return OMNI_STATUS_FINISHED;
}

}
}