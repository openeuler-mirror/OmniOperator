/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Aggregation Base Class
 * Author: Songling Liu
 * Create: 2021-07-01
 * Notes: None
 */
#include "non_group_aggregation.h"
#include "../../jit/annotation.h"
#include "../optimization.h"
#include "../../vector/vector_common.h"
#include "../status.h"

namespace omniruntime {
namespace op {
Operator *AggregationOperatorFactory::CreateOperator()
{
    std::vector<ColumnIndex> aggIndex;
    std::vector<unique_ptr<Aggregator>> aggs;

    for (int32_t i = 0; i < this->aggTypeContext.len; ++i) {
        ColumnIndex c = { static_cast<uint32_t>(i), static_cast<VecType>(this->aggTypeContext.context[i]) };
        aggIndex.push_back(c);

        if (static_cast<AggregateType>(this->aggFuncTypeContext.context[i] == OMNI_AGGREGATION_TYPE_SUM)) {
            switch (this->aggTypeContext.context[i]) {
                case OMNI_VEC_TYPE_INT: {
                    aggs.push_back(make_unique<SumAggregator>(1));
                    break;
                }
                case OMNI_VEC_TYPE_LONG: {
                    aggs.push_back(make_unique<SumAggregator>(2));
                    break;
                }
                case OMNI_VEC_TYPE_DOUBLE: {
                    aggs.push_back(make_unique<SumAggregator>(3));
                    break;
                }
                default: {
                    DebugError("No such type %d", this->aggTypeContext.context[i]);
                    break;
                }
            }
        } else if ((AggregateType)this->aggFuncTypeContext.context[i] == OMNI_AGGREGATION_TYPE_AVG) {
            switch (this->aggTypeContext.context[i]) {
                case OMNI_VEC_TYPE_INT: {
                    aggs.push_back(make_unique<AverageAggregator>(1));
                    break;
                }
                case OMNI_VEC_TYPE_LONG: {
                    aggs.push_back(make_unique<AverageAggregator>(2));
                    break;
                }
                case OMNI_VEC_TYPE_DOUBLE: {
                    aggs.push_back(make_unique<AverageAggregator>(3));
                    break;
                }
                default: {
                    DebugError("No such type %d", this->aggTypeContext.context[i]);
                    break;
                }
            }
        } else if ((AggregateType)this->aggFuncTypeContext.context[i] == OMNI_AGGREGATION_TYPE_MAX) {
            switch (this->aggTypeContext.context[i]) {
                case OMNI_VEC_TYPE_INT: {
                    aggs.push_back(make_unique<MaxAggregator>(1));
                    break;
                }
                case OMNI_VEC_TYPE_LONG: {
                    aggs.push_back(make_unique<MaxAggregator>(2));
                    break;
                }
                case OMNI_VEC_TYPE_DOUBLE: {
                    aggs.push_back(make_unique<MaxAggregator>(3));
                    break;
                }
                default: {
                    DebugError("No such type %d", this->aggTypeContext.context[i]);
                    break;
                }
            }
        } else if ((AggregateType)this->aggFuncTypeContext.context[i] == OMNI_AGGREGATION_TYPE_MIN) {
            switch (this->aggTypeContext.context[i]) {
                case OMNI_VEC_TYPE_INT: {
                    aggs.push_back(make_unique<MinAggregator>(1));
                    break;
                }
                case OMNI_VEC_TYPE_LONG: {
                    aggs.push_back(make_unique<MinAggregator>(2));
                    break;
                }
                case OMNI_VEC_TYPE_DOUBLE: {
                    aggs.push_back(make_unique<MinAggregator>(3));
                    break;
                }
                default: {
                    DebugError("No such type %d", this->aggTypeContext.context[i]);
                    break;
                }
            }
        } else if ((AggregateType)this->aggFuncTypeContext.context[i] == OMNI_AGGREGATION_TYPE_COUNT) {
            switch (this->aggTypeContext.context[i]) {
                case OMNI_VEC_TYPE_INT: {
                    aggs.push_back(make_unique<CountAggregator>(1));
                    break;
                }
                case OMNI_VEC_TYPE_LONG: {
                    aggs.push_back(make_unique<CountAggregator>(2));
                    break;
                }
                case OMNI_VEC_TYPE_DOUBLE: {
                    aggs.push_back(make_unique<CountAggregator>(3));
                    break;
                }
                default: {
                    DebugError("No such type %d", this->aggTypeContext.context[i]);
                    break;
                }
            }
        } else {
            // UDAF
        }
    }

    AggregationOperator *aggOp = new AggregationOperator(aggIndex, aggs, inputRaw, outputPartial);
    return aggOp;
}

int32_t AggregationOperator::AddInput(VectorBatch *vecBatch)
{
#ifdef DEBUG_LEVEL_HIGH
    DebugFuncEntry;
#endif
    this->PreLoop(vecBatch);

    int32_t vectorCount = vecBatch->getVectorCount();
    int32_t aggColNum = this->aggCols.size();
    if (vectorCount != aggColNum) {
        DebugError("Doing pure aggregation needs column number to equal with aggregate column number, but vectorCount "
                   "= %d aggColNum =%d",
            vectorCount, aggColNum);
    }

    int32_t *vectorTypes = (int32_t *)vecBatch->getVectorTypes();

    int32_t *aggFuncTypes = new int32_t[aggColNum];

    for (int32_t i = 0; i < aggColNum; ++i) {
        aggFuncTypes[i] = this->aggregators[i]->GetType();
    }

    int32_t rowCount = vecBatch->getRowCount();
    for (int32_t rowOffst = 0; rowOffst < rowCount; ++rowOffst) {
        this->InLoop(vecBatch->getVectors(), rowOffst, vectorCount, vectorTypes, aggFuncTypes);
    }

    this->PostLoop(vecBatch);
    delete[] aggFuncTypes;
#ifdef DEBUG_LEVEL_HIGH
    DebugFuncExit;
#endif
    return 0;
}

SPECIALIZE(OMNIJIT_NON_GROUP_INLOOP)
void AggregationOperator::InLoop(Vector **vectors, uint32_t offset, int32_t colNum, int32_t *aggDataType,
    int32_t *aggFuncType)
{
    for (int32_t aggIdx = 0; aggIdx < colNum; ++aggIdx) {
        int32_t type = aggDataType[aggIdx];
        void *colPtr = vectors[aggIdx];
        aggregators[aggIdx]->ProcessNonGroup(colPtr, type, offset);
    }
}

void AggregationOperator::FillResultVectors(VectorBatch *vecBatch)
{
    // set result value
    int32_t vectorCount = vecBatch->getVectorCount();
    for (int32_t colIdx = 0; colIdx < vectorCount; ++colIdx) {
        AggregateType aggType = this->aggregators[colIdx]->GetType();
        auto state = this->aggregators[colIdx]->GetNonGroupState();
        auto vector = vecBatch->getVector(colIdx);
        switch (aggType) {
            case OMNI_AGGREGATION_TYPE_SUM:
            case OMNI_AGGREGATION_TYPE_MIN:
            case OMNI_AGGREGATION_TYPE_MAX: {
                switch (vector->getType()) {
                    case 1: {
                        ((IntVector *)vector)->setValue(0, *static_cast<int32_t *>(state.val));
                        break;
                    }
                    case 2: {
                        ((LongVector *)vector)->setValue(0, *static_cast<int64_t *>(state.val));
                        break;
                    }
                    case 3: {
                        ((DoubleVector *)vector)->setValue(0, *static_cast<double *>(state.val));
                        break;
                    }
                    default:
                        break;
                }
                break;
            }
            case OMNI_AGGREGATION_TYPE_COUNT: {
                ((LongVector *)vector)->setValue(0, state.count);
                break;
            }
            case OMNI_AGGREGATION_TYPE_AVG: {
                if (state.count == 0) {
                    DebugError("Divisor is zero! column index = %d", colIdx);
                }
                ((DoubleVector *)vector)->setValue(0, *reinterpret_cast<double *>(state.avgVal));
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
int AggregationOperator::GetOutput(std::vector<VectorBatch *> &result)
{
    uint32_t colSize = aggCols.size();

    int32_t *types = new int32_t[colSize];
    int32_t idx = 0;
    for (int32_t i = 0; i < colSize; ++i) {
        if (aggregators[i]->GetType() == OMNI_AGGREGATION_TYPE_COUNT) {
            types[i] = 2;
            continue;
        }
        if (aggregators[i]->GetType() == OMNI_AGGREGATION_TYPE_AVG) {
            types[i] = 3;
            continue;
        }
        types[i] = aggCols[i].type;
    }

    VectorBatch *vecBatch = new VectorBatch(types, colSize, 1);
    FillResultVectors(vecBatch);
    result.push_back(vecBatch);
    delete[] types;
#ifdef DEBUG_LEVEL_LOW
    std::stringstream os;
    os << std::this_thread::get_id();
    DebugPrint("Thread %s: end of getResult.", os.str().c_str());
#endif
    // set finished.

    SetStatus(OMNI_STATUS_FINISHED);
    return OMNI_STATUS_FINISHED;
}
}
}