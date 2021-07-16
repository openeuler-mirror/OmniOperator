/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Aggregation Source File
 */
#include "non_group_aggregation.h"
#include "../../jit/annotation.h"
#include "../optimization.h"
#include "../../vector/vector_common.h"
#include "../status.h"

namespace omniruntime {
namespace op {

OmniStatus AggregationOperatorFactory::Init()
{
    OmniStatus ret = OMNI_STATUS_NORMAL;
    if (aggTypeContext.len != aggFuncTypeContext.len) {
        ret = OMNI_STATUS_ERROR;
    }
    for (int32_t i = 0; i < aggFuncTypeContext.len; ++i) {
        aggTypes.push_back(aggTypeContext.context[i]);
        switch (aggFuncTypeContext.context[i]) {
            case OMNI_AGGREGATION_TYPE_SUM: {
                aggregatorFactories.push_back(std::make_unique<SumAggregatorFactory>());
                break;
            }
            case OMNI_AGGREGATION_TYPE_COUNT: {
                aggregatorFactories.push_back(std::make_unique<CountAggregatorFactory>());
                break;
            }
            case OMNI_AGGREGATION_TYPE_MAX: {
                aggregatorFactories.push_back(std::make_unique<MaxAggregatorFactory>());
                break;
            }
            case OMNI_AGGREGATION_TYPE_MIN: {
                aggregatorFactories.push_back(std::make_unique<MinAggregatorFactory>());
                break;
            }
            case OMNI_AGGREGATION_TYPE_AVG: {
                aggregatorFactories.push_back(std::make_unique<AverageAggregatorFactory>());
                break;
            }
            default: {
                ret = OMNI_STATUS_ERROR;
            }
        }
    }
    return ret;
}

OmniStatus AggregationOperatorFactory::Close()
{
    return OMNI_STATUS_NORMAL;
}

Operator *AggregationOperatorFactory::CreateOperator()
{
    std::vector<ColumnIndex> aggIndex;
    std::vector<unique_ptr<Aggregator>> aggs;

    for (int32_t i = 0; i < this->aggTypes.size(); ++i) {
        ColumnIndex c = { static_cast<uint32_t>(i), static_cast<VecType>(this->aggTypes[i]) };
        aggIndex.push_back(c);
        auto aggregator = aggregatorFactories[i]->CreateAggregator(this->aggTypes[i]);
        aggs.push_back(std::move(aggregator));
    }

    AggregationOperator *aggOp = new AggregationOperator(aggIndex, std::move(aggs), inputRaw, outputPartial);
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

    int32_t *vectorTypes = reinterpret_cast<int32_t *>(vecBatch->getVectorTypes());

    auto aggFuncTypes = make_unique<int32_t[]>(aggColNum);

    for (int32_t i = 0; i < aggColNum; ++i) {
        aggFuncTypes[i] = this->aggregators[i]->GetType();
    }

    int32_t rowCount = vecBatch->getRowCount();
    for (int32_t rowOffst = 0; rowOffst < rowCount; ++rowOffst) {
        this->InLoop(vecBatch->getVectors(), rowOffst, vectorCount, vectorTypes, aggFuncTypes.get());
    }

    this->PostLoop(vecBatch);
#ifdef DEBUG_LEVEL_HIGH
    DebugFuncExit;
#endif
    return 0;
}

SPECIALIZE(OMNIJIT_NON_GROUP_INLOOP)
void AggregationOperator::InLoop(Vector **vectors, uint32_t offset, int32_t colNum, const int32_t *aggDataType,
    const int32_t *aggFuncType)
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
                    case OMNI_VEC_TYPE_INT: {
                        dynamic_cast<IntVector *>(vector)->setValue(0, *static_cast<int32_t *>(state.val));
                        break;
                    }
                    case OMNI_VEC_TYPE_LONG: {
                        dynamic_cast<LongVector *>(vector)->setValue(0, *static_cast<int64_t *>(state.val));
                        break;
                    }
                    case OMNI_VEC_TYPE_DOUBLE: {
                        dynamic_cast<DoubleVector *>(vector)->setValue(0, *static_cast<double *>(state.val));
                        break;
                    }
                    default:
                        break;
                }
                break;
            }
            case OMNI_AGGREGATION_TYPE_COUNT: {
                dynamic_cast<LongVector *>(vector)->setValue(0, state.count);
                break;
            }
            case OMNI_AGGREGATION_TYPE_AVG: {
                if (state.count == 0) {
                    DebugError("Divisor is zero! column index = %d", colIdx);
                }
                dynamic_cast<DoubleVector *>(vector)->setValue(0, *reinterpret_cast<double *>(state.avgVal));
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

    auto types = make_unique<int32_t[]>(colSize);
    int32_t idx = 0;
    for (int32_t i = 0; i < colSize; ++i) {
        if (aggregators[i]->GetType() == OMNI_AGGREGATION_TYPE_COUNT) {
            types[i] = OMNI_VEC_TYPE_LONG;
            continue;
        }
        if (aggregators[i]->GetType() == OMNI_AGGREGATION_TYPE_AVG) {
            types[i] = OMNI_VEC_TYPE_DOUBLE;
            continue;
        }
        types[i] = aggCols[i].type;
    }

    VectorBatch *vecBatch = new VectorBatch(types.get(), colSize, 1);
    FillResultVectors(vecBatch);
    result.push_back(vecBatch);
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