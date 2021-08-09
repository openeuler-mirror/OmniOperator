/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Hash Aggregation Source File
 */
#include "group_aggregation.h"

#include <cmath>

#include "../../vector/vector_common.h"
#include "../status.h"
#include "../../jit/annotation.h"
#include "../optimization.h"
#include "../../vector/container_vector.h"
#include "../../util/type_util.h"

#if defined(DEBUG_LEVEL_LOW) || defined(DEBUG_LEVEL_HIGH)
#include <sstream>
#endif
namespace omniruntime {
namespace op {
using namespace omniruntime::vec;
OmniStatus HashAggregationOperatorFactory::Init()
{
    OmniStatus ret = OMNI_STATUS_NORMAL;
    if (aggTypeContext.len != aggColContext.len || aggTypeContext.len != aggFuncTypeContext.len ||
        aggFuncTypeContext.len != aggColContext.len || groupByTypeContext.len != groupByColContext.len) {
        ret = OMNI_STATUS_ERROR;
    }
    for (int32_t i = 0; i < aggFuncTypeContext.len; ++i) {
        aggColIdx.push_back(aggColContext.context[i]);
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
    for (int32_t i = 0; i < groupByTypeContext.len; ++i) {
        groupByColIdx.push_back(groupByColContext.context[i]);
        groupByTypes.push_back(groupByTypeContext.context[i]);
    }
    return ret;
}

OmniStatus HashAggregationOperatorFactory::Close()
{
    return OMNI_STATUS_NORMAL;
}

Operator *HashAggregationOperatorFactory::CreateOperator()
{
    std::vector<ColumnIndex> groupByIndex(groupByColIdx.size(), ColumnIndex());
    std::vector<ColumnIndex> aggIndex(aggColIdx.size(), ColumnIndex());
    std::vector<std::unique_ptr<Aggregator>> aggs;

    for (int32_t i = 0; i < this->groupByColIdx.size(); ++i) {
        ColumnIndex c = { this->groupByColIdx[i], static_cast<VecType>(this->groupByTypes[i]) };
        groupByIndex[i] = c;
    }
    for (int32_t i = 0; i < this->aggColIdx.size(); ++i) {
        ColumnIndex c = { this->aggColIdx[i], static_cast<VecType>(this->aggTypes[i]) };
        aggIndex[i] = c;
        auto aggregator = aggregatorFactories[i]->CreateAggregator(this->aggTypes[i], inputRaw, outputPartial);
        aggs.push_back(std::move(aggregator));
    }

    HashAggregationOperator *groupBy =
        new HashAggregationOperator(groupByIndex, aggIndex, std::move(aggs), inputRaw, outputPartial);
    return groupBy;
}

void HashAggregationOperator::PreLoop(VectorBatch *vecBatch)
{
    int32_t colSize = groupByCols.size() + aggCols.size();
    sourceTypes = std::make_unique<int32_t[]>(colSize).release();
    int32_t idx = 0;
    for (auto &c : groupByCols) {
        sourceTypes[idx++] = static_cast<int32_t>(c.type.GetId());
    }
    for (auto &c : aggCols) {
        sourceTypes[idx++] = static_cast<int32_t>(c.type.GetId());
    }
}

void HashAggregationOperator::PostLoop(VectorBatch *vecBatch) const {}

SPECIALIZE(OMNIJIT_HASH_GROUPBY_INLOOP)
void HashAggregationOperator::InLoop(Vector **vectors, uint32_t offset, const int32_t *types, int32_t colNum,
    const int32_t *groupByColIdx, int32_t groupByColNum, const int32_t *aggColIdx, int32_t aggColNum,
    const int32_t *aggFuncTypes)
{
    int64_t combineHashVal = 0;
    MultiChannelHash hashFunc;
    for (int32_t i = 0; i < groupByColNum; ++i) {
        uint64_t hash = 0;
        uint32_t idx = groupByColIdx[i];
        switch (types[idx]) {
            case OMNI_VEC_TYPE_INT: {
                std::hash<int32_t> hashInt32;
                hash = hashInt32(static_cast<IntVector *>(vectors[idx])->GetValue(offset));
                break;
            }
            case OMNI_VEC_TYPE_LONG: {
                std::hash<int64_t> hashInt64;
                hash = hashInt64(static_cast<LongVector *>(vectors[idx])->GetValue(offset));
                break;
            }
            case OMNI_VEC_TYPE_DOUBLE: {
                std::hash<double> hashDouble;
                hash = hashDouble(static_cast<DoubleVector *>(vectors[idx])->GetValue(offset));
                break;
            }
            default: {
                DebugError("No such data type %d", types[idx]);
                break;
            }
        }
        combineHashVal = hashFunc.CombineHash(combineHashVal, hash);
    }
    if (groupedRows.find(combineHashVal) == groupedRows.end()) {
        std::vector<GroupBySlot> groupByTuple(groupByColNum + aggColNum, GroupBySlot());
        for (int32_t i = 0; i < groupByColNum; ++i) {
            // copy col value to map
            void *rowPtr = nullptr;
            uint32_t idx = groupByColIdx[i];
            switch (types[idx]) {
                case OMNI_VEC_TYPE_INT: {
                    auto copyVal = std::make_unique<int32_t>(static_cast<IntVector *>(vectors[idx])->GetValue(offset));
                    rowPtr = reinterpret_cast<void *>(copyVal.release());
                    break;
                }
                case OMNI_VEC_TYPE_LONG: {
                    auto copyVal = std::make_unique<int64_t>(static_cast<LongVector *>(vectors[idx])->GetValue(offset));
                    rowPtr = reinterpret_cast<void *>(copyVal.release());
                    break;
                }
                case OMNI_VEC_TYPE_DOUBLE: {
                    auto copyVal = std::make_unique<double>(static_cast<DoubleVector *>(vectors[idx])->GetValue(offset));
                    rowPtr = reinterpret_cast<void *>(copyVal.release());
                    break;
                }
                default: {
                    DebugError("No such data type %d", types[idx]);
                    break;
                }
            }
            groupByTuple[i].val = rowPtr;
        }
        groupedRows[combineHashVal] = groupByTuple;

        for (int32_t i = 0; i < aggColNum; ++i) {
            int32_t idx = aggColIdx[i];
            int32_t type = types[idx];
            Vector *colPtr = vectors[idx];
            GroupBySlot& state = groupedRows[combineHashVal][groupByColNum + i];
            aggregators[i]->Insert(state, colPtr, type, offset);
        }
    } else {
        for (int32_t i = 0; i < aggColNum; ++i) {
            int32_t idx = aggColIdx[i];
            int32_t type = types[idx];
            Vector *colPtr = vectors[idx];
            GroupBySlot& state = groupedRows[combineHashVal][groupByColNum + i];
            aggregators[i]->ProcessGroup(state, colPtr, type, offset);
        }
    }
}

int32_t HashAggregationOperator::AddInput(VectorBatch *vecBatch)
{
#ifdef DEBUG_LEVEL_HIGH
    DebugFuncEntry;
#endif
    this->PreLoop(vecBatch);
    int32_t vectorCount = vecBatch->GetVectorCount();
    auto vectorTypes = std::make_unique<int32_t[]>(vectorCount);
    vecBatch->GetVectorTypeIds(vectorTypes.get());

    int32_t groupColNum = this->groupByCols.size();
    auto groupByColIdx = std::make_unique<int32_t[]>(groupColNum);
    int32_t aggColNum = this->aggCols.size();
    auto aggColIdx = std::make_unique<int32_t[]>(aggColNum);
    auto aggFuncTypes = std::make_unique<int32_t[]>(aggColNum);

    for (int32_t i = 0; i < groupColNum; ++i) {
        groupByColIdx[i] = this->groupByCols[i].idx;
    }
    for (int32_t i = 0; i < aggColNum; ++i) {
        aggColIdx[i] = this->aggCols[i].idx;
        aggFuncTypes[i] = this->aggregators[i]->GetType();
    }
    int32_t rowCount = vecBatch->GetRowCount();
    Vector **vectors = vecBatch->GetVectors();
    for (int32_t i = 0; i < rowCount; ++i) {
        this->InLoop(vectors, i, vectorTypes.get(), vectorCount, groupByColIdx.get(), groupColNum, aggColIdx.get(),
            aggColNum, aggFuncTypes.get());
    }
    this->PostLoop(vecBatch);
    return 0;
}

int32_t HashAggregationOperator::GetRowSize(std::vector<int32_t>& types, int32_t columnCount)
{
    int32_t rowSize = 0;
    int32_t typeIndex = 0;
    for (auto &i : groupByCols) {
        types.push_back(i.type.GetId());
        rowSize += TypeUtil::GetVarByteSize(i.type.GetId());
    }
    for (int32_t i = 0; i < aggCols.size(); ++i) {
        if (aggregators[i]->GetType() == OMNI_AGGREGATION_TYPE_COUNT) {
            types.push_back(OMNI_VEC_TYPE_LONG);
            rowSize += sizeof(int64_t);
            continue;
        }
        if (aggregators[i]->GetType() == OMNI_AGGREGATION_TYPE_AVG) {
            if (aggregators[i]->IsOutputPartial()) {
                types.push_back(OMNI_VEC_TYPE_CONTAINER);
                rowSize += sizeof(int64_t);
            } else {
                types.push_back(OMNI_VEC_TYPE_DOUBLE);
            }
            rowSize += sizeof(double);
            continue;
        }
        types.push_back(aggCols[i].type.GetId());
        rowSize += TypeUtil::GetVarByteSize(aggCols[i].type.GetId());
    }
    return rowSize;
}

SPECIALIZE(OMNIJIT_HASH_GROUPBY_HASH_COLUMN)
void HashAggregationOperator::FillGroupByVectors(VectorBatch *vecBatch, int startIndex, int endIndex,
    RowIterator &rowIterator, int32_t rowCount)
{
    RowIterator tempRowIterator = rowIterator;
    for (int colIndex = startIndex, groupByIndex = 0; colIndex < endIndex; ++colIndex, ++groupByIndex) {
        tempRowIterator = rowIterator;
        switch (groupByCols[groupByIndex].type.GetId()) {
            case OMNI_VEC_TYPE_INT: {
                IntVector *vector = static_cast<IntVector *>(vecBatch->GetVector(colIndex));
                for (int rowIndex = 0; rowIndex < rowCount && tempRowIterator != groupedRows.end();
                    ++rowIndex, ++tempRowIterator) {
                    vector->SetValue(rowIndex, *(int32_t *)tempRowIterator->second[colIndex].val);
                }
                break;
            }
            case OMNI_VEC_TYPE_LONG: {
                LongVector *vector = static_cast<LongVector *>(vecBatch->GetVector(colIndex));
                for (int rowIndex = 0; rowIndex < rowCount && tempRowIterator != groupedRows.end();
                    ++rowIndex, ++tempRowIterator) {
                    vector->SetValue(rowIndex, *(int64_t *)tempRowIterator->second[colIndex].val);
                }
                break;
            }
            case OMNI_VEC_TYPE_DOUBLE: {
                DoubleVector *vector = static_cast<DoubleVector *>(vecBatch->GetVector(colIndex));
                for (int rowIndex = 0; rowIndex < rowCount && tempRowIterator != groupedRows.end();
                    ++rowIndex, ++tempRowIterator) {
                    vector->SetValue(rowIndex, *(double *)tempRowIterator->second[colIndex].val);
                }
                break;
            }
            default: {
                DebugError("Type %d doesn't support", groupByCols[groupByIndex].type.GetId());
                break;
            }
        }
    }
}

// TODO currently we need to traverse ColumnNum * RowNum times to build the output.
// The overhead need to be optimized.
SPECIALIZE(OMNIJIT_HASH_GROUPBY_AGG_COLUMN)
void HashAggregationOperator::FillAggVectors(VectorBatch *vecBatch, int startIndex, int endIndex, RowIterator &rowIterator,
    int32_t rowCount)
{
    auto resultIterator = rowIterator;
    for (int32_t aggIndex = 0, colIndex = startIndex; colIndex < endIndex; ++aggIndex, ++colIndex) {
        resultIterator = rowIterator;
        auto finalState = resultIterator->second[colIndex];
        AggregateType aggType = this->aggregators[aggIndex]->GetType();
        switch (aggType) {
            case OMNI_AGGREGATION_TYPE_SUM:
            case OMNI_AGGREGATION_TYPE_MIN:
            case OMNI_AGGREGATION_TYPE_MAX: {
                switch (aggCols[aggIndex].type.GetId()) {
                    case OMNI_VEC_TYPE_INT: {
                        IntVector *vector = static_cast<IntVector *>(vecBatch->GetVector(colIndex));
                        for (int32_t rIdx = 0;
                            rIdx < rowCount && resultIterator != groupedRows.end();
                            ++rIdx, resultIterator++) {
                            vector->SetValue(rIdx, *reinterpret_cast<int32_t *>(finalState.val));
                        }
                        break;
                    }
                    case OMNI_VEC_TYPE_LONG: {
                        LongVector *vector = static_cast<LongVector *>(vecBatch->GetVector(colIndex));
                        for (int32_t rIdx = 0;
                            rIdx < rowCount && resultIterator != groupedRows.end();
                            ++rIdx, resultIterator++) {
                            vector->SetValue(rIdx, *reinterpret_cast<int64_t *>(finalState.val));
                        }
                        break;
                    }
                    case OMNI_VEC_TYPE_DOUBLE: {
                        DoubleVector *vector = static_cast<DoubleVector*>(vecBatch->GetVector(colIndex));
                        for (int32_t rIdx = 0;
                            rIdx < rowCount && resultIterator != groupedRows.end();
                            ++rIdx, resultIterator++) {
                            vector->SetValue(rIdx, *reinterpret_cast<double *>(finalState.val));
                        }
                        break;
                    }
                    default:
                        break;
                }
                break;
            }
            case OMNI_AGGREGATION_TYPE_COUNT: {
                LongVector *vector = static_cast<LongVector *>(vecBatch->GetVector(colIndex));
                for (int32_t rIdx = 0;
                    rIdx < rowCount && resultIterator != groupedRows.end();
                    ++rIdx, resultIterator++) {
                    vector->SetValue(rIdx, reinterpret_cast<int64_t>(finalState.count));
                }
                break;
            }
            case OMNI_AGGREGATION_TYPE_AVG: { // TODO process intermediate vectors
                // generate double or row type vector according to the step. Row type if outputPartial == 1 otherwise
                // double vector.
                if (outputPartial) {
                    ContainerVector *vector = static_cast<ContainerVector *>(vecBatch->GetVector(colIndex));
                    for (int32_t rIdx = 0;
                        rIdx < rowCount && resultIterator != groupedRows.end();
                        ++rIdx, resultIterator++) {
                        if (resultIterator->second[colIndex].avgCnt == 0) {
                            DebugError("Divisor is zero! key = %ld", resultIterator->first);
                        }
                        DoubleVector *doubleVector = reinterpret_cast<DoubleVector *>(vector->getValue(0));
                        doubleVector->SetValue(rIdx, *(reinterpret_cast<double *>(finalState.avgVal)));
                        LongVector *longVector = reinterpret_cast<LongVector *>(vector->getValue(1));
                        longVector->SetValue(rIdx, finalState.avgCnt);
                    }
                } else {
                    DoubleVector *vector = static_cast<DoubleVector *>(vecBatch->GetVector(colIndex));
                    for (int32_t rIdx = 0;
                        rIdx < rowCount && resultIterator != groupedRows.end();
                        ++rIdx, resultIterator++) {
                        if (finalState.avgCnt == 0) {
                            DebugError("Divisor is zero! key = %ld", resultIterator->first);
                        }
                        vector->SetValue(rIdx, *(reinterpret_cast<double *>(finalState.avgVal)));
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
    rowIterator = resultIterator;
#ifdef DEBUG_LEVEL_HIGH
    DebugFuncExit;
#endif
}

void SetVectors(VectorBatch *vectorBatch, const std::vector<int32_t>& types, int32_t rowCount)
{
    for (int colIndex = 0; colIndex < vectorBatch->GetVectorCount(); ++colIndex) {
        switch (types[colIndex]) {
            case OMNI_VEC_TYPE_INT: {
                vectorBatch->SetVector(colIndex, new IntVector(nullptr, rowCount));
                break;
            }
            case OMNI_VEC_TYPE_LONG: {
                vectorBatch->SetVector(colIndex, new LongVector(nullptr, rowCount));
                break;
            }
            case OMNI_VEC_TYPE_DOUBLE: {
                vectorBatch->SetVector(colIndex, new DoubleVector(nullptr, rowCount));
                break;
            }
            case OMNI_VEC_TYPE_CONTAINER: {
                DoubleVector *doubleVector = new DoubleVector(nullptr, rowCount);
                LongVector *longVector = new LongVector(nullptr, rowCount);
                Vector **vectorAddresses = new Vector *[2];
                vectorAddresses[0] = doubleVector;
                vectorAddresses[1] = longVector;
                VecType *vecTypes = new VecType[2];
                vecTypes[0] = DoubleVecType::Instance();
                vecTypes[1] = LongVecType::Instance();
                ContainerVector *containerVector =
                    new ContainerVector(nullptr, rowCount, vectorAddresses, AVG_VECTOR_COUNT, vecTypes);
                vectorBatch->SetVector(colIndex, containerVector);
                break;
            }
                // TODO: support other types!!!
            default: {
                break;
            }
        }
    }
}

int32_t HashAggregationOperator::GetOutput(std::vector<VectorBatch *> &result)
{
    uint32_t groupByColSize = groupByCols.size();
    uint32_t aggColSize = aggCols.size();
    uint32_t colCount = groupByColSize + aggColSize;
    std::vector<int32_t> types;
    int32_t vecBatchCount;
    int32_t rowSize = GetRowSize(types, colCount);
    try {
        if (rowSize <= 0) {
            // TODO define our exception class
            throw std::exception();
        }
        int32_t maxRowNum = MAX_TABLE_SIZE_IN_BYTES / rowSize;
        vecBatchCount = std::ceil(static_cast<double>(this->groupedRows.size()) / static_cast<double>(maxRowNum));
        int32_t currentPosition = 0;

        RowIterator rowIterator = groupedRows.begin();
        for (int32_t i = 0; i < vecBatchCount; ++i) {
            int32_t rowCount = std::min(maxRowNum, static_cast<int32_t>((this->groupedRows.size() - currentPosition)));
            auto vecBatch = std::make_unique<VectorBatch>(colCount);
            SetVectors(vecBatch.get(), types, rowCount);
            FillGroupByVectors(vecBatch.get(), 0, groupByColSize, rowIterator, rowCount);
            FillAggVectors(vecBatch.get(), groupByColSize, colCount, rowIterator, rowCount);
            result.push_back(vecBatch.release());
            currentPosition += maxRowNum;
        }
    } catch (std::exception &e) {
        std::cerr << "Hash Aggregation getOutput failed." << std::endl;
    }
    // set finished.
    SetStatus(OMNI_STATUS_FINISHED);
    return vecBatchCount;
}

OmniStatus HashAggregationOperator::Close()
{
    auto groupByColSize = groupByCols.size();
    auto aggColSize = aggCols.size();
    for (auto &item : groupedRows) {
        for (int32_t idx = 0; idx < groupByColSize; ++idx) {
            switch (groupByCols[idx].type.GetId()) {
                case omniruntime::vec::OMNI_VEC_TYPE_INT: {
                    delete reinterpret_cast<int32_t *>(item.second[idx].val);
                    break;
                }
                case omniruntime::vec::OMNI_VEC_TYPE_LONG: {
                    delete reinterpret_cast<int64_t *>(item.second[idx].val);
                    break;
                }
                case omniruntime::vec::OMNI_VEC_TYPE_DOUBLE: {
                    delete reinterpret_cast<double *>(item.second[idx].val);
                    break;
                }
                default:
                    break;
            }
        }
        for (int32_t idx = 0; idx < aggColSize; ++idx) {
            if (aggregators[idx]->GetType() == OMNI_AGGREGATION_TYPE_COUNT) {
                continue;
            }
            if (aggregators[idx]->GetType() == OMNI_AGGREGATION_TYPE_AVG) {
                delete reinterpret_cast<double*>(item.second[groupByColSize + idx].avgVal);
                continue;
            }
            switch (aggCols[idx].type.GetId()) {
                case omniruntime::vec::OMNI_VEC_TYPE_INT: {
                    delete reinterpret_cast<int32_t *>(item.second[groupByColSize + idx].val);
                    break;
                }
                case omniruntime::vec::OMNI_VEC_TYPE_LONG: {
                    delete reinterpret_cast<int64_t *>(item.second[groupByColSize + idx].val);
                    break;
                }
                case omniruntime::vec::OMNI_VEC_TYPE_DOUBLE: {
                    delete reinterpret_cast<double *>(item.second[groupByColSize + idx].val);
                    break;
                }
                default:
                    break;
            }
        }
    }
    groupedRows.clear();
    return OMNI_STATUS_NORMAL;
}
} // end of namespace op
} // end of namespace omniruntime
