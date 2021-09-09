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
#include "../hash_util.h"
#include "../util/operator_util.h"

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
                DebugError("No such agg func type %d", aggFuncTypeContext.context[i]);
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
    groupBy->Init();
    return groupBy;
}

OmniStatus HashAggregationOperator::Init()
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
    return OMNI_STATUS_NORMAL;
}

void HashAggregationOperator::PreLoop(VectorBatch *vecBatch)
{}

void HashAggregationOperator::PostLoop(VectorBatch *vecBatch) const {}

void* ALWAYS_INLINE DuplicateGroupByTuple(int32_t type, Vector* vector, uint32_t offset)
{
    void *rowPtr = nullptr;
    switch (type) {
        case OMNI_VEC_TYPE_INT:
        case OMNI_VEC_TYPE_DATE32: {
            rowPtr = std::make_unique<int32_t>(
                    static_cast<IntVector *>(vector)->GetValue(offset)).release();
            break;
        }
        case OMNI_VEC_TYPE_LONG:
        case OMNI_VEC_TYPE_DECIMAL64: {
            rowPtr = std::make_unique<int64_t>(
                    static_cast<LongVector *>(vector)->GetValue(offset)).release();
            break;
        }
        case OMNI_VEC_TYPE_DOUBLE: {
            rowPtr = std::make_unique<double>(
                    static_cast<DoubleVector *>(vector)->GetValue(offset)).release();
            break;
        }
        case OMNI_VEC_TYPE_DECIMAL128: {
            rowPtr = std::make_unique<Decimal128>(
                    static_cast<Decimal128Vector *>(vector)->GetValue(offset)).release();
            break;
        }
        case OMNI_VEC_TYPE_VARCHAR: {
            uint8_t *data = nullptr;
            int valLen = (static_cast<VarcharVector *>(vector)->GetValue(offset, &data));
            rowPtr = reinterpret_cast<void *>(new std::string(reinterpret_cast<char *>(data), 0, valLen));
            break;
        }
        default: {
            DebugError("No such data type %d", type);
            break;
        }
    }
    return rowPtr;
}

void ALWAYS_INLINE IntVectorHash(const uint32_t start, const uint32_t rowCount,
                                 IntVector *vec, int64_t *combinedHashVal)
{
    uint64_t hash;
    std::hash<int32_t> hashInt32;
    for (int j = 0; j < rowCount; ++j) {
        hash = hashInt32(vec->GetValue(j + start));
        combinedHashVal[j] = HashUtil::CombineHash(combinedHashVal[j], hash);
    }
}

void ALWAYS_INLINE LongVectorHash(const uint32_t start, const uint32_t rowCount,
                                  LongVector *vec, int64_t *combinedHashVal)
{
    uint64_t hash;
    std::hash<int64_t> hashInt64;
    for (int j = 0; j < rowCount; ++j) {
        hash = hashInt64(vec->GetValue(j + start));
        combinedHashVal[j] = HashUtil::CombineHash(combinedHashVal[j], hash);
    }
}

void ALWAYS_INLINE DoubleVectorHash(const uint32_t start, const uint32_t rowCount,
                                    DoubleVector *vec, int64_t *combinedHashVal)
{
    uint64_t hash;
    std::hash<double> hashDouble;
    for (int j = 0; j < rowCount; ++j) {
        hash = hashDouble(vec->GetValue(j + start));
        combinedHashVal[j] = HashUtil::CombineHash(combinedHashVal[j], hash);
    }
}

void ALWAYS_INLINE Decimal128VectorHash(const uint32_t start, const uint32_t rowCount,
                                        Decimal128Vector *vec, int64_t *combinedHashVal)
{
    uint64_t hash;
    for (int j = 0; j < rowCount; ++j) {
        Decimal128 val = vec->GetValue(j + start);
        hash = HashUtil::HashValue(val.LowBits(), val.HighBits());
        combinedHashVal[j] = HashUtil::CombineHash(combinedHashVal[j], hash);
    }
}

void ALWAYS_INLINE VarcharVectorHash(const uint32_t start, const uint32_t rowCount,
                                     VarcharVector *vec, int64_t *combinedHashVal)
{
    uint64_t hash;
    std::hash<std::string> hashVarChar;
    uint8_t *data = nullptr;
    for (int j = 0; j < rowCount; ++j) {
        int valLen = vec->GetValue(j, &data);
        std::string val(reinterpret_cast<char *>(data), 0, valLen);
        hash = hashVarChar(val);
        combinedHashVal[j] = HashUtil::CombineHash(combinedHashVal[j + start], hash);
    }
}

void ALWAYS_INLINE GenerateCombinedHashes(Vector **vectors, const uint32_t start, const uint32_t rowCount,
                                          const int32_t *types, const int32_t colNum,
                                          const int32_t *colIdx, int64_t *combinedHashVal)
{
    for (int32_t i = 0; i < colNum; ++i) {
        uint32_t idx = colIdx[i];
        switch (types[idx]) {
            case OMNI_VEC_TYPE_INT:
            case OMNI_VEC_TYPE_DATE32: {
                IntVectorHash(start, rowCount, static_cast<IntVector *>(vectors[idx]), combinedHashVal);
                break;
            }
            case OMNI_VEC_TYPE_LONG:
            case OMNI_VEC_TYPE_DECIMAL64: {
                LongVectorHash(start, rowCount, static_cast<LongVector *>(vectors[idx]), combinedHashVal);
                break;
            }
            case OMNI_VEC_TYPE_DOUBLE: {
                DoubleVectorHash(start, rowCount, static_cast<DoubleVector *>(vectors[idx]), combinedHashVal);
                break;
            }
            case OMNI_VEC_TYPE_DECIMAL128: {
                Decimal128VectorHash(start, rowCount, static_cast<Decimal128Vector *>(vectors[idx]),
                                     combinedHashVal);
                break;
            }
            case OMNI_VEC_TYPE_VARCHAR: {
                VarcharVectorHash(start, rowCount, static_cast<VarcharVector *>(vectors[idx]), combinedHashVal);
                break;
            }
            default: {
                DebugError("No such data type %d", types[idx]);
                break;
            }
        }
    }
}

SPECIALIZE(OMNIJIT_HASH_GROUPBY_INLOOP)
void HashAggregationOperator::InLoop(Vector **vectors, uint32_t rowCount, const int32_t *types, int32_t colNum,
                                     const int32_t *groupByColIdx, int32_t groupByColNum,
                                     const int32_t *aggColIdx, int32_t aggColNum,
                                     const int32_t *aggFuncTypes)
{
    static const int blockSize = 1024;
    int64_t combinedHashVal[blockSize];
    uint32_t end;
    uint32_t run = blockSize;
    for (uint32_t start = 0; start < rowCount; start = start + blockSize) {
        for (int i = 0; i < blockSize; ++i) {
            combinedHashVal[i] = 0;
        }
        if ((start + blockSize) > rowCount) {
            run = rowCount - start;
        }

        GenerateCombinedHashes(vectors, start, run, types, groupByColNum, groupByColIdx, combinedHashVal);
        for (uint32_t offset = 0; offset < run; ++offset) {
            int64_t hash = combinedHashVal[offset];
            if (groupedRows.find(hash) == groupedRows.end()) {
                std::vector<GroupBySlot> groupByTuple(groupByColNum + aggColNum, GroupBySlot());
                for (int32_t i = 0; i < groupByColNum; ++i) {
                    uint32_t idx = groupByColIdx[i];
                    groupByTuple[i].val = DuplicateGroupByTuple(types[idx], vectors[idx], start + offset);
                }
                groupedRows[hash] = groupByTuple;
                for (int32_t i = 0; i < aggColNum; ++i) {
                    int32_t idx = aggColIdx[i];
                    int32_t type = types[idx];
                    Vector *colPtr = vectors[idx];
                    GroupBySlot &state = groupedRows[hash][groupByColNum + i];
                    aggregators[i]->Insert(state, colPtr, type, start + offset);
                }
            } else {
                for (int32_t i = 0; i < aggColNum; ++i) {
                    int32_t idx = aggColIdx[i];
                    int32_t type = types[idx];
                    Vector *colPtr = vectors[idx];
                    GroupBySlot &state = groupedRows[hash][groupByColNum + i];
                    aggregators[i]->ProcessGroup(state, colPtr, type, start + offset);
                }
            }
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
    uint32_t rowCount = vecBatch->GetRowCount();
    Vector **vectors = vecBatch->GetVectors();

    this->InLoop(vectors, rowCount, vectorTypes.get(), vectorCount, groupByColIdx.get(), groupColNum, aggColIdx.get(),
        aggColNum, aggFuncTypes.get());

    this->PostLoop(vecBatch);
    return 0;
}

int32_t HashAggregationOperator::GetRowSize(std::vector<omniruntime::vec::VecType> &types, int32_t columnCount)
{
    int32_t rowSize = 0;
    int32_t typeIndex = 0;
    for (auto &i : groupByCols) {
        types.push_back(i.type);
        rowSize += OperatorUtil::GetTypeSize(i.type);
    }
    for (int32_t i = 0; i < aggCols.size(); ++i) {
        if (aggregators[i]->GetType() == OMNI_AGGREGATION_TYPE_COUNT) {
            types.push_back(LongVecType::Instance());
            rowSize += sizeof(int64_t);
            continue;
        }
        if (aggregators[i]->GetType() == OMNI_AGGREGATION_TYPE_AVG) {
            if (aggregators[i]->IsOutputPartial()) {
                types.push_back(ContainerVecType::Instance());
                rowSize += sizeof(int64_t);
            } else {
                types.push_back(DoubleVecType::Instance());
            }
            rowSize += sizeof(double);
            continue;
        }
        types.push_back(aggCols[i].type);
        rowSize += OperatorUtil::GetTypeSize(aggCols[i].type);
    }
    return rowSize;
}

SPECIALIZE(OMNIJIT_HASH_GROUPBY_HASH_COLUMN)


    void HashAggregationOperator::FillGroupByDate32(int32_t rowCount, RowIterator &tempRowIterator, int colIndex,
                                                    IntVector *vector)
    {
        for (int rowIndex = 0; rowIndex < rowCount && tempRowIterator != groupedRows.end();
         ++rowIndex, ++tempRowIterator) {
            vector->SetValue(rowIndex, *(int32_t *)tempRowIterator->second[colIndex].val);
        }
    }

    void HashAggregationOperator::FillVectorVal(VectorBatch *vecBatch, int32_t rowCount, RowIterator &tempRowIterator,
                                                int colIndex, int groupByIndex)
{
    switch (groupByCols[groupByIndex].type.GetId()) {
        case OMNI_VEC_TYPE_INT:
        case OMNI_VEC_TYPE_DATE32: {
            IntVector *vector = static_cast<IntVector *>(vecBatch->GetVector(colIndex));
            FillGroupByDate32(rowCount, tempRowIterator, colIndex, vector);
            break;
        }
        case OMNI_VEC_TYPE_LONG:
        case OMNI_VEC_TYPE_DECIMAL64: {
            LongVector *vector = static_cast<LongVector *>(vecBatch->GetVector(colIndex));
            FillGroupByDecimal64(rowCount, tempRowIterator, colIndex, vector);
            break;
        }
        case OMNI_VEC_TYPE_DOUBLE: {
            DoubleVector *vector = static_cast<DoubleVector *>(vecBatch->GetVector(colIndex));
            FillGroupByDouble(rowCount, tempRowIterator, colIndex, vector);
            break;
        }
        case OMNI_VEC_TYPE_VARCHAR: {
            VarcharVector *vector = static_cast<VarcharVector *>(vecBatch->GetVector(colIndex));
            FillGroupByVarChar(rowCount, tempRowIterator, colIndex, vector);
            break;
                    }
        case OMNI_VEC_TYPE_DECIMAL128: {
            Decimal128Vector *vector = static_cast<Decimal128Vector *>(vecBatch->GetVector(colIndex));
            FillGroupByDecimal128(rowCount, tempRowIterator, colIndex, vector);
            break;
        }
        default: {
            DebugError("Type %d doesn't support", groupByCols[groupByIndex].type.GetId());
            break;
        }
    }
}

    void HashAggregationOperator::FillGroupByDecimal128(int32_t rowCount, RowIterator &tempRowIterator, int colIndex,
                                                        Decimal128Vector *vector)
    {
        for (int rowIndex = 0; rowIndex < rowCount && tempRowIterator != groupedRows.end();
         ++rowIndex, ++tempRowIterator) {
            vector->SetValue(rowIndex, *(Decimal128 *) tempRowIterator->second[colIndex].val);
        }
    }

    void HashAggregationOperator::FillGroupByVarChar(int32_t rowCount, RowIterator &tempRowIterator, int colIndex,
                                                     VarcharVector *vector)
    {
        for (int rowIndex = 0; rowIndex < rowCount && tempRowIterator != groupedRows.end();
         ++rowIndex, ++tempRowIterator) {
            vector->SetValue(rowIndex, reinterpret_cast<const uint8_t *>(
                    (*((std::string *) tempRowIterator->second[colIndex].val)).c_str()),
                             (*((std::string *) tempRowIterator->second[colIndex].val)).size());
                    }
    }

    void HashAggregationOperator::FillGroupByDouble(int32_t rowCount, RowIterator &tempRowIterator, int colIndex,
                                                    DoubleVector *vector)
    {
        for (int rowIndex = 0; rowIndex < rowCount && tempRowIterator != groupedRows.end();
         ++rowIndex, ++tempRowIterator) {
            vector->SetValue(rowIndex, *(double *)tempRowIterator->second[colIndex].val);
        }
    }

    void HashAggregationOperator::FillGroupByDecimal64(int32_t rowCount, RowIterator &tempRowIterator, int colIndex,
                                                       LongVector *vector)
    {
        for (int rowIndex = 0; rowIndex < rowCount && tempRowIterator != groupedRows.end();
         ++rowIndex, ++tempRowIterator) {
            vector->SetValue(rowIndex, *(int64_t *)tempRowIterator->second[colIndex].val);
        }
    }

void HashAggregationOperator::FillGroupByVectors(VectorBatch *vecBatch, int startIndex, int endIndex,
    RowIterator &rowIterator, int32_t rowCount)
{
    RowIterator tempRowIterator = rowIterator;
    for (int colIndex = startIndex, groupByIndex = 0; colIndex < endIndex; ++colIndex, ++groupByIndex) {
        tempRowIterator = rowIterator;
        FillVectorVal(vecBatch, rowCount, tempRowIterator, colIndex, groupByIndex);
    }
}

void HashAggregationOperator::FillNormalAgg(VectorBatch* vecBatch,
    int32_t aggIndex, int32_t colIndex, int32_t rowCount, RowIterator &rowIterator)
{
    switch (aggCols[aggIndex].type.GetId()) {
        case OMNI_VEC_TYPE_INT:
        case OMNI_VEC_TYPE_DATE32: {
            IntVector *vector = static_cast<IntVector *>(vecBatch->GetVector(colIndex));
            FillAggDateValue(colIndex, rowCount, rowIterator, vector);
            break;
        }
        case OMNI_VEC_TYPE_LONG:
        case OMNI_VEC_TYPE_DECIMAL64: {
            LongVector *vector = static_cast<LongVector *>(vecBatch->GetVector(colIndex));
            FillAggDecimal64Value(colIndex, rowCount, rowIterator, vector);
            break;
        }
        case OMNI_VEC_TYPE_DOUBLE: {
            DoubleVector *vector = static_cast<DoubleVector *>(vecBatch->GetVector(colIndex));
            FillAggDoubleValue(colIndex, rowCount, rowIterator, vector);
            break;
        }
        case OMNI_VEC_TYPE_DECIMAL128: {
            Decimal128Vector *vector = static_cast<Decimal128Vector *>(vecBatch->GetVector(colIndex));
            FillAggDecimal128Value(colIndex, rowCount, rowIterator, vector);
            break;
        }
        case OMNI_VEC_TYPE_VARCHAR: {
            VarcharVector *vector = static_cast<VarcharVector *>(vecBatch->GetVector(colIndex));
            FillAggVarCharValue(colIndex, rowCount, rowIterator, vector);
            break;
        }
        default: {
            DebugError("No such data type %d", aggCols[aggIndex].type.GetId());
            break;
        }
    }
}

void HashAggregationOperator::FillAggVarCharValue(int32_t colIndex, int32_t rowCount, RowIterator &rowIterator,
                                                  VarcharVector *vector)
{
    for (int32_t rIdx = 0; rIdx < rowCount && rowIterator != groupedRows.end();
         ++rIdx, rowIterator++) {
            vector->SetValue(rIdx,
                             reinterpret_cast<const uint8_t *>(
                                     (*(std::string *) (rowIterator->second[colIndex].val)).c_str()),
                             (*(std::string *) (rowIterator->second[colIndex].val)).size());
        }
    }

void HashAggregationOperator::FillAggDecimal128Value(int32_t colIndex, int32_t rowCount, RowIterator &rowIterator,
                                                     Decimal128Vector *vector)
{
    for (int32_t rIdx = 0; rIdx < rowCount && rowIterator != groupedRows.end();
         ++rIdx, rowIterator++) {
            vector->SetValue(rIdx, *reinterpret_cast<Decimal128 *>(rowIterator->second[colIndex].val));
        }
    }

void HashAggregationOperator::FillAggDoubleValue(int32_t colIndex, int32_t rowCount, RowIterator &rowIterator,
                                                 DoubleVector *vector)
{
    for (int32_t rIdx = 0; rIdx < rowCount && rowIterator != groupedRows.end();
         ++rIdx, rowIterator++) {
            vector->SetValue(rIdx, *reinterpret_cast<double *>(rowIterator->second[colIndex].val));
        }
    }

void HashAggregationOperator::FillAggDecimal64Value(int32_t colIndex, int32_t rowCount, RowIterator &rowIterator,
                                                    LongVector *vector)
{
    for (int32_t rIdx = 0; rIdx < rowCount && rowIterator != groupedRows.end();
         ++rIdx, rowIterator++) {
            vector->SetValue(rIdx, *reinterpret_cast<int64_t *>(rowIterator->second[colIndex].val));
    }
}

void HashAggregationOperator::FillAggDateValue(int32_t colIndex, int32_t rowCount, RowIterator &rowIterator,
                                               IntVector *vector)
{
    for (int32_t rIdx = 0; rIdx < rowCount && rowIterator != groupedRows.end();
         ++rIdx, rowIterator++) {
            vector->SetValue(rIdx, *reinterpret_cast<int32_t *>(rowIterator->second[colIndex].val));
        }
    }

void HashAggregationOperator::FillAvgAgg(VectorBatch* vecBatch,
    int32_t aggIndex, int32_t colIndex, int32_t rowCount, RowIterator &rowIterator)
{
    // TODO support average value type which is decimal
    if (outputPartial) {
        ContainerVector *vector = static_cast<ContainerVector *>(vecBatch->GetVector(colIndex));
        for (int32_t rIdx = 0; rIdx < rowCount && rowIterator != groupedRows.end();
             ++rIdx, rowIterator++) {
            if (rowIterator->second[colIndex].avgCnt == 0) {
                DebugError("Divisor is zero! key = %ld", rowIterator->first);
            }
            DoubleVector *doubleVector = reinterpret_cast<DoubleVector *>(vector->getValue(0));
            doubleVector->SetValue(rIdx, *(reinterpret_cast<double *>(rowIterator->second[colIndex].avgVal)));
            LongVector *longVector = reinterpret_cast<LongVector *>(vector->getValue(1));
            longVector->SetValue(rIdx, rowIterator->second[colIndex].avgCnt);
        }
    } else {
        DoubleVector *vector = static_cast<DoubleVector *>(vecBatch->GetVector(colIndex));
        for (int32_t rIdx = 0; rIdx < rowCount && rowIterator != groupedRows.end();
             ++rIdx, rowIterator++) {
            if (rowIterator->second[colIndex].avgCnt == 0) {
                DebugError("Divisor is zero! key = %ld", rowIterator->first);
            }
            vector->SetValue(rIdx, *(reinterpret_cast<double *>(rowIterator->second[colIndex].avgVal)));
        }
    }
}

// TODO currently we need to traverse ColumnNum * RowNum times to build the output.
// The overhead need to be optimized.
SPECIALIZE(OMNIJIT_HASH_GROUPBY_AGG_COLUMN)
void HashAggregationOperator::FillAggVectors(VectorBatch *vecBatch, int startIndex, int endIndex,
    RowIterator &rowIterator, int32_t rowCount)
{
    auto resultIterator = rowIterator;
    for (int32_t aggIndex = 0, colIndex = startIndex; colIndex < endIndex; ++aggIndex, ++colIndex) {
        resultIterator = rowIterator;
        AggregateType aggType = this->aggregators[aggIndex]->GetType();
        switch (aggType) {
            case OMNI_AGGREGATION_TYPE_SUM:
            case OMNI_AGGREGATION_TYPE_MIN:
            case OMNI_AGGREGATION_TYPE_MAX: {
                FillNormalAgg(vecBatch, aggIndex, colIndex, rowCount, resultIterator);
                break;
            }
            case OMNI_AGGREGATION_TYPE_COUNT: {
                LongVector *vector = static_cast<LongVector *>(vecBatch->GetVector(colIndex));
                for (int32_t rIdx = 0; rIdx < rowCount && resultIterator != groupedRows.end();
                    ++rIdx, resultIterator++) {
                    vector->SetValue(rIdx, reinterpret_cast<int64_t>(resultIterator->second[colIndex].count));
                }
                break;
            }
            case OMNI_AGGREGATION_TYPE_AVG: { // TODO process intermediate vectors
                // generate double or row type vector according to the step. Row type if outputPartial == 1 otherwise
                // double vector.
                FillAvgAgg(vecBatch, aggIndex, colIndex, rowCount, resultIterator);
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

void SetVectors(VectorBatch *vectorBatch, const std::vector<omniruntime::vec::VecType> &types, int32_t rowCount)
{
    for (int colIndex = 0; colIndex < vectorBatch->GetVectorCount(); ++colIndex) {
        switch (types[colIndex].GetId()) {
            case OMNI_VEC_TYPE_INT:
            case OMNI_VEC_TYPE_DATE32: {
                vectorBatch->SetVector(colIndex, new IntVector(nullptr, rowCount));
                break;
            }
            case OMNI_VEC_TYPE_LONG:
            case OMNI_VEC_TYPE_DECIMAL64: {
                vectorBatch->SetVector(colIndex, new LongVector(nullptr, rowCount));
                break;
            }
            case OMNI_VEC_TYPE_DOUBLE: {
                vectorBatch->SetVector(colIndex, new DoubleVector(nullptr, rowCount));
                break;
            }
            case OMNI_VEC_TYPE_DECIMAL128: {
                vectorBatch->SetVector(colIndex, new Decimal128Vector(nullptr, rowCount));
                break;
            }
            case OMNI_VEC_TYPE_VARCHAR: {
                vectorBatch->SetVector(colIndex,
                                       new VarcharVector(static_cast<VectorAllocator *>(nullptr),
                                                         rowCount * (((omniruntime::vec::VarcharVecType &)
                                                         types[colIndex]).GetWidth()),
                                                         rowCount));
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
                DebugError("No such data type %d", types[colIndex].GetId());
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
    std::vector<omniruntime::vec::VecType> types;
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

OmniStatus HashAggregationOperator::CloseGroupBy()
{
    auto groupByColSize = groupByCols.size();
    for (auto &item : groupedRows) {
        for (int32_t idx = 0; idx < groupByColSize; ++idx) {
            switch (groupByCols[idx].type.GetId()) {
                case omniruntime::vec::OMNI_VEC_TYPE_INT:
                case omniruntime::vec::OMNI_VEC_TYPE_DATE32: {
                    delete reinterpret_cast<int32_t *>(item.second[idx].val);
                    break;
                }
                case omniruntime::vec::OMNI_VEC_TYPE_LONG:
                case omniruntime::vec::OMNI_VEC_TYPE_DECIMAL64: {
                    delete reinterpret_cast<int64_t *>(item.second[idx].val);
                    break;
                }
                case omniruntime::vec::OMNI_VEC_TYPE_DOUBLE: {
                    delete reinterpret_cast<double *>(item.second[idx].val);
                    break;
                }
                case omniruntime::vec::OMNI_VEC_TYPE_DECIMAL128: {
                    delete reinterpret_cast<Decimal128 *>(item.second[idx].val);
                            break;
                        }
                case omniruntime::vec::OMNI_VEC_TYPE_VARCHAR: {
                    delete reinterpret_cast<std::string *>(item.second[idx].val);
                    break;
                }
                default: {
                    return OMNI_STATUS_ERROR;
                    break;
                }
            }
        }
    }
    return OMNI_STATUS_NORMAL;
}

OmniStatus HashAggregationOperator::CloseAgg()
{
    auto groupByColSize = groupByCols.size();
    auto aggColSize = aggCols.size();
    for (auto &item : groupedRows) {
        for (int32_t idx = 0; idx < aggColSize; ++idx) {
            if (aggregators[idx]->GetType() == OMNI_AGGREGATION_TYPE_COUNT) {
                continue;
            }
            if (aggregators[idx]->GetType() == OMNI_AGGREGATION_TYPE_AVG) {
                delete reinterpret_cast<double *>(item.second[groupByColSize + idx].avgVal);
                continue;
            }
            switch (aggCols[idx].type.GetId()) {
                case omniruntime::vec::OMNI_VEC_TYPE_INT:
                case omniruntime::vec::OMNI_VEC_TYPE_DATE32: {
                    delete reinterpret_cast<int32_t *>(item.second[groupByColSize + idx].val);
                    break;
                }
                case omniruntime::vec::OMNI_VEC_TYPE_LONG:
                case omniruntime::vec::OMNI_VEC_TYPE_DECIMAL64: {
                    delete reinterpret_cast<int64_t *>(item.second[groupByColSize + idx].val);
                    break;
                }
                case omniruntime::vec::OMNI_VEC_TYPE_DOUBLE: {
                    delete reinterpret_cast<double *>(item.second[groupByColSize + idx].val);
                    break;
                }
                case omniruntime::vec::OMNI_VEC_TYPE_VARCHAR: {
                    delete reinterpret_cast<std::string *>(item.second[groupByColSize + idx].val);
                    break;
                }
                case omniruntime::vec::OMNI_VEC_TYPE_DECIMAL128: {
                    delete reinterpret_cast<Decimal128 *>(item.second[groupByColSize + idx].val);
                    break;
                }
                default: {
                    return OMNI_STATUS_ERROR;
                }
            }
        }
    }
    return OMNI_STATUS_NORMAL;
}

OmniStatus HashAggregationOperator::Close()
{
    delete[] sourceTypes;
    OmniStatus ret = CloseGroupBy();
    ret = CloseAgg();
    if (ret == OMNI_STATUS_NORMAL) {
        return OMNI_STATUS_NORMAL;
    } else {
        return OMNI_STATUS_ERROR;
    }
}
} // end of namespace op
} // end of namespace omniruntime
