/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Hash Aggregation Source File
 */
#include "group_aggregation.h"
#include <cmath>

#include "../../vector/vector_common.h"
#include "../../vector/vector_helper.h"
#include "../status.h"
#include "../../jit/annotation.h"
#include "../optimization.h"
#include "../../vector/container_vector.h"
#include "../../util/type_util.h"
#include "../hash_util.h"
#include "../util/operator_util.h"

#if defined(DEBUG_OPERATOR) && defined(TRACE)
#include <sstream>
#endif
namespace omniruntime {
namespace op {
using namespace omniruntime::vec;

    static constexpr FunctionByDataType GROUP_AGG_FUNCTIONS[VEC_TYPE_MAX_COUNT] = {
            {OMNI_VEC_TYPE_NONE, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr},
            {
             OMNI_VEC_TYPE_INT, HashFuncImpl<IntVector, int32_t>, HashFuncVectImpl<IntVector, int32_t>, IsSameNodeFuncImpl<IntVector, int32_t>,
                                                            DuplicateKeyValueImpl<IntVector, int32_t>, SetVectorImpl<IntVector>, FillValueImpl<IntVector, int32_t>,
                                                                                                       ReleaseMemoryImpl<int32_t>
            },
            {
             OMNI_VEC_TYPE_LONG, HashFuncImpl<LongVector, int64_t>, HashFuncVectImpl<LongVector, int64_t>, IsSameNodeFuncImpl<LongVector, int64_t>,
                    DuplicateKeyValueImpl<LongVector, int64_t>, SetVectorImpl<LongVector>, FillValueImpl<LongVector, int64_t>,
                                                                                                       ReleaseMemoryImpl<int64_t>
            },
            {
             OMNI_VEC_TYPE_DOUBLE, HashFuncImpl<DoubleVector, double>, HashFuncVectImpl<DoubleVector, double>, IsSameNodeFuncImpl<DoubleVector, double>, DuplicateKeyValueImpl<DoubleVector, double>,
                    SetVectorImpl<DoubleVector>, FillValueImpl<DoubleVector, double>, ReleaseMemoryImpl<double>
            },
            {OMNI_VEC_TYPE_BOOLEAN, nullptr, nullptr, nullptr, nullptr, nullptr},
            {OMNI_VEC_TYPE_SHORT, nullptr, nullptr, nullptr, nullptr, nullptr},
            {
             OMNI_VEC_TYPE_DECIMAL64, HashFuncImpl<LongVector, int64_t>, HashFuncVectImpl<LongVector, int64_t>, IsSameNodeFuncImpl<LongVector, int64_t>, DuplicateKeyValueImpl<LongVector, int64_t>,
                    SetVectorImpl<LongVector>, FillValueImpl<LongVector, int64_t>, ReleaseMemoryImpl<int64_t>
            },
            {
             OMNI_VEC_TYPE_DECIMAL128, HashDecimalFunc, HashDecimalVectFunc, IsSameNodeFuncImpl<Decimal128Vector, Decimal128>, DuplicateKeyValueImpl<Decimal128Vector, Decimal128>,
                    SetVectorImpl<Decimal128Vector>, FillValueImpl<Decimal128Vector, Decimal128>, ReleaseMemoryImpl<Decimal128>
            },
            {
             OMNI_VEC_TYPE_DATE32, HashFuncImpl<IntVector, int32_t>, HashFuncVectImpl<IntVector, int32_t>, IsSameNodeFuncImpl<IntVector, int32_t>, DuplicateKeyValueImpl<IntVector, int32_t>,
                    SetVectorImpl<IntVector>, FillValueImpl<IntVector, int32_t>, ReleaseMemoryImpl<int32_t>
            },
            {OMNI_VEC_TYPE_DATE64, nullptr, nullptr, nullptr, nullptr, nullptr},
            {OMNI_VEC_TYPE_TIME32, nullptr, nullptr, nullptr, nullptr, nullptr},
            {OMNI_VEC_TYPE_TIME64, nullptr, nullptr, nullptr, nullptr, nullptr},
            {OMNI_VEC_TYPE_TIMESTAMP, nullptr, nullptr, nullptr, nullptr, nullptr},
            {OMNI_VEC_TYPE_INTERVAL_MONTHS, nullptr, nullptr, nullptr, nullptr, nullptr},
            {OMNI_VEC_TYPE_INTERVAL_DAY_TIME, nullptr, nullptr, nullptr, nullptr, nullptr},
            {
             OMNI_VEC_TYPE_VARCHAR, HashVarcharFuncImpl, HashVarcharVectFuncImpl, IsSameNodeFuncVarcharImpl, DuplicateVarcharKeyValue, SetVarcharVector, FillVarcharValue,
                                                                                                       ReleaseMemoryVarcharImpl
            },
            {OMNI_VEC_TYPE_DICTIONARY, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr},
            {OMNI_VEC_TYPE_CONTAINER, nullptr, nullptr, nullptr, nullptr, SetContainerVector, nullptr, nullptr},
    };


OmniStatus HashAggregationOperatorFactory::Init()
{
    OmniStatus ret = OMNI_STATUS_NORMAL;
    for (int32_t i = 0; i < groupByColContext.len; ++i) {
        groupByColIdx.push_back(groupByColContext.context[i]);
    }
    for (int32_t i = 0; i < aggFuncTypeContext.len; ++i) {
        aggColIdx.push_back(aggColContext.context[i]);
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
                LogError("No such agg func type %d", aggFuncTypeContext.context[i]);
                ret = OMNI_STATUS_ERROR;
            }
        }
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
        auto type = this->groupByTypes.Get()[i];
        ColumnIndex c = { this->groupByColIdx[i], type, type };
        groupByIndex[i] = c;
    }
    for (int32_t i = 0; i < this->aggColIdx.size(); ++i) {
        auto inputType = this->aggInputTypes.Get()[i];
        auto outputType = this->aggOutputTypes.Get()[i];
        ColumnIndex c = { this->aggColIdx[i], inputType, outputType };
        aggIndex[i] = c;
        auto aggregator =
            aggregatorFactories[i]->CreateAggregator(inputType.GetId(), outputType.GetId(), inputRaw, outputPartial);
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
    for (auto &c : groupByCols) {
        sourceTypes[c.idx] = static_cast<int32_t>(c.input.GetId());
    }
    for (auto &c : aggCols) {
        sourceTypes[c.idx] = static_cast<int32_t>(c.input.GetId());
    }
    executionContext = std::make_unique<ExecutionContext>();
    executionContext->getArena()->Allocate(DEFAULT_TEMP_MEM_SIZE);
    return OMNI_STATUS_NORMAL;
}

void HashAggregationOperator::PreLoop(VectorBatch *vecBatch) {}

void HashAggregationOperator::PostLoop(VectorBatch *vecBatch) const {}

static void ALWAYS_INLINE GenerateCombinedHashes(Vector **vectors, uint32_t start, uint32_t rowCount, const int32_t colNum,
    uint64_t *combinedHashVal)
{
    Vector *vector = nullptr;
    for (int32_t i = 0; i < colNum; ++i) {
        vector = vectors[i];
        if (vector->GetTypeId() != OMNI_VEC_TYPE_DICTIONARY) {
            GROUP_AGG_FUNCTIONS[vector->GetTypeId()].hashFuncVect(vector, start, rowCount,
                combinedHashVal);
        } else {
            int32_t newIndexes[rowCount];
            Vector *originalVector =
                static_cast<DictionaryVector *>(vector)->ExtractDictionaryAndIds(start, rowCount, newIndexes);
            GROUP_AGG_FUNCTIONS[originalVector->GetTypeId()].hashFunc(originalVector, rowCount,
                                                                      newIndexes, combinedHashVal);
        }
    }
}

std::vector<BucketIterator> HashAggregationOperator::FindBuckets(uint64_t *hash, int32_t blockSize)
{
    std::vector<BucketIterator> bucktes(blockSize, groupedRows.end());
    for (int32_t i = 0; i < blockSize; ++i) {
        auto bucket = groupedRows.find(hash[i]);
        if (bucket != groupedRows.end()) {
            bucktes[i] = bucket;
        }
    }
    return bucktes;
}

static void ALWAYS_INLINE DuplicateGroupByTuple(GroupBySlot &groupBySlot, Vector *vector, uint32_t offset,
    ExecutionContext *context)
{
    int32_t originalRowIndex;
    Vector *originalVector = VectorHelper::ExpandVectorAndIndex(vector, offset, originalRowIndex);
    GROUP_AGG_FUNCTIONS[originalVector->GetTypeId()].duplicateKey(groupBySlot, originalVector,
                                                                  originalRowIndex, context);
}

static int32_t ALWAYS_INLINE IsSameGroupByTuples(Vector** vectors, const uint32_t offset, const int32_t colNum,
    std::vector<std::vector<GroupBySlot>> &sameBucket)
{
    // early break
    if (sameBucket.empty()) {
        return -1;
    }
    for (auto it = 0; it < sameBucket.size(); it++) {
        bool isSame = true;
        for (int32_t i = 0; i < colNum && isSame; ++i) {
            int32_t originalRowIndex;
            Vector *originalVector = VectorHelper::ExpandVectorAndIndex(vectors[i], offset, originalRowIndex);
            GROUP_AGG_FUNCTIONS[originalVector->GetTypeId()].isSameNode(originalVector, originalRowIndex,
                                                                        sameBucket[it][i], isSame);
        }
        if (isSame)
            return it;
    }
    return -1;
}

SPECIALIZE(OMNIJIT_HASH_GROUPBY_INLOOP)
void HashAggregationOperator::InLoop(Vector **vectors, uint32_t rowCount, const int32_t *types, int32_t colNum,
    const int32_t *groupByColIdx, int32_t groupByColNum, const int32_t *aggColIdx, int32_t aggColNum,
    const int32_t *aggFuncTypes)
{
    static const int blockSize = 1024;
    uint64_t combinedHashVal[blockSize];
    Vector *groupByVectors[groupByColNum];
    Vector *aggrByVectors[aggColNum];
    int32_t aggrByTypes[aggColNum];
    for (int i = 0; i < groupByColNum; ++i) {
        groupByVectors[i] = vectors[groupByColIdx[i]];
    }
    for (int i = 0; i < aggColNum; ++i) {
        int32_t idx = aggColIdx[i];
        aggrByVectors[i] = vectors[idx];
        aggrByTypes[i] = types[idx];
    }
    uint32_t run = blockSize;
    for (uint32_t start = 0; start < rowCount; start = start + blockSize) {
        for (int i = 0; i < blockSize; ++i) {
            combinedHashVal[i] = 0;
        }
        if ((start + blockSize) > rowCount) {
            run = rowCount - start;
        }
        GenerateCombinedHashes(groupByVectors, start, run, groupByColNum, combinedHashVal);
        for (uint32_t offset = 0; offset < run; ++offset) {
            uint64_t hash = combinedHashVal[offset];
            int32_t isSamePos = -1;
            int32_t actualOffset = start + offset;
            auto &bucket = groupedRows[hash];
            isSamePos = IsSameGroupByTuples(groupByVectors, actualOffset, groupByColNum, bucket);
            if (isSamePos == -1) {
                std::vector<GroupBySlot> groupByTuple(groupByColNum + aggColNum, GroupBySlot());
                for (int32_t i = 0; i < groupByColNum; ++i) {
                    DuplicateGroupByTuple(groupByTuple[i], groupByVectors[i], actualOffset, executionContext.get());
                }
                bucket.push_back(groupByTuple);
                size_t chainLength = bucket.size();
                for (int32_t i = 0; i < aggColNum; ++i) {
                    aggregators[i]->Insert(bucket[chainLength - 1][groupByColNum + i], aggrByVectors[i], aggrByTypes[i],
                        actualOffset);
                }
            } else {
                for (int32_t i = 0; i < aggColNum; ++i) {
                    aggregators[i]->ProcessGroup(bucket[isSamePos][groupByColNum + i], aggrByVectors[i], aggrByTypes[i],
                        actualOffset);
                }
            }
        }
    }
}

int32_t HashAggregationOperator::AddInput(VectorBatch *vecBatch)
{
    LogTrace("Enter Func");
    this->PreLoop(vecBatch);
    int32_t vectorCount = vecBatch->GetVectorCount();
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
    // verify whether input types match operator's types
    VERIFY_INPUT_TYPES(vecBatch, groupByColIdx.get(), groupColNum, aggColIdx.get(), aggColNum, this->sourceTypes);

    uint32_t rowCount = vecBatch->GetRowCount();
    Vector **vectors = vecBatch->GetVectors();

    this->InLoop(vectors, rowCount, vecBatch->GetVectorTypeIds(), vectorCount, groupByColIdx.get(), groupColNum,
        aggColIdx.get(), aggColNum, aggFuncTypes.get());

    this->PostLoop(vecBatch);
    return 0;
}

/**
 * @param types
 * @param columnCount
 * @return rowSize
 * All the output data types are determined in this function. Following allocation for output vectors and filling
 * value should use the 'types' parameter instead of using input vector types.
 */
int32_t HashAggregationOperator::GetRowSize(std::vector<VecType> &types, int32_t columnCount)
{
    int32_t rowSize = 0;
    int32_t typeIndex = 0;
    for (auto &i : groupByCols) {
        types.push_back(i.input);
        rowSize += OperatorUtil::GetTypeSize(i.input);
    }
    for (int32_t i = 0; i < aggCols.size(); ++i) {
        // currently aggregation type is fixed . should get right type from output types from engine side.
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
        types.push_back(aggCols[i].output);
        rowSize += OperatorUtil::GetTypeSize(aggCols[i].output);
    }
    return rowSize;
}

SPECIALIZE(OMNIJIT_HASH_GROUPBY_HASH_COLUMN)
void HashAggregationOperator::FillGroupByVectors(VectorBatch *vecBatch, int startIndex, int endIndex,
    ChainIterator &rowIterator, int32_t rowIndex)
{
    for (int colIndex = startIndex, groupByIndex = 0; colIndex < endIndex; ++colIndex, ++groupByIndex) {
        auto typeId = vecBatch->GetVector(colIndex)->GetTypeId();
        GROUP_AGG_FUNCTIONS[typeId].fillValue(vecBatch, rowIndex, rowIterator, colIndex);
    }
}

void HashAggregationOperator::FillAvgAgg(VectorBatch *vecBatch, int32_t aggIndex, int32_t colIndex,
    ChainIterator &rowIterator, int32_t rowIndex)
{
    // TODO support average value type which is decimal
    if (outputPartial) {
        ContainerVector *vector = static_cast<ContainerVector *>(vecBatch->GetVector(colIndex));
        if ((*rowIterator)[colIndex].avgCnt == 0) {
            LogError("Divisor is zero!");
        }
        DoubleVector *doubleVector = reinterpret_cast<DoubleVector *>(vector->getValue(0));
        doubleVector->SetValue(rowIndex, *(reinterpret_cast<double *>((*rowIterator)[colIndex].avgVal)));
        LongVector *longVector = reinterpret_cast<LongVector *>(vector->getValue(1));
        longVector->SetValue(rowIndex, (*rowIterator)[colIndex].avgCnt);
    } else {
        DoubleVector *vector = static_cast<DoubleVector *>(vecBatch->GetVector(colIndex));
        if ((*rowIterator)[colIndex].avgCnt == 0) {
            LogError("Divisor is zero!");
        }
        vector->SetValue(rowIndex, *(reinterpret_cast<double *>((*rowIterator)[colIndex].avgVal)));
    }
}

// TODO currently we need to traverse ColumnNum * RowNum times to build the output.
// The overhead need to be optimized.
SPECIALIZE(OMNIJIT_HASH_GROUPBY_AGG_COLUMN)
void HashAggregationOperator::FillAggVectors(VectorBatch *vecBatch, int startIndex, int endIndex,
    ChainIterator &rowIterator, int32_t rowIndex)
{
    for (int32_t aggIndex = 0, colIndex = startIndex; colIndex < endIndex; ++aggIndex, ++colIndex) {
        AggregateType aggType = this->aggregators[aggIndex]->GetType();
        switch (aggType) {
            case OMNI_AGGREGATION_TYPE_SUM:
            case OMNI_AGGREGATION_TYPE_MIN:
            case OMNI_AGGREGATION_TYPE_MAX: {
                auto typeId = vecBatch->GetVector(colIndex)->GetTypeId();
                GROUP_AGG_FUNCTIONS[typeId].fillValue(vecBatch, rowIndex, rowIterator, colIndex);
                break;
            }
            case OMNI_AGGREGATION_TYPE_COUNT: {
                LongVector *vector = static_cast<LongVector *>(vecBatch->GetVector(colIndex));
                vector->SetValue(rowIndex, (*rowIterator)[colIndex].count);
                break;
            }
            case OMNI_AGGREGATION_TYPE_AVG: { // TODO process intermediate vectors
                // generate double or row type vector according to the step. Row type if outputPartial == 1 otherwise
                // double vector.
                FillAvgAgg(vecBatch, aggIndex, colIndex, rowIterator, rowIndex);
                break;
            }
            default: {
                LogError("No such aggregate type %d\n", aggType);
                break;
            }
        }
    }
}

void SetVectors(VectorAllocator *vecAllocator, VectorBatch *vectorBatch, const std::vector<VecType> &types,
    int32_t rowCount)
{
    for (int colIndex = 0; colIndex < vectorBatch->GetVectorCount(); ++colIndex) {
        VecType type = types[colIndex];
        GROUP_AGG_FUNCTIONS[type.GetId()].setVector(vectorBatch, type, colIndex, vecAllocator, rowCount);
    }
}

int32_t HashAggregationOperator::GetOutput(std::vector<VectorBatch *> &result)
{
    uint32_t groupByColSize = groupByCols.size();
    uint32_t aggColSize = aggCols.size();
    uint32_t colCount = groupByColSize + aggColSize;
    std::vector<VecType> types;
    int32_t rowByteSize = GetRowSize(types, colCount);

    // accumulate whole row count first
    int32_t totalRowCount = 0;
    for (auto it = groupedRows.begin(); it != groupedRows.end(); ++it) {
        totalRowCount += it->second.size();
    }

    if (totalRowCount == 0) {
        return 0;
    }

    int32_t rowsPerBatch = std::ceil(MAX_TABLE_SIZE_IN_BYTES / static_cast<double>(rowByteSize));
    int32_t expectedBatchSize = std::ceil(totalRowCount / static_cast<double>(rowsPerBatch));
    int32_t leftRowCount = totalRowCount;

    // create all output vector batches
    for (int32_t batchId = 0; batchId < expectedBatchSize; ++batchId) {
        auto vecBatch = new VectorBatch(colCount);
        SetVectors(this->vecAllocator, vecBatch, types, std::min(rowsPerBatch, leftRowCount));
        result.push_back(vecBatch);
        leftRowCount -= rowsPerBatch;
    }

    // collect all groups
    std::vector<ChainIterator> allGroups(totalRowCount);
    int32_t groupCount = 0;
    for (auto bucket = groupedRows.begin(); bucket != groupedRows.end(); ++bucket) {
        for (auto group = bucket->second.begin(); group != bucket->second.end(); ++group) {
            allGroups[groupCount++] = group;
        }
    }

    // fill groups to vecbatch
    int32_t filledRowSize = 0;
    VectorBatch *batchToFill = result[0];
    int32_t batchIndex = 0;
    for (auto &group : allGroups) {
        if (filledRowSize >= rowsPerBatch) {
            batchIndex++;
            batchToFill = result[batchIndex];
            filledRowSize = 0;
        }
        FillGroupByVectors(batchToFill, 0, groupByColSize, group, filledRowSize);
        FillAggVectors(batchToFill, groupByColSize, colCount, group, filledRowSize);
        filledRowSize++;
    }
    // set finished.
    SetStatus(OMNI_STATUS_FINISHED);
    return expectedBatchSize;
}

OmniStatus HashAggregationOperator::Close()
{
    delete[] sourceTypes;
    return OMNI_STATUS_NORMAL;
}

template <typename V, typename D>
void ALWAYS_INLINE HashFuncVectImpl(Vector *vector, const uint32_t start, const uint32_t rowCount,
    uint64_t *combinedHash)
{
    uint64_t hash;
    std::hash<D> hasher;
    for (int32_t i = 0; i < rowCount; ++i) {
        int idx = i + start;
        hash = !vector->IsValueNull(idx) * hasher(static_cast<V *>(vector)->GetValue(idx));
        combinedHash[i] = HashUtil::CombineHash(combinedHash[i], hash);
    }
}

void ALWAYS_INLINE HashVarcharVectFuncImpl(Vector *vector, const uint32_t start, const uint32_t rowCount,
    uint64_t *combinedHash)
{
    uint8_t *data = nullptr;
    for (int32_t i = 0; i < rowCount; ++i) {
        int idx = i + start;
        int valLen = static_cast<VarcharVector *>(vector)->GetValue(idx, &data);
        auto val = HashUtil::HashValue(reinterpret_cast<int8_t *>(data), valLen);
        combinedHash[i] = HashUtil::CombineHash(combinedHash[i], !vector->IsValueNull(idx) * val);
    }
}

void ALWAYS_INLINE HashDecimalVectFunc(Vector *vector, const uint32_t start, const uint32_t rowCount,
    uint64_t *combinedHash)
{
    for (int32_t i = 0; i < rowCount; ++i) {
        int idx = i + start;
        Decimal128 val = static_cast<Decimal128Vector *>(vector)->GetValue(idx);
        combinedHash[i] = HashUtil::CombineHash(combinedHash[i],
            !vector->IsValueNull(idx) * HashUtil::HashValue(val.LowBits(), val.HighBits()));
    }
}

template <typename V, typename D>
void ALWAYS_INLINE HashFuncImpl(Vector *vector, const uint32_t rowCount, const int32_t *rowIndexes,
    uint64_t *combinedHash)
{
    uint64_t hash;
    std::hash<D> hasher;
    for (int32_t i = 0; i < rowCount; ++i) {
        int32_t idx = rowIndexes[i];
        hash = !vector->IsValueNull(idx) * hasher(static_cast<V *>(vector)->GetValue(idx));
        combinedHash[i] = HashUtil::CombineHash(combinedHash[i], hash);
    }
}

void ALWAYS_INLINE HashVarcharFuncImpl(Vector *vector, const uint32_t rowCount, const int32_t *rowIndexes,
    uint64_t *combinedHash)
{
    uint8_t *data = nullptr;
    for (int32_t i = 0; i < rowCount; ++i) {
        int32_t idx = rowIndexes[i];
        int valLen = static_cast<VarcharVector *>(vector)->GetValue(idx, &data);
        auto val = HashUtil::HashValue(reinterpret_cast<int8_t *>(data), valLen);
        combinedHash[i] = HashUtil::CombineHash(combinedHash[i], !vector->IsValueNull(idx) * val);
    }
}

void ALWAYS_INLINE HashDecimalFunc(Vector *vector, const uint32_t rowCount, const int32_t *rowIndexes,
    uint64_t *combinedHash)
{
    for (int32_t i = 0; i < rowCount; ++i) {
        int32_t idx = rowIndexes[i];
        Decimal128 val = static_cast<Decimal128Vector *>(vector)->GetValue(idx);
        combinedHash[i] = HashUtil::CombineHash(combinedHash[i],
            !vector->IsValueNull(idx) * HashUtil::HashValue(val.LowBits(), val.HighBits()));
    }
}

template <typename V, typename D>
void IsSameNodeFuncImpl(Vector *vector, const uint32_t offset, GroupBySlot &slot, bool &isSame)
{
    bool isIntermediateNull = static_cast<D *>(slot.val) == nullptr;
    bool isInputNull = vector->IsValueNull(offset);

    if (!isInputNull && !isIntermediateNull) {
        auto intTmp = static_cast<V *>(vector)->GetValue(offset);
        isSame = intTmp == *static_cast<D *>(slot.val);
        return;
    }
    if (isInputNull != isIntermediateNull) {
        isSame = false;
        return;
    }
    isSame = true;
    return;
}

void IsSameNodeFuncVarcharImpl(Vector *vector, const uint32_t offset, GroupBySlot &slot, bool &isSame)
{
    bool isIntermediateNull = slot.strVal == nullptr;
    bool isInputNull = vector->IsValueNull(offset);

    if (!isInputNull && !isIntermediateNull) {
        uint8_t *data = nullptr;
        int32_t valLen = static_cast<VarcharVector *>(vector)->GetValue(offset, &data);
        isSame = valLen == slot.strLen && memcmp(data, slot.strVal, std::min(valLen, slot.strLen)) == 0;
        return;
    }
    if (isInputNull != isIntermediateNull) {
        isSame = false;
        return;
    }
    isSame = true;
    return;
}

template <typename V, typename D>
void ALWAYS_INLINE DuplicateKeyValueImpl(GroupBySlot &groupBySlot, Vector *vector, const uint32_t offset,
    ExecutionContext *context)
{
    if (vector->IsValueNull(offset)) {
        return;
    }
    int32_t len = sizeof(D);
    uint8_t *ptr = context->getArena()->Allocate(len);
    D data = static_cast<V *>(vector)->GetValue(offset);
    memcpy_s(ptr, len, &data, len);
    groupBySlot.val = ptr;
}

void ALWAYS_INLINE DuplicateVarcharKeyValue(GroupBySlot &groupBySlot, Vector *vector, const uint32_t offset,
    ExecutionContext *context)
{
    if (vector->IsValueNull(offset)) {
        return;
    }
    uint8_t *tmp = nullptr;
    int valLen = (static_cast<VarcharVector *>(vector)->GetValue(offset, &tmp));
    uint8_t *data = context->getArena()->Allocate(valLen);
    memcpy_s(data, valLen, tmp, valLen);
    groupBySlot.strVal = data;
    groupBySlot.strLen = valLen;
}

template <typename V>
void SetVectorImpl(VectorBatch *vecBatch, VecType &type, int32_t columnIndex, VectorAllocator *vecAllocator,
    int32_t rowCount)
{
    vecBatch->SetVector(columnIndex, new V(vecAllocator, rowCount));
}

void SetVarcharVector(VectorBatch *vecBatch, VecType &type, int32_t columnIndex, VectorAllocator *vecAllocator,
    int32_t rowCount)
{
    vecBatch->SetVector(columnIndex,
        new VarcharVector(vecAllocator, rowCount * ((VarcharVecType &)type).GetWidth(), rowCount));
}

void SetContainerVector(VectorBatch *vecBatch, VecType &type, int32_t columnIndex, VectorAllocator *vecAllocator,
    int32_t rowCount)
{
    DoubleVector *doubleVector = new DoubleVector(vecAllocator, rowCount);
    LongVector *longVector = new LongVector(vecAllocator, rowCount);
    Vector **vectorAddresses = new Vector *[2];
    vectorAddresses[0] = doubleVector;
    vectorAddresses[1] = longVector;
    VecType *vecTypes = new VecType[2];
    vecTypes[0] = DoubleVecType::Instance();
    vecTypes[1] = LongVecType::Instance();
    ContainerVector *containerVector =
        new ContainerVector(vecAllocator, rowCount, vectorAddresses, op::AVG_VECTOR_COUNT, vecTypes);
    vecBatch->SetVector(columnIndex, containerVector);
}

template <typename V, typename D>
void FillValueImpl(VectorBatch *vecBatch, int32_t rowIndex, ChainIterator &tempRowIterator, int colIndex)
{
    auto vector = static_cast<V *>(vecBatch->GetVector(colIndex));
    if ((*tempRowIterator)[colIndex].val == nullptr) {
        vector->SetValueNull(rowIndex);
        return;
    }
    vector->SetValue(rowIndex, *static_cast<D *>((*tempRowIterator)[colIndex].val));
}

void FillVarcharValue(VectorBatch *vecBatch, int32_t rowIndex, ChainIterator &tempRowIterator, int colIndex)
{
    auto vector = static_cast<VarcharVector *>(vecBatch->GetVector(colIndex));
    if ((*tempRowIterator)[colIndex].val == nullptr) {
        vector->SetValueNull(rowIndex);
        return;
    }
    vector->SetValue(rowIndex, reinterpret_cast<uint8_t *>((*tempRowIterator)[colIndex].strVal),
        (*tempRowIterator)[colIndex].strLen);
}

template <typename T> void ReleaseMemoryImpl(GroupBySlot &columnVal, int32_t columnIndex, VecType &type)
{
    auto p = columnVal.val;
    if (p != nullptr) {
        delete static_cast<T *>(p);
        p = nullptr;
    }
}

void ReleaseMemoryVarcharImpl(GroupBySlot &columnVal, int32_t columnIndex, VecType &type)
{
    auto p = columnVal.strVal;
    if (p != nullptr) {
        delete[] p;
        p = nullptr;
    }
}
} // end of namespace op
} // end of namespace omniruntime
