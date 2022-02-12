/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Hash Aggregation Source File
 */
#include "group_aggregation.h"
#include <cmath>

#include "vector/vector_common.h"
#include "vector/vector_helper.h"
#include "vector/container_vector.h"
#include "operator/status.h"
#include "jit/annotation.h"
#include "operator/optimization.h"
#include "util/type_util.h"
#include "operator/hash_util.h"
#include "operator/util/operator_util.h"
#include "operator/aggregation/aggregator/aggregator_factory.h"

#if defined(DEBUG_OPERATOR) && defined(TRACE)
#include <sstream>
#endif
namespace omniruntime {
namespace op {
using namespace omniruntime::vec;

template void HashFuncImpl<BooleanVector, bool>(Vector *vector, const uint32_t rowCount, const int32_t *rowIndexes,
    uint64_t *combinedHash);
template void HashFuncVectImpl<BooleanVector, bool>(Vector *vector, const uint32_t start, const uint32_t rowCount,
    uint64_t *combinedHash);
template void DuplicateKeyValueImpl<BooleanVector, bool>(AggregateState &state, Vector *vector, const uint32_t offset,
    ExecutionContext *context);
template void IsSameNodeFuncImpl<BooleanVector, bool>(Vector *vector, const uint32_t offset, AggregateState &slot,
    bool &isSame);

static constexpr FunctionByDataType GROUP_AGG_FUNCTIONS[VEC_TYPE_MAX_COUNT] = {
    {OMNI_VEC_TYPE_NONE, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr},
    {
        OMNI_VEC_TYPE_INT, HashFuncImpl<IntVector, int32_t>, HashFuncVectImpl<IntVector, int32_t>, IsSameNodeFuncImpl<IntVector, int32_t>,
        DuplicateKeyValueImpl<IntVector, int32_t>, SetVectorImpl<IntVector>, FillValueImpl<IntVector, int32_t>
    },
    {
        OMNI_VEC_TYPE_LONG, HashFuncImpl<LongVector, int64_t>, HashFuncVectImpl<LongVector, int64_t>, IsSameNodeFuncImpl<LongVector, int64_t>,
        DuplicateKeyValueImpl<LongVector, int64_t>, SetVectorImpl<LongVector>, FillValueImpl<LongVector, int64_t>
    },
    {
        OMNI_VEC_TYPE_DOUBLE, HashFuncImpl<DoubleVector, double>, HashFuncVectImpl<DoubleVector, double>, IsSameNodeFuncImpl<DoubleVector, double>, DuplicateKeyValueImpl<DoubleVector, double>,
        SetVectorImpl<DoubleVector>, FillValueImpl<DoubleVector, double>
    },
    {OMNI_VEC_TYPE_BOOLEAN, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_VEC_TYPE_SHORT, nullptr, nullptr, nullptr, nullptr, nullptr},
    {
        OMNI_VEC_TYPE_DECIMAL64, HashFuncImpl<LongVector, int64_t>, HashFuncVectImpl<LongVector, int64_t>, IsSameNodeFuncImpl<LongVector, int64_t>, DuplicateKeyValueImpl<LongVector, int64_t>,
        SetVectorImpl<LongVector>, FillValueImpl<LongVector, int64_t>
    },
    {
        OMNI_VEC_TYPE_DECIMAL128, HashDecimalFunc, HashDecimalVectFunc, IsSameNodeFuncImpl<Decimal128Vector, Decimal128>, DuplicateKeyValueImpl<Decimal128Vector, Decimal128>,
        SetVectorImpl<Decimal128Vector>, FillValueImpl<Decimal128Vector, Decimal128>
    },
    {
        OMNI_VEC_TYPE_DATE32, HashFuncImpl<IntVector, int32_t>, HashFuncVectImpl<IntVector, int32_t>, IsSameNodeFuncImpl<IntVector, int32_t>, DuplicateKeyValueImpl<IntVector, int32_t>,
        SetVectorImpl<IntVector>, FillValueImpl<IntVector, int32_t>
    },
    {OMNI_VEC_TYPE_DATE64, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_VEC_TYPE_TIME32, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_VEC_TYPE_TIME64, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_VEC_TYPE_TIMESTAMP, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_VEC_TYPE_INTERVAL_MONTHS, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_VEC_TYPE_INTERVAL_DAY_TIME, nullptr, nullptr, nullptr, nullptr, nullptr},
    {
        OMNI_VEC_TYPE_VARCHAR, HashVarcharFuncImpl, HashVarcharVectFuncImpl, IsSameNodeFuncVarcharImpl,
        DuplicateVarcharKeyValue, SetVarcharVector, FillVarcharValue
    },
    {
        OMNI_VEC_TYPE_CHAR, HashVarcharFuncImpl, HashVarcharVectFuncImpl, IsSameNodeFuncVarcharImpl,
        DuplicateVarcharKeyValue, SetVarcharVector, FillVarcharValue
    },
    {OMNI_VEC_TYPE_DICTIONARY, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_VEC_TYPE_CONTAINER, nullptr, nullptr, nullptr, nullptr, SetContainerVector, nullptr},
};


OmniStatus HashAggregationOperatorFactory::Init()
{
    OmniStatus ret = OMNI_STATUS_NORMAL;
    for (int32_t i = 0; i < groupByColsContext.len; ++i) {
        groupByColIdx.push_back(groupByColsContext.context[i]);
    }
    for (int32_t i = 0; i < aggInputColsContext.len; ++i) {
        aggInputCols.push_back(aggInputColsContext.context[i]);
    }
    for (int32_t i = 0; i < aggFuncTypesContext.len; ++i) {
        switch (aggFuncTypesContext.context[i]) {
            case OMNI_AGGREGATION_TYPE_SUM: {
                aggregatorFactories.push_back(std::make_unique<SumAggregatorFactory>());
                break;
            }
            case OMNI_AGGREGATION_TYPE_COUNT_COLUMN: {
                aggregatorFactories.push_back(std::make_unique<CountColumnAggregatorFactory>());
                break;
            }
            case OMNI_AGGREGATION_TYPE_COUNT_ALL: {
                aggregatorFactories.push_back(std::make_unique<CountAllAggregatorFactory>());
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
                LogError("No such agg func type %d", aggFuncTypesContext.context[i]);
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
    std::vector<std::unique_ptr<Aggregator>> aggs;

    for (int32_t i = 0; i < this->groupByColIdx.size(); ++i) {
        auto type = this->groupByTypes.Get()[i];
        ColumnIndex c = { this->groupByColIdx[i], type, type };
        groupByIndex[i] = c;
    }

    uint32_t aggInputChannelIndex = 0;
    for (int32_t i = 0; i < this->aggregatorFactories.size(); ++i) {
        uint32_t aggregateType = aggFuncTypesContext.context[i];
        VecType inputType;
        int32_t aggInputCol;
        if (aggregateType == OMNI_AGGREGATION_TYPE_COUNT_ALL) {
            inputType = VecType(OMNI_VEC_TYPE_NONE);
            aggInputCol = Aggregator::INVALID_INPUT_COL;
        } else {
            inputType = this->aggInputTypes.Get()[aggInputChannelIndex];
            aggInputCol = aggInputCols[aggInputChannelIndex];
            aggInputChannelIndex++;
        }
        auto outputType = this->aggOutputTypes.Get()[i];
        auto aggregator = aggregatorFactories[i]->CreateAggregator(inputType, outputType, aggInputCol,
            inputRaw, outputPartial);
        aggs.push_back(std::move(aggregator));
    }

    HashAggregationOperator *groupBy = new HashAggregationOperator(groupByIndex, aggInputCols, aggInputTypes,
        aggOutputTypes, std::move(aggs), inputRaw, outputPartial);
    groupBy->Init();
    return groupBy;
}

OmniStatus HashAggregationOperator::Init()
{
    int32_t groupByColsSize = groupByCols.size();
    int32_t aggInputColsSize = aggInputCols.size();
    int32_t colSize = groupByColsSize + aggInputColsSize;
    sourceTypes = std::make_unique<int32_t[]>(colSize).release();
    for (auto &c : groupByCols) {
        sourceTypes[c.idx] = static_cast<int32_t>(c.input.GetId());
    }

    for (int32_t idx = 0; idx < aggInputColsSize; idx++) {
        sourceTypes[idx + groupByColsSize] = static_cast<int32_t>(aggInputTypes.Get()[idx].GetId());
    }
    executionContext = std::make_unique<ExecutionContext>();
    executionContext->getArena()->Allocate(DEFAULT_TEMP_MEM_SIZE);
    return OMNI_STATUS_NORMAL;
}

void HashAggregationOperator::PreLoop(VectorBatch *vecBatch) {}

void HashAggregationOperator::PostLoop(VectorBatch *vecBatch) const {}

static void GenerateCombinedHashes(Vector **vectors, uint32_t start, uint32_t rowCount, const int32_t colNum,
    uint64_t *combinedHashVal)
{
    Vector *vector = nullptr;
    for (int32_t i = 0; i < colNum; ++i) {
        vector = vectors[i];
        if (vector->GetTypeId() != OMNI_VEC_TYPE_DICTIONARY) {
            GROUP_AGG_FUNCTIONS[vector->GetTypeId()].hashFuncVect(vector, start, rowCount, combinedHashVal);
        } else {
            int32_t newIndexes[rowCount];
            Vector *originalVector =
                static_cast<DictionaryVector *>(vector)->ExtractDictionaryAndIds(start, rowCount, newIndexes);
            GROUP_AGG_FUNCTIONS[originalVector->GetTypeId()].hashFunc(originalVector, rowCount, newIndexes,
                combinedHashVal);
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

static void DuplicateGroupByTuple(AggregateState &state, Vector *vector, uint32_t offset, ExecutionContext *context)
{
    int32_t originalRowIndex;
    Vector *originalVector = VectorHelper::ExpandVectorAndIndex(vector, offset, originalRowIndex);
    GROUP_AGG_FUNCTIONS[originalVector->GetTypeId()].duplicateKey(state, originalVector, originalRowIndex, context);
}

static int32_t IsSameGroupByTuples(Vector **vectors, const uint32_t offset, const int32_t colNum,
    std::vector<std::vector<AggregateState>> &sameBucket)
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
void HashAggregationOperator::InLoop(VectorBatch *vecBatch, uint32_t rowCount, const int32_t *groupByColIdx,
    int32_t groupByColNum, int32_t aggNum)
{
    static const int blockSize = 1024;
    uint64_t combinedHashVal[blockSize];
    Vector *groupByVectors[groupByColNum];
    for (int i = 0; i < groupByColNum; ++i) {
        groupByVectors[i] = vecBatch->GetVector(groupByColIdx[i]);
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
        for (uint32_t rowIdx = 0; rowIdx < run; ++rowIdx) {
            uint64_t hash = combinedHashVal[rowIdx];
            int32_t isSamePos = -1;
            int32_t actualIdx = start + rowIdx;
            auto &bucket = groupedRows[hash];
            isSamePos = IsSameGroupByTuples(groupByVectors, actualIdx, groupByColNum, bucket);
            if (isSamePos == -1) {
                std::vector<AggregateState> groupByTuple(groupByColNum + aggNum, AggregateState());
                for (int32_t i = 0; i < groupByColNum; ++i) {
                    DuplicateGroupByTuple(groupByTuple[i], groupByVectors[i], actualIdx, executionContext.get());
                }
                bucket.push_back(groupByTuple);
                size_t chainLength = bucket.size();
                for (int32_t i = 0; i < aggNum; ++i) {
                    aggregators[i]->InitiateGroup(bucket[chainLength - 1][groupByColNum + i], vecBatch, actualIdx);
                }
            } else {
                for (int32_t i = 0; i < aggNum; ++i) {
                    aggregators[i]->ProcessGroup(bucket[isSamePos][groupByColNum + i], vecBatch, actualIdx);
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
    int32_t aggColNum = this->aggInputCols.size();
    int32_t aggNum = this->aggregators.size();
    auto aggColIdx = std::make_unique<int32_t[]>(aggColNum);
    auto aggFuncTypes = std::make_unique<int32_t[]>(aggNum);

    for (int32_t i = 0; i < groupColNum; ++i) {
        groupByColIdx[i] = this->groupByCols[i].idx;
    }
    for (int32_t i = 0; i < aggColNum; ++i) {
        aggColIdx[i] = this->aggInputCols[i];
    }
    for (int32_t i = 0; i < aggNum; i++) {
        aggFuncTypes[i] = this->aggregators[i]->GetType();
    }
    // verify whether input types match operator's types
    VERIFY_INPUT_TYPES(vecBatch, groupByColIdx.get(), groupColNum, aggColIdx.get(), aggNum, this->sourceTypes,
        aggFuncTypes);

    uint32_t rowCount = vecBatch->GetRowCount();

    this->InLoop(vecBatch, rowCount, groupByColIdx.get(), groupColNum, aggNum);

    this->PostLoop(vecBatch);
    VectorHelper::FreeVecBatch(vecBatch);
    return 0;
}

/**
 * @param types
 * @param columnCount
 * @return rowSize
 * All the output data types are determined in this function. Following allocation for output vectors and filling
 * value should use the 'types' parameter instead of using input vector types.
 */
int32_t HashAggregationOperator::GetRowSizeAndOutputTypes(std::vector<VecType> &types, int32_t columnCount)
{
    int32_t rowSize = 0;
    int32_t typeIndex = 0;
    for (auto &i : groupByCols) {
        types.push_back(i.input);
        rowSize += OperatorUtil::GetTypeSize(i.input);
    }
    for (int32_t i = 0; i < aggOutputTypes.GetSize(); ++i) {
        types.push_back(aggOutputTypes.Get()[i]);
        rowSize += OperatorUtil::GetTypeSize(aggOutputTypes.Get()[i]);
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

// TODO currently we need to traverse ColumnNum * RowNum times to build the output.
// The overhead need to be optimized.
SPECIALIZE(OMNIJIT_HASH_GROUPBY_AGG_COLUMN)
void HashAggregationOperator::FillAggVectors(VectorBatch *vecBatch, int startIndex, int endIndex,
    ChainIterator &rowIterator, int32_t rowIndex)
{
    for (int32_t aggIndex = 0, colIndex = startIndex; colIndex < endIndex; ++aggIndex, ++colIndex) {
        aggregators[aggIndex]->ExtractValue((*rowIterator)[colIndex], vecBatch->GetVector(colIndex), rowIndex);
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
    uint32_t aggColSize = aggregators.size();
    uint32_t colCount = groupByColSize + aggColSize;
    std::vector<VecType> types;
    int32_t rowByteSize = GetRowSizeAndOutputTypes(types, colCount);

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
        auto rowCount = std::min(rowsPerBatch, leftRowCount);
        auto vecBatch = new VectorBatch(colCount, rowCount);
        SetVectors(this->vecAllocator, vecBatch, types, rowCount);
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
void HashFuncVectImpl(Vector *vector, const uint32_t start, const uint32_t rowCount, uint64_t *combinedHash)
{
    uint64_t hash;
    std::hash<D> hasher;
    for (int32_t i = 0; i < rowCount; ++i) {
        int idx = i + start;
        hash = !vector->IsValueNull(idx) * hasher(static_cast<V *>(vector)->GetValue(idx));
        combinedHash[i] = HashUtil::CombineHash(combinedHash[i], hash);
    }
}

void HashVarcharVectFuncImpl(Vector *vector, const uint32_t start, const uint32_t rowCount, uint64_t *combinedHash)
{
    uint8_t *data = nullptr;
    for (int32_t i = 0; i < rowCount; ++i) {
        int idx = i + start;
        int valLen = static_cast<VarcharVector *>(vector)->GetValue(idx, &data);
        auto val = HashUtil::HashValue(reinterpret_cast<int8_t *>(data), valLen);
        combinedHash[i] = HashUtil::CombineHash(combinedHash[i], !vector->IsValueNull(idx) * val);
    }
}

void HashDecimalVectFunc(Vector *vector, const uint32_t start, const uint32_t rowCount, uint64_t *combinedHash)
{
    for (int32_t i = 0; i < rowCount; ++i) {
        int idx = i + start;
        Decimal128 val = static_cast<Decimal128Vector *>(vector)->GetValue(idx);
        combinedHash[i] = HashUtil::CombineHash(combinedHash[i],
            !vector->IsValueNull(idx) * HashUtil::HashValue(val.LowBits(), val.HighBits()));
    }
}

template <typename V, typename D>
void HashFuncImpl(Vector *vector, const uint32_t rowCount, const int32_t *rowIndexes, uint64_t *combinedHash)
{
    uint64_t hash;
    std::hash<D> hasher;
    for (int32_t i = 0; i < rowCount; ++i) {
        int32_t idx = rowIndexes[i];
        hash = !vector->IsValueNull(idx) * hasher(static_cast<V *>(vector)->GetValue(idx));
        combinedHash[i] = HashUtil::CombineHash(combinedHash[i], hash);
    }
}

void HashVarcharFuncImpl(Vector *vector, const uint32_t rowCount, const int32_t *rowIndexes, uint64_t *combinedHash)
{
    uint8_t *data = nullptr;
    for (int32_t i = 0; i < rowCount; ++i) {
        int32_t idx = rowIndexes[i];
        int valLen = static_cast<VarcharVector *>(vector)->GetValue(idx, &data);
        auto val = HashUtil::HashValue(reinterpret_cast<int8_t *>(data), valLen);
        combinedHash[i] = HashUtil::CombineHash(combinedHash[i], !vector->IsValueNull(idx) * val);
    }
}

void HashDecimalFunc(Vector *vector, const uint32_t rowCount, const int32_t *rowIndexes, uint64_t *combinedHash)
{
    for (int32_t i = 0; i < rowCount; ++i) {
        int32_t idx = rowIndexes[i];
        Decimal128 val = static_cast<Decimal128Vector *>(vector)->GetValue(idx);
        combinedHash[i] = HashUtil::CombineHash(combinedHash[i],
            !vector->IsValueNull(idx) * HashUtil::HashValue(val.LowBits(), val.HighBits()));
    }
}

template <typename V, typename D>
void IsSameNodeFuncImpl(Vector *vector, const uint32_t offset, AggregateState &slot, bool &isSame)
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

void IsSameNodeFuncVarcharImpl(Vector *vector, const uint32_t offset, AggregateState &slot, bool &isSame)
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
void DuplicateKeyValueImpl(AggregateState &state, Vector *vector, const uint32_t offset, ExecutionContext *context)
{
    if (vector->IsValueNull(offset)) {
        return;
    }
    int32_t len = sizeof(D);
    uint8_t *ptr = context->getArena()->Allocate(len);
    D data = static_cast<V *>(vector)->GetValue(offset);
    memcpy_s(ptr, len, &data, len);
    state.val = ptr;
}

void DuplicateVarcharKeyValue(AggregateState &state, Vector *vector, const uint32_t offset, ExecutionContext *context)
{
    if (vector->IsValueNull(offset)) {
        return;
    }
    uint8_t *tmp = nullptr;
    int valLen = (static_cast<VarcharVector *>(vector)->GetValue(offset, &tmp));
    uint8_t *data = context->getArena()->Allocate(valLen);
    memcpy_s(data, valLen, tmp, valLen);
    state.strVal = data;
    state.strLen = valLen;
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
    std::vector<uintptr_t> vectorAddresses(2);
    vectorAddresses[0] = reinterpret_cast<uintptr_t>(doubleVector);
    vectorAddresses[1] = reinterpret_cast<uintptr_t>(longVector);
    std::vector<VecType> vecTypes(2);
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
} // end of namespace op
} // end of namespace omniruntime
