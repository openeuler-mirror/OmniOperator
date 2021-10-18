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
    return OMNI_STATUS_NORMAL;
}

void HashAggregationOperator::PreLoop(VectorBatch *vecBatch) {}

void HashAggregationOperator::PostLoop(VectorBatch *vecBatch) const {}

void ALWAYS_INLINE GenerateCombinedHashes(Vector **vectors, uint32_t start, uint32_t rowCount,
    const int32_t colNum, int64_t *combinedHashVal)
{
    Vector *vector = nullptr;
    for (int32_t i = 0; i < colNum; ++i) {
        vector = vectors[i];
        if (vector->GetTypeId() != OMNI_VEC_TYPE_DICTIONARY) {
            HashAggregationOperator::FUNCTIONS[vector->GetTypeId()].hashFuncVect(vector, start, rowCount, combinedHashVal);
        }
        else {
            int32_t newIndexes[rowCount];
            Vector *originalVector = static_cast<DictionaryVector *>(vector)->
		    ExtractDictionaryAndIds(start, rowCount, newIndexes);
            HashAggregationOperator::FUNCTIONS[originalVector->GetTypeId()].hashFunc(originalVector, rowCount,
                                                                                     newIndexes, combinedHashVal);
        }
    }
}

std::vector<BucketIterator> HashAggregationOperator::FindBuckets(uint64_t* hash, int32_t blockSize)
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

void *ALWAYS_INLINE DuplicateGroupByTuple(Vector *vector, uint32_t offset)
{
    int32_t originalRowIndex;
    Vector *originalVector = VectorHelper::ExpandVectorAndIndex(vector, offset, originalRowIndex);
    return HashAggregationOperator::FUNCTIONS[originalVector->GetTypeId()].duplicateKey(originalVector,
        originalRowIndex);
}

int32_t ALWAYS_INLINE IsSameGroupByTuples(Vector** vectors, const uint32_t offset, const int32_t colNum,
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
            HashAggregationOperator::FUNCTIONS[originalVector->GetTypeId()].isSameNode(originalVector, offset,
                                                                                       sameBucket[it][i].val, isSame);
        }
        if (isSame) return it;
    }
    return -1;
}

SPECIALIZE(OMNIJIT_HASH_GROUPBY_INLOOP)
void HashAggregationOperator::InLoop(Vector **vectors, uint32_t rowCount, const int32_t *types, int32_t colNum,
    const int32_t *groupByColIdx, int32_t groupByColNum, const int32_t *aggColIdx, int32_t aggColNum,
    const int32_t *aggFuncTypes)
{
    static const int blockSize = 1024;
    int64_t combinedHashVal[blockSize];
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
            int64_t hash = combinedHashVal[offset];
            int32_t isSamePos = -1;
            int32_t actualOffset = start + offset;
            auto &bucket = groupedRows[hash];
            isSamePos = IsSameGroupByTuples(groupByVectors, actualOffset, groupByColNum, bucket);
            if (isSamePos == -1) {
                std::vector<GroupBySlot> groupByTuple(groupByColNum + aggColNum, GroupBySlot());
                for (int32_t i = 0; i < groupByColNum; ++i) {
                    groupByTuple[i].val = DuplicateGroupByTuple(groupByVectors[i], actualOffset);
                }
                bucket.push_back(groupByTuple);
                size_t chainLength = bucket.size();
                for (int32_t i = 0; i < aggColNum; ++i) {
                    aggregators[i]->AggInsert(bucket[chainLength - 1][groupByColNum + i],
                                              aggrByVectors[i], aggrByTypes[i], actualOffset);
                }
            } else {
                for (int32_t i = 0; i < aggColNum; ++i) {
                    aggregators[i]->AggProcessGroup(bucket[isSamePos][groupByColNum + i],
                                                    aggrByVectors[i], aggrByTypes[i], actualOffset);
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

    this->InLoop(vectors, rowCount, this->sourceTypes, vectorCount, groupByColIdx.get(), groupColNum, aggColIdx.get(),
        aggColNum, aggFuncTypes.get());

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
                                                 ChainIterator &rowIterator,
                                                 ChainIterator &finalIterator, int32_t rowCount)
{
    ChainIterator tempRowIterator = rowIterator;
    for (int colIndex = startIndex, groupByIndex = 0; colIndex < endIndex; ++colIndex, ++groupByIndex) {
        tempRowIterator = rowIterator;
        auto typeId = vecBatch->GetVector(colIndex)->GetTypeId();
        HashAggregationOperator::FUNCTIONS[typeId].fillValue(*this, vecBatch, rowCount, tempRowIterator, finalIterator, colIndex);
    }
}

void HashAggregationOperator::FillAvgAgg(VectorBatch *vecBatch, int32_t aggIndex, int32_t colIndex, int32_t rowCount,
                                         ChainIterator &rowIterator,
                                         ChainIterator &finalIterator)
{
    // TODO support average value type which is decimal
    if (outputPartial) {
        ContainerVector *vector = static_cast<ContainerVector *>(vecBatch->GetVector(colIndex));
        for (int32_t rIdx = 0; rIdx < rowCount && rowIterator != finalIterator; ++rIdx, rowIterator++) {
            if ((*rowIterator)[colIndex].avgCnt == 0) {
                LogError("Divisor is zero!");
            }
            DoubleVector *doubleVector = reinterpret_cast<DoubleVector *>(vector->getValue(0));
            doubleVector->SetValue(rIdx, *(reinterpret_cast<double *>((*rowIterator)[colIndex].avgVal)));
            LongVector *longVector = reinterpret_cast<LongVector *>(vector->getValue(1));
            longVector->SetValue(rIdx, (*rowIterator)[colIndex].avgCnt);
        }
    } else {
        DoubleVector *vector = static_cast<DoubleVector *>(vecBatch->GetVector(colIndex));
        for (int32_t rIdx = 0; rIdx < rowCount && rowIterator != finalIterator; ++rIdx, rowIterator++) {
            if ((*rowIterator)[colIndex].avgCnt == 0) {
                LogError("Divisor is zero!");
            }
            vector->SetValue(rIdx, *(reinterpret_cast<double *>((*rowIterator)[colIndex].avgVal)));
        }
    }
}

// TODO currently we need to traverse ColumnNum * RowNum times to build the output.
// The overhead need to be optimized.
SPECIALIZE(OMNIJIT_HASH_GROUPBY_AGG_COLUMN)
void HashAggregationOperator::FillAggVectors(VectorBatch *vecBatch, int startIndex, int endIndex,
                                             ChainIterator &rowIterator,
                                             ChainIterator &finalIterator, int32_t rowCount)
{
    auto resultIterator = rowIterator;
    for (int32_t aggIndex = 0, colIndex = startIndex; colIndex < endIndex; ++aggIndex, ++colIndex) {
        resultIterator = rowIterator;
        AggregateType aggType = this->aggregators[aggIndex]->GetType();
        switch (aggType) {
            case OMNI_AGGREGATION_TYPE_SUM:
            case OMNI_AGGREGATION_TYPE_MIN:
            case OMNI_AGGREGATION_TYPE_MAX: {
                auto typeId = vecBatch->GetVector(colIndex)->GetTypeId();
                HashAggregationOperator::FUNCTIONS[typeId].fillValue(*this, vecBatch, rowCount, resultIterator,
                                                                     finalIterator, colIndex);
                break;
            }
            case OMNI_AGGREGATION_TYPE_COUNT: {
                LongVector *vector = static_cast<LongVector *>(vecBatch->GetVector(colIndex));
                for (int32_t rIdx = 0; rIdx < rowCount && resultIterator != finalIterator;
                    ++rIdx, resultIterator++) {
                    vector->SetValue(rIdx, (*resultIterator)[colIndex].count);
                }
                break;
            }
            case OMNI_AGGREGATION_TYPE_AVG: { // TODO process intermediate vectors
                // generate double or row type vector according to the step. Row type if outputPartial == 1 otherwise
                // double vector.
                FillAvgAgg(vecBatch, aggIndex, colIndex, rowCount, resultIterator, finalIterator);
                break;
            }
            default: {
                LogError("No such aggregate type %d\n", aggType);
                break;
            }
        }
    }
    rowIterator = resultIterator;
}

void SetVectors(VectorAllocator *vecAllocator, VectorBatch *vectorBatch, const std::vector<VecType> &types,
    int32_t rowCount)
{
    for (int colIndex = 0; colIndex < vectorBatch->GetVectorCount(); ++colIndex) {
        VecType type = types[colIndex];
        HashAggregationOperator::FUNCTIONS[type.GetId()].setVector(vectorBatch, type, colIndex, vecAllocator, rowCount);
    }
}

int32_t HashAggregationOperator::GetOutput(std::vector<VectorBatch *> &result)
{
    uint32_t groupByColSize = groupByCols.size();
    uint32_t aggColSize = aggCols.size();
    uint32_t colCount = groupByColSize + aggColSize;
    std::vector<VecType> types;
    int32_t vecBatchCount;
    int32_t rowSize = GetRowSize(types, colCount);
    for (auto it = groupedRows.begin(); it != groupedRows.end(); ++it) {
        if (rowSize <= 0) {
            // TODO define our exception class
            LogError("Output row size is less than 0!");
        }
        int32_t maxRowNum = MAX_TABLE_SIZE_IN_BYTES / rowSize;
        vecBatchCount = std::ceil(static_cast<double>(it->second.size()) / static_cast<double>(maxRowNum));
        int32_t currentPosition = 0;

        ChainIterator rowIterator = it->second.begin();
        ChainIterator finalIterator = it->second.end();
        for (int32_t i = 0; i < vecBatchCount; ++i) {
            int32_t rowCount = std::min(maxRowNum, static_cast<int32_t>((it->second.size() - currentPosition)));
            auto vecBatch = std::make_unique<VectorBatch>(colCount);
            SetVectors(this->vecAllocator, vecBatch.get(), types, rowCount);
            FillGroupByVectors(vecBatch.get(), 0, groupByColSize, rowIterator, finalIterator, rowCount);
            FillAggVectors(vecBatch.get(), groupByColSize, colCount, rowIterator, finalIterator, rowCount);
            result.push_back(vecBatch.release());
            currentPosition += maxRowNum;
        }
    }

    // set finished.
    SetStatus(OMNI_STATUS_FINISHED);
    return vecBatchCount;
}

OmniStatus HashAggregationOperator::CloseGroupBy()
{
    auto groupByColSize = groupByCols.size();
    for (auto bucket = groupedRows.begin(); bucket != groupedRows.end(); ++bucket) {
        for (auto node = bucket->second.begin(); node != bucket->second.end(); ++node) {
            for (int32_t idx = 0; idx < groupByColSize; ++idx) {
                HashAggregationOperator::FUNCTIONS[groupByCols[idx].input.GetId()]
                        .releaseMemory(node->at(idx), idx, groupByCols[idx].input);
            }
        }
    }
    return OMNI_STATUS_NORMAL;
}

OmniStatus HashAggregationOperator::CloseAgg()
{
    auto groupByColSize = groupByCols.size();
    auto aggColSize = aggCols.size();
    for (auto bucket = groupedRows.begin(); bucket != groupedRows.end(); ++bucket) {
        for (auto node = bucket->second.begin(); node != bucket->second.end(); ++node) {
            for (int32_t idx = 0; idx < aggColSize; ++idx) {
                if (aggregators[idx]->GetType() == OMNI_AGGREGATION_TYPE_COUNT) {
                    continue;
                }
                if (aggregators[idx]->GetType() == OMNI_AGGREGATION_TYPE_AVG) {
                    delete static_cast<double *>(node->at(idx).avgVal);
                    continue;
                }
                auto typeId = aggCols[idx].output.GetId();
                HashAggregationOperator::FUNCTIONS[typeId]
                        .releaseMemory(node->at(groupByColSize + idx), groupByColSize + idx, aggCols[idx].output);
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

template<typename V, typename D>
void ALWAYS_INLINE
HashFuncVectImpl(Vector *vector, const uint32_t start, const uint32_t rowCount, int64_t *combinedHash)
{
    uint64_t hash;
    std::hash<D> hasher;
    for (int32_t i = 0; i < rowCount; ++i) {
        int idx = i + start;
        hash = !vector->IsValueNull(idx) * hasher(static_cast<V *>(vector)->GetValue(idx));
        combinedHash[i] = HashUtil::CombineHash(combinedHash[i], hash);
    }
}

void ALWAYS_INLINE
HashVarcharVectFuncImpl(Vector *vector, const uint32_t start, const uint32_t rowCount, int64_t *combinedHash)
{
    std::hash<std::string> hashVarChar;
    uint8_t *data = nullptr;
    for (int32_t i = 0; i < rowCount; ++i) {
        int idx = i + start;
        int valLen = static_cast<VarcharVector *>(vector)->GetValue(idx, &data);
        std::string val(reinterpret_cast<char *>(data), valLen);
        combinedHash[i] = HashUtil::CombineHash(combinedHash[i], !vector->IsValueNull(idx) * hashVarChar(val));
    }
}

void ALWAYS_INLINE
HashDecimalVectFunc(Vector *vector, const uint32_t start, const uint32_t rowCount, int64_t *combinedHash)
{
    for (int32_t i = 0; i < rowCount; ++i) {
        int idx = i + start;
        Decimal128 val = static_cast<Decimal128Vector *>(vector)->GetValue(idx);
        combinedHash[i] = HashUtil::CombineHash(combinedHash[i], !vector->IsValueNull(idx) *
                                                                 HashUtil::HashValue(val.LowBits(),
                                                                                     val.HighBits()));
    }
}

template<typename V, typename D>
void ALWAYS_INLINE HashFuncImpl(Vector *vector, const uint32_t rowCount, const int32_t *rowIndexes,
                                int64_t *combinedHash)
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
                                       int64_t *combinedHash)
{
    std::hash<std::string> hashVarChar;
    uint8_t *data = nullptr;
    for (int32_t i = 0; i < rowCount; ++i) {
        int32_t idx = rowIndexes[i];
        int valLen = static_cast<VarcharVector *>(vector)->GetValue(idx, &data);
        std::string val(reinterpret_cast<char *>(data), valLen);
        combinedHash[i] = HashUtil::CombineHash(combinedHash[i], !vector->IsValueNull(idx) * hashVarChar(val));
    }
}

void ALWAYS_INLINE HashDecimalFunc(Vector *vector, const uint32_t rowCount, const int32_t *rowIndexes,
                                   int64_t *combinedHash)
{
    for (int32_t i = 0; i < rowCount; ++i) {
        int32_t idx = rowIndexes[i];
        Decimal128 val = static_cast<Decimal128Vector *>(vector)->GetValue(idx);
        combinedHash[i] = HashUtil::CombineHash(combinedHash[i], !vector->IsValueNull(idx) *
                                                                 HashUtil::HashValue(val.LowBits(),
                                                                                     val.HighBits()));
    }
}

template<typename V, typename D>
void IsSameNodeFuncImpl(Vector* vector,
                        const uint32_t offset,
                        void *val,
                        bool &isSame)
{
    bool isIntermediateNull = static_cast<D *>(val) == nullptr;
    bool isInputNull = vector->IsValueNull(offset);

    if (!isInputNull && !isIntermediateNull) {
        auto intTmp = static_cast<V *>(vector)->GetValue(offset);
        isSame = intTmp == *static_cast<D *>(val);
		return;
    }
    if (isInputNull != isIntermediateNull) {
        isSame = false;
        return;
    }
    isSame = true;
    return;
}

void IsSameNodeFuncVarcharImpl(Vector* vector, const uint32_t offset, void *val, bool &isSame)
{
    bool isIntermediateNull = static_cast<VarcharVector *>(val) == nullptr;
    bool isInputNull = vector->IsValueNull(offset);

    if (!isInputNull && !isIntermediateNull) {
        uint8_t *data = nullptr;
        int valLen = static_cast<VarcharVector *>(vector)->GetValue(offset, &data);
        std::string s(reinterpret_cast<char *>(data), valLen);
        isSame = s == *static_cast<std::string *>(val);
        return;
    }
    if (isInputNull != isIntermediateNull) {
        isSame = false;
        return;
    }
    isSame = true;
    return;
}

template <typename V, typename D> void *ALWAYS_INLINE DuplicateKeyValueImpl(Vector *vector, const uint32_t offset)
{
    if (vector->IsValueNull(offset)) {
        return nullptr;
    }
    return std::make_unique<D>(static_cast<V *>(vector)->GetValue(offset)).release();
}

void *ALWAYS_INLINE DuplicateVarcharKeyValue(Vector *vector, const uint32_t offset)
{
    if (vector->IsValueNull(offset)) {
        return nullptr;
    }
    uint8_t *data = nullptr;
    int valLen = (static_cast<VarcharVector *>(vector)->GetValue(offset, &data));
    return reinterpret_cast<void *>(new std::string(reinterpret_cast<char *>(data), 0, valLen));
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
void FillValueImpl(HashAggregationOperator &hashOperator, VectorBatch *vecBatch, int32_t rowCount,
                   ChainIterator &tempRowIterator,
                   ChainIterator &finalRowIterator, int colIndex)
{
    auto vector = static_cast<V *>(vecBatch->GetVector(colIndex));
    for (int rowIndex = 0; rowIndex < rowCount && tempRowIterator != finalRowIterator;
        ++rowIndex, ++tempRowIterator) {
        if ((*tempRowIterator)[colIndex].val == nullptr) {
            vector->SetValueNull(rowIndex);
            continue;
        }
        vector->SetValue(rowIndex, *static_cast<D *>((*tempRowIterator)[colIndex].val));
    }
}

void FillVarcharValue(HashAggregationOperator &hashOperator, VectorBatch *vecBatch, int32_t rowCount,
                      ChainIterator &tempRowIterator,
                      ChainIterator &finalRowIterator, int colIndex)
{
    auto vector = static_cast<VarcharVector *>(vecBatch->GetVector(colIndex));
    for (int rowIndex = 0; rowIndex < rowCount && tempRowIterator != finalRowIterator;
        ++rowIndex, ++tempRowIterator) {
        if ((*tempRowIterator)[colIndex].val == nullptr) {
            vector->SetValueNull(rowIndex);
            continue;
        }
        vector->SetValue(rowIndex, reinterpret_cast<const uint8_t *>(
                                 (*((std::string *) (*tempRowIterator)[colIndex].val)).c_str()),
                         (*((std::string *) (*tempRowIterator)[colIndex].val)).size());
    }
}

template<typename T>
void ReleaseMemoryImpl(GroupBySlot &columnVal, int32_t columnIndex, VecType &type)
{
    auto p = columnVal.val;
    if (p != nullptr) {
        delete static_cast<T *>(p);
        p = nullptr;
    }
}
} // end of namespace op
} // end of namespace omniruntime
