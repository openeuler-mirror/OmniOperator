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
#include "util/type_util.h"
#include "operator/hash_util.h"
#include "operator/util/operator_util.h"
#include "operator/aggregation/aggregator/aggregator_factory.h"
#ifdef ENABLE_HMPP
#include <HMPP/hmpp.h>
#include "operator/hmpp_hash_util.h"
#include "util/config_util.h"
#endif

#if defined(DEBUG_OPERATOR) && defined(TRACE)
#include <sstream>
#endif
namespace omniruntime {
namespace op {
using namespace omniruntime::type;

template void HashFuncImpl<BooleanVector, bool>(Vector *vector, const uint32_t rowCount, const int32_t *rowIndexes,
    uint64_t *combinedHash);

template void HashFuncVectImpl<BooleanVector, bool>(Vector *vector, const uint32_t start, const uint32_t rowCount,
    uint64_t *combinedHash);

template void DuplicateKeyValueImpl<BooleanVector, bool>(AggregateState &state, Vector *vector, const uint32_t offset,
    ExecutionContext *context);

template void IsSameNodeFuncImpl<BooleanVector, bool>(Vector *vector, const uint32_t offset, AggregateState &slot,
    bool &isSame);

static constexpr FunctionByDataType GROUP_AGG_FUNCTIONS[DATA_TYPE_MAX_COUNT] = {
    {OMNI_NONE, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_INT, HashFuncImplProxy<IntVector, int32_t>, HashFuncVectImplProxy<IntVector, int32_t>,
     IsSameNodeFuncImpl<IntVector, int32_t>, DuplicateKeyValueImpl<IntVector, int32_t>,
     SetVectorImpl<IntVector>, FillValueImpl<IntVector, int32_t>
    },
    {OMNI_LONG, HashFuncImplProxy<LongVector, int64_t>, HashFuncVectImplProxy<LongVector, int64_t>,
     IsSameNodeFuncImpl<LongVector, int64_t>, DuplicateKeyValueImpl<LongVector, int64_t>,
     SetVectorImpl<LongVector>, FillValueImpl<LongVector, int64_t>
    },
    {
        OMNI_DOUBLE, HashFuncImplProxy<DoubleVector, double>, HashFuncVectImplProxy<DoubleVector, double>,
        IsSameNodeFuncImpl<DoubleVector, double>, DuplicateKeyValueImpl<DoubleVector, double>,
        SetVectorImpl<DoubleVector>, FillValueImpl<DoubleVector, double>
    },
    {
        OMNI_BOOLEAN, HashFuncImplProxy<BooleanVector, bool>, HashFuncVectImplProxy<BooleanVector, bool>,
        IsSameNodeFuncImpl<BooleanVector, bool>, DuplicateKeyValueImpl<BooleanVector, bool>,
        SetVectorImpl<BooleanVector>, FillValueImpl<BooleanVector, bool>
    },
    {OMNI_SHORT, HashFuncImplProxy<ShortVector, int16_t>, HashFuncVectImplProxy<ShortVector, int16_t>,
     IsSameNodeFuncImpl<ShortVector, int16_t>, DuplicateKeyValueImpl<ShortVector, int16_t>,
     SetVectorImpl<ShortVector>, FillValueImpl<ShortVector, int16_t>},
    {OMNI_DECIMAL64, HashFuncImplProxy<LongVector, int64_t>, HashFuncVectImplProxy<LongVector, int64_t>,
     IsSameNodeFuncImpl<LongVector, int64_t>, DuplicateKeyValueImpl<LongVector, int64_t>,
     SetVectorImpl<LongVector>, FillValueImpl<LongVector, int64_t>
    },
    {OMNI_DECIMAL128, HashDecimalFuncProxy, HashDecimalVectFuncProxy,
     IsSameNodeFuncImpl<Decimal128Vector, Decimal128>, DuplicateKeyValueImpl<Decimal128Vector, Decimal128>,
     SetVectorImpl<Decimal128Vector>, FillValueImpl<Decimal128Vector, Decimal128>
    },
    {OMNI_DATE32, HashFuncImplProxy<IntVector, int32_t>, HashFuncVectImplProxy<IntVector, int32_t>,
     IsSameNodeFuncImpl<IntVector, int32_t>, DuplicateKeyValueImpl<IntVector, int32_t>,
     SetVectorImpl<IntVector>, FillValueImpl<IntVector, int32_t>
    },
    {OMNI_DATE64, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_TIME32, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_TIME64, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_TIMESTAMP, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_INTERVAL_MONTHS, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_INTERVAL_DAY_TIME, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_VARCHAR, HashVarcharFuncImplProxy, HashVarcharVectFuncImplProxy, IsSameNodeFuncVarcharImpl, DuplicateVarcharKeyValue,
     SetVarcharVector,   FillVarcharValue },
    {OMNI_CHAR, HashVarcharFuncImplProxy, HashVarcharVectFuncImplProxy, IsSameNodeFuncVarcharImpl, DuplicateVarcharKeyValue,
     SetVarcharVector,   FillVarcharValue },
    {OMNI_CONTAINER, nullptr, nullptr, nullptr, nullptr, SetContainerVector, nullptr},
};

OmniStatus HashAggregationOperatorFactory::Init()
{
    OmniStatus ret = OMNI_STATUS_NORMAL;
    for (uint32_t i = 0; i < groupByColsVector.size(); ++i) {
        groupByColIdx.push_back(groupByColsVector[i]);
    }
    for (auto aggInputColsVector : aggsInputColsVector) {
        std::vector<int32_t> aggInputCols;
        for (uint32_t i = 0; i < aggInputColsVector.size(); ++i) {
            aggInputCols.push_back(aggInputColsVector[i]);
        }
        aggsInputCols.push_back(aggInputCols);
    }
    ret = CreateAggregatorFactories(aggregatorFactories, aggFuncTypesVector, GetMaskColumns());

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

    for (uint32_t i = 0; i < this->groupByColIdx.size(); ++i) {
        auto type = this->groupByTypes.GetType(i);
        ColumnIndex c = { this->groupByColIdx[i], type, type };
        groupByIndex[i] = c;
    }

    // refresh inputDateTypes and intputColumnar index for OMNI_AGGREGATION_TYPE_COUNT_ALL type aggregator
    uint32_t aggInputColsSize = 0;
    uint32_t aggCountAllSkipCnt = 0;
    uint32_t aggregateType = OMNI_AGGREGATION_TYPE_INVALIDE;
    for (uint32_t i = 0; i < this->aggregatorFactories.size(); i++) {
        std::vector<int32_t> aggInputColIdxVec;
        std::vector<DataTypePtr> inputDataTypesPtr;
        aggregateType = aggFuncTypesVector[i];

        // for COUNT_ALL aggregator no input(key and columnar index)
        // use aggCountAllSkipCnt to align with aggsInputCols and aggregatorFactories index not same
        if (aggregateType == OMNI_AGGREGATION_TYPE_COUNT_ALL) {
            inputDataTypesPtr.push_back(NoneType());
            aggInputColIdxVec.push_back(-1);
            aggCountAllSkipCnt++;
        } else {
            for (uint32_t j = 0; j < this->aggsInputCols[i - aggCountAllSkipCnt].size(); j++) {
                inputDataTypesPtr.push_back(aggInputTypes[i - aggCountAllSkipCnt].GetType(j));
                aggInputColIdxVec.push_back(aggsInputCols[i - aggCountAllSkipCnt][j]);
                aggInputColsSize++;
            }
        }

        auto inputTypes = DataTypes(inputDataTypesPtr).Instance();
        auto outputTypes = aggOutputTypes[i].Instance();
        auto aggregator = aggregatorFactories[i]->CreateAggregator(inputTypes, outputTypes, aggInputColIdxVec,
            inputRaws[i], outputPartials[i], isOverflowAsNull);
        aggs.push_back(std::move(aggregator));
    }

    auto groupByOperator = new HashAggregationOperator(groupByIndex, aggsInputCols, aggInputColsSize, aggInputTypes,
        aggOutputTypes, std::move(aggs), inputRaws, outputPartials);
    groupByOperator->Init();
    return groupByOperator;
}

OmniStatus HashAggregationOperator::Init()
{
    auto groupByColsSize = groupByCols.size();
    auto colSize = groupByColsSize + aggInputColsSize;
    sourceTypes = new int32_t[colSize];
    // group by source types
    for (auto &c : groupByCols) {
        sourceTypes[c.idx] = static_cast<int32_t>(c.input->GetId());
    }

    // agg source types
    uint32_t sourceTypesIdx = groupByColsSize;
    for (auto &dataTypes : aggInputTypes) {
        for (int32_t idx = 0; idx < dataTypes.GetSize(); idx++) {
            sourceTypes[sourceTypesIdx] = static_cast<int32_t>(dataTypes.GetType(idx)->GetId());
            sourceTypesIdx++;
        }
    }
    executionContext = std::make_unique<ExecutionContext>();
    executionContext->GetArena()->SetAllocator(vecAllocator);
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
        if (vector->GetEncoding() != OMNI_VEC_ENCODING_DICTIONARY) {
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
    for (uint32_t it = 0; it < sameBucket.size(); it++) {
        bool isSame = true;
        for (int32_t i = 0; i < colNum && isSame; ++i) {
            int32_t originalRowIndex;
            Vector *originalVector = VectorHelper::ExpandVectorAndIndex(vectors[i], offset, originalRowIndex);
            GROUP_AGG_FUNCTIONS[originalVector->GetTypeId()].isSameNode(originalVector, originalRowIndex,
                sameBucket[it][i], isSame);
        }
        if (isSame) {
            return it;
        }
    }
    return -1;
}
#ifdef ENABLE_HMPP
void HashAggregationOperator::InLoopHMPP(VectorBatch *vecBatch, uint32_t rowCount, const int32_t *groupByColIdx,
    int32_t groupByColNum, int32_t aggNum)
{
    uint64_t *combinedHashVal = new uint64_t[rowCount]();
    static const int blockSize = 1024;
    Vector *groupByVectors[groupByColNum];
    for (int i = 0; i < groupByColNum; ++i) {
        groupByVectors[i] = vecBatch->GetVector(groupByColIdx[i]);
    }

    // compute hashes in batches using HMPP
    GenerateCombinedHashes(groupByVectors, 0, rowCount, groupByColNum, combinedHashVal);

    uint32_t run = blockSize;
    for (uint32_t start = 0; start < rowCount; start = start + blockSize) {
        if ((start + blockSize) > rowCount) {
            run = rowCount - start;
        }

        for (uint32_t rowIdx = 0; rowIdx < run; ++rowIdx) {
            uint64_t hash = combinedHashVal[start + rowIdx];
            int32_t isSamePos = -1;
            uint32_t actualIdx = start + rowIdx;
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
                    aggregators[i]->InitiateGroup(bucket[chainLength - 1][groupByColNum + i], vecBatch,
                        static_cast<int32_t>(actualIdx));
                }
            } else {
                for (int32_t i = 0; i < aggNum; ++i) {
                    aggregators[i]->ProcessGroup(bucket[isSamePos][groupByColNum + i], vecBatch,
                        static_cast<int32_t>(actualIdx));
                }
            }
        }
    }

    delete[] combinedHashVal;
}
#endif
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
            uint32_t actualIdx = start + rowIdx;
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
                    aggregators[i]->InitiateGroup(bucket[chainLength - 1][groupByColNum + i], vecBatch,
                        static_cast<int32_t>(actualIdx));
                }
            } else {
                for (int32_t i = 0; i < aggNum; ++i) {
                    aggregators[i]->ProcessGroup(bucket[isSamePos][groupByColNum + i], vecBatch,
                        static_cast<int32_t>(actualIdx));
                }
            }
        }
    }
}

void HashAggregationOperator::InLoopProxy(VectorBatch *vecBatch, uint32_t rowCount, const int32_t *groupByColIdx,
    int32_t groupByColNum, int32_t aggNum)
{
#ifdef ENABLE_HMPP
    if (ConfigUtil::IsEnableHMPP()) {
        InLoopHMPP(vecBatch, rowCount, groupByColIdx, groupByColNum, aggNum);
    } else {
        InLoop(vecBatch, rowCount, groupByColIdx, groupByColNum, aggNum);
    }
#else
    InLoop(vecBatch, rowCount, groupByColIdx, groupByColNum, aggNum);
#endif
}

int32_t HashAggregationOperator::AddInput(VectorBatch *vecBatch)
{
    LogTrace("Enter Func");
    this->PreLoop(vecBatch);
    auto groupColNum = this->groupByCols.size();
    auto groupByColIdx = std::make_unique<int32_t[]>(groupColNum);
    auto aggNum = this->aggregators.size();

    for (size_t i = 0; i < groupColNum; ++i) {
        groupByColIdx[i] = this->groupByCols[i].idx;
    }

    uint32_t rowCount = static_cast<uint32_t>(vecBatch->GetRowCount());
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
int32_t HashAggregationOperator::GetRowSizeAndOutputTypes(std::vector<DataTypePtr> &types, int32_t columnCount)
{
    int32_t rowSize = 0;
    for (auto &i : groupByCols) {
        types.push_back(i.input);
        rowSize += OperatorUtil::GetTypeSize(i.input);
    }
    for (auto singleAgg : aggOutputTypes) {
        for (int32_t i = 0; i < singleAgg.GetSize(); i++) {
            types.push_back(singleAgg.GetType(i));
            rowSize += OperatorUtil::GetTypeSize(singleAgg.GetType(i));
        }
    }
    return rowSize;
}

void HashAggregationOperator::FillGroupByVectors(VectorBatch *vecBatch, int startIndex, int endIndex,
    ChainIterator &rowIterator, int32_t rowIndex)
{
    for (int colIndex = startIndex, groupByIndex = 0; colIndex < endIndex; ++colIndex, ++groupByIndex) {
        auto typeId = vecBatch->GetVector(colIndex)->GetTypeId();
        GROUP_AGG_FUNCTIONS[typeId].fillValue(vecBatch, rowIndex, rowIterator, colIndex);
    }
}

// currently we need to traverse ColumnNum * RowNum times to build the output.
// The overhead need to be optimized.
void HashAggregationOperator::FillAggVectors(VectorBatch *vecBatch, int startIndex, int endIndex,
    ChainIterator &rowIterator, int32_t rowIndex)
{
    int aggOutputStartIndex = startIndex;
    for (uint32_t aggIndex = 0; aggIndex < aggregators.size(); aggIndex++) {
        // construct extract vectors for per aggregator
        std::vector<Vector *> extractVectors;
        auto oneAggOutputCols = aggOutputTypes[aggIndex].GetSize();
        for (auto j = 0; j < oneAggOutputCols; j++) {
            extractVectors.push_back(vecBatch->GetVector(aggOutputStartIndex + j));
        }
        aggregators[aggIndex]->ExtractValues((*rowIterator)[startIndex + aggIndex], extractVectors, rowIndex);
        aggOutputStartIndex += oneAggOutputCols;
    }
}

void SetVectors(VectorAllocator *vecAllocator, VectorBatch *vectorBatch, const std::vector<DataTypePtr> &types,
    int32_t rowCount)
{
    for (int colIndex = 0; colIndex < vectorBatch->GetVectorCount(); ++colIndex) {
        DataTypePtr type = types[colIndex];
        GROUP_AGG_FUNCTIONS[type->GetId()].setVector(vectorBatch, *type, colIndex, vecAllocator, rowCount);
    }
}


int32_t HashAggregationOperator::GetOutput(std::vector<VectorBatch *> &result)
{
    uint32_t groupByColSize = groupByCols.size();
    uint32_t aggOutputColSize = 0;
    for (auto oneAggOutputTypes : aggOutputTypes) {
        aggOutputColSize += oneAggOutputTypes.GetSize();
    }
    uint32_t colCount = groupByColSize + aggOutputColSize;
    std::vector<DataTypePtr> types;
    int32_t rowByteSize = GetRowSizeAndOutputTypes(types, colCount);

    // accumulate whole row count first
    int32_t totalRowCount = 0;
    for (auto it = groupedRows.begin(); it != groupedRows.end(); ++it) {
        totalRowCount += static_cast<int32_t>(it->second.size());
    }

    if (totalRowCount == 0) {
        return 0;
    }

    auto rowsPerBatch = OperatorUtil::GetMaxRowCount(rowByteSize);
    auto expectedBatchSize = OperatorUtil::GetVecBatchCount(totalRowCount, rowsPerBatch);
    auto leftRowCount = totalRowCount;

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
    FillOutputVecBatch(result, groupByColSize, colCount, rowsPerBatch, allGroups);

    // set finished.
    SetStatus(OMNI_STATUS_FINISHED);
    return expectedBatchSize;
}

void HashAggregationOperator::FillOutputVecBatch(std::vector<VectorBatch *> &result, uint32_t groupByColSize,
    uint32_t colCount, int32_t rowsPerBatch, std::vector<ChainIterator> &allGroups)
{
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
        try {
            FillAggVectors(batchToFill, groupByColSize, colCount, group, filledRowSize);
        } catch (const OmniException &e) {
            // release VectorBatch when aggregator.ExtractValues throw exception
            // when spark hash agg sum/avg decimal overflow, it will throw exception when
            // OverflowConfigId==OVERFLOW_CONFIG_EXCEPTION
            for (auto vecBatch : result) {
                VectorHelper::FreeVecBatch(vecBatch);
            }
            throw e;
        }
        filledRowSize++;
    }
}

OmniStatus HashAggregationOperator::Close()
{
    delete[] sourceTypes;
    return OMNI_STATUS_NORMAL;
}

#ifdef ENABLE_HMPP
template <typename V, typename D>
void HashFuncVectImplHMPP(Vector *vector, const uint32_t start, const uint32_t rowCount, uint64_t *combinedHash)
{
    LogDebug("HMPP-HASHAGG-hash");
    HmppResult result = HmppHashUtil::ComputeHash(vector, reinterpret_cast<int64_t *>(combinedHash));
    if (result != HMPP_STS_NO_ERR) {
        throw OmniException("HMPP ERROR", "AGG HMPPS_ComputeHash failed for hmpp error");
    }
    return;
}

void HashVarcharVectFuncImplHMPP(Vector *vector, const uint32_t start, const uint32_t rowCount, uint64_t *combinedHash)
{
    int8_t *nullAddr = nullptr;
    int64_t *resultHash = new int64_t[rowCount]();
    int32_t positionOffset = vector->GetPositionOffset();
    uint8_t *varcharVectorAddr = static_cast<uint8_t *>((vector)->GetValues());
    int32_t *offest = static_cast<int32_t *>((vector)->GetValueOffsets()) + positionOffset;
    LogDebug("HMPP-HASHAGG-hashVarchar");
    if (vector->MayHaveNull()) {
        nullAddr = static_cast<int8_t *>(vector->GetValueNulls()) + positionOffset;
    }
    HmppResult result = HMPPS_Hash_varchar(varcharVectorAddr, offest, rowCount, nullAddr, resultHash);
    if (result != HMPP_STS_NO_ERR) {
        delete[] resultHash;
        throw OmniException("HMPP ERROR", "AGG HMPPS_Hash_decimal64 failed for hmpp error");
    }
    result = HMPPS_CombineHash(reinterpret_cast<int64_t *>(combinedHash), resultHash, rowCount,
        reinterpret_cast<int64_t *>(combinedHash));
    if (result != HMPP_STS_NO_ERR) {
        delete[] resultHash;
        throw OmniException("HMPP ERROR", "AGG HMPPS_Hash_decimal64 failed for hmpp error");
    }
    delete[] resultHash;
    return;
}

void HashDecimalVectFuncHMPP(Vector *vector, const uint32_t start, const uint32_t rowCount, uint64_t *combinedHash)
{
    int64_t *resultHash = new int64_t[rowCount]();
    int8_t *nullAddr = nullptr;
    int32_t positionOffset = vector->GetPositionOffset();
    HmppDecimal128 *decimalAddr = static_cast<HmppDecimal128 *>((vector)->GetValues()) + positionOffset;
    LogDebug("HMPP-HASHAGG-hashDecimal");
    if (vector->MayHaveNull()) {
        nullAddr = static_cast<int8_t *>(vector->GetValueNulls()) + positionOffset;
    }
    HmppResult result = HMPPS_Hash_decimal128(decimalAddr, rowCount, nullAddr, resultHash);
    if (result != HMPP_STS_NO_ERR) {
        delete[] resultHash;
        throw OmniException("HMPP ERROR", "AGG HMPPS_Hash_decimal64 failed for hmpp error");
    }
    result = HMPPS_CombineHash(reinterpret_cast<int64_t *>(combinedHash), resultHash, rowCount,
        reinterpret_cast<int64_t *>(combinedHash));
    if (result != HMPP_STS_NO_ERR) {
        delete[] resultHash;
        throw OmniException("HMPP ERROR", "AGG HMPPS_Hash_decimal64 failed for hmpp error");
    }
    delete[] resultHash;
    return;
}

template <typename V, typename D>
void HashFuncImplHMPP(Vector *vector, const uint32_t rowCount, const int32_t *rowIndexes, uint64_t *combinedHash)
{
    int32_t rowSize = vector->GetSize();
    int64_t *tempCombinedHash = new int64_t[rowSize]();
    LogDebug("HMPP-HASHAGG-hash");
    HmppResult result = HmppHashUtil::ComputeHash(vector, tempCombinedHash);
    if (result != HMPP_STS_NO_ERR) {
        delete[] tempCombinedHash;
        throw OmniException("HMPP ERROR", "AGG HMPPS_Hash failed for hmpp error");
    }
    for (uint32_t i = 0; i < rowCount; ++i) {
        combinedHash[i] = tempCombinedHash[rowIndexes[i]];
    }
    delete[] tempCombinedHash;
    return;
}

void HashVarcharFuncImplHMPP(Vector *vector, const uint32_t rowCount, const int32_t *rowIndexes, uint64_t *combinedHash)
{
    int32_t rowSize = vector->GetSize();
    int8_t *nullAddr = nullptr;
    int64_t *resultHash = new int64_t[rowSize]();
    int64_t *tempCombinedHash = new int64_t[rowSize]();
    uint8_t *varcharVectorAddr = static_cast<uint8_t *>((vector)->GetValues());
    int32_t positionOffset = vector->GetPositionOffset();
    int32_t *offest = static_cast<int32_t *>((vector)->GetValueOffsets()) + positionOffset;
    LogDebug("HMPP-HASHAGG-hashVarchar");
    if (vector->MayHaveNull()) {
        nullAddr = static_cast<int8_t *>(vector->GetValueNulls()) + positionOffset;
    }
    HmppResult result = HMPPS_Hash_varchar(varcharVectorAddr, offest, rowSize, nullAddr, resultHash);
    if (result != HMPP_STS_NO_ERR) {
        delete[] resultHash;
        delete[] tempCombinedHash;
        throw OmniException("HMPP ERROR", "AGG HMPPS_Hash_decimal64 failed for hmpp error");
    }
    result = HMPPS_CombineHash(tempCombinedHash, resultHash, rowSize, tempCombinedHash);
    if (result != HMPP_STS_NO_ERR) {
        delete[] resultHash;
        delete[] tempCombinedHash;
        throw OmniException("HMPP ERROR", "AGG HMPPS_Hash_decimal64 failed for hmpp error");
    }
    for (uint32_t i = 0; i < rowCount; ++i) {
        combinedHash[i] = tempCombinedHash[rowIndexes[i]];
    }
    delete[] resultHash;
    delete[] tempCombinedHash;
    return;
}

void HashDecimalFuncHMPP(Vector *vector, const uint32_t rowCount, const int32_t *rowIndexes, uint64_t *combinedHash)
{
    int32_t rowSize = vector->GetSize();
    int64_t *resultHash = new int64_t[rowSize]();
    int64_t *tempCombinedHash = new int64_t[rowSize]();
    int32_t positionOffset = vector->GetPositionOffset();
    int8_t *nullAddr = nullptr;
    HmppDecimal128 *decimalAddr = static_cast<HmppDecimal128 *>((vector)->GetValues()) + positionOffset;
    LogDebug("HMPP-HASHAGG-hashDecimal");
    if (vector->MayHaveNull()) {
        nullAddr = static_cast<int8_t *>(vector->GetValueNulls()) + positionOffset;
    }
    HmppResult result = HMPPS_Hash_decimal128(decimalAddr, rowSize, nullAddr, resultHash);
    if (result != HMPP_STS_NO_ERR) {
        delete[] resultHash;
        delete[] tempCombinedHash;
        throw OmniException("HMPP ERROR", "AGG HMPPS_Hash_decimal64 failed for hmpp error");
    }
    result = HMPPS_CombineHash(reinterpret_cast<int64_t *>(tempCombinedHash), resultHash, rowSize, tempCombinedHash);
    if (result != HMPP_STS_NO_ERR) {
        delete[] resultHash;
        delete[] tempCombinedHash;
        throw OmniException("HMPP ERROR", "AGG HMPPS_Hash_decimal64 failed for hmpp error");
    }
    for (uint32_t i = 0; i < rowCount; ++i) {
        combinedHash[i] = tempCombinedHash[rowIndexes[i]];
    }
    delete[] resultHash;
    delete[] tempCombinedHash;
    return;
}
#endif

template <typename V, typename D>
void HashFuncVectImpl(Vector *vector, const uint32_t start, const uint32_t rowCount, uint64_t *combinedHash)
{
    uint64_t hash;
    std::hash<D> hasher;
    for (uint32_t i = 0; i < rowCount; ++i) {
        auto idx = i + start;
        hash = !vector->IsValueNull(idx) * hasher(static_cast<V *>(vector)->GetValue(idx));
        combinedHash[i] = HashUtil::CombineHash(static_cast<int64_t>(combinedHash[i]), static_cast<int64_t>(hash));
    }
}

void HashVarcharVectFuncImpl(Vector *vector, const uint32_t start, const uint32_t rowCount, uint64_t *combinedHash)
{
    uint8_t *data = nullptr;
    for (uint32_t i = 0; i < rowCount; ++i) {
        auto idx = i + start;
        auto valLen = static_cast<VarcharVector *>(vector)->GetValue(idx, &data);
        auto val = HashUtil::HashValue(reinterpret_cast<int8_t *>(data), valLen);
        auto hash = HashUtil::CombineHash(static_cast<int64_t>(combinedHash[i]), !vector->IsValueNull(idx) * val);
        combinedHash[i] = static_cast<uint64_t>(hash);
    }
}

void HashDecimalVectFunc(Vector *vector, const uint32_t start, const uint32_t rowCount, uint64_t *combinedHash)
{
    for (uint32_t i = 0; i < rowCount; ++i) {
        auto idx = i + start;
        Decimal128 val = static_cast<Decimal128Vector *>(vector)->GetValue(idx);
        auto hash = HashUtil::CombineHash(static_cast<int64_t>(combinedHash[i]),
            !vector->IsValueNull(idx) * HashUtil::HashValue(val.LowBits(), val.HighBits()));
        combinedHash[i] = static_cast<uint64_t>(hash);
    }
}

template <typename V, typename D>
void HashFuncImpl(Vector *vector, const uint32_t rowCount, const int32_t *rowIndexes, uint64_t *combinedHash)
{
    uint64_t hash;
    std::hash<D> hasher;
    for (uint32_t i = 0; i < rowCount; ++i) {
        auto idx = rowIndexes[i];
        hash = !vector->IsValueNull(idx) * hasher(static_cast<V *>(vector)->GetValue(idx));
        combinedHash[i] = HashUtil::CombineHash(static_cast<int64_t>(combinedHash[i]), static_cast<int64_t>(hash));
    }
}

void HashVarcharFuncImpl(Vector *vector, const uint32_t rowCount, const int32_t *rowIndexes, uint64_t *combinedHash)
{
    uint8_t *data = nullptr;
    for (uint32_t i = 0; i < rowCount; ++i) {
        auto idx = rowIndexes[i];
        auto valLen = static_cast<VarcharVector *>(vector)->GetValue(idx, &data);
        auto val = HashUtil::HashValue(reinterpret_cast<int8_t *>(data), valLen);
        combinedHash[i] = HashUtil::CombineHash(static_cast<int64_t>(combinedHash[i]), !vector->IsValueNull(idx) * val);
    }
}

void HashDecimalFunc(Vector *vector, const uint32_t rowCount, const int32_t *rowIndexes, uint64_t *combinedHash)
{
    for (uint32_t i = 0; i < rowCount; ++i) {
        int32_t idx = rowIndexes[i];
        Decimal128 val = static_cast<Decimal128Vector *>(vector)->GetValue(idx);
        auto hash = HashUtil::CombineHash(combinedHash[i],
            !vector->IsValueNull(idx) * HashUtil::HashValue(val.LowBits(), val.HighBits()));
        combinedHash[i] = static_cast<uint64_t>(hash);
    }
}

template <typename V, typename D>
void HashFuncVectImplProxy(Vector *vector, const uint32_t start, const uint32_t rowCount, uint64_t *combinedHash)
{
#ifdef ENABLE_HMPP
    if (ConfigUtil::IsEnableHMPP()) {
        HashFuncVectImplHMPP<V, D>(vector, start, rowCount, combinedHash);
    } else {
        HashFuncVectImpl<V, D>(vector, start, rowCount, combinedHash);
    }
#else
    HashFuncVectImpl<V, D>(vector, start, rowCount, combinedHash);
#endif
}

void HashVarcharVectFuncImplProxy(Vector *vector, const uint32_t start, const uint32_t rowCount, uint64_t *combinedHash)
{
#ifdef ENABLE_HMPP
    if (ConfigUtil::IsEnableHMPP()) {
        HashVarcharVectFuncImplHMPP(vector, start, rowCount, combinedHash);
    } else {
        HashVarcharVectFuncImpl(vector, start, rowCount, combinedHash);
    }
#else
    HashVarcharVectFuncImpl(vector, start, rowCount, combinedHash);
#endif
}

void HashDecimalVectFuncProxy(Vector *vector, const uint32_t start, const uint32_t rowCount, uint64_t *combinedHash)
{
#ifdef ENABLE_HMPP
    if (ConfigUtil::IsEnableHMPP()) {
        HashDecimalVectFuncHMPP(vector, start, rowCount, combinedHash);
    } else {
        HashDecimalVectFunc(vector, start, rowCount, combinedHash);
    }
#else
    HashDecimalVectFunc(vector, start, rowCount, combinedHash);
#endif
}

template <typename V, typename D>
void HashFuncImplProxy(Vector *vector, const uint32_t rowCount, const int32_t *rowIndexes, uint64_t *combinedHash)
{
#ifdef ENABLE_HMPP
    if (ConfigUtil::IsEnableHMPP()) {
        HashFuncImplHMPP<V, D>(vector, rowCount, rowIndexes, combinedHash);
    } else {
        HashFuncImpl<V, D>(vector, rowCount, rowIndexes, combinedHash);
    }
#else
    HashFuncImpl<V, D>(vector, rowCount, rowIndexes, combinedHash);
#endif
}

void HashVarcharFuncImplProxy(Vector *vector, const uint32_t rowCount, const int32_t *rowIndexes,
    uint64_t *combinedHash)
{
#ifdef ENABLE_HMPP
    if (ConfigUtil::IsEnableHMPP()) {
        HashVarcharFuncImplHMPP(vector, rowCount, rowIndexes, combinedHash);
    } else {
        HashVarcharFuncImpl(vector, rowCount, rowIndexes, combinedHash);
    }
#else
    HashVarcharFuncImpl(vector, rowCount, rowIndexes, combinedHash);
#endif
}

void HashDecimalFuncProxy(Vector *vector, const uint32_t rowCount, const int32_t *rowIndexes, uint64_t *combinedHash)
{
#ifdef ENABLE_HMPP
    if (ConfigUtil::IsEnableHMPP()) {
        HashDecimalFuncHMPP(vector, rowCount, rowIndexes, combinedHash);
    } else {
        HashDecimalFunc(vector, rowCount, rowIndexes, combinedHash);
    }
#else
    HashDecimalFunc(vector, rowCount, rowIndexes, combinedHash);
#endif
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
        isSame = (valLen == slot.strLen) && (memcmp(data, slot.strVal, static_cast<size_t>(valLen)) == 0);
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
    auto len = sizeof(D);
    uint8_t *ptr = context->GetArena()->Allocate(len);
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
    int32_t valLen = (static_cast<VarcharVector *>(vector)->GetValue(offset, &tmp));
    uint8_t *data = context->GetArena()->Allocate(valLen);
    memcpy_s(data, valLen, tmp, static_cast<size_t>(valLen));
    state.strVal = data;
    state.strLen = valLen;
}

template <typename V>
void SetVectorImpl(VectorBatch *vecBatch, DataType &type, int32_t columnIndex, VectorAllocator *vecAllocator,
    int32_t rowCount)
{
    vecBatch->SetVector(columnIndex, new V(vecAllocator, rowCount));
}

void SetVarcharVector(VectorBatch *vecBatch, DataType &type, int32_t columnIndex, VectorAllocator *vecAllocator,
    int32_t rowCount)
{
    vecBatch->SetVector(columnIndex, new VarcharVector(vecAllocator,
        static_cast<uint32_t>(rowCount) * static_cast<const VarcharDataType &>(type).GetWidth(), rowCount));
}

void SetContainerVector(VectorBatch *vecBatch, DataType &type, int32_t columnIndex, VectorAllocator *vecAllocator,
    int32_t rowCount)
{
    auto doubleVector = new DoubleVector(vecAllocator, rowCount);
    auto longVector = new LongVector(vecAllocator, rowCount);
    std::vector<uintptr_t> vectorAddresses(op::AVG_VECTOR_COUNT);
    vectorAddresses[0] = reinterpret_cast<uintptr_t>(doubleVector);
    vectorAddresses[1] = reinterpret_cast<uintptr_t>(longVector);
    std::vector<DataTypePtr> dataTypes { DoubleType(), LongType() };
    auto containerVector =
        new ContainerVector(vecAllocator, rowCount, vectorAddresses, op::AVG_VECTOR_COUNT, dataTypes);
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
