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

#if defined(DEBUG_LEVEL_LOW) || defined(DEBUG_LEVEL_HIGH)
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
                DebugError("No such agg func type %d", aggFuncTypeContext.context[i]);
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
    int32_t idx = 0;
    for (auto &c : groupByCols) {
        sourceTypes[idx++] = static_cast<int32_t>(c.input.GetId());
    }
    for (auto &c : aggCols) {
        sourceTypes[idx++] = static_cast<int32_t>(c.input.GetId());
    }
    return OMNI_STATUS_NORMAL;
}

void HashAggregationOperator::PreLoop(VectorBatch *vecBatch) {}

void HashAggregationOperator::PostLoop(VectorBatch *vecBatch) const {}

void ALWAYS_INLINE GenerateCombinedHashes(Vector **vectors, uint32_t start, uint32_t rowCount, const int32_t *types,
    const int32_t colNum, const int32_t *colIdx, int64_t *combinedHashVal)
{
    for (int32_t i = 0; i < colNum; ++i) {
        uint32_t idx = colIdx[i];
        HashAggregationOperator::FUNCTIONS[static_cast<VecTypeId>(types[idx])].hashFunc(vectors[idx], start, rowCount,
            combinedHashVal);
    }
}

void *ALWAYS_INLINE DuplicateGroupByTuple(int32_t type, Vector *vector, uint32_t offset)
{
    return HashAggregationOperator::FUNCTIONS[static_cast<VecTypeId>(type)].duplicateKey(vector, offset);
}

SPECIALIZE(OMNIJIT_HASH_GROUPBY_INLOOP)
void HashAggregationOperator::InLoop(Vector **vectors, uint32_t rowCount, const int32_t *types, int32_t colNum,
    const int32_t *groupByColIdx, int32_t groupByColNum, const int32_t *aggColIdx, int32_t aggColNum,
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
                auto &columnState = groupedRows[hash];
                for (int32_t i = 0; i < aggColNum; ++i) {
                    int32_t idx = aggColIdx[i];
                    int32_t type = types[idx];
                    Vector *colPtr = vectors[idx];
                    GroupBySlot &state = columnState[groupByColNum + i];
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
    RowIterator &rowIterator, int32_t rowCount)
{
    RowIterator tempRowIterator = rowIterator;
    for (int colIndex = startIndex, groupByIndex = 0; colIndex < endIndex; ++colIndex, ++groupByIndex) {
        tempRowIterator = rowIterator;
        auto typeId = vecBatch->GetVector(colIndex)->GetType().GetId();
        HashAggregationOperator::FUNCTIONS[typeId].fillValue(*this, vecBatch, rowCount, tempRowIterator, colIndex);
    }
}

void HashAggregationOperator::FillAvgAgg(VectorBatch *vecBatch, int32_t aggIndex, int32_t colIndex, int32_t rowCount,
    RowIterator &rowIterator)
{
    // TODO support average value type which is decimal
    if (outputPartial) {
        ContainerVector *vector = static_cast<ContainerVector *>(vecBatch->GetVector(colIndex));
        for (int32_t rIdx = 0; rIdx < rowCount && rowIterator != groupedRows.end(); ++rIdx, rowIterator++) {
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
        for (int32_t rIdx = 0; rIdx < rowCount && rowIterator != groupedRows.end(); ++rIdx, rowIterator++) {
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
                auto typeId = vecBatch->GetVector(colIndex)->GetType().GetId();
                HashAggregationOperator::FUNCTIONS[typeId].fillValue(*this, vecBatch, rowCount, resultIterator,
                    colIndex);
                break;
            }
            case OMNI_AGGREGATION_TYPE_COUNT: {
                LongVector *vector = static_cast<LongVector *>(vecBatch->GetVector(colIndex));
                for (int32_t rIdx = 0; rIdx < rowCount && resultIterator != groupedRows.end();
                    ++rIdx, resultIterator++) {
                    vector->SetValue(rIdx, resultIterator->second[colIndex].count);
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

void SetVectors(VectorBatch *vectorBatch, const std::vector<VecType> &types, int32_t rowCount)
{
    for (int colIndex = 0; colIndex < vectorBatch->GetVectorCount(); ++colIndex) {
        VecType type = types[colIndex];
        HashAggregationOperator::FUNCTIONS[type.GetId()].setVector(vectorBatch, type, colIndex, nullptr, rowCount);
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
    for (auto item = groupedRows.begin(); item != groupedRows.end(); ++item) {
        for (int32_t idx = 0; idx < groupByColSize; ++idx) {
            HashAggregationOperator::FUNCTIONS[groupByCols[idx].input.GetId()].releaseMemory(item, idx,
                groupByCols[idx].input);
        }
    }
    return OMNI_STATUS_NORMAL;
}

OmniStatus HashAggregationOperator::CloseAgg()
{
    auto groupByColSize = groupByCols.size();
    auto aggColSize = aggCols.size();
    for (auto item = groupedRows.begin(); item != groupedRows.end(); ++item) {
        for (int32_t idx = 0; idx < aggColSize; ++idx) {
            if (aggregators[idx]->GetType() == OMNI_AGGREGATION_TYPE_COUNT) {
                continue;
            }
            if (aggregators[idx]->GetType() == OMNI_AGGREGATION_TYPE_AVG) {
                delete reinterpret_cast<double *>(item->second[groupByColSize + idx].avgVal);
                continue;
            }
            auto typeId = aggCols[idx].output.GetId();
            HashAggregationOperator::FUNCTIONS[typeId].releaseMemory(item, groupByColSize + idx, aggCols[idx].output);
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

template <typename V, typename D>
void ALWAYS_INLINE HashFuncImpl(Vector *vector, const uint32_t start, const uint32_t rowCount, int64_t *combinedHash)
{
    uint64_t hash;
    std::hash<D> hasher;
    auto v = static_cast<V *>(vector);
    for (int32_t i = 0; i < rowCount; ++i) {
        if (v->IsValueNull(i + start)) {
            continue;
        }
        hash = hasher(v->GetValue(i + start));
        combinedHash[i] = HashUtil::CombineHash(combinedHash[i], hash);
    }
}

void ALWAYS_INLINE HashVarcharFuncImpl(Vector *vector, const uint32_t start, const uint32_t rowCount,
    int64_t *combinedHash)
{
    std::hash<std::string> hashVarChar;
    uint8_t *data = nullptr;
    auto v = static_cast<VarcharVector *>(vector);
    for (int32_t i = 0; i < rowCount; ++i) {
        if (v->IsValueNull(i + start)) {
            continue;
        }
        int valLen = v->GetValue(i + start, &data);
        std::string val(reinterpret_cast<char *>(data), valLen);
        combinedHash[i] = HashUtil::CombineHash(combinedHash[i], hashVarChar(val));
    }
}

void ALWAYS_INLINE HashDecimalFunc(Vector *vector, const uint32_t start, const uint32_t rowCount, int64_t *combinedHash)
{
    auto v = static_cast<Decimal128Vector *>(vector);
    for (int32_t i = 0; i < rowCount; ++i) {
        if (v->IsValueNull(i + start)) {
            continue;
        }
        Decimal128 val = v->GetValue(i + start);
        combinedHash[i] = HashUtil::CombineHash(combinedHash[i], HashUtil::HashValue(val.LowBits(), val.HighBits()));
    }
}

void ALWAYS_INLINE HashDictionaryFunc(Vector *vector, const uint32_t start, const uint32_t rowCount,
    int64_t *combinedHash)
{
    auto dictVector = static_cast<DictionaryVector *>(vector);
    auto dictType = dictVector->GetDictionaryType();
    for (int32_t i = 0; i < rowCount; ++i) {
        int32_t rowIndex = i + start;
        auto dictionary = VectorHelper::GetDictionary(vector, rowIndex);
        if (dictionary->IsValueNull(rowIndex)) {
            continue;
        }
        // todo: need support more typs
        if (dictType.GetId() == OMNI_VEC_TYPE_INT) {
            std::hash<int32_t> hasher;
            combinedHash[i] = HashUtil::CombineHash(combinedHash[i],
                hasher(static_cast<IntVector *>(dictionary)->GetValue(rowIndex)));
        } else if (dictType.GetId() == OMNI_VEC_TYPE_LONG) {
            std::hash<int64_t> hasher;
            combinedHash[i] = HashUtil::CombineHash(combinedHash[i],
                hasher(static_cast<LongVector *>(dictionary)->GetValue(rowIndex)));
        } else if (dictType.GetId() == OMNI_VEC_TYPE_VARCHAR) {
            std::hash<std::string> hashVarChar;
            uint8_t *data = nullptr;
            auto v = static_cast<VarcharVector *>(dictionary);
            int valLen = v->GetValue(rowIndex, &data);
            std::string val(reinterpret_cast<char *>(data), valLen);
            combinedHash[i] = HashUtil::CombineHash(combinedHash[i], hashVarChar(val));
        }
    }
}
template <typename V, typename D> void *ALWAYS_INLINE DuplicateKeyValueImpl(Vector *vector, const uint32_t offset)
{
    return std::make_unique<D>(static_cast<V *>(vector)->GetValue(offset)).release();
}

void *ALWAYS_INLINE DuplicateVarcharKeyValue(Vector *vector, const uint32_t offset)
{
    uint8_t *data = nullptr;
    int valLen = (static_cast<VarcharVector *>(vector)->GetValue(offset, &data));
    return reinterpret_cast<void *>(new std::string(reinterpret_cast<char *>(data), 0, valLen));
}

void *ALWAYS_INLINE DuplicateDictionaryKeyValue(Vector *vector, const uint32_t offset)
{
    auto dictVec = static_cast<DictionaryVector *>(vector);
    auto dictType = dictVec->GetDictionaryType().GetId();
    auto v = VectorHelper::GetDictionary(vector, (int32_t &)(offset));

    // todo: need support more typs
    if (dictType == OMNI_VEC_TYPE_INT) {
        return std::make_unique<int32_t>(static_cast<IntVector *>(v)->GetValue(offset)).release();
    } else if (dictType == OMNI_VEC_TYPE_LONG) {
        return std::make_unique<int64_t>(static_cast<LongVector *>(v)->GetValue(offset)).release();
    } else if (dictType == OMNI_VEC_TYPE_VARCHAR) {
        uint8_t *data = nullptr;
        int valLen = (static_cast<VarcharVector *>(v)->GetValue(offset, &data));
        return reinterpret_cast<void *>(new std::string(reinterpret_cast<char *>(data), 0, valLen));
    }
    return nullptr;
}

template <typename V>
void SetVectorImpl(VectorBatch *vectorBatch, VecType &type, int32_t columnIndex, VectorAllocator *allocator,
    int32_t rowCount)
{
    vectorBatch->SetVector(columnIndex, new V(allocator, rowCount));
}

void SetVarcharVector(VectorBatch *vectorBatch, VecType &type, int32_t columnIndex, VectorAllocator *allocator,
    int32_t rowCount)
{
    vectorBatch->SetVector(columnIndex,
        new VarcharVector(allocator, rowCount * ((VarcharVecType &)type).GetWidth(), rowCount));
}

void SetContainerVector(VectorBatch *vectorBatch, VecType &type, int32_t columnIndex, VectorAllocator *allocator,
    int32_t rowCount)
{
    DoubleVector *doubleVector = new DoubleVector(nullptr, rowCount);
    LongVector *longVector = new LongVector(nullptr, rowCount);
    Vector **vectorAddresses = new Vector *[2];
    vectorAddresses[0] = doubleVector;
    vectorAddresses[1] = longVector;
    VecType *vecTypes = new VecType[2];
    vecTypes[0] = DoubleVecType::Instance();
    vecTypes[1] = LongVecType::Instance();
    ContainerVector *containerVector =
        new ContainerVector(nullptr, rowCount, vectorAddresses, op::AVG_VECTOR_COUNT, vecTypes);
    vectorBatch->SetVector(columnIndex, containerVector);
}

template <typename V, typename D>
void FillValueImpl(HashAggregationOperator &hashOperator, VectorBatch *vecBatch, int32_t rowCount,
    RowIterator &tempRowIterator, int colIndex)
{
    auto vector = static_cast<V *>(vecBatch->GetVector(colIndex));
    for (int rowIndex = 0; rowIndex < rowCount && tempRowIterator != hashOperator.groupedRows.end();
        ++rowIndex, ++tempRowIterator) {
        if (tempRowIterator->second[colIndex].val == nullptr) {
            vector->SetValueNull(rowIndex);
            continue;
        }
        vector->SetValue(rowIndex, *static_cast<D *>(tempRowIterator->second[colIndex].val));
    }
}

void FillVarcharValue(HashAggregationOperator &hashOperator, VectorBatch *vecBatch, int32_t rowCount,
    RowIterator &tempRowIterator, int colIndex)
{
    auto vector = static_cast<VarcharVector *>(vecBatch->GetVector(colIndex));
    for (int rowIndex = 0; rowIndex < rowCount && tempRowIterator != hashOperator.groupedRows.end();
        ++rowIndex, ++tempRowIterator) {
        if (tempRowIterator->second[colIndex].val == nullptr) {
            vector->SetValueNull(rowIndex);
            continue;
        }
        vector->SetValue(rowIndex,
            reinterpret_cast<const uint8_t *>((*((std::string *)tempRowIterator->second[colIndex].val)).c_str()),
            (*((std::string *)tempRowIterator->second[colIndex].val)).size());
    }
}

template <typename T> void ReleaseMemoryImpl(RowIterator &rowIterator, int32_t columnIndex, VecType &type)
{
    delete reinterpret_cast<T *>(rowIterator->second[columnIndex].val);
}
} // end of namespace op
} // end of namespace omniruntime
