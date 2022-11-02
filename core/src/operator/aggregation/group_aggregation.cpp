/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Description: Hash Aggregation Source File
 */
#include "group_aggregation.h"
#include <cmath>
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
    {OMNI_INT, HashFuncImpl<IntVector, int32_t>, HashFuncVectImplProxy<IntVector, int32_t>,
     IsSameNodeFuncImpl<IntVector, int32_t>, DuplicateKeyValueImpl<IntVector, int32_t>,
     SetVectorImpl<IntVector>, FillValueImpl<IntVector, int32_t>
    },
    {OMNI_LONG, HashFuncImpl<LongVector, int64_t>, HashFuncVectImplProxy<LongVector, int64_t>,
     IsSameNodeFuncImpl<LongVector, int64_t>, DuplicateKeyValueImpl<LongVector, int64_t>,
     SetVectorImpl<LongVector>, FillValueImpl<LongVector, int64_t>
    },
    {
        OMNI_DOUBLE, HashFuncImpl<DoubleVector, double>, HashFuncVectImplProxy<DoubleVector, double>,
        IsSameNodeFuncImpl<DoubleVector, double>, DuplicateKeyValueImpl<DoubleVector, double>,
        SetVectorImpl<DoubleVector>, FillValueImpl<DoubleVector, double>
    },
    {
        OMNI_BOOLEAN, HashFuncImpl<BooleanVector, bool>, HashFuncVectImplProxy<BooleanVector, bool>,
        IsSameNodeFuncImpl<BooleanVector, bool>, DuplicateKeyValueImpl<BooleanVector, bool>,
        SetVectorImpl<BooleanVector>, FillValueImpl<BooleanVector, bool>
    },
    {OMNI_SHORT, HashFuncImpl<ShortVector, int16_t>, HashFuncVectImplProxy<ShortVector, int16_t>,
     IsSameNodeFuncImpl<ShortVector, int16_t>, DuplicateKeyValueImpl<ShortVector, int16_t>,
     SetVectorImpl<ShortVector>, FillValueImpl<ShortVector, int16_t>},
    {OMNI_DECIMAL64, HashFuncImpl<LongVector, int64_t>, HashFuncVectImplProxy<LongVector, int64_t>,
     IsSameNodeFuncImpl<LongVector, int64_t>, DuplicateKeyValueImpl<LongVector, int64_t>,
     SetVectorImpl<LongVector>, FillValueImpl<LongVector, int64_t>
    },
    {OMNI_DECIMAL128, HashDecimalFunc, HashDecimalVectFuncProxy,
     IsSameNodeFuncImpl<Decimal128Vector, Decimal128>, DuplicateKeyValueImpl<Decimal128Vector, Decimal128>,
     SetVectorImpl<Decimal128Vector>, FillValueImpl<Decimal128Vector, Decimal128>
    },
    {OMNI_DATE32, HashFuncImpl<IntVector, int32_t>, HashFuncVectImplProxy<IntVector, int32_t>,
     IsSameNodeFuncImpl<IntVector, int32_t>, DuplicateKeyValueImpl<IntVector, int32_t>,
     SetVectorImpl<IntVector>, FillValueImpl<IntVector, int32_t>
    },
    {OMNI_DATE64, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_TIME32, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_TIME64, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_TIMESTAMP, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_INTERVAL_MONTHS, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_INTERVAL_DAY_TIME, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_VARCHAR, HashVarcharFuncImpl, HashVarcharVectFuncImplProxy, IsSameNodeFuncVarcharImpl,
     DuplicateVarcharKeyValue, SetVarcharVector, FillVarcharValue },
    {OMNI_CHAR, HashVarcharFuncImpl, HashVarcharVectFuncImplProxy, IsSameNodeFuncVarcharImpl,
     DuplicateVarcharKeyValue, SetVarcharVector, FillVarcharValue },
    {OMNI_CONTAINER, nullptr, nullptr, nullptr, nullptr, SetContainerVector, nullptr},
};

OmniStatus HashAggregationOperatorFactory::Init()
{
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
    ChooseGroupByType();
    return CreateAggregatorFactories(aggregatorFactories, aggFuncTypesVector, GetMaskColumns());
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
        auto aggregator = aggregatorFactories[i]->CreateAggregator(*inputTypes, *outputTypes, aggInputColIdxVec,
            inputRaws[i], outputPartials[i], isOverflowAsNull);
        if (aggregator == nullptr) {
            throw OmniException("create group aggregation operator", "return nullptr when create aggregator");
        }
        aggs.push_back(std::move(aggregator));
    }

    auto groupByOperator = new HashAggregationOperator(groupByIndex, aggsInputCols, aggInputColsSize, aggInputTypes,
        aggOutputTypes, std::move(aggs), inputRaws, outputPartials);
    groupByOperator->SetGroupByColumnsHandleType(handleType);

    groupByOperator->Init();
    return groupByOperator;
}

void HashAggregationOperatorFactory::ChooseGroupByType()
{
    // Currently, only the serialization method is used for all column types that need to be grouped by.
    // The method can be continuously evolved based on different types.
    handleType = GroupByFieldHandleType::serialize;
}

void HashAggregationOperator::SetGroupByColumnsHandleType(GroupByFieldHandleType t)
{
    groupByColumnsHandleType = t;
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

    if (groupByColumnsHandleType == GroupByFieldHandleType::serialize) {
        serialize = std::make_unique<decltype(serialize)::element_type>();
    } else {
        // only the serialization method is used now
        return OMNI_STATUS_ERROR;
    }
    return OMNI_STATUS_NORMAL;
}

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

void HashAggregationOperator::InLoop(VectorBatch *vecBatch, uint32_t rowCount, const int32_t *groupByColIdx,
    int32_t groupByColNum, int32_t aggNum)
{
    VectorBatch groupVectors(groupByColNum);
    for (size_t i = 0; i < groupByColNum; ++i) {
        groupVectors.SetVector(i, vecBatch->GetVector(groupByColIdx[i]));
    }

    if (groupByColumnsHandleType == GroupByFieldHandleType::serialize) {
        Emplace(serialize, vecBatch, groupVectors);
    } else {
        // only serialize method are used now
        LogError("can not support groupByColumnsHandleType : %d.", groupByColumnsHandleType);
        throw OmniException("no t supported operation", "groupByColumnsHandleType error");
        return;
    }
}

int32_t HashAggregationOperator::AddInput(VectorBatch *vecBatch)
{
    auto groupColNum = this->groupByCols.size();
    auto groupByColIdx = std::make_unique<int32_t[]>(groupColNum);
    auto aggNum = this->aggregators.size();

    for (size_t i = 0; i < groupColNum; ++i) {
        groupByColIdx[i] = this->groupByCols[i].idx;
    }

    uint32_t rowCount = static_cast<uint32_t>(vecBatch->GetRowCount());
    this->InLoop(vecBatch, rowCount, groupByColIdx.get(), groupColNum, aggNum);

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
int32_t HashAggregationOperator::GetRowSizeAndOutputTypes(std::vector<DataTypePtr> &types, uint32_t columnCount)
{
    uint32_t rowSize = 0;
    for (auto &i : groupByCols) {
        types.push_back(i.input);
        rowSize += OperatorUtil::GetTypeSize(i.input);
    }
    for (auto &singleAgg : aggOutputTypes) {
        for (int32_t i = 0; i < singleAgg.GetSize(); i++) {
            types.push_back(singleAgg.GetType(i));
            rowSize += OperatorUtil::GetTypeSize(singleAgg.GetType(i));
        }
    }
    return rowSize;
}


void HashAggregationOperator::SetVectors(VectorAllocator *vecAllocator, VectorBatch *vectorBatch,
    const std::vector<DataTypePtr> &types, int32_t rowCount)
{
    for (int colIndex = 0; colIndex < vectorBatch->GetVectorCount(); ++colIndex) {
        DataTypePtr type = types[colIndex];
        GROUP_AGG_FUNCTIONS[type->GetId()].setVector(vectorBatch, *type, colIndex, vecAllocator, rowCount);
    }
}

int32_t HashAggregationOperator::GetOutput(std::vector<VectorBatch *> &result)
{
    int32_t expectedBatchSize = 0;
    if (groupByColumnsHandleType == GroupByFieldHandleType::serialize) {
        expectedBatchSize = Output(serialize, result);
    } else {
        SetStatus(OMNI_STATUS_ERROR);
        LogError("other groupby field handle type %d not implement now ", groupByColumnsHandleType);
        throw std::out_of_range("other groupby field handle type not implement");
    }

    SetStatus(OMNI_STATUS_FINISHED);
    return expectedBatchSize;
}
OmniStatus HashAggregationOperator::Close()
{
    delete[] sourceTypes;
    return OMNI_STATUS_NORMAL;
}
#ifdef ENABLE_HMPP
void HashFuncVectImplHMPP(Vector *vector, const uint32_t start, const uint32_t rowCount, uint64_t *combinedHash)
{
    LogDebug("HMPP-HASHAGG-hash");
    HmppResult result = HmppHashUtil::ComputeHash(vector, reinterpret_cast<int64_t *>(combinedHash), start, rowCount);
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
        nullAddr = static_cast<int8_t *>(vector->GetValueNulls()) + positionOffset + start;
    }
    HmppResult result = HMPPS_Hash_varchar(varcharVectorAddr, offest + start, rowCount, nullAddr, resultHash);
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
        nullAddr = static_cast<int8_t *>(vector->GetValueNulls()) + positionOffset + start;
    }
    HmppResult result = HMPPS_Hash_decimal128(decimalAddr + start, rowCount, nullAddr, resultHash);
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
        HashFuncVectImplHMPP(vector, start, rowCount, combinedHash);
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
    vecBatch->SetVector(columnIndex, new VarcharVector(vecAllocator, rowCount));
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

template <typename Serialize>
void HashAggregationOperator::Emplace(Serialize &emplaceKey, VectorBatch *vecBatch, VectorBatch &groupVectors)
{
    auto rowCount = vecBatch->GetRowCount();
    std::vector<AggregateStatePtr> rowStates(rowCount);
    auto aggNum = aggregators.size();
    for (size_t i = 0; i < rowCount; ++i) {
        AggregateStatePtr aggPointer = nullptr;
        auto ret = emplaceKey->InsertValueToHashmap(i, &groupVectors, *executionContext);
        if (ret.IsInsert()) {
            aggPointer = reinterpret_cast<AggregateStatePtr>(
                executionContext->GetArena()->Allocate(aggNum * sizeof(AggregateState)));
            for (int i = 0; i < aggNum; i++) {
                auto *p = new (aggPointer + sizeof(AggregateState) * i) AggregateState;
                memset(p,  0, sizeof(AggregateState));
            }
            ret.SetValue(aggPointer);
        } else {
            aggPointer = (ret.GetValue());
        }
        rowStates[i] = aggPointer;
    }
    AggregateAllState(vecBatch, aggNum, rowStates);
}

void HashAggregationOperator::AggregateAllState(VectorBatch *vecBatch, uint32_t aggNum,
    std::vector<AggregateStatePtr> &rowStates)
{
    auto rowCount = rowStates.size();
    for (uint32_t i = 0; i < aggNum; ++i) {
        auto &curAggregator = aggregators.at(i);
        for (uint32_t j = 0; j < rowCount; ++j) {
            auto *oneRowStates = rowStates.at(j);
            auto *oneState = reinterpret_cast<AggregateState *>(oneRowStates) + i;
            auto &s = *oneState;
            curAggregator->ProcessGroup(s, vecBatch, j);
        }
    }
}
template <typename Deserialize>
void HashAggregationOperator::FillOutputResultVectors(Deserialize &deserializeHashmap, uint32_t groupByColSize,
    uint32_t aggOutputColSize, std::vector<VectorBatch *> &result)
{
    auto &hashmap = deserializeHashmap->hashmap;
    int32_t totalRowCount = hashmap.GetElementsSize();

    uint32_t colCount = groupByColSize + aggOutputColSize;
    std::vector<DataTypePtr> types;
    int32_t rowByteSize = GetRowSizeAndOutputTypes(types, colCount);
    auto rowsPerBatch = OperatorUtil::GetMaxRowCount(rowByteSize);
    auto expectedBatchSize = OperatorUtil::GetVecBatchCount(totalRowCount, rowsPerBatch);
    auto leftRowCount = totalRowCount;

    // create all output vector batches
    for (uint32_t batchId = 0; batchId < expectedBatchSize; ++batchId) {
        auto rowCount = std::min(rowsPerBatch, leftRowCount);
        auto vecBatch = new VectorBatch(colCount, rowCount);
        SetVectors(this->vecAllocator, vecBatch, types, rowCount);
        result.push_back(vecBatch);
        leftRowCount -= rowCount;
    }
}

template <typename Deserialize>
void HashAggregationOperator::TraverseHashmapToGetResults(Deserialize &deserializeHashmap, VectorBatch &groupVectors,
    VectorBatch &aggregateVectors, std::vector<VectorBatch *> &result)
{
    auto curBatchId = 0;
    auto &hashmap = deserializeHashmap->hashmap;
    int lambdaRowIndex = 0;
    int groupByColSize = groupVectors.GetVectorCount();
    int aggOutputColSize = aggregateVectors.GetVectorCount();
    auto rowsPerBatch = result.at(0)->GetRowCount();
    auto aggNum = this->aggregators.size();

    hashmap.ForEachKV([&](const auto &key, auto &mapped) mutable {
        if (lambdaRowIndex >= rowsPerBatch) {
            lambdaRowIndex %= rowsPerBatch;
            ++curBatchId;
            for (int i = 0; i < groupByColSize; ++i) {
                groupVectors.SetVector(i, result.at(curBatchId)->GetVector(i));
            }
            for (int i = 0; i < aggOutputColSize; ++i) {
                aggregateVectors.SetVector(i, result.at(curBatchId)->GetVector(i + groupByColSize));
            }
        }
        std::remove_reference<decltype(deserializeHashmap)>::type::element_type::ParseKeyToCols(key, groupVectors,
            lambdaRowIndex);
        int aggOutputStartIndex = 0;
        for (uint32_t aggIndex = 0; aggIndex < aggNum; ++aggIndex) {
            auto *states = reinterpret_cast<AggregateStatePtr>(mapped);
            auto &state = *(reinterpret_cast<AggregateState *>(states + sizeof(AggregateState) * aggIndex));
            auto oneAggOutputCols = aggOutputTypes[aggIndex].GetSize();
            std::vector<Vector *> adaptAggVectors;
            adaptAggVectors.resize(oneAggOutputCols);
            for (auto j = 0; j < oneAggOutputCols; j++) {
                adaptAggVectors[j] = (aggregateVectors.GetVector(aggOutputStartIndex + j));
            }
            try {
                aggregators[aggIndex]->ExtractValues(state, adaptAggVectors, lambdaRowIndex);
            } catch (const OmniException &oneException) {
                // release VectorBatch when aggregator.ExtractValues throw exception
                // when spark hash agg sum/avg decimal overflow, it will throw exception when
                // OverflowConfigId==OVERFLOW_CONFIG_EXCEPTION
                for (auto vecBatch : result) {
                    VectorHelper::FreeVecBatch(vecBatch);
                }
                throw oneException;
            }
            aggOutputStartIndex += oneAggOutputCols;
        }
        ++lambdaRowIndex;
    });
}
template <typename Deserialize>
int32_t HashAggregationOperator::Output(Deserialize &deserializeHashmap, std::vector<VectorBatch *> &result)
{
    uint32_t groupByColSize = groupByCols.size();
    uint32_t aggOutputColSize = 0;
    for (auto &oneAggOutputTypes : aggOutputTypes) {
        aggOutputColSize += oneAggOutputTypes.GetSize();
    }
    auto &hashmap = deserializeHashmap->hashmap;
    int32_t totalRowCount = hashmap.GetElementsSize();
    if (totalRowCount == 0) {
        return 0;
    }
    FillOutputResultVectors(deserializeHashmap, groupByColSize, aggOutputColSize, result);

    VectorBatch aggregateVectors(aggOutputColSize);
    VectorBatch groupVectors(groupByColSize);
    std::vector<size_t> colIds(groupByColSize, 0);
    int curBatchId = 0;
    for (uint32_t i = 0; i < groupByColSize; ++i) {
        colIds[i] = groupByCols.at(i).idx;
        groupVectors.SetVector(i, result.at(curBatchId)->GetVector(i));
    }
    for (uint32_t i = 0; i < aggOutputColSize; ++i) {
        aggregateVectors.SetVector(i, result.at(curBatchId)->GetVector(i + groupByColSize));
    }
    TraverseHashmapToGetResults(deserializeHashmap, groupVectors, aggregateVectors, result);
    return result.size();
}
} // end of namespace op
} // end of namespace omniruntime
