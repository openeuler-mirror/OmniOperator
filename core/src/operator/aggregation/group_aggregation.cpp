/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2024. All rights reserved.
 * Description: Hash Aggregation Source File
 */
#include "group_aggregation.h"
#include <cmath>
#include "vector/vector_helper.h"
#include "operator/status.h"
#include "operator/util/operator_util.h"
#include "util/type_util.h"
#include "util/debug.h"
#include "operator/aggregation/aggregator/aggregator_factory.h"

#if defined(DEBUG_OPERATOR) && defined(TRACE)
#include <sstream>
#endif
namespace omniruntime {
namespace op {
using namespace omniruntime::type;

static constexpr int32_t UNSPILL_ROW_COUNT_ONE_BATCH = 128;

template void HashFuncImpl<Vector<bool>, bool>(BaseVector *vector, const uint32_t rowCount, const int32_t *rowIndexes,
    uint64_t *combinedHash);

template void HashFuncVectImpl<Vector<bool>, bool>(BaseVector *vector, const uint32_t start, const uint32_t rowCount,
    uint64_t *combinedHash);

template void DuplicateKeyValueImpl<Vector<bool>, bool>(AggregateState &state, BaseVector *vector,
    const uint32_t offset, ExecutionContext *context);

template void IsSameNodeFuncImpl<Vector<bool>, bool>(BaseVector *vector, const uint32_t offset,
    const AggregateState &slot, bool &isSame);

static constexpr FunctionByDataType GROUP_AGG_FUNCTIONS[DATA_TYPE_MAX_COUNT] = {
    {OMNI_NONE, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr},
    {OMNI_INT, HashFuncImpl<Vector<int32_t>, int32_t>, HashFuncVectImplProxy<Vector<int32_t>, int32_t>,
     IsSameNodeFuncImpl<Vector<int32_t>, int32_t>, DuplicateKeyValueImpl<Vector<int32_t>, int32_t>,
     SetVectorImpl<Vector<int32_t>>, FillValueImpl<Vector<int32_t>, int32_t>
    },
    {OMNI_LONG, HashFuncImpl<Vector<int64_t>, int64_t>, HashFuncVectImplProxy<Vector<int64_t>, int64_t>,
     IsSameNodeFuncImpl<Vector<int64_t>, int64_t>, DuplicateKeyValueImpl<Vector<int64_t>, int64_t>,
     SetVectorImpl<Vector<int64_t>>, FillValueImpl<Vector<int64_t>, int64_t>
    },
    {
        OMNI_DOUBLE, HashFuncImpl<Vector<double>, double>, HashFuncVectImplProxy<Vector<double>, double>,
        IsSameNodeFuncImpl<Vector<double>, double>, DuplicateKeyValueImpl<Vector<double>, double>,
        SetVectorImpl<Vector<double>>, FillValueImpl<Vector<double>, double>
    },
    {
        OMNI_BOOLEAN, HashFuncImpl<Vector<bool>, bool>, HashFuncVectImplProxy<Vector<bool>, bool>,
        IsSameNodeFuncImpl<Vector<bool>, bool>, DuplicateKeyValueImpl<Vector<bool>, bool>,
        SetVectorImpl<Vector<bool>>, FillValueImpl<Vector<bool>, bool>
    },
    {OMNI_SHORT, HashFuncImpl<Vector<short>, int16_t>, HashFuncVectImplProxy<Vector<short>, int16_t>,
     IsSameNodeFuncImpl<Vector<short>, int16_t>, DuplicateKeyValueImpl<Vector<short>, int16_t>,
     SetVectorImpl<Vector<short>>, FillValueImpl<Vector<short>, int16_t>},
    {OMNI_DECIMAL64, HashFuncImpl<Vector<int64_t>, int64_t>, HashFuncVectImplProxy<Vector<int64_t>, int64_t>,
     IsSameNodeFuncImpl<Vector<int64_t>, int64_t>, DuplicateKeyValueImpl<Vector<int64_t>, int64_t>,
     SetVectorImpl<Vector<int64_t>>, FillValueImpl<Vector<int64_t>, int64_t>
    },
    {OMNI_DECIMAL128, HashDecimalFunc, HashDecimalVectFuncProxy,
     IsSameNodeFuncImpl<Vector<Decimal128>, Decimal128>, DuplicateKeyValueImpl<Vector<Decimal128>, Decimal128>,
     SetVectorImpl<Vector<Decimal128>>, FillValueImpl<Vector<Decimal128>, Decimal128>
    },
    {OMNI_DATE32, HashFuncImpl<Vector<int32_t>, int32_t>, HashFuncVectImplProxy<Vector<int32_t>, int32_t>,
     IsSameNodeFuncImpl<Vector<int32_t>, int32_t>, DuplicateKeyValueImpl<Vector<int32_t>, int32_t>,
     SetVectorImpl<Vector<int32_t>>, FillValueImpl<Vector<int32_t>, int32_t>
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
    for (auto groupByCol : groupByColsVector) {
        groupByColIndices.push_back(groupByCol);
    }
    for (auto aggInputColsVector : aggsInputColsVector) {
        std::vector<int32_t> aggInputCols;
        for (uint32_t i = 0; i < aggInputColsVector.size(); ++i) {
            aggInputCols.push_back(aggInputColsVector[i]);
        }
        aggsInputCols.push_back(aggInputCols);
    }
    ChooseGroupByType();
    auto ret = CreateAggregatorFactories(aggregatorFactories, aggFuncTypesVector, GetMaskColumns());

    return ret;
}

OmniStatus HashAggregationOperatorFactory::Close()
{
    return OMNI_STATUS_NORMAL;
}

Operator *HashAggregationOperatorFactory::CreateOperator()
{
    std::vector<ColumnIndex> groupByIndex(groupByColIndices.size(), ColumnIndex());
    std::vector<std::unique_ptr<Aggregator>> aggs;

    for (uint32_t i = 0; i < this->groupByColIndices.size(); ++i) {
        auto &type = this->groupByTypes.GetType(i);
        groupByIndex[i] = { this->groupByColIndices[i], type, type };
    }

    // refresh inputDateTypes and inputColumnar index for OMNI_AGGREGATION_TYPE_COUNT_ALL type aggregator
    uint32_t aggInputColsSize = 0;
    uint32_t aggCountAllSkipCnt = 0;
    uint32_t aggregateType = OMNI_AGGREGATION_TYPE_INVALID;
    for (uint32_t i = 0; i < this->aggregatorFactories.size(); i++) {
        std::vector<int32_t> aggInputColIdxVec;
        std::vector<DataTypePtr> inputDataTypesPtr;
        aggregateType = aggFuncTypesVector[i];

        // for COUNT_ALL aggregator no input(key and columnar index)
        // use aggCountAllSkipCnt to align with aggsInputCols and aggregatorFactories index not same
        if (aggregateType == OMNI_AGGREGATION_TYPE_COUNT_ALL && inputRaws[i]) {
            inputDataTypesPtr.push_back(NoneType());
            aggInputColIdxVec.push_back(-1);
            aggCountAllSkipCnt++;
        } else {
            auto aggInputIdx = i - aggCountAllSkipCnt;
            for (uint32_t j = 0; j < this->aggsInputCols[aggInputIdx].size(); j++) {
                inputDataTypesPtr.push_back(aggInputTypes[aggInputIdx].GetType(j));
                aggInputColIdxVec.push_back(aggsInputCols[aggInputIdx][j]);
                aggInputColsSize++;
            }
        }

        auto inputTypes = DataTypes(inputDataTypesPtr).Instance();
        auto outputTypes = aggOutputTypes[i].Instance();
        auto aggregator = aggregatorFactories[i]->CreateAggregator(*inputTypes, *outputTypes, aggInputColIdxVec,
            inputRaws[i], outputPartials[i], isOverflowAsNull);
        if (UNLIKELY(aggregator == nullptr)) {
            throw OmniException("OPERATOR_RUNTIME_ERROR", "Unable to create aggregator " + std::to_string(i) + " / " +
                std::to_string(this->aggregatorFactories.size()));
        }
        aggs.push_back(std::move(aggregator));
    }

    auto groupByOperator = new HashAggregationOperator(groupByIndex, aggsInputCols, aggInputColsSize, aggInputTypes,
        aggOutputTypes, std::move(aggs), inputRaws, outputPartials, hasAggFilters, operatorConfig);
    groupByOperator->SetGroupByColumnsHandleType(handleType);

    groupByOperator->Init();
    return groupByOperator;
}

void HashAggregationOperatorFactory::ChooseGroupByType()
{
    // Currently, only the serialization method is used for all column types that need to be grouped by.
    // The method can be continuously evolved based on different types.
    handleType = HandleType::serialize;
}

void HashAggregationOperator::SetGroupByColumnsHandleType(HandleType t)
{
    groupByColumnsHandleType = t;
}

OmniStatus HashAggregationOperator::Init()
{
    if (isInited) {
        return OMNI_STATUS_NORMAL;
    }
    isInited = true;
    SetOperatorName(metricsNameHashAgg);
    // put at beginning so that we do not allocate memory if there is error
    if (groupByColumnsHandleType == HandleType::serialize) {
        serialize = std::make_unique<decltype(serialize)::element_type>();
        serialize->InitSize(groupByCols.size());
    } else {
        // only the serialization method is used now
        std::string omniExceptionInfo =
            "In function HashAggregationOperator::Init, can not support groupByColumnsHandleType " +
            std::to_string(static_cast<int>(groupByColumnsHandleType));
        throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR", omniExceptionInfo);
    }

    auto colSize = groupByCols.size() + aggInputColsSize;
    sourceTypes = new int32_t[colSize];
    // group by source types
    for (const auto &c : groupByCols) {
        sourceTypes[c.idx] = static_cast<int32_t>(c.input->GetId());
        memoryChunkSize += OperatorUtil::GetTypeSize(c.input);
    }

    // agg source types
    for (size_t i = 0; i < aggInputCols.size(); ++i) {
        for (size_t j = 0; j < aggInputCols[i].size(); ++j) {
            sourceTypes[aggInputCols[i][j]] = aggInputTypes[i].GetType(j)->GetId();
        }
    }

    for (auto &aggregator : aggregators) {
        const std::vector<DataTypePtr> &aggTypes = aggregator->GetOutputTypes().Get();
        for (auto dataType : aggTypes) {
            memoryChunkSize += OperatorUtil::GetTypeSize(dataType);
        }
    }
    memoryChunkSize += static_cast<int64_t>(aggregators.size() * sizeof(AggregateState));
    executionContext->GetArena()->SetMinChunkSize(memoryChunkSize * 8);

    int32_t rowByteSize = InitMaxRowCountAndOutputTypes();
    rowsPerBatch = OperatorUtil::GetMaxRowCount(rowByteSize);
    return OMNI_STATUS_NORMAL;
}

int32_t HashAggregationOperator::AddInput(VectorBatch *vecBatch)
{
    auto rowCount = vecBatch->GetRowCount();
    if (rowCount <= 0) {
        VectorHelper::FreeVecBatch(vecBatch);
        ResetInputVecBatch();
        return 0;
    }

    UpdateAddInputInfo(rowCount);
    auto groupColNum = static_cast<int32_t>(this->groupByCols.size());
    serialize->ResetSerializer();
    BaseVector *groupVectors[groupColNum];
    for (int32_t i = 0; i < groupColNum; ++i) {
        auto &groupByCol = this->groupByCols[i];
        auto curVector = vecBatch->Get(groupByCol.idx);
        auto omniId = groupByCol.input->GetId();

        if (curVector->GetEncoding() == Encoding::OMNI_DICTIONARY) {
            serialize->PushBackSerializer(dicVectorSerializerCenter[omniId]);
        } else {
            serialize->PushBackSerializer(vectorSerializerCenter[omniId]);
        }
        serialize->PushBackDeSerializer(vectorDeSerializerCenter[omniId]);
        groupVectors[i] = curVector;
    }

    if (LIKELY(groupByColumnsHandleType == HandleType::serialize)) {
        Emplace(serialize, vecBatch, groupVectors, groupColNum);
    } else {
        // only serialize method are used now
        VectorHelper::FreeVecBatch(vecBatch);
        ResetInputVecBatch();
        LogError("can not support groupByColumnsHandleType : %d.", groupByColumnsHandleType);
        throw OmniException("no t supported operation", "groupByColumnsHandleType error");
    }
    VectorHelper::FreeVecBatch(vecBatch);
    ResetInputVecBatch();
    if (operatorConfig.GetSpillConfig()->NeedSpill(serialize->GetElementsSize())) {
        auto result = SpillHashMap();
        executionContext->GetArena()->Reset();
        serialize->ResetHashmap();
        if (UNLIKELY(result != ErrorCode::SUCCESS)) {
            throw omniruntime::exception::OmniException(GetErrorCode(result), GetErrorMessage(result));
        }
    }
    return 0;
}

/**
 * @param types
 * @return rowSize
 * All the output data types are determined in this function. Following allocation for output vectors and filling
 * value should use the 'types' parameter instead of using input vector types.
 */
int32_t HashAggregationOperator::InitMaxRowCountAndOutputTypes()
{
    int32_t rowSize = 0;
    for (auto &i : groupByCols) {
        outputTypes.push_back(i.input);
        rowSize += OperatorUtil::GetTypeSize(i.input);
    }
    for (auto &aggregator : aggregators) {
        const std::vector<DataTypePtr> &aggTypes = aggregator->GetOutputTypes().Get();
        for (auto dataType : aggTypes) {
            outputTypes.push_back(dataType);
            rowSize += OperatorUtil::GetTypeSize(dataType);
        }
    }
    return rowSize;
}

void HashAggregationOperator::InitSpillInfos()
{
    spillTypes.push_back(VarcharType());
    for (auto &aggregator : aggregators) {
        auto currentSpillType = aggregator->GetSpillType();
        aggTypes.insert(aggTypes.end(), currentSpillType.begin(), currentSpillType.end());
        spillTypes.insert(spillTypes.end(), currentSpillType.begin(), currentSpillType.end());
    }
    SortOrder sortOrder;
    sortOrders.resize(1, sortOrder);
    groupByCloIdx.resize(1, 0);
    aggregationSort = std::make_unique<AggregationSort>(aggregators);
}

void HashAggregationOperator::SetVectors(VectorBatch *output, const std::vector<DataTypePtr> &types, int32_t rowCount)
{
    auto colSize = types.size();
    for (size_t colIndex = 0; colIndex < colSize; ++colIndex) {
        const DataTypePtr &type = types[colIndex];
        GROUP_AGG_FUNCTIONS[type->GetId()].setVector(output, rowCount);
    }
}

int32_t HashAggregationOperator::GetOutput(VectorBatch **outputVecBatch)
{
    int32_t expectedBatchSize = 0;
    if (LIKELY(groupByColumnsHandleType == HandleType::serialize)) {
        expectedBatchSize = Output(serialize, outputVecBatch);
    } else {
        SetStatus(OMNI_STATUS_ERROR);
        LogError("other groupby field handle type %d not implement now ", groupByColumnsHandleType);
        throw std::out_of_range("other groupby field handle type not implement");
    }
    return expectedBatchSize;
}

OmniStatus HashAggregationOperator::Close()
{
    delete[] sourceTypes;
    sourceTypes = nullptr;
    // delete spiller object when exception occurs
    if (spiller != nullptr) {
        spiller->RemoveSpillFiles();
    }
    delete spiller;
    spiller = nullptr;
    delete spillMerger;
    spillMerger = nullptr;

    executionContext->GetArena()->Reset();
    UpdateCloseInfo();
    return OMNI_STATUS_NORMAL;
}

void SetVarcharVector(VectorBatch *vecBatch, int32_t rowCount)
{
    vecBatch->Append(new Vector<LargeStringContainer<std::string_view>>(rowCount));
}

void SetContainerVector(VectorBatch *vecBatch, int32_t rowCount)
{
    auto doubleVector = std::make_unique<Vector<double>>(rowCount);
    auto longVector = std::make_unique<Vector<int64_t>>(rowCount);
    std::vector<int64_t> vectorAddresses(AVG_VECTOR_COUNT);
    vectorAddresses[0] = reinterpret_cast<int64_t>(doubleVector.get());
    vectorAddresses[1] = reinterpret_cast<int64_t>(longVector.get());
    std::vector<DataTypePtr> dataTypes { DoubleType(), LongType() };
    auto containerVector = new ContainerVector(rowCount, vectorAddresses, dataTypes);
    doubleVector.release();
    longVector.release();
    vecBatch->Append(containerVector);
}

void FillVarcharValue(BaseVector *vector, int32_t rowIndex, AggregateState &state)
{
    if (state.val == 0) {
        static_cast<Vector<LargeStringContainer<std::string_view>> *>(vector)->SetNull(rowIndex);
    } else {
        std::string_view str(reinterpret_cast<char *>(state.val), state.count);
        static_cast<Vector<LargeStringContainer<std::string_view>> *>(vector)->SetValue(rowIndex, str);
    }
}

template <typename Serialize>
void HashAggregationOperator::Emplace(Serialize &emplaceKey, VectorBatch *vecBatch, BaseVector **groupVectors,
    int32_t groupColNum)
{
    int32_t rowCount = vecBatch->GetRowCount();
    auto &arenaAllocator = *(executionContext->GetArena());
    size_t aggNum = aggregators.size();
    if (aggNum == 0) {
        // no aggregator, so just perform groupby
        for (int32_t rowIdx = 0; rowIdx < rowCount; rowIdx++) {
            emplaceKey->InsertValueToHashmap(groupVectors, groupColNum, rowIdx, arenaAllocator);
        }
        return;
    }

    // aggNum > 0
    std::vector<AggregateState *> currentRowStates(rowCount);
    auto currentGroupStateSize = static_cast<int64_t>(aggNum * sizeof(AggregateState));
    AggregateState *currentGroupStates = nullptr;
    std::vector<AggregateState *> newGroupStates;

    for (int32_t rowIdx = 0; rowIdx < rowCount; rowIdx++) {
        auto ret = emplaceKey->InsertValueToHashmap(groupVectors, groupColNum, rowIdx, arenaAllocator);
        if (ret.IsInsert()) {
            currentGroupStates = reinterpret_cast<AggregateState *>(arenaAllocator.Allocate(currentGroupStateSize));
            ret.SetValue(currentGroupStates);
            newGroupStates.emplace_back(currentGroupStates);
        } else {
            currentGroupStates = ret.GetValue();
            arenaAllocator.RollBackContinualMem();
        }
        currentRowStates[rowIdx] = currentGroupStates;
    }

    if (aggFiltersCount > 0) {
        int32_t filterOffset = vecBatch->GetVectorCount() - aggFiltersCount;
        for (size_t aggIdx = 0; aggIdx < aggNum; ++aggIdx) {
            auto &aggregator = aggregators[aggIdx];
            if (!newGroupStates.empty()) {
                aggregator->InitStates(newGroupStates, aggIdx);
            }
            if (hasAggFilters[aggIdx] == 1) {
                aggregator->ProcessGroupFilter(currentRowStates, aggIdx, vecBatch, filterOffset, 0);
                filterOffset++;
            } else {
                aggregator->ProcessGroup(currentRowStates, aggIdx, vecBatch, 0);
            }
        }
    } else {
        for (size_t aggIdx = 0; aggIdx < aggNum; ++aggIdx) {
            auto &aggregator = aggregators[aggIdx];
            if (!newGroupStates.empty()) {
                aggregator->InitStates(newGroupStates, aggIdx);
            }
            aggregator->ProcessGroup(currentRowStates, aggIdx, vecBatch, 0);
        }
    }
}

template <typename Deserialize>
void HashAggregationOperator::TraverseHashmapToGetOneResult(Deserialize &deserializeHashmap, VectorBatch *output)
{
    const int32_t expectSize = output->GetRowCount();
    int32_t groupColNum = static_cast<int32_t>(this->groupByCols.size());
    std::vector<BaseVector *> groupOutputVectors(groupColNum);
    for (int32_t i = 0; i < groupColNum; i++) {
        groupOutputVectors[i] = output->Get(i);
    }

    int32_t lambdaRowIndex = 0;
    OutputState curOutputState;
    auto &hashmap = deserializeHashmap->hashmap;
    {
        auto statefulMachine = hashmap.GetOutputMachine(outputState.outputHashmapPos, outputState.hasBeenOutputNum);

        curOutputState = statefulMachine.HandleElements(expectSize, [&](const auto &key, auto &mapped) mutable {
            deserializeHashmap->ParseKeyToCols(key, groupOutputVectors, groupColNum, lambdaRowIndex);
            ++lambdaRowIndex;
        });
    }

    const size_t aggNum = this->aggregators.size();
    if (aggNum > 0) {
        std::vector<AggregateState *> groupStates(expectSize);
        int32_t lambdaRowIndex = 0;
        {
            auto statefulMachine = hashmap.GetOutputMachine(outputState.outputHashmapPos, outputState.hasBeenOutputNum);
            statefulMachine.HandleElements(expectSize, [&](const auto &key, auto &mapped) mutable {
                groupStates[lambdaRowIndex] = mapped;
                lambdaRowIndex++;
            });
        }
        auto aggOutputStartIndex = groupColNum;
        for (size_t aggIndex = 0; aggIndex < aggNum; ++aggIndex) {
            auto &aggregator = aggregators[aggIndex];
            const auto oneAggOutputCols = aggOutputTypes[aggIndex].GetSize();
            std::vector<BaseVector *> adaptAggVectors(oneAggOutputCols);
            for (auto j = 0; j < oneAggOutputCols; j++) {
                adaptAggVectors[j] = output->Get(aggOutputStartIndex + j);
            }
            aggOutputStartIndex += oneAggOutputCols;
            aggregator->ExtractValuesBatch(groupStates, aggIndex, adaptAggVectors, 0, expectSize);
        }
    }

    outputState.UpdateState(curOutputState);
}

ErrorCode HashAggregationOperator::SpillToDisk()
{
    auto &spillHashMap = serialize->hashmap;
    auto totalSpillCount = spillHashMap.GetElementsSize();
    aggregationSort->ResizeKvVector(totalSpillCount);

    auto aggregationSortPtr = aggregationSort.get();
    size_t lambdaRowIndex = 0;
    OutputState curOutputState;
    {
        auto statefulMachine =
            spillHashMap.GetOutputMachine(spillOutputState.outputHashmapPos, spillOutputState.hasBeenOutputNum);

        curOutputState = statefulMachine.HandleElements(totalSpillCount, [&](const auto &key, auto &value) mutable {
            aggregationSortPtr->ParseHashMapToVector(key, value, lambdaRowIndex);
            ++lambdaRowIndex;
        });
    }
    spillOutputState.UpdateState(curOutputState);

    aggregationSort->SortKvVector();
    auto rowCount = aggregationSort->GetRowCount();
    LogDebug("Spill data to disk starting in hash aggregation operator, rowCount=%lld\n", rowCount);
    ErrorCode result = spiller->Spill(aggregationSort.get());
    LogDebug("Spill data to disk finished in hash aggregation operator, rowCount=%lld\n", rowCount);
    aggregationSort->ClearVector();
    return result;
}

ErrorCode HashAggregationOperator::SpillHashMap()
{
    auto rowCount = serialize->GetElementsSize();
    if (rowCount == 0) {
        return ErrorCode::SUCCESS;
    }

    if (spiller == nullptr) {
        auto spillConfig = operatorConfig.GetSpillConfig();
        OperatorConfig::CheckSpillConfig(spillConfig);
        InitSpillInfos();
        spiller = new Spiller(DataTypes(spillTypes), groupByCloIdx, sortOrders,
            operatorConfig.GetSpillConfig()->GetSpillPath(), spillConfig->GetMaxSpillBytes(),
            spillConfig->GetWriteBufferSize());
        hasSpill = true;
    }
    UpdateSpillTimesInfo();
    auto result = SpillToDisk();
    spillOutputState.hasBeenOutputNum = 0;
    spillOutputState.outputHashmapPos = 0;
    return result;
}

uint64_t HashAggregationOperator::GetSpilledBytes()
{
    return spilledBytes;
}

void HashAggregationOperator::SetStateOutputVecBatch(VectorBatch *outputVecBatch, int32_t rowCount, int32_t groupColNum,
    int32_t aggNum)
{
    std::vector<BaseVector *> adaptAggVectors;
    auto aggOutputStartIndex = groupColNum;
    for (int32_t aggIndex = 0; aggIndex < aggNum; aggIndex++) {
        auto &aggregator = aggregators[aggIndex];
        const auto oneAggOutputCols = aggOutputTypes[aggIndex].GetSize();
        adaptAggVectors.resize(oneAggOutputCols);
        for (auto j = 0; j < oneAggOutputCols; j++) {
            adaptAggVectors[j] = outputVecBatch->Get(aggOutputStartIndex + j);
        }
        aggOutputStartIndex += oneAggOutputCols;
        try {
            aggregator->ExtractValuesBatch(rowStates, aggIndex, adaptAggVectors, 0, rowCount);
        } catch (const OmniException &oneException) {
            // release VectorBatch when aggregator.ExtractValues throw exception
            // when spark hash agg sum/avg decimal overflow, it will throw exception when
            // OverflowConfigId==OVERFLOW_CONFIG_EXCEPTION
            throw oneException;
        }
    }
}

static VectorBatch *GetVectorBatchFromSlice(VectorBatch *vectorBatch, int32_t rowCount)
{
    auto outputColCount = vectorBatch->GetVectorCount();
    auto *sliceBatch = new VectorBatch(rowCount);
    for (int32_t columnIdx = 0; columnIdx < outputColCount; columnIdx++) {
        auto *vector = vectorBatch->Get(columnIdx);
        sliceBatch->Append(vec::VectorHelper::SliceVector(vector, 0, rowCount));
    }
    return sliceBatch;
}

VectorBatch *HashAggregationOperator::GetOutputFromDiskWithoutAgg(VectorBatch *output)
{
    auto groupColNum = static_cast<int32_t>(this->groupByCols.size());
    std::vector<BaseVector *> groupOutputVectors(groupColNum);
    for (int32_t i = 0; i < groupColNum; i++) {
        groupOutputVectors[i] = output->Get(i);
    }

    auto rowCount = output->GetRowCount();
    int32_t rowIdx = 0;
    bool isEqual = false;
    bool nextKeyIsNew = true;
    for (;;) {
        auto currentVecBatch = spillMerger->CurrentBatchWithEqual(isEqual);
        if (currentVecBatch == nullptr) {
            // construct the final output
            if (rowIdx < rowCount) {
                return GetVectorBatchFromSlice(output, rowIdx);
            } else {
                return output;
            }
        }
        auto currentRowIndex = spillMerger->CurrentRowIndex();
        if (nextKeyIsNew) {
            // construct the final output
            auto keyVector = static_cast<Vector<LargeStringContainer<std::string_view>> *>(currentVecBatch->Get(0));
            auto key = keyVector->GetValue(currentRowIndex);
            StringRef keyRef(const_cast<char *>(key.data()), key.size());
            serialize->ParseKeyToCols(keyRef, groupOutputVectors, groupColNum, rowIdx);
            rowIdx++;
        }

        nextKeyIsNew = !isEqual;
        spillMerger->Pop();
        spillRowOffset++;
        if (nextKeyIsNew && rowIdx >= rowCount) {
            return output;
        }
    }
}

VectorBatch *HashAggregationOperator::GetOutputFromDiskWithAgg(VectorBatch *output)
{
    auto groupColNum = static_cast<int32_t>(this->groupByCols.size());
    std::vector<BaseVector *> groupOutputVectors(groupColNum);
    for (int32_t i = 0; i < groupColNum; i++) {
        groupOutputVectors[i] = output->Get(i);
    }

    auto rowCount = output->GetRowCount();
    int32_t rowIdx = 0;
    auto aggNum = static_cast<int32_t>(aggregators.size());
    auto groupStatesPtr = groupStates.get();
    rowStates.resize(rowCount);

    std::vector<UnspillRowInfo> unspillRows(UNSPILL_ROW_COUNT_ONE_BATCH);
    std::vector<AggregateState *> newGroupStates;
    int32_t offset = 0;

    bool isEqual = false;
    bool nextKeyIsNew = true;
    AggregateState *currentGroupStates = nullptr;
    for (;;) {
        auto currentVecBatch = spillMerger->CurrentBatchWithEqual(isEqual);
        if (currentVecBatch == nullptr) {
            if (!newGroupStates.empty()) {
                for (int32_t aggIdx = 0; aggIdx < aggNum; aggIdx++) {
                    aggregators[aggIdx]->InitStates(newGroupStates, aggIdx);
                }
                newGroupStates.clear();
            }
            if (offset > 0) {
                int32_t vectorIndex = 1;
                for (int32_t aggIdx = 0; aggIdx < aggNum; aggIdx++) {
                    aggregators[aggIdx]->ProcessGroupUnspill(unspillRows, offset, aggIdx, vectorIndex);
                }
            }

            // construct the final output
            SetStateOutputVecBatch(output, rowIdx, groupColNum, aggNum);
            if (rowIdx < rowCount) {
                return GetVectorBatchFromSlice(output, rowIdx);
            } else {
                return output;
            }
        }
        bool isLastRow = false;
        auto currentRowIndex = spillMerger->CurrentRowIndex(isLastRow);
        if (nextKeyIsNew) {
            // this is a new key
            auto keyVector = static_cast<Vector<LargeStringContainer<std::string_view>> *>(currentVecBatch->Get(0));
            auto key = keyVector->GetValue(currentRowIndex);
            StringRef keyRef(const_cast<char *>(key.data()), key.size());
            serialize->ParseKeyToCols(keyRef, groupOutputVectors, groupColNum, rowIdx);

            currentGroupStates = groupStatesPtr + rowIdx * aggNum;
            newGroupStates.emplace_back(currentGroupStates);
            rowStates[rowIdx] = currentGroupStates;
            rowIdx++;
        }
        auto &unspillRow = unspillRows[offset];
        unspillRow.state = currentGroupStates;
        unspillRow.batch = currentVecBatch;
        unspillRow.rowIdx = currentRowIndex;
        offset++;
        if (offset >= UNSPILL_ROW_COUNT_ONE_BATCH || isLastRow) {
            if (!newGroupStates.empty()) {
                for (int32_t aggIdx = 0; aggIdx < aggNum; aggIdx++) {
                    aggregators[aggIdx]->InitStates(newGroupStates, aggIdx);
                }
                newGroupStates.clear();
            }
            int32_t vectorIndex = 1;
            for (int32_t aggIdx = 0; aggIdx < aggNum; aggIdx++) {
                aggregators[aggIdx]->ProcessGroupUnspill(unspillRows, offset, aggIdx, vectorIndex);
            }
            offset = 0;
        }

        nextKeyIsNew = !isEqual;
        spillMerger->Pop();
        spillRowOffset++;
        if (nextKeyIsNew && rowIdx >= rowCount) {
            if (!newGroupStates.empty()) {
                for (int32_t aggIdx = 0; aggIdx < aggNum; aggIdx++) {
                    aggregators[aggIdx]->InitStates(newGroupStates, aggIdx);
                }
                newGroupStates.clear();
            }
            if (offset > 0) {
                int32_t vectorIndex = 1;
                for (int32_t aggIdx = 0; aggIdx < aggNum; aggIdx++) {
                    aggregators[aggIdx]->ProcessGroupUnspill(unspillRows, offset, aggIdx, vectorIndex);
                }
            }

            SetStateOutputVecBatch(output, rowIdx, groupColNum, aggNum);
            return output;
        }
    }
}

void HashAggregationOperator::GetOutputFromDisk(VectorBatch **outputVecBatch)
{
    if (spillMerger == nullptr) {
        if (serialize->GetElementsSize() > 0) {
            auto result = SpillHashMap();
            executionContext->GetArena()->Reset();
            serialize->ResetHashmap();
            if (UNLIKELY(result != ErrorCode::SUCCESS)) {
                throw omniruntime::exception::OmniException(GetErrorCode(result), GetErrorMessage(result));
            }
        }

        spilledBytes = spiller->GetSpilledBytes();
        auto spillFiles = spiller->FinishSpill();
        UpdateSpillFileInfo(spillFiles.size());
        spillMerger = spiller->CreateSpillMerger(spillFiles);
        delete spiller;
        spiller = nullptr;
        if (spillMerger == nullptr) {
            throw omniruntime::exception::OmniException("SPILL_FAILED", "Create spill merger failed.");
        }
        spillTotalRowCount = spillMerger->GetTotalRowCount();

        if (!aggregators.empty()) {
            groupStates = std::make_unique<AggregateState[]>(aggregators.size() * rowsPerBatch);
        }
    }

    auto rowCount = std::min(rowsPerBatch, static_cast<int32_t>(spillTotalRowCount - spillRowOffset));
    auto output = std::make_unique<VectorBatch>(rowCount);
    auto outputPtr = output.get();
    SetVectors(outputPtr, outputTypes, rowCount);
    VectorBatch *result = nullptr;
    if (aggregators.empty()) {
        result = GetOutputFromDiskWithoutAgg(outputPtr);
    } else {
        result = GetOutputFromDiskWithAgg(outputPtr);
    }

    if (result == outputPtr) {
        // it means the result is not sliced
        result = output.release();
    }
    *outputVecBatch = result;
}

template <typename Deserialize>
int32_t HashAggregationOperator::Output(Deserialize &deserializeHashmap, VectorBatch **outputVecBatch)
{
    if (hasSpill) {
        GetOutputFromDisk(outputVecBatch);
        executionContext->GetArena()->Reset();
        if (*outputVecBatch != nullptr) {
            UpdateGetOutputInfo((*outputVecBatch)->GetRowCount());
        } else {
            UpdateGetOutputInfo(0);
        }
        if (spillTotalRowCount == spillRowOffset) {
            SetStatus(OmniStatus::OMNI_STATUS_FINISHED);
        }
        return 1;
    }

    auto &hashmap = deserializeHashmap->hashmap;
    int32_t totalRowCount = hashmap.GetElementsSize();
    if (totalRowCount == 0) {
        SetStatus(OmniStatus::OMNI_STATUS_FINISHED);
        return 0;
    }

    // The iteration output only contains one result, create only one output vector batches
    int32_t curRemainHandleOutput = totalRowCount - static_cast<int32_t>(outputState.hasBeenOutputNum);
    auto curRowCount = std::min(rowsPerBatch, curRemainHandleOutput);
    auto output = std::make_unique<VectorBatch>(curRowCount);
    auto outputPtr = output.get();
    SetVectors(outputPtr, outputTypes, curRowCount);

    TraverseHashmapToGetOneResult(deserializeHashmap, outputPtr);

    *outputVecBatch = output.release();
    UpdateGetOutputInfo((*outputVecBatch)->GetRowCount());
    if (static_cast<int32_t>(outputState.hasBeenOutputNum) == totalRowCount) {
        SetStatus(OmniStatus::OMNI_STATUS_FINISHED);
    }
    return 1;
}
} // end of namespace op
} // end of namespace omniruntime
