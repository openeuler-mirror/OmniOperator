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
    }

    // agg source types
    for (size_t i = 0; i < aggInputCols.size(); ++i) {
        for (size_t j = 0; j < aggInputCols[i].size(); ++j) {
            sourceTypes[aggInputCols[i][j]] = aggInputTypes[i].GetType(j)->GetId();
        }
    }
    executionContext = std::make_unique<op::ExecutionContext>();

    int32_t rowByteSize = InitMaxRowCountAndOutputTypes();
    rowsPerBatch = OperatorUtil::GetMaxRowCount(rowByteSize);
    return OMNI_STATUS_NORMAL;
}

int32_t HashAggregationOperator::AddInput(VectorBatch *vecBatch)
{
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
        SpillHashMap();
        serialize->ResetHashmap();
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
    int32_t spillRowSize = 0;
    for (auto &i : groupByCols) {
        spillTypes.push_back(i.input);
        spillRowSize += OperatorUtil::GetTypeSize(i.input);
    }
    std::vector<DataTypeId> currentSpillType;
    finalTypes.push_back(std::make_shared<VarcharDataType>());
    for (uint64_t i = 0; i < aggregators.size(); i++) {
        currentSpillType.clear();
        aggregators[i]->GetSpillType(currentSpillType);
        for (auto &type : currentSpillType) {
            auto dataTypePtr = std::make_shared<DataType>(type);
            spillTypes.push_back(dataTypePtr);
            aggTypes.push_back(dataTypePtr);
            finalTypes.push_back(dataTypePtr);
            spillRowSize += OperatorUtil::GetTypeSize(dataTypePtr);
        }
    }
    spillRowsPerPagesIndexs = OperatorUtil::GetMaxRowCount(spillRowSize);
    std::vector<type::DataTypePtr> sortTypes;
    sortTypes.push_back(std::make_shared<DataType>(OMNI_VARCHAR));
    sortTypes.push_back(std::make_shared<DataType>(OMNI_LONG));
    pagesIndex = new PagesIndex(DataTypes(sortTypes));
    finalPagesIndex = new PagesIndex(DataTypes(finalTypes));
    size_t groupNum = groupByCols.size();
    nullsFirst.resize(groupNum, true);
    ascendings.resize(groupNum, true);
    SortOrder sortOrder;
    sortOrders.resize(groupNum, sortOrder);
    sortOrders1.resize(1, sortOrder);
    groupByClomIdx.resize(1, 0);
    sortCol.resize(1, 0);
    sortColAscendings.resize(1, 1);
    sortColnullsFirst.resize(1, 1);
    for (int i = 0; i < aggregators.size() + 1; i++) {
        outPutRows.push_back(i);
    }
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
    delete pagesIndex;
    pagesIndex = nullptr;
    executionContext->GetArena()->Reset();
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

void FillVarcharValue(BaseVector *v, int32_t rowIndex, const AggregateState &state)
{
    if (state.val == nullptr) {
        static_cast<Vector<LargeStringContainer<std::string_view>> *>(v)->SetNull(rowIndex);
    } else {
        std::string_view str(reinterpret_cast<char *>(state.val), state.count);
        static_cast<Vector<LargeStringContainer<std::string_view>> *>(v)->SetValue(rowIndex, str);
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
    std::vector<AggregateState *> rowStates(rowCount);
    auto currentGroupStateSize = static_cast<int64_t>(aggNum * sizeof(AggregateState));
    AggregateState *currentGroupStates = nullptr;

    for (int32_t rowIdx = 0; rowIdx < rowCount; rowIdx++) {
        auto ret = emplaceKey->InsertValueToHashmap(groupVectors, groupColNum, rowIdx, arenaAllocator);
        if (ret.IsInsert()) {
            currentGroupStates = reinterpret_cast<AggregateState *>(arenaAllocator.Allocate(currentGroupStateSize));
            for (size_t j = 0; j < aggNum; ++j) {
                aggregators[j]->InitState(currentGroupStates[j]);
            }
            ret.SetValue(currentGroupStates);
        } else {
            currentGroupStates = ret.GetValue();
            arenaAllocator.RollBackContinualMem();
        }

        rowStates[rowIdx] = currentGroupStates;
    }

    if (aggFiltersCount > 0) {
        int32_t filterOffset = vecBatch->GetVectorCount() - aggFiltersCount;
        for (size_t i = 0; i < aggNum; ++i) {
            if (hasAggFilters[i] == 1) {
                aggregators[i]->ProcessGroupFilter(rowStates, i, vecBatch, filterOffset, 0);
                filterOffset++;
            } else {
                aggregators[i]->ProcessGroup(rowStates, i, vecBatch, 0);
            }
        }
    } else {
        for (size_t i = 0; i < aggNum; ++i) {
            aggregators[i]->ProcessGroup(rowStates, i, vecBatch, 0);
        }
    }
}

template <typename Deserialize>
void HashAggregationOperator::TraverseHashmapToGetOneResult(Deserialize &deserializeHashmap, VectorBatch *output)
{
    const int32_t expectSize = output->GetRowCount();
    const size_t aggNum = this->aggregators.size();
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

    auto aggOutputStartIndex = groupColNum;
    for (size_t aggIndex = 0; aggIndex < aggNum; ++aggIndex) {
        lambdaRowIndex = 0;
        auto &aggregator = aggregators[aggIndex];
        const auto oneAggOutputCols = aggOutputTypes[aggIndex].GetSize();
        std::vector<BaseVector *> adaptAggVectors(oneAggOutputCols);
        for (auto j = 0; j < oneAggOutputCols; j++) {
            adaptAggVectors[j] = output->Get(aggOutputStartIndex + j);
        }
        aggOutputStartIndex += oneAggOutputCols;
        {
            auto statefulMachine = hashmap.GetOutputMachine(outputState.outputHashmapPos, outputState.hasBeenOutputNum);
            statefulMachine.HandleElements(expectSize, [&](const auto &key, auto &mapped) mutable {
                auto &state = mapped[aggIndex];

                try {
                    aggregator->ExtractValues(state, adaptAggVectors, lambdaRowIndex);
                } catch (const OmniException &oneException) {
                    // release VectorBatch when aggregator.ExtractValues throw exception
                    // when spark hash agg sum/avg decimal overflow, it will throw exception when
                    // OverflowConfigId==OVERFLOW_CONFIG_EXCEPTION
                    throw oneException;
                }
                lambdaRowIndex++;
            });
        }
    }
    outputState.UpdateState(curOutputState);
}

void HashAggregationOperator::ConvertHashMap2PageIndex()
{
    auto &spillHashMap = serialize->hashmap;
    auto totalSpillCount = spillHashMap.GetElementsSize();
    auto hashmapInVector = new VectorBatch(totalSpillCount);
    hashmapInVector->Append(new Vector<LargeStringContainer<std::string_view>>(totalSpillCount));
    hashmapInVector->Append(new Vector<int64_t>(totalSpillCount));
    const size_t aggNum = this->aggregators.size();
    int32_t groupColNum = static_cast<int32_t>(this->groupByCols.size());
    int32_t lambdaRowIndex = 0;
    OutputState curOutputState;
    {
        auto statefulMachine =
            spillHashMap.GetOutputMachine(spillOutputState.outputHashmapPos, spillOutputState.hasBeenOutputNum);

        curOutputState = statefulMachine.HandleElements(totalSpillCount, [&](const auto &key, auto &mapped) mutable {
            serialize->InsertKeyToVector(key, reinterpret_cast<int64_t>(mapped), hashmapInVector, groupColNum,
                lambdaRowIndex);
            ++lambdaRowIndex;
        });
    }
    spillOutputState.UpdateState(curOutputState);
    pagesIndex->AddVecBatch(hashmapVecBatch.release());
    pagesIndex->Prepare();
    pagesIndex->Sort(sortCol.data(), sortColAscendings.data(), sortColnullsFirst.data(), 1, 0, totalSpillCount);
    auto sortedVec = new VectorBatch(totalSpillCount);
    std::vector<type::DataTypePtr> sortTypes;
    sortTypes.push_back(std::make_shared<DataType>(OMNI_VARCHAR));
    sortTypes.push_back(std::make_shared<DataType>(OMNI_LONG));
    std::vector<int> sorceTypes = { OMNI_VARCHAR, OMNI_LONG };
    SetVectors(sortedVec, sortTypes, totalSpillCount);
    pagesIndex->GetOutput(outPutRows.data(), aggregators.size() + 1, sortedVec, sorceTypes.data(), 0, totalSpillCount,
        0);
    auto vectorToBeWrited = new VectorBatch(totalSpillCount);
    vectorToBeWrited->Append(sortedVec->Get(0));
    if (aggregators.size() > 0) {
        auto currentValueRows = reinterpret_cast<Vector<int64_t> *>(sortedVec->Get(1));
        auto aggVector = new VectorBatch(totalSpillCount);
        SetVectors(aggVector, aggTypes, totalSpillCount);
        for (int i = 0; i < totalSpillCount; i++) {
            auto aggOutputStartIndex = 0;
            for (size_t aggIndex = 0; aggIndex < aggNum; ++aggIndex) {
                auto &aggregator = aggregators[aggIndex];
                auto currentAggType = aggregator->GetType();
                int32_t oneAggOutputCols = 1;
                if (currentAggType == FunctionType::OMNI_AGGREGATION_TYPE_SUM ||
                    currentAggType == FunctionType::OMNI_AGGREGATION_TYPE_AVG ||
                    currentAggType == FunctionType::OMNI_AGGREGATION_TYPE_FIRST_INCLUDENULL ||
                    currentAggType == FunctionType::OMNI_AGGREGATION_TYPE_FIRST_IGNORENULL) {
                    // Among these types, there will be two columns of arrays to store spill data.
                    oneAggOutputCols = 2;
                }
                std::vector<BaseVector *> adaptAggVectors(oneAggOutputCols);
                for (auto j = 0; j < oneAggOutputCols; j++) {
                    adaptAggVectors[j] = aggVector->Get(aggOutputStartIndex + j);
                }
                aggOutputStartIndex += oneAggOutputCols;
                auto state = (reinterpret_cast<AggregateState *>(currentValueRows->GetValue(i)))[aggIndex];
                aggregator->ExtractSpillValues(state, adaptAggVectors, i);
            }
        }
        for (int i = 0; i < aggVector->GetVectorCount(); i++) {
            vectorToBeWrited->Append(aggVector->Get(i));
        }
    }
    finalPagesIndex->AddVecBatch(vectorToBeWrited);
    finalPagesIndex->Prepare();
}

void HashAggregationOperator::SpillHashMap()
{
    if (spiller == nullptr) {
        auto spillConfig = operatorConfig.GetSpillConfig();
        OperatorConfig::CheckSpillConfig(spillConfig);
        InitSpillInfos();
        spiller = new Spiller(DataTypes(spillTypes), groupByClomIdx, sortOrders, spillConfig->GetSpillPath(),
            spillConfig->GetMaxSpillBytes());
        hasSpill = true;
    }
    while (spillOutputState.hasBeenOutputNum != this->serialize->GetElementsSize()) {
        ConvertHashMap2PageIndex();
        auto rowCount = pagesIndex->GetRowCount();
        LogDebug("Spill data to disk starting in hash aggregation operator, rowCount=%lld\n", rowCount);
        spiller->Spill(pagesIndex, false, false);
        LogDebug("Spill data to disk finished in hash aggregation operator, rowCount=%lld\n", rowCount);
        pagesIndex->Clear();
        finalPagesIndex->Clear();
    }
    spillOutputState.hasBeenOutputNum = 0;
    spillOutputState.outputHashmapPos = 0;
}

uint64_t HashAggregationOperator::GetSpilledBytes()
{
    return spilledBytes;
}

template <typename T>
void HashAggregationOperator::SetSpillOutputVector(BaseVector *outputVector, int32_t outputRowCount, int32_t outputCol)
{
    for (int32_t i = 0; i < outputRowCount; i++) {
        auto batch = batches[i];
        auto inputRowIdx = rowIdxes[i];
        if constexpr (std::is_same_v<T, std::string_view>) {
            using VarcharVector = Vector<LargeStringContainer<std::string_view>>;
            auto inputVector = static_cast<VarcharVector *>(batch->Get(outputCol));
            if (inputVector->IsNull(inputRowIdx)) {
                static_cast<VarcharVector *>(outputVector)->SetNull(i);
            } else {
                auto value = inputVector->GetValue(inputRowIdx);
                static_cast<VarcharVector *>(outputVector)->SetValue(i, value);
            }
        } else {
            auto inputVector = static_cast<Vector<T> *>(batch->Get(outputCol));
            if (inputVector->IsNull(inputRowIdx)) {
                static_cast<Vector<T> *>(outputVector)->SetNull(i);
            } else {
                static_cast<Vector<T> *>(outputVector)->SetValue(i, inputVector->GetValue(inputRowIdx));
            }
        }
    }
}

void HashAggregationOperator::SetSpillOutputVecBatch(VectorBatch *outputVecBatch, int32_t rowCount, int32_t groupColNum)
{
    for (int32_t i = 0; i < groupColNum; i++) {
        auto outputVector = outputVecBatch->Get(i);
        auto outputTypeId = outputTypes[i]->GetId();
        switch (outputTypeId) {
            case OMNI_INT:
            case OMNI_DATE32:
                SetSpillOutputVector<int32_t>(outputVector, rowCount, i);
                break;
            case OMNI_LONG:
            case OMNI_DECIMAL64:
            case OMNI_TIMESTAMP:
                SetSpillOutputVector<int64_t>(outputVector, rowCount, i);
                break;
            case OMNI_DOUBLE:
                SetSpillOutputVector<double>(outputVector, rowCount, i);
                break;
            case OMNI_BOOLEAN:
                SetSpillOutputVector<bool>(outputVector, rowCount, i);
                break;
            case OMNI_SHORT:
                SetSpillOutputVector<int16_t>(outputVector, rowCount, i);
                break;
            case OMNI_DECIMAL128:
                SetSpillOutputVector<Decimal128>(outputVector, rowCount, i);
                break;
            case OMNI_VARCHAR:
            case OMNI_CHAR:
                SetSpillOutputVector<std::string_view>(outputVector, rowCount, i);
                break;
            default:
                break;
        }
    }
}

void HashAggregationOperator::SetStateOutputVecBatch(VectorBatch *outputVecBatch, int32_t rowCount, int32_t groupColNum,
    int32_t aggNum)
{
    auto aggOutputStartIndex = groupColNum;
    for (int32_t aggIndex = 0; aggIndex < aggNum; aggIndex++) {
        auto &aggregator = aggregators[aggIndex];
        const auto oneAggOutputCols = aggOutputTypes[aggIndex].GetSize();
        std::vector<BaseVector *> adaptAggVectors(oneAggOutputCols);
        for (auto j = 0; j < oneAggOutputCols; j++) {
            adaptAggVectors[j] = outputVecBatch->Get(aggOutputStartIndex + j);
        }
        aggOutputStartIndex += oneAggOutputCols;
        try {
            for (int32_t i = 0; i < rowCount; i++) {
                aggregator->ExtractValues(rowStates[i][aggIndex], adaptAggVectors, i);
            }
        } catch (const OmniException &oneException) {
            // release VectorBatch when aggregator.ExtractValues throw exception
            // when spark hash agg sum/avg decimal overflow, it will throw exception when
            // OverflowConfigId==OVERFLOW_CONFIG_EXCEPTION
            throw oneException;
        }
    }
}

void HashAggregationOperator::SetSpillKeyOutputVector(VectorBatch *outputVecBatch, int32_t outputRowCount,
    int32_t groupColNum)
{
    using VarcharVector = Vector<LargeStringContainer<std::string_view>>;
    std::vector<BaseVector *> groupOutputVectors(groupColNum);
    for (int32_t i = 0; i < groupColNum; i++) {
        groupOutputVectors[i] = outputVecBatch->Get(i);
    }
    for (int32_t i = 0; i < outputRowCount; i++) {
        auto batch = batches[i];
        auto inputRowIdx = rowIdxes[i];
        auto keyVector = static_cast<VarcharVector *>(batch->Get(0));
        auto key = keyVector->GetValue(inputRowIdx);
        StringRef keyRef = StringRef(std::string(key));
        serialize->ParseKeyToCols(keyRef, groupOutputVectors, groupColNum, i);
    }
}

template <typename Deserialize>
void HashAggregationOperator::GetOutputFromDisk(Deserialize &deserializeHashmap, VectorBatch **outputVecBatch)
{
    if (spillMerger == nullptr) {
        if (serialize->GetElementsSize() > 0) {
            SpillHashMap();
            serialize->ResetHashmap();
        }

        spilledBytes = spiller->GetSpilledBytes();
        auto spillFiles = spiller->FinishSpill();
        spillMerger = spiller->CreateSpillMerger(spillFiles);
        delete spiller;
        spiller = nullptr;
        delete pagesIndex;
        pagesIndex = nullptr;
        if (spillMerger == nullptr) {
            throw omniruntime::exception::OmniException("SPILL_FAILED", "Create spill merger failed.");
        }
        spillTotalRowCount = spillMerger->GetTotalRowCount();
    }

    auto rowCount = std::min(static_cast<int64_t>(rowsPerBatch), spillTotalRowCount);
    batches.resize(rowCount);
    rowIdxes.resize(rowCount);
    auto &arenaAllocator = *(executionContext->GetArena());
    int32_t aggNum = aggregators.size();
    int32_t groupColNum = static_cast<int32_t>(this->groupByCols.size());
    int32_t rowIdx = 0;

    bool isEqual = false;
    if (aggNum == 0) {
        do {
            batches[rowIdx] = spillMerger->CurrentBatchWithEqual(isEqual);
            rowIdxes[rowIdx] = spillMerger->CurrentRowIndex();
            rowIdx++;
            spillTotalRowCount--;
            spillMerger->Pop();
            // process the same key
            while (isEqual) {
                spillMerger->CurrentBatchWithEqual(isEqual);
                spillMerger->CurrentRowIndex();
                spillTotalRowCount--;
                spillMerger->Pop();
            }
        } while (rowIdx < rowCount && spillTotalRowCount > 0);
        auto output = new VectorBatch(rowIdx);
        SetVectors(output, outputTypes, rowIdx);
        SetSpillKeyOutputVector(output, rowIdx, groupColNum);
        *outputVecBatch = output;
        return;
    }

    rowStates.resize(rowCount);
    do {
        batches[rowIdx] = spillMerger->CurrentBatchWithEqual(isEqual);
        rowIdxes[rowIdx] = spillMerger->CurrentRowIndex();
        spillTotalRowCount--;
        spillMerger->Pop();
        // init AggregateState
        int32_t vectorIndex = 1;
        auto currentGroupStateSize = static_cast<int64_t>(aggNum * sizeof(AggregateState));
        auto *currentGroupStates = reinterpret_cast<AggregateState *>(arenaAllocator.Allocate(currentGroupStateSize));
        for (int32_t i = 0; i < aggNum; i++) {
            aggregators[i]->InitState(currentGroupStates[i]);
            aggregators[i]->ProcessGroupAfterSpill(currentGroupStates[i], batches[rowIdx], vectorIndex,
                rowIdxes[rowIdx]);
        }

        while (isEqual) {
            VectorBatch *currentVectorBatch = spillMerger->CurrentBatchWithEqual(isEqual);
            int32_t currentRowIdx = spillMerger->CurrentRowIndex();

            // aggregation operation is performed for the same key
            int32_t currentVectorIndex = 1;
            for (int32_t i = 0; i < aggNum; i++) {
                aggregators[i]->ProcessGroupAfterSpill(currentGroupStates[i], currentVectorBatch, currentVectorIndex,
                    currentRowIdx);
            }
            spillTotalRowCount--;
            spillMerger->Pop();
        }
        rowStates[rowIdx] = currentGroupStates;
        rowIdx++;
    } while (rowIdx < rowCount && spillTotalRowCount > 0);
    auto output = new VectorBatch(rowIdx);
    SetVectors(output, outputTypes, rowIdx);
    SetSpillKeyOutputVector(output, rowIdx, groupColNum);
    SetStateOutputVecBatch(output, rowIdx, groupColNum, aggNum);
    *outputVecBatch = output;
}

template <typename Deserialize>
int32_t HashAggregationOperator::Output(Deserialize &deserializeHashmap, VectorBatch **outputVecBatch)
{
    if (hasSpill) {
        GetOutputFromDisk(deserializeHashmap, outputVecBatch);
        if (spillTotalRowCount == 0) {
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
    if (static_cast<int32_t>(outputState.hasBeenOutputNum) == totalRowCount) {
        SetStatus(OmniStatus::OMNI_STATUS_FINISHED);
    }
    return 1;
}
} // end of namespace op
} // end of namespace omniruntime
