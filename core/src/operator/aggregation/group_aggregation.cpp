/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2023. All rights reserved.
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
#include "agg_util.h"

#if defined(DEBUG_OPERATOR) && defined(TRACE)
#include <sstream>
#endif
namespace omniruntime {
namespace op {
using namespace omniruntime::type;

template void HashFuncImpl<BooleanVector, bool>(BaseVector *vector, const uint32_t rowCount, const int32_t *rowIndexes,
    uint64_t *combinedHash);

template void HashFuncVectImpl<BooleanVector, bool>(BaseVector *vector, const uint32_t start, const uint32_t rowCount,
    uint64_t *combinedHash);

template void DuplicateKeyValueImpl<BooleanVector, bool>(AggregateState &state, BaseVector *vector, const uint32_t offset,
    ExecutionContext *context);

template void IsSameNodeFuncImpl<BooleanVector, bool>(BaseVector *vector, const uint32_t offset, const AggregateState &slot,
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
    auto ret = CreateAggregatorFactories(aggregatorFactories, aggFuncTypesVector, GetMaskColumns());

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
        auto &type = this->groupByTypes.GetType(i);
        ColumnIndex c = { this->groupByColIdx[i], type, type };
        groupByIndex[i] = c;
    }

    // refresh inputDateTypes and inputColumnar index for OMNI_AGGREGATION_TYPE_COUNT_ALL type aggregator
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
            throw OmniException("OPERATOR_RUNTIME_ERROR", "Unable to create aggregator " + std::to_string(i) + " / " +
                std::to_string(this->aggregatorFactories.size()));
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
    if (isInited) {
        return OMNI_STATUS_NORMAL;
    }
    isInited = true;
    // put at beginning so that we do not allocate memory if there is error
    if (groupByColumnsHandleType == GroupByFieldHandleType::serialize) {
        serialize = std::make_unique<decltype(serialize)::element_type>();
        serialize->InitSize(groupByCols.size());
    } else {
        LogError("can not support groupByColumnsHandleType : %d.", groupByColumnsHandleType);
        // only the serialization method is used now
        return OMNI_STATUS_ERROR;
    }

    auto groupByColsSize = groupByCols.size();
    auto colSize = groupByColsSize + aggInputColsSize;
    sourceTypes = new int32_t[colSize];
    // group by source types
    for (auto &c : groupByCols) {
        sourceTypes[c.idx] = static_cast<int32_t>(c.input->GetId());
    }

    // agg source types
    for (int i = 0; i < aggInputCols.size(); ++i) {
        for (int j = 0; j < aggInputCols[i].size(); ++j) {
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
    auto groupColNum = this->groupByCols.size();
    serialize->ResetSerializer();
    VectorBatch groupVectors(vecBatch->GetRowCount());
    for (size_t i = 0; i < groupColNum; ++i) {
        auto curVector = vecBatch->Get(this->groupByCols[i].idx);
        groupVectors.Append(curVector);
        auto omniId = groupByCols[i].input->GetId();

        if (curVector->GetEncoding() == Encoding::OMNI_DICTIONARY) {
            serialize->PushBackSerializer(dicVectorSerializerCenter[omniId]);
        } else {
            serialize->PushBackSerializer(vectorSerializerCenter[omniId]);
        }
        serialize->PushBackDeSerializer(vectorDeSerializerCenter[omniId]);
    }

    if (groupByColumnsHandleType == GroupByFieldHandleType::serialize) {
        Emplace(serialize, vecBatch, groupVectors);
    } else {
        // only serialize method are used now
        VectorHelper::FreeVecBatch(vecBatch);
        LogError("can not support groupByColumnsHandleType : %d.", groupByColumnsHandleType);
        throw OmniException("no t supported operation", "groupByColumnsHandleType error");
    }

    VectorHelper::FreeVecBatch(vecBatch);
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

void HashAggregationOperator::SetVectors(mem::Allocator *vecAllocator, VectorBatch *vectorBatch,
    const std::vector<DataTypePtr> &types, int32_t rowCount)
{
    for (int colIndex = 0; colIndex < types.size(); ++colIndex) {
        const DataTypePtr& type = types[colIndex];
        GROUP_AGG_FUNCTIONS[type->GetId()].setVector(vectorBatch, *type, colIndex, vecAllocator, rowCount);
    }
}

int32_t HashAggregationOperator::GetOutput(VectorBatch **outputVecBatch)
{
    int32_t expectedBatchSize = 0;
    if (groupByColumnsHandleType == GroupByFieldHandleType::serialize) {
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
    if (sourceTypes != nullptr) {
        delete[] sourceTypes;
        sourceTypes = nullptr;
    }
    return OMNI_STATUS_NORMAL;
}

void SetVarcharVector(VectorBatch *vecBatch, DataType &type, int32_t columnIndex, mem::Allocator *vecAllocator,
    int32_t rowCount)
{
    vecBatch->Append(new VarcharVector(rowCount));
}

void SetContainerVector(VectorBatch *vecBatch, DataType &type, int32_t columnIndex, mem::Allocator *vecAllocator,
    int32_t rowCount)
{
    auto doubleVector = new DoubleVector(rowCount);
    auto longVector = new LongVector(rowCount);
    std::vector<int64_t> vectorAddresses(AVG_VECTOR_COUNT);
    vectorAddresses[0] = reinterpret_cast<int64_t>(doubleVector);
    vectorAddresses[1] = reinterpret_cast<int64_t>(longVector);
    std::vector<DataTypePtr> dataTypes { DoubleType(), LongType() };
    auto containerVector =
        new ContainerVector(rowCount, vectorAddresses, dataTypes);
    vecBatch->Append(containerVector);
}

void FillVarcharValue(BaseVector *v, int32_t rowIndex, const AggregateState &state)
{
    if (state.val == nullptr) {
        static_cast<VarcharVector *>(v)->SetNull(rowIndex);
    } else {
        std::string_view str(reinterpret_cast<char *>(state.val), state.count);
        static_cast<VarcharVector *>(v)->SetValue(rowIndex, str);
    }
}

template <typename Serialize>
void HashAggregationOperator::Emplace(Serialize &emplaceKey, VectorBatch *vecBatch, VectorBatch &groupVectors)
{
    int32_t rowCount = vecBatch->GetRowCount();
    size_t aggNum = aggregators.size();
    if (aggNum == 0) {
        // no aggregator, so just perform groupby
        for (int32_t i = 0; i < rowCount; ++i) {
            emplaceKey->InsertValueToHashmap(i, &groupVectors, *executionContext);
        }
        return;
    }

    // aggNum > 0
    std::vector<AggregateState *> rowStates(rowCount);
    AggregateState *currentGroupStates = nullptr;

    for (int32_t i = 0; i < rowCount; ++i) {
        auto ret = emplaceKey->InsertValueToHashmap(i, &groupVectors, *executionContext);
        if (ret.IsInsert()) {
            currentGroupStates = reinterpret_cast<AggregateState *>(
                executionContext->GetArena()->Allocate(aggNum * sizeof(AggregateState)));
            for (size_t j = 0; j < aggNum; ++j) {
                aggregators[j]->InitState(currentGroupStates[j]);
            }
            ret.SetValue(currentGroupStates);
        } else {
            currentGroupStates = ret.GetValue();
            executionContext->GetArena()->RollBackContinualMem();
        }

        rowStates[i] = currentGroupStates;
    }

    if (ConfigUtil::GetSupportExprFilterRule() == SupportExprFilterRule::EXPR_FILTER) {
        int32_t filterStart = vecBatch->GetVectorCount() - uint32_t(aggNum);
        for (size_t i = 0; i < aggNum; ++i) {
            aggregators[i]->ProcessGroupFilter(rowStates, i, vecBatch, filterStart, 0);
        }
    } else {
        for (size_t i = 0; i < aggNum; ++i) {
            aggregators[i]->ProcessGroup(rowStates, i, vecBatch, 0);
        }
    }
}

void HashAggregationOperator::FillOutputResultVectors(const int32_t totalRowCount, std::vector<VectorBatch *> &result)
{
    auto expectedBatchSize = OperatorUtil::GetVecBatchCount(totalRowCount, rowsPerBatch);
    auto leftRowCount = totalRowCount;

    // create all output vector batches
    for (int32_t batchId = 0; batchId < expectedBatchSize; ++batchId) {
        auto rowCount = std::min(rowsPerBatch, leftRowCount);
        auto vecBatch = new VectorBatch(rowCount);
        SetVectors(this->executionContext->GetArena()->GetAllocator(), vecBatch, outputTypes, rowCount);
        result.push_back(vecBatch);
        leftRowCount -= rowCount;
        if (leftRowCount <= 0) {
            break;
        }
    }
}

template <typename Deserialize>
void HashAggregationOperator::TraverseHashmapToGetResults(Deserialize &deserializeHashmap, const int32_t groupByColSize,
    std::vector<VectorBatch *> &result)
{
    const size_t aggNum = this->aggregators.size();

    int32_t curBatchId = 0;
    int32_t lambdaRowIndex = rowsPerBatch;
    VectorBatch *curBatch = nullptr;

    auto &hashmap = deserializeHashmap->hashmap;

    hashmap.ForEachKV([&](const auto &key, auto &mapped) mutable {
        if (lambdaRowIndex == rowsPerBatch) {
            lambdaRowIndex = 0;
            curBatch = result[curBatchId++];
        }
        deserializeHashmap->ParseKeyToCols(key, curBatch, 0, groupByColSize, lambdaRowIndex);
        ++lambdaRowIndex;
    });

    int aggOutputStartIndex = groupByColSize;
    for (size_t aggIndex = 0; aggIndex < aggNum; ++aggIndex) {
        curBatchId = 0;
        lambdaRowIndex = rowsPerBatch;
        auto &aggregator = aggregators[aggIndex];
        const auto oneAggOutputCols = aggOutputTypes[aggIndex].GetSize();
        std::vector<BaseVector *> adaptAggVectors(oneAggOutputCols);

        hashmap.ForEachKV([&](const auto &key, auto &mapped) mutable {
            auto &state = mapped[aggIndex];

            if (lambdaRowIndex == rowsPerBatch) {
                lambdaRowIndex = 0;
                curBatch = result[curBatchId++];
                for (auto j = 0; j < oneAggOutputCols; j++) {
                    adaptAggVectors[j] = curBatch->Get(aggOutputStartIndex + j);
                }
            }

            try {
                aggregator->ExtractValues(state, adaptAggVectors, lambdaRowIndex);
            } catch (const OmniException &oneException) {
                // release VectorBatch when aggregator.ExtractValues throw exception
                // when spark hash agg sum/avg decimal overflow, it will throw exception when
                // OverflowConfigId==OVERFLOW_CONFIG_EXCEPTION
                VectorHelper::FreeVecBatches(result);
                throw oneException;
            }
            lambdaRowIndex++;
        });
        aggOutputStartIndex += oneAggOutputCols;
    }
}

template <typename Deserialize>
void HashAggregationOperator::TraverseHashmapToGetOneResult(Deserialize &deserializeHashmap,
    const int32_t groupByColSize, VectorBatch *result)
{
    const int32_t expectSize = result->GetRowCount();
    const size_t aggNum = this->aggregators.size();

    int32_t lambdaRowIndex = 0;
    OutputState curOutputState;
    auto &hashmap = deserializeHashmap->hashmap;
    {
        auto statefulMachine = hashmap.GetOutputMachine(outputState.outputHashmapPos, outputState.hasBeenOutputNum);

        curOutputState = statefulMachine.HandleElements(expectSize, [&](const auto &key, auto &mapped) mutable {
            deserializeHashmap->ParseKeyToCols(key, result, 0, groupByColSize, lambdaRowIndex);
            ++lambdaRowIndex;
        });
    }

    auto aggOutputStartIndex = groupByColSize;

    for (size_t aggIndex = 0; aggIndex < aggNum; ++aggIndex) {
        lambdaRowIndex = 0;
        auto &aggregator = aggregators[aggIndex];
        const auto oneAggOutputCols = aggOutputTypes[aggIndex].GetSize();
        std::vector<BaseVector *> adaptAggVectors(oneAggOutputCols);
        for (auto j = 0; j < oneAggOutputCols; j++) {
            adaptAggVectors[j] = result->Get(aggOutputStartIndex + j);
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
                    VectorHelper::FreeVecBatch(result);
                    throw oneException;
                }
                lambdaRowIndex++;
            });
        }
    }
    outputState.UpdateState(curOutputState);
}

template <typename Deserialize>
int32_t HashAggregationOperator::Output(Deserialize &deserializeHashmap, VectorBatch **outputVecBatch)
{
    auto &hashmap = deserializeHashmap->hashmap;
    int32_t totalRowCount = hashmap.GetElementsSize();
    if (totalRowCount == 0) {
        SetStatus(OmniStatus::OMNI_STATUS_FINISHED);
        return 0;
    }
    // The iteration output only contains one result, create only one output vector batches
    int32_t curRemainHandleOutput = totalRowCount - static_cast<int32_t>(outputState.hasBeenOutputNum);
    auto curRowCount = std::min(rowsPerBatch, curRemainHandleOutput);
    auto output = new VectorBatch(curRowCount);
    SetVectors(this->executionContext->GetArena()->GetAllocator(), output, outputTypes, curRowCount);

    TraverseHashmapToGetOneResult(deserializeHashmap, groupByCols.size(), output);

    *outputVecBatch = output;
    if (static_cast<int32_t>(outputState.hasBeenOutputNum) == totalRowCount) {
        SetStatus(OmniStatus::OMNI_STATUS_FINISHED);
    }
    return 1;
}
} // end of namespace op
} // end of namespace omniruntime
