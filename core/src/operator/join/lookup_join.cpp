/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2024. All rights reserved.
 * @Description: lookup join implementations
 */
#include <vector>
#include <memory>
#include "operator/util/operator_util.h"
#include "vector/vector_helper.h"
#include "simd/simd.h"
#include "hash_builder.h"
#include "lookup_join.h"


static constexpr int8_t BUFFER_SIZE = 8;

using namespace omniruntime::vec;
namespace omniruntime::op {

void LookupJoinOperatorFactory::CommonInitActions(const type::DataTypes &probeTypes, int32_t *probeOutputCols,
    int32_t probeOutputColsCount, int32_t *probeHashCols, int32_t probeHashColsCount, int32_t *buildOutputCols,
    int32_t buildOutputColsCount, const type::DataTypes &buildOutputTypes, int32_t *outputList)
{
    int32_t tempProbeHashColTypes[probeHashColsCount];
    auto probeTypeIds = probeTypes.GetIds();
    for (int32_t i = 0; i < probeHashColsCount; i++) {
        tempProbeHashColTypes[i] = probeTypeIds[probeHashCols[i]];
    }
    if (probeOutputCols != nullptr) {
    	this->probeOutputCols = std::vector<int>(probeOutputCols, probeOutputCols + probeOutputColsCount);
    }
    this->probeHashCols = std::vector<int>(probeHashCols, probeHashCols + probeHashColsCount);
    if (buildOutputCols != nullptr) {
        this->buildOutputCols = std::vector<int>(buildOutputCols, buildOutputCols + buildOutputColsCount);
    }
    if (outputList != nullptr) {
    	this->outputList = std::vector<int>(outputList, outputList + buildOutputColsCount + probeOutputColsCount);
    }
    this->probeHashColTypes = std::vector<int32_t>(tempProbeHashColTypes, tempProbeHashColTypes + probeHashColsCount);
    this->rowSize = OperatorUtil::GetOutputRowSize(probeTypes.Get(), probeOutputCols, probeOutputColsCount);
    if (buildOutputColsCount != 0) {
        this->rowSize += OperatorUtil::GetRowSize(buildOutputTypes.Get());
    }
}

LookupJoinOperatorFactory::LookupJoinOperatorFactory(const type::DataTypes &probeTypes, int32_t *probeOutputCols,
    int32_t probeOutputColsCount, int32_t *probeHashCols, int32_t probeHashColsCount, int32_t *buildOutputCols,
    int32_t buildOutputColsCount, const type::DataTypes &buildOutputTypes, HashTableVariants *hashTables,
    omniruntime::expressions::Expr *filterExpr, bool isShuffleExchangeBuildPlan, OverflowConfig *overflowConfig)
    : probeTypes(probeTypes),
      buildOutputTypes(buildOutputTypes),
      hashTables(hashTables),
      isShuffleExchangeBuildPlan(isShuffleExchangeBuildPlan),
      originalProbeColsCount(probeTypes.GetSize())
{
    CommonInitActions(probeTypes, probeOutputCols, probeOutputColsCount, probeHashCols, probeHashColsCount,
        buildOutputCols, buildOutputColsCount, buildOutputTypes);
    std::visit([&](auto &&arg) { arg.SetProbeTypes(&(this->probeTypes)); }, *hashTables);
    JoinFilterCodeGen(filterExpr, overflowConfig);
}

LookupJoinOperatorFactory::LookupJoinOperatorFactory(const type::DataTypes &probeTypes, int32_t *probeOutputCols,
    int32_t probeOutputColsCount, int32_t *probeHashCols, int32_t probeHashColsCount, int32_t *buildOutputCols,
    int32_t buildOutputColsCount, const type::DataTypes &buildOutputTypes, HashTableVariants *hashTables,
    omniruntime::expressions::Expr *filterExpr, int32_t originalProbeColsCount,
    bool isShuffleExchangeBuildPlan, OverflowConfig *overflowConfig, int32_t *outputList)
    : probeTypes(probeTypes),
      buildOutputTypes(buildOutputTypes),
      hashTables(hashTables),
      isShuffleExchangeBuildPlan(isShuffleExchangeBuildPlan),
      originalProbeColsCount(originalProbeColsCount)
{
    CommonInitActions(probeTypes, probeOutputCols, probeOutputColsCount, probeHashCols, probeHashColsCount,
        buildOutputCols, buildOutputColsCount, buildOutputTypes, outputList);
    std::visit([&](auto &&arg) { arg.SetProbeTypes(&(this->probeTypes)); }, *hashTables);
    JoinFilterCodeGen(filterExpr, overflowConfig);
}

LookupJoinOperatorFactory::~LookupJoinOperatorFactory()
{
    delete simpleFilter;
    simpleFilter = nullptr;
}

LookupJoinOperatorFactory *LookupJoinOperatorFactory::CreateLookupJoinOperatorFactory(const DataTypes &probeTypes,
    int32_t *probeOutputCols, int32_t probeOutputColsCount, int32_t *probeHashCols, int32_t probeHashColsCount,
    int32_t *buildOutputCols, int32_t buildOutputColsCount, const DataTypes &buildOutputTypes,
    int64_t hashBuilderFactoryAddr, omniruntime::expressions::Expr *filterExpr,
    bool isShuffleExchangeBuildPlan, OverflowConfig *overflowConfig)
{
    auto hashBuilderFactory = reinterpret_cast<HashBuilderOperatorFactory *>(hashBuilderFactoryAddr);
    auto pOperatorFactory = new LookupJoinOperatorFactory(probeTypes, probeOutputCols, probeOutputColsCount,
        probeHashCols, probeHashColsCount, buildOutputCols, buildOutputColsCount, buildOutputTypes,
        hashBuilderFactory->GetHashTablesVariants(), filterExpr, isShuffleExchangeBuildPlan, overflowConfig);
    return pOperatorFactory;
}

LookupJoinOperatorFactory *LookupJoinOperatorFactory::CreateLookupJoinOperatorFactory(const DataTypes &probeTypes,
    int32_t *probeOutputCols, int32_t probeOutputColsCount, int32_t *probeHashCols, int32_t probeHashColsCount,
    int32_t *buildOutputCols, int32_t buildOutputColsCount, const DataTypes &buildOutputTypes,
    int64_t hashBuilderFactoryAddr, omniruntime::expressions::Expr *filterExpr,
    int32_t originalProbeColsCount, bool isShuffleExchangeBuildPlan,
    OverflowConfig *overflowConfig, int32_t *outputList)
{
    auto hashBuilderFactory = reinterpret_cast<HashBuilderOperatorFactory *>(hashBuilderFactoryAddr);
    auto pOperatorFactory = new LookupJoinOperatorFactory(probeTypes, probeOutputCols, probeOutputColsCount,
        probeHashCols, probeHashColsCount, buildOutputCols, buildOutputColsCount, buildOutputTypes,
        hashBuilderFactory->GetHashTablesVariants(), filterExpr,
        originalProbeColsCount, isShuffleExchangeBuildPlan, overflowConfig, outputList);
    return pOperatorFactory;
}

LookupJoinOperatorFactory *LookupJoinOperatorFactory::CreateLookupJoinOperatorFactory(
    std::shared_ptr<const HashJoinNode> planNode, HashBuilderOperatorFactory *hashBuilderOperatorFactory,
    const config::QueryConfig &queryConfig)
{
    // Extract necessary information from planNode
    auto joinType = planNode->GetJoinType();
    auto buildOutputTypes = planNode->RightOutputType();
    auto buildOutputColsCount = buildOutputTypes->GetSize();
    if (planNode->IsLeftSemi() || planNode->IsLeftAnti()) {
        buildOutputColsCount = 0;
    } else if (planNode->IsExistence()) {
        buildOutputColsCount = 1;
        buildOutputTypes = std::make_shared<DataTypes>(DataTypes({BooleanType()}));
    }
    std::vector<int32_t> buildOutputCols;
    if (planNode->IsExistence()) {
        buildOutputCols.emplace_back(1);
    } else {
        for (size_t index = 0; index < buildOutputColsCount; index++) {
            buildOutputCols.emplace_back(index);
        }
    }

    auto probeOutputTypes = planNode->LeftOutputType();
    auto probeOutputColsCount = probeOutputTypes->GetSize();
    std::vector<int32_t> probeOutputCols;
    for (size_t index = 0; index < probeOutputColsCount; index++) {
        probeOutputCols.emplace_back(index);
    }

    std::vector<int32_t> probeHashCols;
    for (const auto &key : planNode->LeftKeys()) {
        auto fieldKey = dynamic_cast<FieldExpr *>(key);
        probeHashCols.emplace_back(fieldKey->colVal);
    }
    auto probeHashColsCount = (int32_t) probeHashCols.size();

    auto filter = planNode->Filter();
    auto isShuffle = planNode->IsShuffle();

    auto overflowConfig = queryConfig.IsOverFlowASNull()
                              ? new OverflowConfig(OVERFLOW_CONFIG_NULL)
                              : new OverflowConfig(OVERFLOW_CONFIG_EXCEPTION);

    auto pLookupJoinOperatorFactory = new LookupJoinOperatorFactory(*probeOutputTypes, probeOutputCols.data(), probeOutputColsCount,
        probeHashCols.data(), probeHashColsCount, buildOutputCols.data(),
        buildOutputColsCount, *buildOutputTypes,
        hashBuilderOperatorFactory->GetHashTablesVariants(),
        filter, isShuffle, overflowConfig);

    delete overflowConfig;
    overflowConfig = nullptr;
    return pLookupJoinOperatorFactory;
}

Operator *LookupJoinOperatorFactory::CreateOperator()
{
    auto pLookupJoinOperator = new LookupJoinOperator(probeTypes, probeOutputCols, probeHashCols, probeHashColTypes,
        buildOutputCols, buildOutputTypes, hashTables, simpleFilter,
        originalProbeColsCount, rowSize, isShuffleExchangeBuildPlan, outputList);
    return pLookupJoinOperator;
}

void LookupJoinOperatorFactory::JoinFilterCodeGen(Expr *filterExpr, OverflowConfig *overflowConfig)
{
    if (filterExpr == nullptr) {
        return;
    }
    simpleFilter = new SimpleFilter(*filterExpr);
    auto result = simpleFilter->Initialize(overflowConfig);
    if (!result) {
        delete simpleFilter;
        simpleFilter = nullptr;
        throw omniruntime::exception::OmniException("EXPRESSION_NOT_SUPPORT", "The expression is not supported yet.");
    }
}

LookupJoinOperator::LookupJoinOperator(const type::DataTypes &probeTypes, std::vector<int32_t> &probeOutputCols,
    std::vector<int32_t> &probeHashCols, std::vector<int32_t> &probeHashColTypes, std::vector<int32_t> &buildOutputCols,
    const type::DataTypes &buildOutputTypes, HashTableVariants *hashTables, SimpleFilter *simpleFilter,
    int32_t originalProbeColsCount, int32_t outputRowSize, bool isShuffleExchangeBuildPlan)
    : probeTypes(probeTypes),
      probeOutputCols(probeOutputCols),
      probeHashCols(probeHashCols),
      probeHashColTypes(probeHashColTypes),
      buildOutputCols(buildOutputCols),
      buildOutputTypes(buildOutputTypes),
      hashTables(hashTables),
      simpleFilter(simpleFilter),
      originalProbeColsCount(originalProbeColsCount),
      isShuffleExchangeBuildPlan(isShuffleExchangeBuildPlan)
{
    std::vector<DataTypePtr> tmpProbeOutputTypesVec;
    for (size_t i = 0; i < probeOutputCols.size(); i++) {
        tmpProbeOutputTypesVec.emplace_back(probeTypes.GetType(probeOutputCols[i]));
    }
    this->probeOutputTypes = DataTypes(tmpProbeOutputTypesVec);

    this->outputBuilder = std::make_unique<LookupJoinOutputBuilder>(probeOutputCols, probeOutputTypes.GetIds(),
        buildOutputCols, buildOutputTypes.GetIds(), outputRowSize);
    this->probeHashColumns = new BaseVector *[probeHashCols.size()]();     // 2D array
    this->probeOutputColumns = new BaseVector *[probeOutputCols.size()](); // 2D array
    SetOperatorName(opNameForLookUpJoin);
    if (simpleFilter != nullptr) {
        auto usedColumns = simpleFilter->GetVectorIndexes();
        for (const auto &col : usedColumns) {
            if (col < originalProbeColsCount) {
                probeFilterCols.emplace_back(col);
            } else {
                buildFilterCols.emplace_back(col);
            }
        }
        auto buildColsCount = std::visit([&](auto &&arg) { return arg.GetBuildDataTypes()->GetSize(); }, *hashTables);
        auto allColsCount = originalProbeColsCount + buildColsCount;
        this->values = new int64_t[allColsCount]();
        this->nulls = new bool[allColsCount]();
        this->lengths = new int32_t[allColsCount]();

        this->probeFilterColumns = new BaseVector *[this->probeFilterCols.size()]();
        auto probeFilterColsCount = this->probeFilterCols.size();
        auto buildFilterColsCount = this->buildFilterCols.size();
        this->probeFilterTypeIds = new int32_t[probeFilterColsCount];
        this->buildFilterTypeIds = new int32_t[buildFilterColsCount];
        auto probeTypeIds = probeTypes.GetIds();
        for (size_t i = 0; i < probeFilterColsCount; i++) {
            this->probeFilterTypeIds[i] = probeTypeIds[this->probeFilterCols[i]];
        }
        auto buildTypeIds = std::visit([&](auto &&arg) { return arg.GetBuildDataTypes()->GetIds(); }, *hashTables);
        for (size_t i = 0; i < buildFilterColsCount; i++) {
            this->buildFilterTypeIds[i] = buildTypeIds[this->buildFilterCols[i] - originalProbeColsCount];
        }
    }
}

LookupJoinOperator::LookupJoinOperator(const type::DataTypes &probeTypes, std::vector<int32_t> &probeOutputCols,
    std::vector<int32_t> &probeHashCols, std::vector<int32_t> &probeHashColTypes, std::vector<int32_t> &buildOutputCols,
    const type::DataTypes &buildOutputTypes, HashTableVariants *hashTables, SimpleFilter *simpleFilter,
    int32_t originalProbeColsCount, int32_t outputRowSize, bool isShuffleExchangeBuildPlan, std::vector<int32_t> &outputList)
    : LookupJoinOperator(probeTypes, probeOutputCols, probeHashCols, probeHashColTypes, buildOutputCols, buildOutputTypes,
    hashTables, simpleFilter, originalProbeColsCount, outputRowSize, isShuffleExchangeBuildPlan)
{
	this->outputBuilder = std::make_unique<LookupJoinOutputBuilder>(probeOutputCols, probeOutputTypes.GetIds(),
																	buildOutputCols, buildOutputTypes.GetIds(),
																    outputRowSize, outputList);
}

LookupJoinOperator::~LookupJoinOperator()
{
    delete[] probeHashColumns;
    delete[] probeOutputColumns;
    delete[] probeFilterColumns;
    delete[] values;
    delete[] nulls;
    delete[] lengths;
    delete[] probeFilterTypeIds;
    delete[] buildFilterTypeIds;
}

BlockingReason LookupJoinOperator::IsBlocked(ContinueFuture* future)
{
    OmniStatus tableStatus = std::visit([&](auto&& arg) { return arg.GetStatus(); }, *hashTables);
    if (tableStatus == OmniStatus::OMNI_STATUS_NORMAL) {
        return BlockingReason::kWaitForJoinBuild;
    }
    return BlockingReason::kNotBlocked;
}

void LookupJoinOperator::PrepareCurrentProbe()
{
    int32_t columnCount = probeTypes.GetSize();
    BaseVector *probeAllColumns[columnCount];
    for (int32_t columnIdx = 0; columnIdx < columnCount; columnIdx++) {
        probeAllColumns[columnIdx] = curInputBatch->Get(columnIdx);
    }
    for (size_t j = 0; j < probeHashCols.size(); ++j) {
        probeHashColumns[j] = probeAllColumns[probeHashCols[j]];
    }
    for (size_t j = 0; j < probeOutputCols.size(); ++j) {
        probeOutputColumns[j] = probeAllColumns[probeOutputCols[j]];
    }
    probeSimd =
        isSingleHT &&
        std::visit([&](auto&& arg) { return arg.CanProbeSIMD(probeHashColumns, probeHashCols.size(), partitionMask); },
                   *hashTables);
    if (isSingleHT) {
        if (!probeSimd) {
            curProbeNulls.resize(curInputBatch->GetRowCount());
            std::fill(curProbeNulls.begin(), curProbeNulls.end(), 0);
            PopulateProbeNulls();
        }
    } else {
        curProbeNulls.resize(curInputBatch->GetRowCount());
        std::fill(curProbeNulls.begin(), curProbeNulls.end(), 0);
        curProbeHashes.resize(curInputBatch->GetRowCount());
        std::fill(curProbeHashes.begin(), curProbeHashes.end(), 0);
        PopulateProbeHashes();
    }

    if (this->simpleFilter != nullptr) {
        int32_t index = 0;
        for (const auto &col : probeFilterCols) {
            probeFilterColumns[index++] = probeAllColumns[col];
        }
    }
}

void LookupJoinOperator::PrepareSerializers()
{
    probeSerializers.clear();
    for (size_t i = 0; i < probeHashCols.size(); ++i) {
        auto curVector = probeHashColumns[i];
        if (curVector->GetEncoding() == Encoding::OMNI_DICTIONARY) {
            PushBackProbeSerializer(dicVectorSerializerIgnoreNullCenter[curVector->GetTypeId()]);
        } else {
            PushBackProbeSerializer(vectorSerializerIgnoreNullCenter[curVector->GetTypeId()]);
        }
    }
}

void LookupJoinOperator::ProcessProbe()
{
    bool hasFilter = simpleFilter != nullptr;
    switch (joinType) {
        case OMNI_JOIN_TYPE_INNER:
            if (hasFilter && isSingleHT) {
                ProbeBatchForInnerJoin<true, true>();
            } else if (hasFilter) {
                ProbeBatchForInnerJoin<true, false>();
            } else if (isSingleHT) {
                ProbeBatchForInnerJoin<false, true>();
            } else {
                ProbeBatchForInnerJoin<false, false>();
            }
            break;
        case OMNI_JOIN_TYPE_LEFT:
        case OMNI_JOIN_TYPE_RIGHT:
            if (buildSide == (joinType == OMNI_JOIN_TYPE_LEFT ? OMNI_BUILD_LEFT : OMNI_BUILD_RIGHT)) {
                if (hasFilter && isSingleHT) {
                    ProbeBatchForSameSideOuterJoin<true, true>();
                } else if (hasFilter) {
                    ProbeBatchForSameSideOuterJoin<true, false>();
                } else if (isSingleHT) {
                    ProbeBatchForSameSideOuterJoin<false, true>();
                } else {
                    ProbeBatchForSameSideOuterJoin<false, false>();
                }
            } else {
                if (hasFilter && isSingleHT) {
                    ProbeBatchForOppositeSideOuterJoin<true, true>();
                } else if (hasFilter) {
                    ProbeBatchForOppositeSideOuterJoin<true, false>();
                } else if (isSingleHT) {
                    ProbeBatchForOppositeSideOuterJoin<false, true>();
                } else {
                    ProbeBatchForOppositeSideOuterJoin<false, false>();
                }
            }
            break;
        case OMNI_JOIN_TYPE_FULL:
            if (hasFilter && isSingleHT) {
                ProbeBatchForFullJoin<true, true>();
            } else if (hasFilter) {
                ProbeBatchForFullJoin<true, false>();
            } else if (isSingleHT) {
                ProbeBatchForFullJoin<false, true>();
            } else {
                ProbeBatchForFullJoin<false, false>();
            }
            break;
        case OMNI_JOIN_TYPE_LEFT_SEMI:
            if (hasFilter && isSingleHT) {
                ProbeBatchForLeftSemiJoin<true, true>();
            } else if (hasFilter) {
                ProbeBatchForLeftSemiJoin<true, false>();
            } else if (isSingleHT) {
                ProbeBatchForLeftSemiJoin<false, true>();
            } else {
                ProbeBatchForLeftSemiJoin<false, false>();
            }
            break;
        case OMNI_JOIN_TYPE_LEFT_ANTI:
            if (hasFilter && isSingleHT) {
                ProbeBatchForLeftAntiJoin<true, true>();
            } else if (hasFilter) {
                ProbeBatchForLeftAntiJoin<true, false>();
            } else if (isSingleHT) {
                ProbeBatchForLeftAntiJoin<false, true>();
            } else {
                ProbeBatchForLeftAntiJoin<false, false>();
            }
            break;
        case OMNI_JOIN_TYPE_EXISTENCE:
            if (hasFilter && isSingleHT) {
                ProbeBatchForExistenceJoin<true, true>();
            } else if (hasFilter) {
                ProbeBatchForExistenceJoin<true, false>();
            } else if (isSingleHT) {
                ProbeBatchForExistenceJoin<false, true>();
            } else {
                ProbeBatchForExistenceJoin<false, false>();
            }
            break;
        default: {
            std::string omniExceptionInfo = "Error in ProcessProbe, no such join type " + std::to_string(joinType);
            throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR", omniExceptionInfo);
        }
    }
}

int32_t LookupJoinOperator::AddInput(VectorBatch *vecBatch)
{
    if (vecBatch->GetRowCount() <= 0) {
        VectorHelper::FreeVecBatch(vecBatch);
        ResetInputVecBatch();
        SetStatus(OMNI_STATUS_NORMAL);
        return 0;
    }
    UpdateAddInputInfo(vecBatch->GetRowCount());
    if (firstVecBatch) {
        firstVecBatch = false;
        InitFirst();
    }
    curInputBatch = vecBatch;
    curProbePosition = 0;

    PrepareCurrentProbe();
    PrepareSerializers();

    // maybe the data has been pulled after the previous probe, and the operator status will be set to finished
    // and needs to be reset to normal.
    SetStatus(OMNI_STATUS_NORMAL);
    return 0;
}

int32_t LookupJoinOperator::GetOutput(VectorBatch **outputVecBatch)
{
    if (curInputBatch == nullptr) {
        if (noMoreInput_) {
            SetStatus(OMNI_STATUS_FINISHED);
        }
        return 0;
    }

    auto inputRowCount = curInputBatch->GetRowCount();
    if (curProbePosition < inputRowCount) {
        // start probe
        ProcessProbe();
        executionContext->GetArena()->Reset();
    }

    // handle the output
    if (!outputBuilder->IsEmpty()) {
        outputBuilder->BuildOutput(probeOutputColumns, joinType, isShuffleExchangeBuildPlan, outputVecBatch, buildSide);
    }
    if (curProbePosition >= inputRowCount && outputBuilder->IsEmpty()) {
        VectorHelper::FreeVecBatch(curInputBatch);
        curInputBatch = nullptr;
        curProbePosition = 0;
        outputBuilder->Clear();

        if (noMoreInput_) {
            SetStatus(OMNI_STATUS_FINISHED);
        }
    }
    if ((*outputVecBatch != nullptr)) {
        UpdateGetOutputInfo((*outputVecBatch)->GetRowCount());
    } else {
        UpdateGetOutputInfo(0);
    }
    return 0;
}

OmniStatus LookupJoinOperator::Close()
{
    if (curInputBatch != nullptr) {
        VectorHelper::FreeVecBatch(curInputBatch);
        curInputBatch = nullptr;
    }
    UpdateCloseInfo();
    return OMNI_STATUS_NORMAL;
}

void LookupJoinOperator::InitFirst()
{
    joinType = std::visit([&](auto &&arg) { return arg.GetJoinType(); }, *hashTables);
    buildSide = std::visit([&](auto &&arg) { return arg.GetBuildSide(); }, *hashTables);
    auto hashTableCount = std::visit([&](auto &&arg) { return arg.GetHashTableCount(); }, *hashTables);
    partitionMask = hashTableCount - 1;
    isSingleHT = hashTableCount == 1;

    if (simpleFilter != nullptr) {
        std::visit([&](
            auto &&arg) { arg.InitBuildFilterCols(buildFilterCols, originalProbeColsCount, tableBuildFilterColPtrs); },
            *hashTables);
    }
}

template <typename T, bool hasJoinFilter, JoinType joinType, bool hasNull>
void LookupJoinOperator::ArrayJoinProbe(BaseVector*** buildColumns, size_t probeHashColsCount, T&& arg,
                                        ExecutionContext* contextPtr)
{
    int32_t probePosition = curProbePosition;
    int32_t numProbeRows = curInputBatch->GetRowCount();
    bool hasProduceRow = false;
    for (; probePosition < numProbeRows; probePosition++) {
        if constexpr (hasNull) {
            if (probeHashColumns[0]->IsNull(probePosition)) {
                if constexpr (joinType == OMNI_JOIN_TYPE_LEFT || joinType == OMNI_JOIN_TYPE_FULL ||
                              joinType == OMNI_JOIN_TYPE_LEFT_ANTI) {
                    outputBuilder->AppendRow(probePosition, nullptr, 0);
                }
                continue;
            }
        }
        auto result =
            arg.Find(probeSerializers, contextPtr, probeHashColumns, probeHashColsCount, probePosition, partitionMask);
        if constexpr (joinType == OMNI_JOIN_TYPE_LEFT || joinType == OMNI_JOIN_TYPE_FULL ||
                      joinType == OMNI_JOIN_TYPE_LEFT_ANTI) {
            hasProduceRow = false;
        }
        if (result.IsInsert()) {
            auto it = result.GetValue()->Begin();
            ProbeJoinPosition<hasJoinFilter>(probePosition);
            while (it.IsOk()) {
                uint64_t address;
                if constexpr (joinType != OMNI_JOIN_TYPE_LEFT_ANTI && joinType != OMNI_JOIN_TYPE_FULL) {
                    address = LookupJoinOutputBuilder::EncodeAddress(it->rowIdx, it->vecBatchIdx);
                }
                bool filterResult = true;
                if constexpr (hasJoinFilter) {
                    filterResult = BuildJoinPosition(partitionMask, it->rowIdx, it->vecBatchIdx, contextPtr);
                    if (filterResult) {
                        if constexpr (joinType != OMNI_JOIN_TYPE_LEFT_ANTI && joinType != OMNI_JOIN_TYPE_FULL) {
                            outputBuilder->AppendRow(probePosition, buildColumns, address);
                        }
                        if constexpr (joinType == OMNI_JOIN_TYPE_RIGHT) {
                            arg.PositionVisited(it);
                        }
                        if constexpr (joinType == OMNI_JOIN_TYPE_LEFT || joinType == OMNI_JOIN_TYPE_LEFT_ANTI) {
                            hasProduceRow = true;
                        }
                        if constexpr (joinType == OMNI_JOIN_TYPE_LEFT_SEMI || joinType == OMNI_JOIN_TYPE_LEFT_ANTI) {
                            break;
                        }
                    }
                } else {
                    if constexpr (joinType != OMNI_JOIN_TYPE_LEFT_ANTI && joinType != OMNI_JOIN_TYPE_FULL) {
                        outputBuilder->AppendRow(probePosition, buildColumns, address);
                    }
                    if constexpr (joinType == OMNI_JOIN_TYPE_RIGHT) {
                        arg.PositionVisited(it);
                    }
                    if constexpr (joinType == OMNI_JOIN_TYPE_LEFT || joinType == OMNI_JOIN_TYPE_LEFT_ANTI) {
                        hasProduceRow = true;
                    }
                    if constexpr (joinType == OMNI_JOIN_TYPE_LEFT_SEMI || joinType == OMNI_JOIN_TYPE_LEFT_ANTI) {
                        break;
                    }
                }
                if constexpr (joinType == OMNI_JOIN_TYPE_FULL) {
                    if (filterResult) {
                        address = LookupJoinOutputBuilder::EncodeAddress(it->rowIdx, it->vecBatchIdx);
                        outputBuilder->AppendRow(probePosition, buildColumns, address);
                        arg.PositionVisited(it);
                        hasProduceRow = true;
                    }
                }
                ++it;
            }
        }
        if constexpr (joinType == OMNI_JOIN_TYPE_LEFT || joinType == OMNI_JOIN_TYPE_FULL ||
                      joinType == OMNI_JOIN_TYPE_LEFT_ANTI) {
            if (!hasProduceRow) {
                outputBuilder->AppendRow(probePosition, nullptr, 0);
            }
        }
        if (outputBuilder->IsFull()) {
            curProbePosition = probePosition + 1;
            return;
        }
    }
    curProbePosition = numProbeRows;
}

template <typename T, bool hasJoinFilter, JoinType joinType>
void LookupJoinOperator::DealWithProbeMatchResult(int64_t *matchRowsData, int64_t *matchSlotsData,
                                                  int64_t *noMatchRowsData, int32_t matchRowCnt, int32_t noMatchRowCnt,
                                                  T &&arg, ExecutionContext *contextPtr, BaseVector ***buildColumns)
{
    bool hasProduceRow = false;
    auto &arrayTable = arg.GetArrayTable(partitionMask);
    for (int32_t j = 0; j < matchRowCnt; j++) {
        auto it = arrayTable->TransformPtr(matchSlotsData[j])->Begin();
        auto rowIndex = matchRowsData[j];
        if constexpr (hasJoinFilter && (joinType == OMNI_JOIN_TYPE_LEFT || joinType == OMNI_JOIN_TYPE_FULL ||
                                        joinType == OMNI_JOIN_TYPE_LEFT_ANTI)) {
            hasProduceRow = false;
        }
        ProbeJoinPosition<hasJoinFilter>(rowIndex);
        while (it.IsOk()) {
            uint64_t address;
            if constexpr (joinType != OMNI_JOIN_TYPE_LEFT_ANTI && joinType != OMNI_JOIN_TYPE_FULL) {
                address = LookupJoinOutputBuilder::EncodeAddress(it->rowIdx, it->vecBatchIdx);
            }
            bool filterResult = true;
            if constexpr (hasJoinFilter) {
                filterResult = BuildJoinPosition(partitionMask, it->rowIdx, it->vecBatchIdx, contextPtr);
                if (filterResult) {
                    if constexpr (joinType != OMNI_JOIN_TYPE_LEFT_ANTI && joinType != OMNI_JOIN_TYPE_FULL) {
                        outputBuilder->AppendRow(rowIndex, buildColumns, address);
                    }
                    if constexpr (joinType == OMNI_JOIN_TYPE_LEFT || joinType == OMNI_JOIN_TYPE_RIGHT ||
                                  joinType == OMNI_JOIN_TYPE_LEFT_ANTI) {
                        hasProduceRow = true;
                    }
                    if constexpr (joinType == OMNI_JOIN_TYPE_RIGHT) {
                        arg.PositionVisited(it);
                    }
                    if constexpr (joinType == OMNI_JOIN_TYPE_LEFT_SEMI || joinType == OMNI_JOIN_TYPE_LEFT_ANTI) {
                        break;
                    }
                }
            } else {
                if constexpr (joinType != OMNI_JOIN_TYPE_LEFT_ANTI && joinType != OMNI_JOIN_TYPE_FULL) {
                    outputBuilder->AppendRow(rowIndex, buildColumns, address);
                }
                if constexpr (joinType == OMNI_JOIN_TYPE_RIGHT) {
                    arg.PositionVisited(it);
                }
                if constexpr (joinType == OMNI_JOIN_TYPE_LEFT_SEMI || joinType == OMNI_JOIN_TYPE_LEFT_ANTI) {
                    break;
                }
            }
            if constexpr (joinType == OMNI_JOIN_TYPE_FULL) {
                if (filterResult) {
                    address = LookupJoinOutputBuilder::EncodeAddress(it->rowIdx, it->vecBatchIdx);
                    outputBuilder->AppendRow(rowIndex, buildColumns, address);
                    arg.PositionVisited(it);
                    hasProduceRow = true;
                }
            }
            ++it;
        }
        if constexpr (hasJoinFilter && (joinType == OMNI_JOIN_TYPE_LEFT || joinType == OMNI_JOIN_TYPE_FULL ||
                                        joinType == OMNI_JOIN_TYPE_LEFT_ANTI)) {
            if (!hasProduceRow) {
                outputBuilder->AppendRow(rowIndex, nullptr, 0);
            }
        }
    }
    if constexpr (joinType == OMNI_JOIN_TYPE_LEFT || joinType == OMNI_JOIN_TYPE_FULL ||
                  joinType == OMNI_JOIN_TYPE_LEFT_ANTI) {
        for (int32_t j = 0; j < noMatchRowCnt; j++) {
            outputBuilder->AppendRow(noMatchRowsData[j], nullptr, 0);
        }
    }
}

template <typename T, typename KeyType, bool hasJoinFilter, JoinType joinType, bool hasNull>
void LookupJoinOperator::DealWithProbeMatchResultNeon(KeyType *hashes, KeyType *matches, int32_t len, int32_t rowStart,
                                                      const int64_t *slots, const bool *isAssigned, T &&arg,
                                                      ExecutionContext *contextPtr, BaseVector ***buildColumns)
{
    bool hasProduceRow = false;
    auto &arrayTable = arg.GetArrayTable(partitionMask);
    auto probeVector = probeHashColumns[0];
    for (int32_t j = 0; j < len; j++) {
        bool notNull = true;
        int32_t rowIndex = rowStart + j;
        if constexpr (hasNull) {
            notNull = !probeVector->IsNull(rowIndex);
        }
        if (matches[j] && notNull && isAssigned[hashes[j]]) {
            auto it = arrayTable->TransformPtr(slots[hashes[j]])->Begin();
            if constexpr (hasJoinFilter && (joinType == OMNI_JOIN_TYPE_LEFT || joinType == OMNI_JOIN_TYPE_FULL ||
                                            joinType == OMNI_JOIN_TYPE_LEFT_ANTI)) {
                hasProduceRow = false;
            }
            ProbeJoinPosition<hasJoinFilter>(rowIndex);
            while (it.IsOk()) {
                uint64_t address;
                if constexpr (joinType != OMNI_JOIN_TYPE_LEFT_ANTI && joinType != OMNI_JOIN_TYPE_FULL) {
                    address = LookupJoinOutputBuilder::EncodeAddress(it->rowIdx, it->vecBatchIdx);
                }
                bool filterResult = true;
                if constexpr (hasJoinFilter) {
                    filterResult = BuildJoinPosition(partitionMask, it->rowIdx, it->vecBatchIdx, contextPtr);
                    if (filterResult) {
                        if constexpr (joinType != OMNI_JOIN_TYPE_LEFT_ANTI && joinType != OMNI_JOIN_TYPE_FULL) {
                            outputBuilder->AppendRow(rowIndex, buildColumns, address);
                        }
                        if constexpr (joinType == OMNI_JOIN_TYPE_LEFT || joinType == OMNI_JOIN_TYPE_RIGHT ||
                                      joinType == OMNI_JOIN_TYPE_LEFT_ANTI) {
                            hasProduceRow = true;
                        }
                        if constexpr (joinType == OMNI_JOIN_TYPE_RIGHT) {
                            arg.PositionVisited(it);
                        }
                        if constexpr (joinType == OMNI_JOIN_TYPE_LEFT_SEMI || joinType == OMNI_JOIN_TYPE_LEFT_ANTI) {
                            break;
                        }
                    }
                } else {
                    if constexpr (joinType != OMNI_JOIN_TYPE_LEFT_ANTI && joinType != OMNI_JOIN_TYPE_FULL) {
                        outputBuilder->AppendRow(rowIndex, buildColumns, address);
                    }
                    if constexpr (joinType == OMNI_JOIN_TYPE_RIGHT) {
                        arg.PositionVisited(it);
                    }
                    if constexpr (joinType == OMNI_JOIN_TYPE_LEFT_SEMI || joinType == OMNI_JOIN_TYPE_LEFT_ANTI) {
                        break;
                    }
                }
                if constexpr (joinType == OMNI_JOIN_TYPE_FULL) {
                    if (filterResult) {
                        address = LookupJoinOutputBuilder::EncodeAddress(it->rowIdx, it->vecBatchIdx);
                        outputBuilder->AppendRow(rowIndex, buildColumns, address);
                        arg.PositionVisited(it);
                        hasProduceRow = true;
                    }
                }
                ++it;
            }
            if constexpr (hasJoinFilter && (joinType == OMNI_JOIN_TYPE_LEFT || joinType == OMNI_JOIN_TYPE_FULL ||
                                            joinType == OMNI_JOIN_TYPE_LEFT_ANTI)) {
                if (!hasProduceRow) {
                    outputBuilder->AppendRow(rowIndex, nullptr, 0);
                }
            }
        } else if constexpr (joinType == OMNI_JOIN_TYPE_LEFT || joinType == OMNI_JOIN_TYPE_FULL ||
                             joinType == OMNI_JOIN_TYPE_LEFT_ANTI) {
            outputBuilder->AppendRow(rowIndex, nullptr, 0);
        }
    }
}

void Int16ArrayCompareNeon(int16_t *originKey, int16_t *keyResult, int16_t *cmpResult,
                           int16x8_t vMin, int16x8_t vMax, int16_t minValue, int16_t maxValue)
{
    int16x8x3_t vCmp;
    int16x8x3_t vecKey;
    int16x8x3_t vIndex;

    vecKey = vld3q_s16(originKey + 8);

    keyResult[0] = originKey[0] - minValue;
    keyResult[1] = originKey[1] - minValue;
    vIndex.val[0] = vsubq_s16(vecKey.val[0], vMin);
    keyResult[2] = originKey[2] - minValue;
    keyResult[3] = originKey[3] - minValue;
    keyResult[4] = originKey[4] - minValue;
    keyResult[5] = originKey[5] - minValue;
    vIndex.val[1] = vsubq_s16(vecKey.val[1], vMin);
    keyResult[6] = originKey[6] - minValue;
    keyResult[7] = originKey[7] - minValue;
    vIndex.val[2] = vsubq_s16(vecKey.val[2], vMin);
    vst3q_s16(keyResult + 8, vIndex);

    cmpResult[0] = maxValue >= originKey[0] && originKey[0] >= minValue;
    cmpResult[1] = maxValue >= originKey[1] && originKey[1] >= minValue;
    cmpResult[2] = maxValue >= originKey[2] && originKey[2] >= minValue;
    vCmp.val[0] = (int16x8_t)vcgeq_s16(vecKey.val[0], vMin);
    vCmp.val[0] = vandq_s16(vCmp.val[0], (int16x8_t)vcleq_s16(vecKey.val[0], vMax));
    cmpResult[3] = maxValue >= originKey[3] && originKey[3] >= minValue;
    cmpResult[4] = maxValue >= originKey[4] && originKey[4] >= minValue;
    cmpResult[5] = maxValue >= originKey[5] && originKey[5] >= minValue;
    vCmp.val[1] = (int16x8_t)vcgeq_s16(vecKey.val[1], vMin);
    vCmp.val[1] = vandq_s16(vCmp.val[1], (int16x8_t)vcleq_s16(vecKey.val[1], vMax));
    cmpResult[6] = maxValue >= originKey[6] && originKey[6] >= minValue;
    cmpResult[7] = maxValue >= originKey[7] && originKey[7] >= minValue;
    vCmp.val[2] = (int16x8_t)vcgeq_s16(vecKey.val[2], vMin);
    vCmp.val[2] = vandq_s16(vCmp.val[2], (int16x8_t)vcleq_s16(vecKey.val[2], vMax));

    vst3q_s16(cmpResult + 8, vCmp);
}

void Int32ArrayCompareNeon(int32_t *originKey, int32_t *keyResult, int32_t *cmpResult,
                           int32x4_t vMin, int32x4_t vMax, int32_t minValue, int32_t maxValue)
{
    int32x4x3_t vCmp;
    int32x4x3_t vecKey;
    int32x4x3_t vIndex;

    vecKey = vld3q_s32(originKey + 4);

    keyResult[0] = originKey[0] - minValue;
    vIndex.val[0] = vsubq_s32(vecKey.val[0], vMin);
    keyResult[1] = originKey[1] - minValue;
    vIndex.val[1] = vsubq_s32(vecKey.val[1], vMin);
    keyResult[2] = originKey[2] - minValue;
    vIndex.val[2] = vsubq_s32(vecKey.val[2], vMin);
    keyResult[3] = originKey[3] - minValue;
    vst3q_s32(keyResult + 4, vIndex);

    cmpResult[0] = maxValue >= originKey[0] && originKey[0] >= minValue;
    vCmp.val[0] = (int32x4_t)vcgeq_s32(vecKey.val[0], vMin);
    vCmp.val[0] = vandq_s32(vCmp.val[0], (int32x4_t)vcleq_s32(vecKey.val[0], vMax));
    cmpResult[1] = maxValue >= originKey[1] && originKey[1] >= minValue;
    vCmp.val[1] = (int32x4_t)vcgeq_s32(vecKey.val[1], vMin);
    vCmp.val[1] = vandq_s32(vCmp.val[1], (int32x4_t)vcleq_s32(vecKey.val[1], vMax));
    cmpResult[2] = maxValue >= originKey[2] && originKey[2] >= minValue;
    vCmp.val[2] = (int32x4_t)vcgeq_s32(vecKey.val[2], vMin);
    vCmp.val[2] = vandq_s32(vCmp.val[2], (int32x4_t)vcleq_s32(vecKey.val[2], vMax));
    cmpResult[3] = maxValue >= originKey[3] && originKey[3] >= minValue;

    vst3q_s32(cmpResult + 4, vCmp);
}

void Int64ArrayCompareNeon(int64_t *originKey, int64_t *keyResult, int64_t *cmpResult,
                           int64x2_t vMin, int64x2_t vMax, int64_t minValue, int64_t maxValue)
{
    int64x2x3_t vCmp;
    int64x2x3_t vecKey;
    int64x2x3_t vHashes;

    vecKey = vld3q_s64(originKey + 2);

    keyResult[0] = originKey[0] - minValue;
    vHashes.val[0] = vsubq_s64(vecKey.val[0], vMin);
    vHashes.val[1] = vsubq_s64(vecKey.val[1], vMin);
    keyResult[1] = originKey[1] - minValue;
    vHashes.val[2] = vsubq_s64(vecKey.val[2], vMin);
    vst3q_s64(keyResult + 2, vHashes);

    cmpResult[0] = maxValue >= originKey[0] && originKey[0] >= minValue;
    vCmp.val[0] = (int64x2_t)vcgeq_s64(vecKey.val[0], vMin);
    vCmp.val[0] = vandq_s64(vCmp.val[0], (int64x2_t)vcleq_s64(vecKey.val[0], vMax));
    vCmp.val[1] = (int64x2_t)vcgeq_s64(vecKey.val[1], vMin);
    vCmp.val[1] = vandq_s64(vCmp.val[1], (int64x2_t)vcleq_s64(vecKey.val[1], vMax));

    cmpResult[1] = maxValue >= originKey[1] && originKey[1] >= minValue;
    vCmp.val[2] = (int64x2_t)vcgeq_s64(vecKey.val[2], vMin);
    vCmp.val[2] = vandq_s64(vCmp.val[2], (int64x2_t)vcleq_s64(vecKey.val[2], vMax));

    vst3q_s64(cmpResult + 2, vCmp);
}

template <typename T, bool hasJoinFilter, JoinType joinType, bool hasNull>
void LookupJoinOperator::ArrayJoinProbeSIMDNeon(BaseVector ***buildColumns, size_t probeHashColsCount, T &&arg,
                                                ExecutionContext *contextPtr)
{
    // 4*128/8
    constexpr int32_t simdLen = 64;
    auto inputRowCount = curInputBatch->GetRowCount();
    auto probeVector = arg.GetSingleProbeHashKeyBase(probeHashColumns);
    using KeyType = typename std::remove_pointer<decltype(probeVector)>::type;
    // get KeyType from hashmap
    constexpr int32_t vecLanes = simdLen / sizeof(KeyType);
    int32_t probePosition = curProbePosition;
    probeVector += probePosition;
    __builtin_prefetch(probeVector, 0, 3);
    int32_t end = inputRowCount / vecLanes * vecLanes;
    auto maxMinPair = arg.GetmaxMinValue(partitionMask);
    auto minValue = static_cast<KeyType>(maxMinPair.second);
    auto maxValue = static_cast<KeyType>(maxMinPair.first);
    auto &arrayTable = arg.GetArrayTable(partitionMask);
    bool *isAssigned = arrayTable->GetAssigned();
    auto slots = reinterpret_cast<int64_t *>(arrayTable->GetSlots());
    alignas(ALIGNMENT_SIZE) KeyType hashes[vecLanes];
    alignas(ALIGNMENT_SIZE) KeyType matches[vecLanes];
    if constexpr (std::is_same_v<KeyType, int16_t>) {
        int16x8_t vMin = vdupq_n_s16(minValue);
        int16x8_t vMax = vdupq_n_s16(maxValue);
        for (; probePosition < end; probePosition += vecLanes) {
            Int16ArrayCompareNeon(probeVector, hashes, matches, vMin, vMax, minValue, maxValue);
            probeVector += vecLanes;
            __builtin_prefetch(probeVector, 0, 3);

            DealWithProbeMatchResultNeon<decltype(arg), KeyType, hasJoinFilter, joinType, hasNull>(
                hashes, matches, vecLanes, probePosition, slots, isAssigned, arg, contextPtr, buildColumns);

            if (UNLIKELY(outputBuilder->IsFull())) {
                curProbePosition = probePosition + vecLanes;
                return;
            }
        }
    } else if constexpr (std::is_same_v<KeyType, int32_t>) {
        int32x4_t vMin = vdupq_n_s32(minValue);
        int32x4_t vMax = vdupq_n_s32(maxValue);
        for (; probePosition < end; probePosition += vecLanes) {
            Int32ArrayCompareNeon(probeVector, hashes, matches, vMin, vMax, minValue, maxValue);
            probeVector += vecLanes;
            __builtin_prefetch(probeVector, 0, 3);

            DealWithProbeMatchResultNeon<decltype(arg), KeyType, hasJoinFilter, joinType, hasNull>(
                hashes, matches, vecLanes, probePosition, slots, isAssigned, arg, contextPtr, buildColumns);

            if (UNLIKELY(outputBuilder->IsFull())) {
                curProbePosition = probePosition + vecLanes;
                return;
            }
        }
    } else if constexpr (std::is_same_v<KeyType, int64_t>) {
        int64x2_t vMin = vdupq_n_s64(minValue);
        int64x2_t vMax = vdupq_n_s64(maxValue);
        for (; probePosition < end; probePosition += vecLanes) {
            Int64ArrayCompareNeon(probeVector, hashes, matches, vMin, vMax, minValue, maxValue);
            probeVector += vecLanes;
            __builtin_prefetch(probeVector, 0, 3);

            DealWithProbeMatchResultNeon<decltype(arg), KeyType, hasJoinFilter, joinType, hasNull>(
                hashes, matches, vecLanes, probePosition, slots, isAssigned, arg, contextPtr, buildColumns);

            if (UNLIKELY(outputBuilder->IsFull())) {
                curProbePosition = probePosition + vecLanes;
                return;
            }
        }
    }
    curProbePosition = probePosition;
    // deal with rest of rows
    ArrayJoinProbe<decltype(arg), hasJoinFilter, joinType, hasNull>(buildColumns, probeHashColsCount, arg, contextPtr);
}

template <bool hasJoinFilter, bool singleHT> void LookupJoinOperator::ProbeBatchForInnerJoin()
{
    std::visit(
        [&](auto &&arg) {
            auto inputRowCount = curInputBatch->GetRowCount();
            auto contextPtr = executionContext.get();
            uint32_t partition = partitionMask;
            auto buildColumns = arg.GetColumns(partition);
            auto probeHashColsCount = probeHashCols.size();
            int32_t probePosition = curProbePosition;
            InitForProbe<hasJoinFilter>(partition);
            bool isInsert = true;
            typename std::decay_t<decltype(arg)>::Mapped *rowRefList = nullptr;
            // get KeyType from hashmap
            using KeyType = decltype(arg.keyType);
            KeyType cacheKeyValue;
            if constexpr (singleHT && std::remove_reference<decltype(arg)>::type::IS_SIMPLE_KEY) {
                if (probeSimd) {
                    if (probeHashColumns[0]->HasNull()) {
                        ArrayJoinProbeSIMDNeon<decltype(arg), hasJoinFilter, OMNI_JOIN_TYPE_INNER, true>(
                            buildColumns, probeHashColsCount, arg, contextPtr);
                    } else {
                        ArrayJoinProbeSIMDNeon<decltype(arg), hasJoinFilter, OMNI_JOIN_TYPE_INNER, false>(
                            buildColumns, probeHashColsCount, arg, contextPtr);
                    }
                    return;
                }
            }
            if (std::remove_reference_t<decltype(arg)>::IS_SIMPLE_KEY && !arg.GetIsMultiCols()) {
                for (; probePosition < inputRowCount; probePosition++) {
                    if (curProbeNulls[probePosition]) {
                        continue;
                    }
                    if constexpr (!singleHT) {
                        partition = HashUtil::GetRawHashPartition(curProbeHashes[probePosition], partitionMask);
                    }

                    auto result = arg.Find(probeSerializers, contextPtr, probeHashColumns, probeHashColsCount,
                                           probePosition, partition);
                    isInsert = result.IsInsert();
                    if (!isInsert) {
                        continue;
                    }
                    rowRefList = result.GetValue();
                    cacheKeyValue = arg.GetKeyValue(probeHashColumns, probePosition);
                    break;
                }
            }

            for (; probePosition < inputRowCount; probePosition++) {
                if (curProbeNulls[probePosition]) {
                    continue;
                }

                if constexpr (!singleHT) {
                    partition = HashUtil::GetRawHashPartition(curProbeHashes[probePosition], partitionMask);
                    buildColumns = arg.GetColumns(partition);
                    InitForProbe<hasJoinFilter>(partition, false);
                }
                // only support single probe column
                if (std::remove_reference_t<decltype(arg)>::IS_SIMPLE_KEY && !arg.GetIsMultiCols()) {
                    auto keyValue = arg.GetKeyValue(probeHashColumns, probePosition);
                    if (keyValue != cacheKeyValue) {
                        cacheKeyValue = keyValue;
                        auto result = arg.Find(probeSerializers, contextPtr, probeHashColumns, probeHashColsCount,
                            probePosition, partition);
                        isInsert = result.IsInsert();
                        if (!isInsert) {
                            continue;
                        }
                        rowRefList = result.GetValue();
                    } else {
                        if (!isInsert) {
                            continue;
                        }
                    }
                } else {
                    auto result = arg.Find(probeSerializers, contextPtr, probeHashColumns, probeHashColsCount,
                        probePosition, partition);
                    isInsert = result.IsInsert();
                    if (!isInsert) {
                        continue;
                    }
                    rowRefList = result.GetValue();
                }

                // probe matched in hash table
                auto it = rowRefList->Begin();
                ProbeJoinPosition<hasJoinFilter>(probePosition);

                while (it.IsOk()) {
                    auto address = LookupJoinOutputBuilder::EncodeAddress(it->rowIdx, it->vecBatchIdx);
                    if constexpr (hasJoinFilter) {
                        auto filterResult = BuildJoinPosition(partition, it->rowIdx, it->vecBatchIdx, contextPtr);
                        if (filterResult) {
                            outputBuilder->AppendRow(probePosition, buildColumns, address);
                        }
                    } else {
                        outputBuilder->AppendRow(probePosition, buildColumns, address);
                    }
                    ++it;
                }

                if (outputBuilder->IsFull()) {
                    curProbePosition = probePosition + 1;
                    return;
                }
            }
            curProbePosition = inputRowCount;
        },
        *hashTables);
}

template <bool hasJoinFilter, bool singleHT> void LookupJoinOperator::ProbeBatchForOppositeSideOuterJoin()
{
    std::visit(
        [&](auto &&arg) {
            auto inputRowCount = curInputBatch->GetRowCount();
            auto contextPtr = executionContext.get();
            uint32_t partition = partitionMask;
            auto buildColumns = arg.GetColumns(partition);
            auto probeHashColsCount = probeHashCols.size();
            InitForProbe<hasJoinFilter>(partition);
            if constexpr (singleHT && std::remove_reference<decltype(arg)>::type::IS_SIMPLE_KEY) {
                if (probeSimd) {
                    // OMNI_JOIN_TYPE_LEFT and OMNI_JOIN_TYPE_RIGHT use same logical ProcessProbe
                    // So we use OMNI_JOIN_TYPE_LEFT for OppositeSideOuterJoin
                    if (probeHashColumns[0]->HasNull()) {
                        ArrayJoinProbeSIMDNeon<decltype(arg), hasJoinFilter, OMNI_JOIN_TYPE_LEFT, true>(
                            buildColumns, probeHashColsCount, arg, contextPtr);
                    } else {
                        ArrayJoinProbeSIMDNeon<decltype(arg), hasJoinFilter, OMNI_JOIN_TYPE_LEFT, false>(
                            buildColumns, probeHashColsCount, arg, contextPtr);
                    }
                    return;
                }
            }
            for (int32_t probePosition = curProbePosition; probePosition < inputRowCount; probePosition++) {
                if (curProbeNulls[probePosition]) {
                    outputBuilder->AppendRow(probePosition, nullptr, 0);
                    continue;
                }

                if constexpr (!singleHT) {
                    partition = HashUtil::GetRawHashPartition(curProbeHashes[probePosition], partitionMask);
                    buildColumns = arg.GetColumns(partition);
                    InitForProbe<hasJoinFilter>(partition, false);
                }
                bool hasProduceRow = false;
                auto result = arg.Find(probeSerializers, contextPtr, probeHashColumns, probeHashColsCount,
                    probePosition, partition);
                if (result.IsInsert()) {
                    // probe matched in hash table
                    auto it = result.GetValue()->Begin();
                    ProbeJoinPosition<hasJoinFilter>(probePosition);
                    while (it.IsOk()) {
                        auto address = LookupJoinOutputBuilder::EncodeAddress(it->rowIdx, it->vecBatchIdx);
                        if constexpr (hasJoinFilter) {
                            auto filterResult = BuildJoinPosition(partition, it->rowIdx, it->vecBatchIdx, contextPtr);
                            if (filterResult) {
                                outputBuilder->AppendRow(probePosition, buildColumns, address);
                                hasProduceRow = true;
                            }
                        } else {
                            outputBuilder->AppendRow(probePosition, buildColumns, address);
                            hasProduceRow = true;
                        }
                        ++it;
                    }
                }
                if (!hasProduceRow) {
                    outputBuilder->AppendRow(probePosition, nullptr, 0);
                }
                if (outputBuilder->IsFull()) {
                    curProbePosition = probePosition + 1;
                    return;
                }
            }
            curProbePosition = inputRowCount;
        },
        *hashTables);
}

template <bool hasJoinFilter, bool singleHT> void LookupJoinOperator::ProbeBatchForSameSideOuterJoin()
{
    std::visit(
        [&](auto &&arg) {
            using Mapped = typename std::decay_t<decltype(arg)>::Mapped;
            if constexpr (std::is_same_v<Mapped, RowRefListWithFlags>) {
                auto inputRowCount = curInputBatch->GetRowCount();
                auto contextPtr = executionContext.get();
                uint32_t partition = partitionMask;
                auto buildColumns = arg.GetColumns(partition);
                auto probeHashColsCount = probeHashCols.size();
                InitForProbe<hasJoinFilter>(partition);
                if constexpr (singleHT && std::remove_reference<decltype(arg)>::type::IS_SIMPLE_KEY) {
                    if (probeSimd) {
                        // OMNI_JOIN_TYPE_RIGHT and OMNI_JOIN_TYPE_RIGHT use same logical ProcessProbe
                        // So we use OMNI_JOIN_TYPE_RIGHT for SameSideOuterJoin
                        if (probeHashColumns[0]->HasNull()) {
                            ArrayJoinProbeSIMDNeon<decltype(arg), hasJoinFilter, OMNI_JOIN_TYPE_RIGHT, true>(
                                buildColumns, probeHashColsCount, arg, contextPtr);
                        } else {
                            ArrayJoinProbeSIMDNeon<decltype(arg), hasJoinFilter, OMNI_JOIN_TYPE_RIGHT, false>(
                                buildColumns, probeHashColsCount, arg, contextPtr);
                        }
                        return;
                    }
                }
                for (int32_t probePosition = curProbePosition; probePosition < inputRowCount; probePosition++) {
                    if (curProbeNulls[probePosition]) {
                        continue;
                    }

                    if constexpr (!singleHT) {
                        partition = HashUtil::GetRawHashPartition(curProbeHashes[probePosition], partitionMask);
                        buildColumns = arg.GetColumns(partition);
                        InitForProbe<hasJoinFilter>(partition, false);
                    }

                    auto result = arg.Find(probeSerializers, contextPtr, probeHashColumns, probeHashColsCount,
                        probePosition, partition);
                    if (result.IsInsert()) {
                        auto it = result.GetValue()->Begin();
                        ProbeJoinPosition<hasJoinFilter>(probePosition);
                        while (it.IsOk()) {
                            auto address = LookupJoinOutputBuilder::EncodeAddress(it->rowIdx, it->vecBatchIdx);
                            if constexpr (hasJoinFilter) {
                                auto filterResult =
                                    BuildJoinPosition(partition, it->rowIdx, it->vecBatchIdx, contextPtr);
                                if (filterResult) {
                                    outputBuilder->AppendRow(probePosition, buildColumns, address);
                                    arg.PositionVisited(it);
                                }
                            } else {
                                outputBuilder->AppendRow(probePosition, buildColumns, address);
                                arg.PositionVisited(it);
                            }
                            ++it;
                        }
                    }
                    if (outputBuilder->IsFull()) {
                        curProbePosition = probePosition + 1;
                        return;
                    }
                }
                curProbePosition = inputRowCount;
            }
        },
        *hashTables);
}

template <bool hasJoinFilter, bool singleHT> void LookupJoinOperator::ProbeBatchForFullJoin()
{
    std::visit(
        [&](auto &&arg) {
            using Mapped = typename std::decay_t<decltype(arg)>::Mapped;
            if constexpr (std::is_same_v<Mapped, RowRefListWithFlags>) {
                auto inputRowCount = curInputBatch->GetRowCount();
                auto contextPtr = executionContext.get();
                uint32_t partition = partitionMask;
                auto buildColumns = arg.GetColumns(partition);
                auto probeHashColsCount = probeHashCols.size();
                InitForProbe<hasJoinFilter>(partition);
                if constexpr (singleHT && std::remove_reference<decltype(arg)>::type::IS_SIMPLE_KEY) {
                    if (probeSimd) {
                        if (probeHashColumns[0]->HasNull()) {
                            ArrayJoinProbeSIMDNeon<decltype(arg), hasJoinFilter, OMNI_JOIN_TYPE_FULL, true>(
                                buildColumns, probeHashColsCount, arg, contextPtr);
                        } else {
                            ArrayJoinProbeSIMDNeon<decltype(arg), hasJoinFilter, OMNI_JOIN_TYPE_FULL, false>(
                                buildColumns, probeHashColsCount, arg, contextPtr);
                        }
                        return;
                    }
                }
                for (int32_t probePosition = curProbePosition; probePosition < inputRowCount; probePosition++) {
                    if (curProbeNulls[probePosition]) {
                        outputBuilder->AppendRow(probePosition, nullptr, 0);
                        continue;
                    }
                    if constexpr (!singleHT) {
                        partition = HashUtil::GetRawHashPartition(curProbeHashes[probePosition], partitionMask);
                        buildColumns = arg.GetColumns(partition);
                        InitForProbe<hasJoinFilter>(partition, false);
                    }
                    bool hasProduceRow = false;
                    auto result = arg.Find(probeSerializers, contextPtr, probeHashColumns, probeHashColsCount,
                                           probePosition, partition);
                    if (result.IsInsert()) {
                        // probe matched in hash table
                        auto it = result.GetValue()->Begin();
                        ProbeJoinPosition<hasJoinFilter>(probePosition);

                        while (it.IsOk()) {
                            bool appendRow = true;
                            if constexpr (hasJoinFilter) {
                                appendRow = BuildJoinPosition(partition, it->rowIdx, it->vecBatchIdx, contextPtr);
                            }
                            if (appendRow) {
                                auto address = LookupJoinOutputBuilder::EncodeAddress(it->rowIdx, it->vecBatchIdx);
                                outputBuilder->AppendRow(probePosition, buildColumns, address);
                                arg.PositionVisited(it);
                                hasProduceRow = true;
                            }
                            ++it;
                        }
                    }
                    if (!hasProduceRow) {
                        outputBuilder->AppendRow(probePosition, nullptr, 0);
                    }

                    if (outputBuilder->IsFull()) {
                        curProbePosition = probePosition + 1;
                        return;
                    }
                }
                curProbePosition = inputRowCount;
            }
        },
        *hashTables);
}

template <bool hasJoinFilter, bool singleHT> void LookupJoinOperator::ProbeBatchForLeftSemiJoin()
{
    std::visit(
        [&](auto &&arg) {
            auto inputRowCount = curInputBatch->GetRowCount();
            auto contextPtr = executionContext.get();
            uint32_t partition = partitionMask;
            auto buildColumns = arg.GetColumns(partition);
            auto probeHashColsCount = probeHashCols.size();
            InitForProbe<hasJoinFilter>(partition);
            if constexpr (singleHT && std::remove_reference<decltype(arg)>::type::IS_SIMPLE_KEY) {
                if (probeSimd) {
                    if (probeHashColumns[0]->HasNull()) {
                        ArrayJoinProbeSIMDNeon<decltype(arg), hasJoinFilter, OMNI_JOIN_TYPE_LEFT_SEMI, true>(
                            buildColumns, probeHashColsCount, arg, contextPtr);
                    } else {
                        ArrayJoinProbeSIMDNeon<decltype(arg), hasJoinFilter, OMNI_JOIN_TYPE_LEFT_SEMI, false>(
                            buildColumns, probeHashColsCount, arg, contextPtr);
                    }
                    return;
                }
            }
            for (int32_t probePosition = curProbePosition; probePosition < inputRowCount; probePosition++) {
                if (curProbeNulls[probePosition]) {
                    continue;
                }
                if constexpr (!singleHT) {
                    partition = HashUtil::GetRawHashPartition(curProbeHashes[probePosition], partitionMask);
                    buildColumns = arg.GetColumns(partition);
                    InitForProbe<hasJoinFilter>(partition, false);
                }

                auto result = arg.Find(probeSerializers, contextPtr, probeHashColumns, probeHashColsCount,
                    probePosition, partition);
                if (!result.IsInsert()) {
                    continue;
                }
                // probe matched in hash table
                auto it = result.GetValue()->Begin();
                ProbeJoinPosition<hasJoinFilter>(probePosition);

                while (it.IsOk()) {
                    auto address = LookupJoinOutputBuilder::EncodeAddress(it->rowIdx, it->vecBatchIdx);
                    if constexpr (hasJoinFilter) {
                        auto filterResult = BuildJoinPosition(partition, it->rowIdx, it->vecBatchIdx, contextPtr);
                        if (filterResult) {
                            outputBuilder->AppendRow(probePosition, buildColumns, address);
                            break;
                        }
                    } else {
                        outputBuilder->AppendRow(probePosition, buildColumns, address);
                        break;
                    }
                    ++it;
                }

                if (outputBuilder->IsFull()) {
                    curProbePosition = probePosition + 1;
                    return;
                }
            }
            curProbePosition = inputRowCount;
        },
        *hashTables);
}

template <bool hasJoinFilter, bool singleHT> void LookupJoinOperator::ProbeBatchForLeftAntiJoin()
{
    std::visit(
        [&](auto &&arg) {
            auto inputRowCount = curInputBatch->GetRowCount();
            auto contextPtr = executionContext.get();
            uint32_t partition = partitionMask;
            auto probeHashColsCount = probeHashCols.size();
            InitForProbe<hasJoinFilter>(partition);
            if constexpr (singleHT && std::remove_reference<decltype(arg)>::type::IS_SIMPLE_KEY) {
                if (probeSimd) {
                    if (probeHashColumns[0]->HasNull()) {
                        ArrayJoinProbeSIMDNeon<decltype(arg), hasJoinFilter, OMNI_JOIN_TYPE_LEFT_ANTI, true>(
                            nullptr, probeHashColsCount, arg, contextPtr);
                    } else {
                        ArrayJoinProbeSIMDNeon<decltype(arg), hasJoinFilter, OMNI_JOIN_TYPE_LEFT_ANTI, false>(
                            nullptr, probeHashColsCount, arg, contextPtr);
                    }
                    return;
                }
            }
            for (int32_t probePosition = curProbePosition; probePosition < inputRowCount; probePosition++) {
                if (curProbeNulls[probePosition]) {
                    outputBuilder->AppendRow(probePosition, nullptr, 0);
                    continue;
                }

                if constexpr (!singleHT) {
                    partition = HashUtil::GetRawHashPartition(curProbeHashes[probePosition], partitionMask);
                    InitForProbe<hasJoinFilter>(partition, false);
                }
                bool hasProduceRow = false;
                auto result = arg.Find(probeSerializers, contextPtr, probeHashColumns, probeHashColsCount,
                    probePosition, partition);
                if (result.IsInsert()) {
                    // probe matched in hash table
                    auto it = result.GetValue()->Begin();
                    ProbeJoinPosition<hasJoinFilter>(probePosition);

                    while (it.IsOk()) {
                        if constexpr (hasJoinFilter) {
                            auto filterResult = BuildJoinPosition(partition, it->rowIdx, it->vecBatchIdx, contextPtr);
                            if (filterResult) {
                                hasProduceRow = true;
                                break;
                            }
                        } else {
                            hasProduceRow = true;
                            break;
                        }
                        ++it;
                    }
                }

                if (!hasProduceRow) {
                    outputBuilder->AppendRow(probePosition, nullptr, 0);
                }

                if (outputBuilder->IsFull()) {
                    curProbePosition = probePosition + 1;
                    return;
                }
            }
            curProbePosition = inputRowCount;
        },
        *hashTables);
}

template <bool hasJoinFilter, bool singleHT> void LookupJoinOperator::ProbeBatchForExistenceJoin()
{
    std::visit(
        [&](auto &&arg) {
            auto inputRowCount = curInputBatch->GetRowCount();
            auto contextPtr = executionContext.get();
            uint32_t partition = partitionMask;
            auto probeHashColsCount = probeHashCols.size();
            InitForProbe<hasJoinFilter>(partition);
            for (int32_t probePosition = curProbePosition; probePosition < inputRowCount; probePosition++) {
                if (curProbeNulls[probePosition]) {
                    outputBuilder->AppendExistenceRow<false>(probePosition);
                    continue;
                }
                if constexpr (!singleHT) {
                    partition = HashUtil::GetRawHashPartition(curProbeHashes[probePosition], partitionMask);
                    InitForProbe<hasJoinFilter>(partition);
                }
                auto result = arg.Find(probeSerializers, contextPtr, probeHashColumns, probeHashColsCount,
                                       probePosition, partition);
                bool hasProduceRow = false;
                if (result.IsInsert()) {
                    // probe matched in hash table
                    auto it = result.GetValue()->Begin();
                    ProbeJoinPosition<hasJoinFilter>(probePosition);
                    while (it.IsOk()) {
                        if constexpr (hasJoinFilter) {
                            hasProduceRow = BuildJoinPosition(partition, it->rowIdx, it->vecBatchIdx, contextPtr);
                            if (hasProduceRow) {
                                outputBuilder->AppendExistenceRow<true>(probePosition);
                                break;
                            }
                        } else {
                            hasProduceRow = true;
                            outputBuilder->AppendExistenceRow<true>(probePosition);
                            break;
                        }
                        ++it;
                    }
                }

                if (!hasProduceRow) {
                    outputBuilder->AppendExistenceRow<false>(probePosition);
                }

                // if the output row count exceeds the maxRowCount
                // then construct output to avoid probeIndex and buildIndex
                // consume excessive memory
                if (outputBuilder->IsFull()) {
                    curProbePosition = probePosition + 1;
                    return;
                }
            }
            curProbePosition = inputRowCount;
        },
        *hashTables);
}

template <bool hasJoinFilter>
void ALWAYS_INLINE LookupJoinOperator::ProbeJoinPosition(int32_t probePosition)
{
    if constexpr (hasJoinFilter) {
        for (size_t j = 0; j < probeFilterColsSize; ++j) {
            uint32_t colIdx = probeFilterCols[j];
            auto probeVec = probeFilterColumns[j];
            nulls[colIdx] = probeVec->IsNull(probePosition);
            values[colIdx] =
                OperatorUtil::GetValuePtrAndLength(probeVec,
                probePosition, lengths + colIdx, probeFilterTypeIds[j]);
        }
    }
}

bool ALWAYS_INLINE LookupJoinOperator::BuildJoinPosition(
    uint32_t partition, uint32_t buildRowIdx, uint32_t buildBatchIdx, ExecutionContext *contextPtr)
{
    for (size_t j = 0; j < buildFilterColsSize; ++j) {
        uint32_t colIdx = buildFilterCols[j];
        auto buildVec = (*buildFilterColPtrs)[j][buildBatchIdx];
        nulls[colIdx] = buildVec->IsNull(buildRowIdx);
        values[colIdx] =
            OperatorUtil::GetValuePtrAndLength(buildVec, buildRowIdx, lengths + colIdx, buildFilterTypeIds[j]);
    }
    return simpleFilter->Evaluate(values, nulls, lengths, reinterpret_cast<int64_t>(contextPtr));
}

template <typename T>
void CalculateColHashes(BaseVector *vec, int32_t rowCount, int64_t *hashes, std::vector<int8_t> &nulls)
{
    int64_t hash;
    if (vec->GetEncoding() != OMNI_DICTIONARY) {
        for (int32_t i = 0; i < rowCount; ++i) {
            if (vec->IsNull(i)) {
                nulls[i] = 1;
                continue;
            }
            hash = HashUtil::HashValue(static_cast<Vector<T> *>(vec)->GetValue(i));
            hashes[i] = HashUtil::CombineHash(hashes[i], hash);
        }
    } else {
        for (int32_t i = 0; i < rowCount; ++i) {
            if (vec->IsNull(i)) {
                nulls[i] = 1;
                continue;
            }
            hash = HashUtil::HashValue(static_cast<Vector<DictionaryContainer<T>> *>(vec)->GetValue(i));
            hashes[i] = HashUtil::CombineHash(hashes[i], hash);
        }
    }
}

void CalculateColDec64Hashes(BaseVector *vec, int32_t rowCount, int64_t *hashes, std::vector<int8_t> &nulls)
{
    int64_t hash;
    if (vec->GetEncoding() != OMNI_DICTIONARY) {
        for (int32_t i = 0; i < rowCount; ++i) {
            if (vec->IsNull(i)) {
                nulls[i] = 1;
                continue;
            }
            hash = HashUtil::HashDecimal64Value(static_cast<Vector<int64_t> *>(vec)->GetValue(i));
            hashes[i] = HashUtil::CombineHash(hashes[i], hash);
        }
    } else {
        for (int32_t i = 0; i < rowCount; ++i) {
            if (vec->IsNull(i)) {
                nulls[i] = 1;
                continue;
            }
            hash = HashUtil::HashDecimal64Value(static_cast<Vector<DictionaryContainer<int64_t>> *>(vec)->GetValue(i));
            hashes[i] = HashUtil::CombineHash(hashes[i], hash);
        }
    }
}

void CalculateColDec128Hashes(BaseVector *vec, int32_t rowCount, int64_t *hashes, std::vector<int8_t> &nulls)
{
    int64_t hash;
    Decimal128 decimal128Value;
    if (vec->GetEncoding() != OMNI_DICTIONARY) {
        for (int32_t i = 0; i < rowCount; ++i) {
            if (vec->IsNull(i)) {
                nulls[i] = 1;
                continue;
            }
            decimal128Value = static_cast<Vector<Decimal128> *>(vec)->GetValue(i);
            hash = HashUtil::HashValue(static_cast<int64_t>(decimal128Value.LowBits()), decimal128Value.HighBits());
            hashes[i] = HashUtil::CombineHash(hashes[i], hash);
        }
    } else {
        for (int32_t i = 0; i < rowCount; ++i) {
            if (vec->IsNull(i)) {
                nulls[i] = 1;
                continue;
            }
            decimal128Value = static_cast<Vector<DictionaryContainer<Decimal128>> *>(vec)->GetValue(i);
            hash = HashUtil::HashValue(static_cast<int64_t>(decimal128Value.LowBits()), decimal128Value.HighBits());
            hashes[i] = HashUtil::CombineHash(hashes[i], hash);
        }
    }
}

void CalculateColVarcharHashes(BaseVector *vec, int32_t rowCount, int64_t *hashes, std::vector<int8_t> &nulls)
{
    int64_t hash;
    if (vec->GetEncoding() != OMNI_DICTIONARY) {
        for (int32_t i = 0; i < rowCount; ++i) {
            if (vec->IsNull(i)) {
                nulls[i] = 1;
                continue;
            }
            auto varcharValue = static_cast<Vector<LargeStringContainer<std::string_view>> *>(vec)->GetValue(i);
            hash = HashUtil::HashValue(reinterpret_cast<int8_t *>(const_cast<char *>(varcharValue.data())),
                static_cast<int32_t>(varcharValue.length()));
            hashes[i] = HashUtil::CombineHash(hashes[i], hash);
        }
    } else {
        for (int32_t i = 0; i < rowCount; ++i) {
            if (vec->IsNull(i)) {
                nulls[i] = 1;
                continue;
            }
            auto varcharValue = static_cast<Vector<DictionaryContainer<std::string_view>> *>(vec)->GetValue(i);
            hash = HashUtil::HashValue(reinterpret_cast<int8_t *>(const_cast<char *>(varcharValue.data())),
                static_cast<int32_t>(varcharValue.length()));
            hashes[i] = HashUtil::CombineHash(hashes[i], hash);
        }
    }
}

void ALWAYS_INLINE LookupJoinOperator::PopulateProbeHashes()
{
    int32_t rowCount = curInputBatch->GetRowCount();
    int64_t *hashes = curProbeHashes.data();
    auto probeHashColsCount = probeHashCols.size();

    for (size_t j = 0; j < probeHashColsCount; ++j) {
        BaseVector *hashCol = probeHashColumns[j];
        switch (probeHashColTypes[j]) {
            case omniruntime::type::OMNI_INT:
            case omniruntime::type::OMNI_DATE32:
                CalculateColHashes<int32_t>(hashCol, rowCount, hashes, curProbeNulls);
                break;
            case omniruntime::type::OMNI_SHORT:
                CalculateColHashes<int16_t>(hashCol, rowCount, hashes, curProbeNulls);
                break;
            case omniruntime::type::OMNI_TIMESTAMP:
            case omniruntime::type::OMNI_LONG:
                CalculateColHashes<int64_t>(hashCol, rowCount, hashes, curProbeNulls);
                break;
            case omniruntime::type::OMNI_DOUBLE:
                CalculateColHashes<double>(hashCol, rowCount, hashes, curProbeNulls);
                break;
            case omniruntime::type::OMNI_BOOLEAN:
                CalculateColHashes<bool>(hashCol, rowCount, hashes, curProbeNulls);
                break;
            case omniruntime::type::OMNI_DECIMAL64:
                CalculateColDec64Hashes(hashCol, rowCount, hashes, curProbeNulls);
                break;
            case omniruntime::type::OMNI_DECIMAL128:
                CalculateColDec128Hashes(hashCol, rowCount, hashes, curProbeNulls);
                break;
            case omniruntime::type::OMNI_VARCHAR:
            case omniruntime::type::OMNI_CHAR:
                CalculateColVarcharHashes(hashCol, rowCount, hashes, curProbeNulls);
                break;
            default:
                break;
        }
    }
}

void ALWAYS_INLINE LookupJoinOperator::PopulateProbeNulls()
{
    int32_t rowCount = curInputBatch->GetRowCount();
    auto probeHashColsCount = probeHashCols.size();
    for (size_t j = 0; j < probeHashColsCount; ++j) {
        auto probeHashColumn = probeHashColumns[j];
        if (probeHashColumn->HasNull()) {
            for (int32_t i = 0; i < rowCount; ++i) {
                curProbeNulls[i] |= probeHashColumn->IsNull(i);
            }
        }
    }
}

LookupJoinOutputBuilder::LookupJoinOutputBuilder(std::vector<int32_t> &probeOutputCols, const int32_t *probeOutputTypes,
    std::vector<int32_t> &buildOutputCols, const int32_t *buildOutputTypes, int32_t outputRowSize)
    : probeOutputCols(probeOutputCols),
      probeOutputTypes(probeOutputTypes),
      buildOutputCols(buildOutputCols),
      buildOutputTypes(buildOutputTypes)
{
    // if the probe and build do not have output columns, the row size is setted to DEFAULT_ROW_SIZE
    this->maxRowCount = OperatorUtil::GetMaxRowCount((outputRowSize != 0) ? outputRowSize : DEFAULT_ROW_SIZE);
    if (!probeOutputCols.empty() || !buildOutputCols.empty()) {
        probeBuildIndex.reserve(maxRowCount);
    }
}

LookupJoinOutputBuilder::LookupJoinOutputBuilder(std::vector<int32_t> &probeOutputCols, const int32_t *probeOutputTypes,
    std::vector<int32_t> &buildOutputCols, const int32_t *buildOutputTypes, int32_t outputRowSize, std::vector<int32_t> &outputList)
    : probeOutputCols(probeOutputCols),
      probeOutputTypes(probeOutputTypes),
      buildOutputCols(buildOutputCols),
      buildOutputTypes(buildOutputTypes),
      outputList(outputList)
{
    // if the probe and build do not have output columns, the row size is setted to DEFAULT_ROW_SIZE
    this->maxRowCount = OperatorUtil::GetMaxRowCount((outputRowSize != 0) ? outputRowSize : DEFAULT_ROW_SIZE);
    if (!probeOutputCols.empty() || !buildOutputCols.empty()) {
        probeBuildIndex.reserve(maxRowCount);
    }
}

void NO_INLINE LookupJoinOutputBuilder::AppendRow(int32_t probePosition, BaseVector ***array, uint64_t address)
{
    probeRowCount++;
    auto rowIdx = LookupJoinOutputBuilder::DecodeRowId(address);
    auto vecBatchIdx = LookupJoinOutputBuilder::DecodeVectorBatchId(address);
    if (!probeOutputCols.empty() || !buildOutputCols.empty()) {
        probeBuildIndex.emplace_back(probePosition, array, vecBatchIdx, rowIdx);
    }
}

template <typename T, bool isInnerJoin, bool isShuffleExchangeBuildPlan>
static NO_INLINE BaseVector *ConstructBuildColumn(
    const std::tuple<int32_t, BaseVector ***, uint32_t, uint32_t> *buildTemp, uint32_t outputCol, int32_t numRows,
    int8_t parallelism)
{
    auto ret = std::make_unique<Vector<T>>(numRows);
    T value;
    uint32_t preVecBatchIdx = 0;
    T rowIdxes[parallelism];
    T getResult[parallelism];
    int8_t parallelNum = 0;
    const ScalableTag<T> d;
    BaseVector *preVector = nullptr;
    for (int32_t i = 0; i < numRows; ++i) {
        BaseVector ***array = std::get<1>(buildTemp[i]);
        auto vecBatchIdx = std::get<2>(buildTemp[i]);
        auto rowIdx = std::get<3>(buildTemp[i]);
        bool nullFlag;
        if constexpr (!isInnerJoin) {
            nullFlag = array == nullptr || array[outputCol][vecBatchIdx]->IsNull(rowIdx);
        } else {
            nullFlag = array[outputCol][vecBatchIdx]->IsNull(rowIdx);
        }
        if (nullFlag) {
            ret->SetNull(i);
            for (int32_t j = 0; j < parallelNum; ++j) {
                auto currRow = i + j - parallelNum;
                if (!(ret->IsNull(currRow))) {
                    if constexpr (isShuffleExchangeBuildPlan) {
                        value = static_cast<Vector<T> *>(preVector)->GetValue(rowIdxes[j]);
                    } else {
                        if (preVector->GetEncoding() == OMNI_DICTIONARY) {
                            value = static_cast<Vector<DictionaryContainer<T>> *>(preVector)->GetValue(rowIdxes[j]);
                        } else {
                            value = static_cast<Vector<T> *>(preVector)->GetValue(rowIdxes[j]);
                        }
                    }
                    ret->SetValue(currRow, value);
                }
            }
            parallelNum = 0;
            preVector = array == nullptr ? nullptr : array[outputCol][vecBatchIdx];
            continue;
        }

        BaseVector *buildVector = array[outputCol][vecBatchIdx];
        if (vecBatchIdx == preVecBatchIdx) {
            rowIdxes[parallelNum] = rowIdx;
            parallelNum++;
            if (parallelNum == parallelism) {
                T *buildVecSrc;
                if (buildVector->GetEncoding() == OMNI_DICTIONARY) {
                    auto dictionaryVector = static_cast<Vector<DictionaryContainer<T>> *>(buildVector);
                    buildVecSrc = unsafe::UnsafeDictionaryVector::GetDictionary(dictionaryVector);
                    int *ids = unsafe::UnsafeDictionaryVector::GetIds(dictionaryVector);
                    for (int j = 0; j < parallelNum; ++j) {
                        rowIdxes[j] = ids[rowIdxes[j]];
                    }
                } else {
                    buildVecSrc = unsafe::UnsafeVector::GetRawValues(static_cast<Vector<T> *>(buildVector));
                }
                auto result = GatherIndex(d, buildVecSrc, LoadU(d, rowIdxes));
                Store(result, d, getResult);
                ret->SetValues(i - parallelNum + 1, static_cast<void *>(getResult), parallelNum);
                parallelNum = 0;
            }
        } else {
            // Processing the data of the remaining rowIdxes of the previous vecBatch during cross-vecBatch.
            for (int32_t j = 0; j < parallelNum; ++j) {
                auto currRow = i + j - parallelNum;
                if (!(ret->IsNull(currRow))) {
                    if constexpr (isShuffleExchangeBuildPlan) {
                        value = static_cast<Vector<T> *>(preVector)->GetValue(rowIdxes[j]);
                    } else {
                        if (preVector->GetEncoding() == OMNI_DICTIONARY) {
                            value = static_cast<Vector<DictionaryContainer<T>> *>(preVector)->GetValue(rowIdxes[j]);
                        } else {
                            value = static_cast<Vector<T> *>(preVector)->GetValue(rowIdxes[j]);
                        }
                    }
                    ret->SetValue(currRow, value);
                }
            }
            parallelNum = 0;
            rowIdxes[parallelNum] = rowIdx;
            parallelNum++;
        }
        preVecBatchIdx = vecBatchIdx;
        preVector = buildVector;
    }

    // When the last row is traversed and the number of data is less than one instruction, the value is
    // assigned cyclically.
    if (parallelNum != 0) {
        for (int32_t j = 0; j < parallelNum; ++j) {
            auto currRow = numRows - parallelNum + j;
            if (!(ret->IsNull(currRow))) {
                if constexpr (isShuffleExchangeBuildPlan) {
                    value = static_cast<Vector<T> *>(preVector)->GetValue(rowIdxes[j]);
                } else {
                    if (preVector->GetEncoding() == OMNI_DICTIONARY) {
                        value = static_cast<Vector<DictionaryContainer<T>> *>(preVector)->GetValue(rowIdxes[j]);
                    } else {
                        value = static_cast<Vector<T> *>(preVector)->GetValue(rowIdxes[j]);
                    }
                }
                ret->SetValue(currRow, value);
            }
        }
        parallelNum = 0;
    }
    return ret.release();
}

template <typename T, bool isInnerJoin, bool isShuffleExchangeBuildPlan>
static NO_INLINE BaseVector *ConstructBuildColumn(
    const std::tuple<int32_t, BaseVector ***, uint32_t, uint32_t> *buildTemp, uint32_t outputCol, int32_t numRows)
{
    auto ret = new Vector<T>(numRows);
    T value;
    for (int32_t i = 0; i < numRows; ++i) {
        BaseVector ***array = std::get<1>(buildTemp[i]);
        if constexpr (!isInnerJoin) {
            if (array == nullptr) {
                ret->SetNull(i);
                continue;
            }
        }
        auto vecBatchIndex = std::get<2>(buildTemp[i]);
        auto buildRowIdx = std::get<3>(buildTemp[i]);
        BaseVector *buildVector = array[outputCol][vecBatchIndex];
        if (buildVector->IsNull(buildRowIdx)) {
            ret->SetNull(i);
            continue;
        }
        if constexpr (isShuffleExchangeBuildPlan) {
            value = static_cast<Vector<T> *>(buildVector)->GetValue(buildRowIdx);
        } else {
            if (buildVector->GetEncoding() == OMNI_DICTIONARY) {
                value = static_cast<Vector<DictionaryContainer<T>> *>(buildVector)->GetValue(buildRowIdx);
            } else {
                value = static_cast<Vector<T> *>(buildVector)->GetValue(buildRowIdx);
            }
        }
        ret->SetValue(i, value);
    }
    return ret;
}

template<bool isInnerJoin, bool isShuffleExchangeBuildPlan>
static NO_INLINE BaseVector *ConstructBuildVarcharColumn(
    const std::tuple<int32_t, BaseVector ***, uint32_t, uint32_t> *buildTemp, uint32_t outputCol, int32_t numRows)
{
    using VarcharVector = Vector<LargeStringContainer<std::string_view>>;
    using DictionaryVector = Vector<DictionaryContainer<std::string_view>>;
    auto *ret = new VarcharVector(numRows);
    std::string_view value;
    for (int32_t i = 0; i < numRows; ++i) {
        BaseVector ***array = std::get<1>(buildTemp[i]);
        if constexpr (!isInnerJoin) {
            if (array == nullptr) {
                static_cast<VarcharVector *>(ret)->SetNull(i);
                continue;
            }
        }
        auto vecBatchIndex = std::get<2>(buildTemp[i]);
        auto buildRowIdx = std::get<3>(buildTemp[i]);
        auto buildVector = array[outputCol][vecBatchIndex];
        if (buildVector->IsNull(buildRowIdx)) {
            ret->SetNull(i);
            continue;
        }
        if constexpr (isShuffleExchangeBuildPlan) {
            value = static_cast<VarcharVector *>(buildVector)->GetValue(buildRowIdx);
        } else {
            if (buildVector->GetEncoding() == OMNI_DICTIONARY) {
                value = static_cast<DictionaryVector *>(buildVector)->GetValue(buildRowIdx);
            } else {
                value = static_cast<VarcharVector *>(buildVector)->GetValue(buildRowIdx);
            }
        }
        ret->SetValue(i, value);
    }
    return ret;
}

void NO_INLINE LookupJoinOutputBuilder::ConstructProbeColumns(VectorBatch *vectorBatch, BaseVector **probeOutputColumns,
    int32_t rowCount)
{
    bool isSequentialProbeIndices = true;
    if (rowCount > 1) { // <= 1 must be sequential
        auto tmpProbeIndex = probeBuildIndex.data() + probeRowOffset;
        for (int32_t i = 1; i < rowCount; ++i) {
            if (std::get<0>(tmpProbeIndex[i]) != std::get<0>(tmpProbeIndex[i - 1]) + 1) {
                isSequentialProbeIndices = false;
                break;
            }
        }
    }

    if (!isSequentialProbeIndices || rowCount == 0) {
        // probeIndices are discrete
        ConstructProbeColumnsFromPositions(vectorBatch, probeOutputColumns, rowCount);
    } else if (rowCount == probeOutputColumns[0]->GetSize()) {
        // probeIndices are a simple covering of the vector
        ConstructProbeColumnsFromReuse(vectorBatch, probeOutputColumns, rowCount);
    } else {
        // probeIndices are sequential without holes
        ConstructProbeColumnsFromSlice(vectorBatch, probeOutputColumns, rowCount);
    }
}

template <bool isInnerJoin, bool isShuffleExchangeBuildPlan>
void NO_INLINE LookupJoinOutputBuilder::ConstructBuildColumns(VectorBatch *vectorBatch, int32_t rowCount)
{
    // preprocess the pointer to build table vectors -- doing a few levels of
    // pointer chasing first
    const std::tuple<int32_t, BaseVector ***, uint32_t, uint32_t> *buildTemp = probeBuildIndex.data() + probeRowOffset;
    for (size_t j = 0; j < buildOutputCols.size(); j++) {
        uint32_t outputCol = buildOutputCols[j];
        BaseVector *buildColumn = nullptr;
        switch (buildOutputTypes[j]) {
            case OMNI_LONG:
            case OMNI_TIMESTAMP:
            case OMNI_DECIMAL64:
                buildColumn = ConstructBuildColumn<int64_t, isInnerJoin, isShuffleExchangeBuildPlan>(buildTemp, outputCol, rowCount, OMNI_LANES(int64_t));
                break;
            case OMNI_INT:
            case OMNI_DATE32:
                buildColumn = ConstructBuildColumn<int32_t, isInnerJoin, isShuffleExchangeBuildPlan>(buildTemp, outputCol, rowCount, OMNI_LANES(int32_t));
                break;
            case OMNI_VARCHAR:
            case OMNI_CHAR:
                buildColumn = ConstructBuildVarcharColumn<isInnerJoin, isShuffleExchangeBuildPlan>(buildTemp, outputCol, rowCount);
                break;
            case OMNI_DECIMAL128:
                buildColumn = ConstructBuildColumn<Decimal128, isInnerJoin, isShuffleExchangeBuildPlan>(buildTemp, outputCol, rowCount);
                break;
            case OMNI_SHORT:
                buildColumn = ConstructBuildColumn<int16_t, isInnerJoin, isShuffleExchangeBuildPlan>(buildTemp, outputCol, rowCount);
                break;
            case OMNI_DOUBLE:
                buildColumn = ConstructBuildColumn<double, isInnerJoin, isShuffleExchangeBuildPlan>(buildTemp, outputCol, rowCount);
                break;
            case OMNI_BOOLEAN:
                buildColumn = ConstructBuildColumn<bool, isInnerJoin, isShuffleExchangeBuildPlan>(buildTemp, outputCol, rowCount);
                break;
            default:
                break;
        }
        vectorBatch->Append(buildColumn);
    }
}

void LookupJoinOutputBuilder::BuildOutput(
    BaseVector **probeOutputColumns,
    JoinType joinType,
    bool isShuffleExchangeBuildPlan,
    VectorBatch **outputVecBatch,
    BuildSide buildSide)
{
    auto rowCount = std::min(probeRowCount, maxRowCount);
    auto output = std::make_unique<VectorBatch>(rowCount);
    auto outputPtr = output.get();

    if (joinType == OMNI_JOIN_TYPE_EXISTENCE) {
        if (!probeOutputCols.empty()) {
            // only probe side will produce dic vector
            ConstructProbeColumns(outputPtr, probeOutputColumns, rowCount);
        }
        ConstructExistenceColumn(outputPtr);
    } else {
        if (!probeOutputCols.empty()) {
            // only probe side will produce dic vector
            ConstructProbeColumns(outputPtr, probeOutputColumns, rowCount);
        }
        if (!buildOutputCols.empty()) {
            bool isInnerJoin = joinType == OMNI_JOIN_TYPE_INNER;
            if (isInnerJoin && isShuffleExchangeBuildPlan) {
                ConstructBuildColumns<true, true>(outputPtr, rowCount);
            } else if (isInnerJoin) {
                ConstructBuildColumns<true, false>(outputPtr, rowCount);
            } else if (isShuffleExchangeBuildPlan) {
                ConstructBuildColumns<false, true>(outputPtr, rowCount);
            } else {
                ConstructBuildColumns<false, false>(outputPtr, rowCount);
            }
        }
    }
    if (buildSide == OMNI_BUILD_LEFT) {
    	BaseVector **pVector = output->GetVectors();
    	std::rotate(pVector, pVector + probeOutputCols.size(), pVector + output->GetVectorCount());
    }
    std::vector<int32_t> tmpVec(outputList);
    if (!tmpVec.empty()) {
    	BaseVector **pVector = output->GetVectors();
    	for (int i =0; i < tmpVec.size(); i++) {
    		if (tmpVec[i] != i) {
    			auto temp = pVector[i];
    			int src = tmpVec[i];
    			int dst = i;
    			do {
    				pVector[dst] = pVector[src];
    				tmpVec[dst] = dst;
    				dst = src;
    				src = tmpVec[dst];
    			} while (src != i);
    			pVector[dst] = temp;
    			tmpVec[dst] = dst;
    		}
    	}
    }
    probeRowOffset += rowCount;
    probeRowCount -= rowCount;
    *outputVecBatch = output.release();
}

template<bool isMatched>
void ALWAYS_INLINE LookupJoinOutputBuilder::AppendExistenceRow(int32_t probePosition)
{
    probeRowCount++;
    if (probeOutputCols.size() > 0) {
        probeBuildIndex.emplace_back(probePosition, nullptr, 0, 0);
    }
    existJoinBuildIndex.emplace_back(isMatched);
}

void NO_INLINE LookupJoinOutputBuilder::ConstructExistenceColumn(VectorBatch *vectorBatch)
{
    auto numRows = probeRowCount;
    auto ret = new Vector<bool>(numRows);
    auto values = omniruntime::vec::unsafe::UnsafeVector::GetRawValues(ret);
    std::copy(existJoinBuildIndex.begin(), existJoinBuildIndex.end(), values);
    vectorBatch->Append(ret);
}
} // end of omniruntime
