/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2024. All rights reserved.
 * @Description: lookup join implementations
 */
#include <vector>
#include <memory>
#include "operator/util/operator_util.h"
#include "vector/vector_helper.h"
#include "hash_builder.h"
#include "lookup_join.h"

using namespace omniruntime::vec;
namespace omniruntime::op {
void LookupJoinOperatorFactory::CommonInitActions(const type::DataTypes &probeTypes, int32_t *probeOutputCols,
    int32_t probeOutputColsCount, int32_t *probeHashCols, int32_t probeHashColsCount, int32_t *buildOutputCols,
    int32_t buildOutputColsCount, const type::DataTypes &buildOutputTypes)
{
    int32_t tempProbeHashColTypes[probeHashColsCount];
    auto probeTypeIds = probeTypes.GetIds();
    for (int32_t i = 0; i < probeHashColsCount; i++) {
        tempProbeHashColTypes[i] = probeTypeIds[probeHashCols[i]];
    }
    this->probeOutputCols = std::vector<int>(probeOutputCols, probeOutputCols + probeOutputColsCount);
    this->probeHashCols = std::vector<int>(probeHashCols, probeHashCols + probeHashColsCount);
    this->buildOutputCols = std::vector<int>(buildOutputCols, buildOutputCols + buildOutputColsCount);
    this->probeHashColTypes = std::vector<int32_t>(tempProbeHashColTypes, tempProbeHashColTypes + probeHashColsCount);
    this->rowSize = OperatorUtil::GetOutputRowSize(probeTypes.Get(), probeOutputCols, probeOutputColsCount);
    if (buildOutputColsCount != 0) {
        this->rowSize += OperatorUtil::GetRowSize(buildOutputTypes.Get());
    }
}

LookupJoinOperatorFactory::LookupJoinOperatorFactory(const type::DataTypes &probeTypes, int32_t *probeOutputCols,
    int32_t probeOutputColsCount, int32_t *probeHashCols, int32_t probeHashColsCount, int32_t *buildOutputCols,
    int32_t buildOutputColsCount, const type::DataTypes &buildOutputTypes, HashTableVariants *hashTables,
    omniruntime::expressions::Expr *filterExpr, OverflowConfig *overflowConfig)
    : probeTypes(probeTypes),
      buildOutputTypes(buildOutputTypes),
      hashTables(hashTables),
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
    omniruntime::expressions::Expr *filterExpr, int32_t originalProbeColsCount, OverflowConfig *overflowConfig)
    : probeTypes(probeTypes),
      buildOutputTypes(buildOutputTypes),
      hashTables(hashTables),
      originalProbeColsCount(originalProbeColsCount)
{
    CommonInitActions(probeTypes, probeOutputCols, probeOutputColsCount, probeHashCols, probeHashColsCount,
        buildOutputCols, buildOutputColsCount, buildOutputTypes);
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
    int64_t hashBuilderFactoryAddr, omniruntime::expressions::Expr *filterExpr, OverflowConfig *overflowConfig)
{
    auto hashBuilderFactory = reinterpret_cast<HashBuilderOperatorFactory *>(hashBuilderFactoryAddr);
    auto pOperatorFactory = new LookupJoinOperatorFactory(probeTypes, probeOutputCols, probeOutputColsCount,
        probeHashCols, probeHashColsCount, buildOutputCols, buildOutputColsCount, buildOutputTypes,
        hashBuilderFactory->GetHashTablesVariants(), filterExpr, overflowConfig);
    return pOperatorFactory;
}

LookupJoinOperatorFactory *LookupJoinOperatorFactory::CreateLookupJoinOperatorFactory(const DataTypes &probeTypes,
    int32_t *probeOutputCols, int32_t probeOutputColsCount, int32_t *probeHashCols, int32_t probeHashColsCount,
    int32_t *buildOutputCols, int32_t buildOutputColsCount, const DataTypes &buildOutputTypes,
    int64_t hashBuilderFactoryAddr, omniruntime::expressions::Expr *filterExpr, int32_t originalProbeColsCount,
    OverflowConfig *overflowConfig)
{
    auto hashBuilderFactory = reinterpret_cast<HashBuilderOperatorFactory *>(hashBuilderFactoryAddr);
    auto pOperatorFactory = new LookupJoinOperatorFactory(probeTypes, probeOutputCols, probeOutputColsCount,
        probeHashCols, probeHashColsCount, buildOutputCols, buildOutputColsCount, buildOutputTypes,
        hashBuilderFactory->GetHashTablesVariants(), filterExpr, originalProbeColsCount, overflowConfig);
    return pOperatorFactory;
}

Operator *LookupJoinOperatorFactory::CreateOperator()
{
    auto pLookupJoinOperator = new LookupJoinOperator(probeTypes, probeOutputCols, probeHashCols, probeHashColTypes,
        buildOutputCols, buildOutputTypes, hashTables, simpleFilter, originalProbeColsCount, rowSize);
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
    int32_t originalProbeColsCount, int32_t outputRowSize)
    : probeTypes(probeTypes),
      probeOutputCols(probeOutputCols),
      probeHashCols(probeHashCols),
      probeHashColTypes(probeHashColTypes),
      buildOutputCols(buildOutputCols),
      buildOutputTypes(buildOutputTypes),
      hashTables(hashTables),
      simpleFilter(simpleFilter),
      originalProbeColsCount(originalProbeColsCount)
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
    SetOperatorName(metricsNameLookUpJoin);
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
    curProbeHashes.resize(curInputBatch->GetRowCount());
    std::fill(curProbeHashes.begin(), curProbeHashes.end(), 0);
    curProbeNulls.resize(curInputBatch->GetRowCount());
    std::fill(curProbeNulls.begin(), curProbeNulls.end(), 0);
    if (isSingleHT) {
        PopulateProbeNulls();
    } else {
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
        SetStatus(OMNI_STATUS_FINISHED);
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
        outputBuilder->BuildOutput(probeOutputColumns, outputVecBatch);
    }
    if (curProbePosition >= inputRowCount && outputBuilder->IsEmpty()) {
        VectorHelper::FreeVecBatch(curInputBatch);
        curInputBatch = nullptr;
        curProbePosition = 0;
        outputBuilder->Clear();
        SetStatus(OMNI_STATUS_FINISHED);
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

template <bool hasJoinFilter, bool singleHT> void LookupJoinOperator::ProbeBatchForInnerJoin()
{
    std::visit(
        [&](auto &&arg) {
            auto inputRowCount = curInputBatch->GetRowCount();
            auto contextPtr = executionContext.get();
            uint32_t partition = partitionMask;
            auto buildColumns = arg.GetColumns(partition);
            auto probeHashColsCount = probeHashCols.size();
            for (int32_t probePosition = curProbePosition; probePosition < inputRowCount; probePosition++) {
                if (curProbeNulls[probePosition]) {
                    continue;
                }

                if constexpr (!singleHT) {
                    partition = HashUtil::GetRawHashPartition(curProbeHashes[probePosition], partitionMask);
                    buildColumns = arg.GetColumns(partition);
                }

                auto result = arg.Find(probeSerializers, contextPtr, probeHashColumns, probeHashColsCount,
                    probePosition, partition);
                if (!result.IsInsert()) {
                    continue;
                }
                // probe matched in hash table
                auto it = result.GetValue()->Begin();
                while (it.IsOk()) {
                    auto address = LookupJoinOutputBuilder::EncodeAddress(it->rowIdx, it->vecBatchIdx);
                    if constexpr (hasJoinFilter) {
                        auto filterResult = IsJoinPositionEligible(partition, address, probePosition, contextPtr);
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
            for (int32_t probePosition = curProbePosition; probePosition < inputRowCount; probePosition++) {
                if (curProbeNulls[probePosition]) {
                    outputBuilder->AppendRow(probePosition, nullptr, 0);
                    continue;
                }
                if constexpr (!singleHT) {
                    partition = HashUtil::GetRawHashPartition(curProbeHashes[probePosition], partitionMask);
                    buildColumns = arg.GetColumns(partition);
                }
                bool hasProduceRow = false;
                auto result = arg.Find(probeSerializers, contextPtr, probeHashColumns, probeHashColsCount,
                    probePosition, partition);
                if (result.IsInsert()) {
                    // probe matched in hash table
                    auto it = result.GetValue()->Begin();
                    while (it.IsOk()) {
                        auto address = LookupJoinOutputBuilder::EncodeAddress(it->rowIdx, it->vecBatchIdx);
                        if constexpr (hasJoinFilter) {
                            auto filterResult = IsJoinPositionEligible(partition, address, probePosition, contextPtr);
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
                for (int32_t probePosition = curProbePosition; probePosition < inputRowCount; probePosition++) {
                    if (curProbeNulls[probePosition]) {
                        continue;
                    }
                    if constexpr (!singleHT) {
                        partition = HashUtil::GetRawHashPartition(curProbeHashes[probePosition], partitionMask);
                        buildColumns = arg.GetColumns(partition);
                    }
                    auto result = arg.Find(probeSerializers, contextPtr, probeHashColumns, probeHashColsCount,
                        probePosition, partition);
                    if (result.IsInsert()) {
                        auto it = result.GetValue()->Begin();
                        while (it.IsOk()) {
                            auto address = LookupJoinOutputBuilder::EncodeAddress(it->rowIdx, it->vecBatchIdx);
                            if constexpr (hasJoinFilter) {
                                auto filterResult =
                                    IsJoinPositionEligible(partition, address, probePosition, contextPtr);
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
                for (int32_t probePosition = curProbePosition; probePosition < inputRowCount; probePosition++) {
                    if (curProbeNulls[probePosition]) {
                        outputBuilder->AppendRow(probePosition, nullptr, 0);
                        continue;
                    }
                    if constexpr (!singleHT) {
                        partition = HashUtil::GetRawHashPartition(curProbeHashes[probePosition], partitionMask);
                        buildColumns = arg.GetColumns(partition);
                    }
                    bool hasProduceRow = false;
                    auto result = arg.Find(probeSerializers, contextPtr, probeHashColumns, probeHashColsCount,
                        probePosition, partition);
                    if (result.IsInsert()) {
                        // probe matched in hash table
                        auto it = result.GetValue()->Begin();
                        while (it.IsOk()) {
                            auto address = LookupJoinOutputBuilder::EncodeAddress(it->rowIdx, it->vecBatchIdx);
                            if constexpr (hasJoinFilter) {
                                auto filterResult =
                                    IsJoinPositionEligible(partition, address, probePosition, contextPtr);
                                if (filterResult) {
                                    outputBuilder->AppendRow(probePosition, buildColumns, address);
                                    arg.PositionVisited(it);
                                    hasProduceRow = true;
                                }
                            } else {
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
            for (int32_t probePosition = curProbePosition; probePosition < inputRowCount; probePosition++) {
                if (curProbeNulls[probePosition]) {
                    continue;
                }
                if constexpr (!singleHT) {
                    partition = HashUtil::GetRawHashPartition(curProbeHashes[probePosition], partitionMask);
                    buildColumns = arg.GetColumns(partition);
                }

                auto result = arg.Find(probeSerializers, contextPtr, probeHashColumns, probeHashColsCount,
                    probePosition, partition);
                if (!result.IsInsert()) {
                    continue;
                }
                // probe matched in hash table
                auto it = result.GetValue()->Begin();
                while (it.IsOk()) {
                    auto address = LookupJoinOutputBuilder::EncodeAddress(it->rowIdx, it->vecBatchIdx);
                    if constexpr (hasJoinFilter) {
                        auto filterResult = IsJoinPositionEligible(partition, address, probePosition, contextPtr);
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
            for (int32_t probePosition = curProbePosition; probePosition < inputRowCount; probePosition++) {
                if (curProbeNulls[probePosition]) {
                    outputBuilder->AppendRow(probePosition, nullptr, 0);
                    continue;
                }

                if constexpr (!singleHT) {
                    partition = HashUtil::GetRawHashPartition(curProbeHashes[probePosition], partitionMask);
                }
                bool hasProduceRow = false;
                auto result = arg.Find(probeSerializers, contextPtr, probeHashColumns, probeHashColsCount,
                    probePosition, partition);
                if (result.IsInsert()) {
                    // probe matched in hash table
                    auto it = result.GetValue()->Begin();
                    while (it.IsOk()) {
                        if constexpr (hasJoinFilter) {
                            auto address = LookupJoinOutputBuilder::EncodeAddress(it->rowIdx, it->vecBatchIdx);
                            auto filterResult = IsJoinPositionEligible(partition, address, probePosition, contextPtr);
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

NO_INLINE bool LookupJoinOperator::IsJoinPositionEligible(uint32_t partition, uint64_t buildAddress, uint32_t probeRow,
    ExecutionContext *executionContextPtr)
{
    auto probeFilterColsCount = probeFilterCols.size();
    for (size_t j = 0; j < probeFilterColsCount; ++j) {
        uint32_t colIdx = probeFilterCols[j];
        auto probeVec = probeFilterColumns[j];
        nulls[colIdx] = probeVec->IsNull(probeRow);
        values[colIdx] =
            OperatorUtil::GetValuePtrAndLength(probeVec, probeRow, lengths + colIdx, probeFilterTypeIds[j]);
    }

    auto &buildFilterColPtrs = GetBuildFilterColPtrs(partition);
    auto buildBatchIdx = LookupJoinOutputBuilder::DecodeVectorBatchId(buildAddress);
    auto buildRowIdx = LookupJoinOutputBuilder::DecodeRowId(buildAddress);
    auto buildFilterColsCount = buildFilterCols.size();
    for (size_t j = 0; j < buildFilterColsCount; ++j) {
        uint32_t colIdx = buildFilterCols[j];
        auto buildVec = buildFilterColPtrs[j][buildBatchIdx];
        nulls[colIdx] = buildVec->IsNull(buildRowIdx);
        values[colIdx] =
            OperatorUtil::GetValuePtrAndLength(buildVec, buildRowIdx, lengths + colIdx, buildFilterTypeIds[j]);
    }
    return simpleFilter->Evaluate(values, nulls, lengths, reinterpret_cast<int64_t>(executionContextPtr));
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
    if (!probeOutputCols.empty()) {
        probeIndex.reserve(maxRowCount);
    }
    if (!buildOutputCols.empty()) {
        buildIndex.reserve(maxRowCount);
    }
}

void NO_INLINE LookupJoinOutputBuilder::AppendRow(int32_t probePosition, BaseVector ***array, uint64_t address)
{
    probeRowCount++;
    if (!probeOutputCols.empty()) {
        probeIndex.emplace_back(probePosition);
    }
    if (!buildOutputCols.empty()) {
        buildIndex.emplace_back(array, address);
    }
}

template <typename T>
static NO_INLINE BaseVector *ConstructBuildColumn(const std::pair<BaseVector ***, uint64_t> *buildTemp,
    uint32_t outputCol, int32_t numRows)
{
    auto ret = new Vector<T>(numRows);
    T value;
    for (int32_t i = 0; i < numRows; ++i) {
        BaseVector ***array = buildTemp[i].first;
        if (array == nullptr) {
            static_cast<Vector<T> *>(ret)->SetNull(i);
        } else {
            uint64_t address = buildTemp[i].second;
            auto buildRowIdx = LookupJoinOutputBuilder::DecodeRowId(address);
            auto vecBatchIndex = LookupJoinOutputBuilder::DecodeVectorBatchId(address);
            BaseVector *buildVector = array[outputCol][vecBatchIndex];
            if (buildVector->IsNull(buildRowIdx)) {
                ret->SetNull(i);
                continue;
            }

            if (buildVector->GetEncoding() == OMNI_DICTIONARY) {
                value = static_cast<Vector<DictionaryContainer<T>> *>(buildVector)->GetValue(buildRowIdx);
            } else {
                value = static_cast<Vector<T> *>(buildVector)->GetValue(buildRowIdx);
            }
            static_cast<Vector<T> *>(ret)->SetValue(i, value);
        }
    }
    return ret;
}

static NO_INLINE BaseVector *ConstructBuildVarcharColumn(const std::pair<BaseVector ***, uint64_t> *buildTemp,
    uint32_t outputCol, int32_t numRows)
{
    using VarcharVector = Vector<LargeStringContainer<std::string_view>>;
    using DictionaryVector = Vector<DictionaryContainer<std::string_view>>;
    auto *ret = new VarcharVector(numRows);
    std::string_view value;
    for (int32_t i = 0; i < numRows; ++i) {
        BaseVector ***array = buildTemp[i].first;
        if (array == nullptr) {
            static_cast<VarcharVector *>(ret)->SetNull(i);
        } else {
            uint64_t address = buildTemp[i].second;
            auto buildRowIdx = LookupJoinOutputBuilder::DecodeRowId(address);
            auto vecBatchIndex = LookupJoinOutputBuilder::DecodeVectorBatchId(address);
            auto buildVector = array[outputCol][vecBatchIndex];
            if (buildVector->IsNull(buildRowIdx)) {
                static_cast<VarcharVector *>(ret)->SetNull(i);
                continue;
            }

            if (buildVector->GetEncoding() == OMNI_DICTIONARY) {
                value = static_cast<DictionaryVector *>(buildVector)->GetValue(buildRowIdx);
            } else {
                value = static_cast<VarcharVector *>(buildVector)->GetValue(buildRowIdx);
            }
            static_cast<VarcharVector *>(ret)->SetValue(i, value);
        }
    }
    return ret;
}

void NO_INLINE LookupJoinOutputBuilder::ConstructProbeColumns(VectorBatch *vectorBatch, BaseVector **probeOutputColumns,
    int32_t rowCount)
{
    bool isSequentialProbeIndices = true;
    if (rowCount > 1) { // <= 1 must be sequential
        auto tmpProbeIndex = probeIndex.data() + probeRowOffset;
        for (int32_t i = 1; i < rowCount; ++i) {
            if (tmpProbeIndex[i] != tmpProbeIndex[i - 1] + 1) {
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

void NO_INLINE LookupJoinOutputBuilder::ConstructBuildColumns(VectorBatch *vectorBatch, int32_t rowCount)
{
    // preprocess the pointer to build table vectors -- doing a few levels of
    // pointer chasing first
    const std::pair<BaseVector ***, uint64_t> *buildTemp = buildIndex.data() + probeRowOffset;
    for (size_t j = 0; j < buildOutputCols.size(); j++) {
        uint32_t outputCol = buildOutputCols[j];
        BaseVector *buildColumn = nullptr;
        switch (buildOutputTypes[j]) {
            case OMNI_LONG:
            case OMNI_TIMESTAMP:
            case OMNI_DECIMAL64:
                buildColumn = ConstructBuildColumn<int64_t>(buildTemp, outputCol, rowCount);
                break;
            case OMNI_INT:
            case OMNI_DATE32:
                buildColumn = ConstructBuildColumn<int32_t>(buildTemp, outputCol, rowCount);
                break;
            case OMNI_VARCHAR:
            case OMNI_CHAR:
                buildColumn = ConstructBuildVarcharColumn(buildTemp, outputCol, rowCount);
                break;
            case OMNI_DECIMAL128:
                buildColumn = ConstructBuildColumn<Decimal128>(buildTemp, outputCol, rowCount);
                break;
            case OMNI_SHORT:
                buildColumn = ConstructBuildColumn<int16_t>(buildTemp, outputCol, rowCount);
                break;
            case OMNI_DOUBLE:
                buildColumn = ConstructBuildColumn<double>(buildTemp, outputCol, rowCount);
                break;
            case OMNI_BOOLEAN:
                buildColumn = ConstructBuildColumn<bool>(buildTemp, outputCol, rowCount);
                break;
            default:
                break;
        }
        vectorBatch->Append(buildColumn);
    }
}

void LookupJoinOutputBuilder::BuildOutput(BaseVector **probeOutputColumns, VectorBatch **outputVecBatch)
{
    auto rowCount = std::min(probeRowCount, maxRowCount);
    auto output = std::make_unique<VectorBatch>(rowCount);
    auto outputPtr = output.get();
    if (!probeOutputCols.empty()) {
        // only probe side will produce dic vector
        ConstructProbeColumns(outputPtr, probeOutputColumns, rowCount);
    }
    if (!buildOutputCols.empty()) {
        ConstructBuildColumns(outputPtr, rowCount);
    }

    probeRowOffset += rowCount;
    probeRowCount -= rowCount;
    *outputVecBatch = output.release();
}
} // end of omniruntime
