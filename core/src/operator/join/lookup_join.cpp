/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: lookup join implementations
 */
#include <vector>
#include <memory>
#include "hash_builder.h"
#include "vector/vector_common.h"
#include "operator/util/operator_util.h"
#include "vector/vector_helper.h"
#include "operator/pages_hash_strategy.h"
#include "lookup_join.h"
#ifdef ENABLE_HMPP
#include <HMPP/hmpp.h>
#include "operator/hmpp_hash_util.h"
#include "util/config_util.h"
#endif

using namespace omniruntime::vec;
namespace omniruntime {
namespace op {
void LookupJoinOperatorFactory::CommonInitActions(const type::DataTypes &probeTypes, int32_t *probeOutputCols,
    int32_t probeOutputColsCount, int32_t *probeHashCols, int32_t probeHashColsCount, int32_t *buildOutputCols,
    int32_t buildOutputColsCount, const type::DataTypes &buildOutputTypes)
{
    int32_t tempProbeHashColTypes[probeHashColsCount];
    auto probeTypeIds = probeTypes.GetIds();
    for (int32_t i = 0; i < probeHashColsCount; i++) {
        tempProbeHashColTypes[i] = probeTypeIds[probeHashCols[i]];
    }
    this->probeOutputCols.insert(this->probeOutputCols.end(), probeOutputCols, probeOutputCols + probeOutputColsCount);
    this->probeHashCols.insert(this->probeHashCols.end(), probeHashCols, probeHashCols + probeHashColsCount);
    this->probeHashColTypes.insert(this->probeHashColTypes.end(), tempProbeHashColTypes,
        tempProbeHashColTypes + probeHashColsCount);
    this->buildOutputCols.insert(this->buildOutputCols.end(), buildOutputCols, buildOutputCols + buildOutputColsCount);
    this->rowSize = OperatorUtil::GetOutputRowSize(probeTypes.Get(), probeOutputCols, probeOutputColsCount);
    if (buildOutputColsCount != 0) {
        this->rowSize += OperatorUtil::GetRowSize(buildOutputTypes.Get());
    }
}

LookupJoinOperatorFactory::LookupJoinOperatorFactory(const type::DataTypes &probeTypes, int32_t *probeOutputCols,
    int32_t probeOutputColsCount, int32_t *probeHashCols, int32_t probeHashColsCount, int32_t *buildOutputCols,
    int32_t buildOutputColsCount, const type::DataTypes &buildOutputTypes, JoinType joinType,
    JoinHashTables *hashTables, OverflowConfig *overflowConfig)
    : probeTypes(probeTypes), buildOutputTypes(buildOutputTypes), joinType(joinType), hashTables(hashTables)
{
    CommonInitActions(probeTypes, probeOutputCols, probeOutputColsCount, probeHashCols, probeHashColsCount,
        buildOutputCols, buildOutputColsCount, buildOutputTypes);
    this->hashTables->SetOriginalProbeColsCount(probeTypes.GetSize());
    this->hashTables->SetProbeTypes(&(this->probeTypes));
    this->hashTables->JoinFilterCodeGen(overflowConfig);
}

LookupJoinOperatorFactory::LookupJoinOperatorFactory(const type::DataTypes &probeTypes, int32_t *probeOutputCols,
    int32_t probeOutputColsCount, int32_t *probeHashCols, int32_t probeHashColsCount, int32_t *buildOutputCols,
    int32_t buildOutputColsCount, const type::DataTypes &buildOutputTypes, JoinType joinType,
    JoinHashTables *hashTables, int32_t originalProbeColsCount, OverflowConfig *overflowConfig)
    : probeTypes(probeTypes), buildOutputTypes(buildOutputTypes), joinType(joinType), hashTables(hashTables)
{
    CommonInitActions(probeTypes, probeOutputCols, probeOutputColsCount, probeHashCols, probeHashColsCount,
        buildOutputCols, buildOutputColsCount, buildOutputTypes);
    this->hashTables->SetOriginalProbeColsCount(originalProbeColsCount);
    this->hashTables->SetProbeTypes(&(this->probeTypes));
    this->hashTables->JoinFilterCodeGen(overflowConfig);
}

LookupJoinOperatorFactory::~LookupJoinOperatorFactory() = default;

LookupJoinOperatorFactory *LookupJoinOperatorFactory::CreateLookupJoinOperatorFactory(const DataTypes &probeTypes,
    int32_t *probeOutputCols, int32_t probeOutputColsCount, int32_t *probeHashCols, int32_t probeHashColsCount,
    int32_t *buildOutputCols, int32_t buildOutputColsCount, const DataTypes &buildOutputTypes, JoinType inputJoinType,
    int64_t hashBuilderFactoryAddr, OverflowConfig *overflowConfig)
{
    auto hashBuilderFactory = reinterpret_cast<HashBuilderOperatorFactory *>(hashBuilderFactoryAddr);
    auto pOperatorFactory = new LookupJoinOperatorFactory(probeTypes, probeOutputCols, probeOutputColsCount,
        probeHashCols, probeHashColsCount, buildOutputCols, buildOutputColsCount, buildOutputTypes, inputJoinType,
        hashBuilderFactory->GetHashTables(), overflowConfig);
    return pOperatorFactory;
}

LookupJoinOperatorFactory *LookupJoinOperatorFactory::CreateLookupJoinOperatorFactory(const DataTypes &probeTypes,
    int32_t *probeOutputCols, int32_t probeOutputColsCount, int32_t *probeHashCols, int32_t probeHashColsCount,
    int32_t *buildOutputCols, int32_t buildOutputColsCount, const DataTypes &buildOutputTypes, JoinType inputJoinType,
    int64_t hashBuilderFactoryAddr, int32_t originalProbeColsCount, OverflowConfig *overflowConfig)
{
    auto hashBuilderFactory = reinterpret_cast<HashBuilderOperatorFactory *>(hashBuilderFactoryAddr);
    auto pOperatorFactory = new LookupJoinOperatorFactory(probeTypes, probeOutputCols, probeOutputColsCount,
        probeHashCols, probeHashColsCount, buildOutputCols, buildOutputColsCount, buildOutputTypes, inputJoinType,
        hashBuilderFactory->GetHashTables(), originalProbeColsCount, overflowConfig);
    return pOperatorFactory;
}

Operator *LookupJoinOperatorFactory::CreateOperator()
{
    auto pLookupJoinOperator = new LookupJoinOperator(probeTypes, probeOutputCols, probeHashCols, probeHashColTypes,
        buildOutputCols, buildOutputTypes, joinType, hashTables, rowSize);
    return pLookupJoinOperator;
}

LookupJoinOperator::LookupJoinOperator(const DataTypes &probeTypes, std::vector<int32_t> &probeOutputCols,
    std::vector<int32_t> &probeHashCols, std::vector<int32_t> &probeHashColTypes, std::vector<int32_t> &buildOutputCols,
    const type::DataTypes &buildOutputTypes, JoinType joinType, JoinHashTables *hashTables, int32_t outputRowSize)
    : joinType(joinType),
      probeTypes(probeTypes),
      probeOutputCols(probeOutputCols),
      probeHashCols(probeHashCols),
      probeHashColTypes(probeHashColTypes),
      buildOutputCols(buildOutputCols),
      buildOutputTypes(buildOutputTypes),
      hashTables(hashTables)
{
    std::vector<DataTypePtr> tmpProbeOutputTypesVec;
    for (size_t i = 0; i < probeOutputCols.size(); i++) {
        tmpProbeOutputTypesVec.emplace_back(probeTypes.GetType(probeOutputCols[i]));
    }
    this->probeOutputTypes = DataTypes(tmpProbeOutputTypesVec);

    this->outputBuilder = std::make_unique<LookupJoinOutputBuilder>(probeOutputCols.data(), probeOutputCols.size(),
        probeOutputTypes.GetIds(), buildOutputCols.data(), buildOutputCols.size(), buildOutputTypes.GetIds(),
        outputRowSize);
    this->executionContext = new ExecutionContext();
    this->probeHashColumns = new BaseVector *[probeHashCols.size()]();
    this->probeOutputColumns = new BaseVector *[probeOutputCols.size()]();

    // probe output types
    this->outputTypes.insert(this->outputTypes.end(), tmpProbeOutputTypesVec.begin(), tmpProbeOutputTypesVec.end());
    // build output types
    this->outputTypes.insert(this->outputTypes.end(), this->buildOutputTypes.Get().begin(),
        this->buildOutputTypes.Get().end());

    this->simpleFilter = hashTables->GetSimpleFilter();
    if (this->simpleFilter != nullptr) {
        auto originalProbeColsCount = hashTables->GetOriginalProbeColsCount();
        auto buildColsCount = hashTables->GetBuildDataTypes()->GetSize();
        auto allColsCount = originalProbeColsCount + buildColsCount;
        this->values = new int64_t[allColsCount]();
        this->nulls = new bool[allColsCount]();
        this->lengths = new int32_t[allColsCount]();
        this->probeFilterCols = hashTables->GetProbeFilterCols();
        this->buildFilterCols = hashTables->GetBuildFilterCols();
        this->probeFilterColumns = new BaseVector *[this->probeFilterCols.size()]();

        auto probeFilterColsCount = this->probeFilterCols.size();
        auto buildFilterColsCount = this->buildFilterCols.size();
        this->probeFilterTypeIds = new int32_t[probeFilterColsCount];
        this->buildFilterTypeIds = new int32_t[buildFilterColsCount];
        auto probeTypeIds = probeTypes.GetIds();
        for (size_t i = 0; i < probeFilterColsCount; i++) {
            this->probeFilterTypeIds[i] = probeTypeIds[this->probeFilterCols[i]];
        }
        auto buildTypeIds = hashTables->GetBuildDataTypes()->GetIds();
        for (size_t i = 0; i < buildFilterColsCount; i++) {
            this->buildFilterTypeIds[i] = buildTypeIds[this->buildFilterCols[i] - originalProbeColsCount];
        }
    }
}

LookupJoinOperator::~LookupJoinOperator()
{
    delete executionContext;
    executionContext = nullptr;
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
    for (uint32_t j = 0; j < probeHashCols.size(); ++j) {
        probeHashColumns[j] = probeAllColumns[probeHashCols[j]];
    }
    for (uint32_t j = 0; j < probeOutputCols.size(); ++j) {
        probeOutputColumns[j] = probeAllColumns[probeOutputCols[j]];
    }
    curProbeHashes.resize(curInputBatch->GetRowCount());
    std::fill(curProbeHashes.begin(), curProbeHashes.end(), 0);
    curProbeNulls.resize(curInputBatch->GetRowCount());
    std::fill(curProbeNulls.begin(), curProbeNulls.end(), 0);
    PopulateProbeHashes();

    if (this->simpleFilter != nullptr) {
        int32_t index = 0;
        for (auto col : probeFilterCols) {
            probeFilterColumns[index++] = probeAllColumns[col];
        }
    }
}

void LookupJoinOperator::ProcessProbe(bool hasFilter)
{
    switch (joinType) {
        case OMNI_JOIN_TYPE_INNER:
            if (hasFilter) {
                ProbeBatchForInnerJoin<true>();
            } else {
                ProbeBatchForInnerJoin<false>();
            }
            break;
        case OMNI_JOIN_TYPE_LEFT:
            if (hasFilter) {
                ProbeBatchForLeftJoin<true>();
            } else {
                ProbeBatchForLeftJoin<false>();
            }
            break;
        case OMNI_JOIN_TYPE_RIGHT:
            if (hasFilter) {
                ProbeBatchForRightJoin<true>();
            } else {
                ProbeBatchForRightJoin<false>();
            }
            break;
        case OMNI_JOIN_TYPE_FULL:
            if (hasFilter) {
                ProbeBatchForFullJoin<true>();
            } else {
                ProbeBatchForFullJoin<false>();
            }
            break;
        case OMNI_JOIN_TYPE_LEFT_SEMI:
            if (hasFilter) {
                ProbeBatchForLeftSemiJoin<true>();
            } else {
                ProbeBatchForLeftSemiJoin<false>();
            }
            break;
        case OMNI_JOIN_TYPE_LEFT_ANTI:
            if (hasFilter) {
                ProbeBatchForLeftAntiJoin<true>();
            } else {
                ProbeBatchForLeftAntiJoin<false>();
            }
            break;
        default: {
            LogError("Unsupported join type: %u.", joinType);
            break;
        }
    }
}

int32_t LookupJoinOperator::AddInput(VectorBatch *vecBatch)
{
    if (!firstVecBatch && simpleFilter != nullptr) {
        firstVecBatch = true;
        hashTables->InitBuildFilterCols();
    }
    curInputBatch = vecBatch;

    PrepareCurrentProbe();

    // maybe the data has been pulled after the previous probe, and the operator status will be set to finished
    // and needs to be reset to normal.
    SetStatus(OMNI_STATUS_NORMAL);
    return 0;
}

int32_t LookupJoinOperator::GetOutput(VectorBatch **outputVecBatch)
{
    // start probe
    ProcessProbe(simpleFilter != nullptr);

    if (curOutputBatch) {
        *outputVecBatch = curOutputBatch;
        curOutputBatch = nullptr;
        if (curProbePosition == curInputBatch->GetRowCount()) {
            VectorHelper::FreeVecBatch(curInputBatch);
            curInputBatch = nullptr;
            curProbePosition = 0;
            SetStatus(OMNI_STATUS_FINISHED);
        }
        return 0;
    }

    // handle the remaining output
    if (!outputBuilder->IsEmpty()) {
        outputBuilder->BuildOutput(probeOutputColumns, outputVecBatch);
    }
    VectorHelper::FreeVecBatch(curInputBatch);
    curInputBatch = nullptr;
    curProbePosition = 0;
    SetStatus(OMNI_STATUS_FINISHED);
    return 0;
}

template <bool hasJoinFilter> void LookupJoinOperator::ProbeBatchForInnerJoin()
{
    auto inputRowCount = curInputBatch->GetRowCount();
    for (int32_t probePosition = curProbePosition; probePosition < inputRowCount; probePosition++) {
        if (curProbeNulls[probePosition]) {
            continue;
        }
        uint32_t partition;
        uint32_t joinPosition;
        hashTables->GetJoinPosition(probePosition, probeHashColumns, curProbeHashes[probePosition], partition,
            joinPosition);
        while (joinPosition != INVALID_POSITION) {
            auto hashTable = hashTables->GetHashTable(partition);
            if constexpr (hasJoinFilter) {
                auto filterResult = IsJoinPositionEligible(partition, joinPosition, probePosition);
                if (filterResult) {
                    outputBuilder->AppendRow(probePosition, hashTable, joinPosition);
                }
            } else {
                outputBuilder->AppendRow(probePosition, hashTable, joinPosition);
            }
            joinPosition = hashTable->GetNextJoinPosition(joinPosition);
        }

        // if the output row count exceeds the maxRowCount, then construct output to avoid probeIndex and buildIndex
        // consume excessive memory
        if (outputBuilder->IsFull()) {
            outputBuilder->BuildOutput(probeOutputColumns, &curOutputBatch);
            curProbePosition = probePosition + 1;
            return;
        }
    }
}

template <bool hasJoinFilter> void LookupJoinOperator::ProbeBatchForLeftJoin()
{
    auto inputRowCount = curInputBatch->GetRowCount();
    for (int32_t probePosition = curProbePosition; probePosition < inputRowCount; probePosition++) {
        if (curProbeNulls[probePosition]) {
            outputBuilder->AppendRow(probePosition, nullptr, 0);
            continue;
        }
        uint32_t partition;
        uint32_t joinPosition;
        hashTables->GetJoinPosition(probePosition, probeHashColumns, curProbeHashes[probePosition], partition,
            joinPosition);
        bool hasProduceRow = false;
        while (joinPosition != INVALID_POSITION) {
            auto hashTable = hashTables->GetHashTable(partition);
            if constexpr (hasJoinFilter) {
                auto filterResult = IsJoinPositionEligible(partition, joinPosition, probePosition);
                if (filterResult) {
                    outputBuilder->AppendRow(probePosition, hashTable, joinPosition);
                    hasProduceRow = true;
                }
            } else {
                outputBuilder->AppendRow(probePosition, hashTable, joinPosition);
                hasProduceRow = true;
            }
            joinPosition = hashTable->GetNextJoinPosition(joinPosition);
        }
        if (!hasProduceRow) {
            outputBuilder->AppendRow(probePosition, nullptr, 0);
        }

        // if the output row count exceeds the maxRowCount, then construct output to avoid probeIndex and buildIndex
        // consume excessive memory
        if (outputBuilder->IsFull()) {
            outputBuilder->BuildOutput(probeOutputColumns, &curOutputBatch);
            curProbePosition = probePosition + 1;
            return;
        }
    }
}

template <bool hasJoinFilter> void LookupJoinOperator::ProbeBatchForRightJoin()
{
    auto inputRowCount = curInputBatch->GetRowCount();
    for (int32_t probePosition = curProbePosition; probePosition < inputRowCount; probePosition++) {
        if (curProbeNulls[probePosition]) {
            continue;
        }
        uint32_t partition;
        uint32_t joinPosition;
        hashTables->GetJoinPosition(probePosition, probeHashColumns, curProbeHashes[probePosition], partition,
            joinPosition);
        while (joinPosition != INVALID_POSITION) {
            auto hashTable = hashTables->GetHashTable(partition);
            if constexpr (hasJoinFilter) {
                auto filterResult = IsJoinPositionEligible(partition, joinPosition, probePosition);
                if (filterResult) {
                    outputBuilder->AppendRow(probePosition, hashTable, joinPosition);
                    hashTables->PositionVisited(partition, joinPosition);
                }
            } else {
                outputBuilder->AppendRow(probePosition, hashTable, joinPosition);
                hashTables->PositionVisited(partition, joinPosition);
            }
            joinPosition = hashTable->GetNextJoinPosition(joinPosition);
        }

        // if the output row count exceeds the maxRowCount, then construct output to avoid probeIndex and buildIndex
        // consume excessive memory
        if (outputBuilder->IsFull()) {
            outputBuilder->BuildOutput(probeOutputColumns, &curOutputBatch);
            curProbePosition = probePosition + 1;
            return;
        }
    }
}

template <bool hasJoinFilter> void LookupJoinOperator::ProbeBatchForFullJoin()
{
    auto inputRowCount = curInputBatch->GetRowCount();
    for (int32_t probePosition = curProbePosition; probePosition < inputRowCount; probePosition++) {
        if (curProbeNulls[probePosition]) {
            outputBuilder->AppendRow(probePosition, nullptr, 0);
            continue;
        }
        uint32_t partition;
        uint32_t joinPosition;
        hashTables->GetJoinPosition(probePosition, probeHashColumns, curProbeHashes[probePosition], partition,
            joinPosition);
        bool hasProduceRow = false;
        while (joinPosition != INVALID_POSITION) {
            auto hashTable = hashTables->GetHashTable(partition);
            if constexpr (hasJoinFilter) {
                auto filterResult = IsJoinPositionEligible(partition, joinPosition, probePosition);
                if (filterResult) {
                    outputBuilder->AppendRow(probePosition, hashTable, joinPosition);
                    hasProduceRow = true;
                    hashTables->PositionVisited(partition, joinPosition);
                }
            } else {
                outputBuilder->AppendRow(probePosition, hashTable, joinPosition);
                hasProduceRow = true;
                hashTables->PositionVisited(partition, joinPosition);
            }
            joinPosition = hashTable->GetNextJoinPosition(joinPosition);
        }
        if (!hasProduceRow) {
            outputBuilder->AppendRow(probePosition, nullptr, 0);
        }

        // if the output row count exceeds the maxRowCount, then construct output to avoid probeIndex and buildIndex
        // consume excessive memory
        if (outputBuilder->IsFull()) {
            outputBuilder->BuildOutput(probeOutputColumns, &curOutputBatch);
            curProbePosition = probePosition + 1;
            return;
        }
    }
}

template <bool hasJoinFilter> void LookupJoinOperator::ProbeBatchForLeftSemiJoin()
{
    auto inputRowCount = curInputBatch->GetRowCount();
    for (int32_t probePosition = curProbePosition; probePosition < inputRowCount; probePosition++) {
        if (curProbeNulls[probePosition]) {
            continue;
        }
        uint32_t partition;
        uint32_t joinPosition;
        hashTables->GetJoinPosition(probePosition, probeHashColumns, curProbeHashes[probePosition], partition,
            joinPosition);
        while (joinPosition != INVALID_POSITION) {
            auto hashTable = hashTables->GetHashTable(partition);
            if constexpr (hasJoinFilter) {
                auto filterResult = IsJoinPositionEligible(partition, joinPosition, probePosition);
                if (filterResult) {
                    outputBuilder->AppendRow(probePosition, hashTable, joinPosition);
                    break;
                }
            } else {
                outputBuilder->AppendRow(probePosition, hashTable, joinPosition);
                break;
            }
            joinPosition = hashTable->GetNextJoinPosition(joinPosition);
        }

        // if the output row count exceeds the maxRowCount, then construct output to avoid probeIndex and buildIndex
        // consume excessive memory
        if (outputBuilder->IsFull()) {
            outputBuilder->BuildOutput(probeOutputColumns, &curOutputBatch);
            curProbePosition = probePosition + 1;
            return;
        }
    }
}

template <bool hasJoinFilter> void LookupJoinOperator::ProbeBatchForLeftAntiJoin()
{
    auto inputRowCount = curInputBatch->GetRowCount();
    for (int32_t probePosition = curProbePosition; probePosition < inputRowCount; probePosition++) {
        if (curProbeNulls[probePosition]) {
            outputBuilder->AppendRow(probePosition, nullptr, 0);
            continue;
        }
        uint32_t partition;
        uint32_t joinPosition;
        hashTables->GetJoinPosition(probePosition, probeHashColumns, curProbeHashes[probePosition], partition,
            joinPosition);
        bool hasProduceRow = false;
        while (joinPosition != INVALID_POSITION) {
            auto hashTable = hashTables->GetHashTable(partition);
            if constexpr (hasJoinFilter) {
                auto filterResult = IsJoinPositionEligible(partition, joinPosition, probePosition);
                if (filterResult) {
                    hasProduceRow = true;
                    break;
                }
            } else {
                hasProduceRow = true;
                break;
            }
            joinPosition = hashTable->GetNextJoinPosition(joinPosition);
        }
        if (!hasProduceRow) {
            outputBuilder->AppendRow(probePosition, nullptr, 0);
        }

        // if the output row count exceeds the maxRowCount, then construct output to avoid probeIndex and buildIndex
        // consume excessive memory
        if (outputBuilder->IsFull()) {
            outputBuilder->BuildOutput(probeOutputColumns, &curOutputBatch);
            curProbePosition = probePosition + 1;
            return;
        }
    }
}

ALWAYS_INLINE bool LookupJoinOperator::IsJoinPositionEligible(uint32_t partition, uint32_t joinPosition,
    uint32_t probeRow)
{
    auto hashTable = hashTables->GetHashTable(partition);
    auto pagesHash = hashTable->GetPagesHash();
    auto probeFilterColsCount = probeFilterCols.size();
    for (uint32_t j = 0; j < probeFilterColsCount; ++j) {
        uint32_t colIdx = probeFilterCols[j];
        auto probeVec = probeFilterColumns[j];
        nulls[colIdx] = probeVec->IsNull(probeRow);
        values[colIdx] =
            OperatorUtil::GetValuePtrAndLength(probeVec, probeRow, lengths + colIdx, probeFilterTypeIds[j]);
    }

    auto &buildFilterColPtrs = hashTables->GetBuildFilterColPtrs(partition);
    uint64_t buildAddress = pagesHash->GetAddresses()[joinPosition];
    auto buildBatchIdx = DecodeSliceIndex(buildAddress);
    auto buildRowIdx = DecodePosition(buildAddress);
    auto buildFilterColsCount = buildFilterCols.size();
    for (uint32_t j = 0; j < buildFilterColsCount; ++j) {
        uint32_t colIdx = buildFilterCols[j];
        auto buildVec = buildFilterColPtrs[j][buildBatchIdx];
        nulls[colIdx] = buildVec->IsNull(buildRowIdx);
        values[colIdx] =
            OperatorUtil::GetValuePtrAndLength(buildVec, buildRowIdx, lengths + colIdx, buildFilterTypeIds[j]);
    }
    return simpleFilter->Evaluate(values, nulls, lengths, reinterpret_cast<int64_t>(executionContext));
}

#ifdef ENABLE_HMPP
template <type::DataTypeId dataTypeId>
void CalculateColHashesHMPP(BaseVector *vector, int32_t rowCount, int64_t *combinedHash, std::vector<int8_t> &nulls)
{
    using T = typename NativeType<dataTypeId>::type;
    if (vector->GetEncoding() != OMNI_DICTIONARY) {
        LogDebug("HMPP-Join-hash");
        if (vector->HasNull()) {
            for (int32_t i = 0; i < rowCount; ++i) {
                if (vector->IsNull(i)) {
                    nulls[i] = 1;
                    continue;
                }
            }
        }
        HmppResult result = HmppHashUtil::ComputeHash<dataTypeId>(vector, combinedHash, 0, rowCount);
        if (result != HMPP_STS_NO_ERR) {
            throw OmniException("HMPP ERROR", "Join HMPPS_ComputeHash failed for hmpp error");
        }
    } else {
        int64_t hash;
        for (int32_t i = 0; i < rowCount; ++i) {
            if (vector->IsNull(i)) {
                nulls[i] = 1;
                continue;
            }
            hash = HashUtil::HashValue(static_cast<Vector<DictionaryContainer<T>> *>(vector)->GetValue(i));
            combinedHash[i] = HashUtil::CombineHash(combinedHash[i], hash);
        }
    }
}

void CalculateColDec64HashesHMPP(BaseVector *vector, int32_t rowCount, int64_t *combinedHash,
    std::vector<int8_t> &nulls)
{
    if (vector->GetEncoding() != OMNI_DICTIONARY) {
        LogDebug("HMPP-Join-hashDec64");
        const auto *decimalAddr =
            reinterpret_cast<const int64_t *>(VectorHelper::UnsafeGetValues(vector, OMNI_DECIMAL64));
        auto *resultHash = new int64_t[rowCount]();
        int8_t *nullAddr = nullptr;
        if (vector->HasNull()) {
            nullAddr = reinterpret_cast<int8_t *>(unsafe::UnsafeBaseVector::GetNulls(vector));
            for (int32_t i = 0; i < rowCount; ++i) {
                if (vector->IsNull(i)) {
                    nulls[i] = 1;
                    continue;
                }
            }
        }
        HmppResult result = HMPPS_Hash_decimal64(decimalAddr, rowCount, nullAddr, resultHash);
        if (result != HMPP_STS_NO_ERR) {
            delete[] resultHash;
            throw OmniException("HMPP ERROR", "Join HMPPS_Hash_decimal64 failed for hmpp error");
        }
        result = HMPPS_CombineHash(combinedHash, resultHash, rowCount, combinedHash);
        if (result != HMPP_STS_NO_ERR) {
            delete[] resultHash;
            throw OmniException("HMPP ERROR", "Join HMPPS_CombineHash_decimal64 failed for hmpp error");
        }
        delete[] resultHash;
    } else {
        int64_t hash;
        for (int32_t i = 0; i < rowCount; ++i) {
            if (vector->IsNull(i)) {
                nulls[i] = 1;
                continue;
            }
            hash = HashUtil::HashDecimal64Value(
                reinterpret_cast<Vector<DictionaryContainer<int64_t>> *>(vector)->GetValue(i));
            combinedHash[i] = HashUtil::CombineHash(combinedHash[i], hash);
        }
    }
}

void CalculateColDec128HashesHMPP(BaseVector *vector, int32_t rowCount, int64_t *combinedHash,
    std::vector<int8_t> &nulls)
{
    if (vector->GetEncoding() != OMNI_DICTIONARY) {
        LogDebug("HMPP-Join-hashDec128");
        auto *resultHash = new int64_t[rowCount]();
        int8_t *nullAddr = nullptr;
        auto *decimalAddr = reinterpret_cast<HmppDecimal128 *>(VectorHelper::UnsafeGetValues(vector, OMNI_DECIMAL128));
        if (vector->HasNull()) {
            nullAddr = reinterpret_cast<int8_t *>(unsafe::UnsafeBaseVector::GetNulls(vector));
            for (int32_t i = 0; i < rowCount; ++i) {
                if (vector->IsNull(static_cast<int32_t>(i))) {
                    nulls[i] = 1;
                    continue;
                }
            }
        }
        HmppResult result = HMPPS_Hash_decimal128(decimalAddr, rowCount, nullAddr, resultHash);
        if (result != HMPP_STS_NO_ERR) {
            delete[] resultHash;
            throw OmniException("HMPP ERROR", "Join HMPPS_Hash_decimal128 failed for hmpp error");
        }
        result = HMPPS_CombineHash(reinterpret_cast<int64_t *>(combinedHash), resultHash, rowCount,
            reinterpret_cast<int64_t *>(combinedHash));
        if (result != HMPP_STS_NO_ERR) {
            delete[] resultHash;
            throw OmniException("HMPP ERROR", "Join HMPPS_Hash_decimal128 failed for hmpp error");
        }
        delete[] resultHash;
    } else {
        int64_t hash;
        for (int32_t i = 0; i < rowCount; ++i) {
            if (vector->IsNull(i)) {
                nulls[i] = 1;
                continue;
            }
            Decimal128 decimal128Value =
                reinterpret_cast<Vector<DictionaryContainer<Decimal128>> *>(vector)->GetValue(i);
            hash = HashUtil::HashValue(static_cast<int64_t>(decimal128Value.LowBits()), decimal128Value.HighBits());
            combinedHash[i] = HashUtil::CombineHash(combinedHash[i], hash);
        }
    }
}

void CalculateColVarcharHashesHMPP(BaseVector *vector, int32_t rowCount, int64_t *combinedHash,
    std::vector<int8_t> &nulls)
{
    if (vector->GetEncoding() != OMNI_DICTIONARY) {
        LogDebug("HMPP-Join-hashVarchar");
        int8_t *nullAddr = nullptr;
        auto *resultHash = new int64_t[rowCount]();
        auto *varcharVectorAddr = reinterpret_cast<uint8_t *>(VectorHelper::UnsafeGetValues(vector, OMNI_VARCHAR));
        auto *offset = static_cast<int32_t *>(VectorHelper::UnsafeGetOffsetsAddr(vector, OMNI_VARCHAR));
        if (vector->HasNull()) {
            nullAddr = reinterpret_cast<int8_t *>(unsafe::UnsafeBaseVector::GetNulls(vector));
            for (int32_t i = 0; i < rowCount; ++i) {
                if (vector->IsNull(i)) {
                    nulls[i] = 1;
                    continue;
                }
            }
        }
        HmppResult result = HMPPS_Hash_varchar(varcharVectorAddr, offset, rowCount, nullAddr, resultHash);
        if (result != HMPP_STS_NO_ERR) {
            delete[] resultHash;
            throw OmniException("HMPP ERROR", "Join HMPPS_Hash_varchar failed for hmpp error");
        }
        result = HMPPS_CombineHash(reinterpret_cast<int64_t *>(combinedHash), resultHash, rowCount, combinedHash);
        if (result != HMPP_STS_NO_ERR) {
            delete[] resultHash;
            throw OmniException("HMPP ERROR", "Join HMPPS_Hash_varchar failed for hmpp error");
        }
        delete[] resultHash;
    } else {
        int64_t hash;
        for (int32_t i = 0; i < rowCount; ++i) {
            if (vector->IsNull(i)) {
                nulls[i] = 1;
                continue;
            }
            std::string_view varcharValue =
                reinterpret_cast<Vector<DictionaryContainer<std::string_view>> *>(vector)->GetValue(i);
            hash = HashUtil::HashValue(reinterpret_cast<int8_t *>(const_cast<char *>(varcharValue.data())),
                varcharValue.length());
            combinedHash[i] = HashUtil::CombineHash(combinedHash[i], hash);
        }
    }
}
#endif

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

template <type::DataTypeId dataTypeId>
void CalculateColHashesProxy(BaseVector *vec, int32_t rowCount, int64_t *hashes, std::vector<int8_t> &nulls)
{
    using T = typename NativeType<dataTypeId>::type;
#ifdef ENABLE_HMPP
    if (ConfigUtil::IsEnableHMPP()) {
        CalculateColHashesHMPP<dataTypeId>(vec, rowCount, hashes, nulls);
    } else {
        CalculateColHashes<T>(vec, rowCount, hashes, nulls);
    }
#else
    CalculateColHashes<T>(vec, rowCount, hashes, nulls);
#endif
}

void CalculateColDec64HashesProxy(BaseVector *vec, int32_t rowCount, int64_t *hashes, std::vector<int8_t> &nulls)
{
#ifdef ENABLE_HMPP
    if (ConfigUtil::IsEnableHMPP()) {
        CalculateColDec64HashesHMPP(vec, rowCount, hashes, nulls);
    } else {
        CalculateColDec64Hashes(vec, rowCount, hashes, nulls);
    }
#else
    CalculateColDec64Hashes(vec, rowCount, hashes, nulls);
#endif
}

void CalculateColDec128HashesProxy(BaseVector *vec, int32_t rowCount, int64_t *hashes, std::vector<int8_t> &nulls)
{
#ifdef ENABLE_HMPP
    if (ConfigUtil::IsEnableHMPP()) {
        CalculateColDec128HashesHMPP(vec, rowCount, hashes, nulls);
    } else {
        CalculateColDec128Hashes(vec, rowCount, hashes, nulls);
    }
#else
    CalculateColDec128Hashes(vec, rowCount, hashes, nulls);
#endif
}

void CalculateColVarcharHashesProxy(BaseVector *vec, int32_t rowCount, int64_t *hashes, std::vector<int8_t> &nulls)
{
#ifdef ENABLE_HMPP
    if (ConfigUtil::IsEnableHMPP()) {
        CalculateColVarcharHashesHMPP(vec, rowCount, hashes, nulls);
    } else {
        CalculateColVarcharHashes(vec, rowCount, hashes, nulls);
    }
#else
    CalculateColVarcharHashes(vec, rowCount, hashes, nulls);
#endif
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
                CalculateColHashesProxy<OMNI_INT>(hashCol, rowCount, hashes, curProbeNulls);
                break;
            case omniruntime::type::OMNI_SHORT:
                CalculateColHashesProxy<OMNI_SHORT>(hashCol, rowCount, hashes, curProbeNulls);
                break;
            case omniruntime::type::OMNI_LONG:
                CalculateColHashesProxy<OMNI_LONG>(hashCol, rowCount, hashes, curProbeNulls);
                break;
            case omniruntime::type::OMNI_DOUBLE:
                CalculateColHashesProxy<OMNI_DOUBLE>(hashCol, rowCount, hashes, curProbeNulls);
                break;
            case omniruntime::type::OMNI_BOOLEAN:
                CalculateColHashesProxy<OMNI_BOOLEAN>(hashCol, rowCount, hashes, curProbeNulls);
                break;
            case omniruntime::type::OMNI_DECIMAL64:
                CalculateColDec64HashesProxy(hashCol, rowCount, hashes, curProbeNulls);
                break;
            case omniruntime::type::OMNI_DECIMAL128:
                CalculateColDec128HashesProxy(hashCol, rowCount, hashes, curProbeNulls);
                break;
            case omniruntime::type::OMNI_VARCHAR:
            case omniruntime::type::OMNI_CHAR:
                CalculateColVarcharHashesProxy(hashCol, rowCount, hashes, curProbeNulls);
                break;
            default:
                break;
        }
    }
}

LookupJoinOutputBuilder::LookupJoinOutputBuilder(int32_t *probeOutputCols, int32_t probeOutputColsCount,
    const int32_t *probeOutputTypes, int32_t *buildOutputCols, int32_t buildOutputColsCount,
    const int32_t *buildOutputTypes, int32_t outputRowSize)
    : probeOutputCols(probeOutputCols),
      probeOutputColsCount(probeOutputColsCount),
      probeOutputTypes(probeOutputTypes),
      buildOutputCols(buildOutputCols),
      buildOutputColsCount(buildOutputColsCount),
      buildOutputTypes(buildOutputTypes)
{
    // if the probe and build do not have output columns, the row size is setted to DEFAULT_ROW_SIZE
    this->maxRowCount = OperatorUtil::GetMaxRowCount((outputRowSize != 0) ? outputRowSize : DEFAULT_ROW_SIZE);
    if (probeOutputColsCount > 0) {
        probeIndex.reserve(maxRowCount);
    }
    if (buildOutputColsCount > 0) {
        buildIndex.reserve(maxRowCount);
    }
}

void ALWAYS_INLINE LookupJoinOutputBuilder::AppendRow(int32_t probePosition, const JoinHashTable *hashTable,
    uint32_t joinPosition)
{
    probeRowCount++;
    if (probeOutputColsCount > 0) {
        probeIndex.emplace_back(probePosition);
    }
    if (buildOutputColsCount > 0) {
        if (hashTable != nullptr) {
            auto array = hashTable->GetBuildColumns();
            auto address = hashTable->GetBuildValueAddresses()[joinPosition];
            buildIndex.emplace_back(std::make_pair(array, address));
        } else {
            buildIndex.emplace_back(std::make_pair(nullptr, 0));
        }
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
            auto vecBatchIndex = DecodeSliceIndex(address);
            auto buildRowIdx = DecodePosition(address);
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
            auto vecBatchIndex = DecodeSliceIndex(address);
            auto buildRowIdx = DecodePosition(address);
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

void NO_INLINE LookupJoinOutputBuilder::ConstructProbeColumns(VectorBatch *vectorBatch, BaseVector **probeOutputColumns)
{
    bool isSequentialProbeIndices = true;
    if (probeRowCount > 1) { // <= 1 must be sequential
        for (int32_t i = 1; i < probeRowCount; ++i) {
            if (probeIndex[i] != probeIndex[i - 1] + 1) {
                isSequentialProbeIndices = false;
                break;
            }
        }
    }

    if (!isSequentialProbeIndices || probeRowCount == 0) {
        // probeIndices are discrete
        ConstructProbeColumnsFromPositions(vectorBatch, probeOutputColumns);
    } else if (probeRowCount == probeOutputColumns[0]->GetSize()) {
        // probeIndices are a simple covering of the vector
        ConstructProbeColumnsFromReuse(vectorBatch, probeOutputColumns);
    } else {
        // probeIndices are sequential without holes
        ConstructProbeColumnsFromSlice(vectorBatch, probeOutputColumns);
    }
}

void NO_INLINE LookupJoinOutputBuilder::ConstructBuildColumns(VectorBatch *vectorBatch)
{
    // preprocess the pointer to build table vectors -- doing a few levels of
    // pointer chasing first
    const std::pair<BaseVector ***, uint64_t> *buildTemp = buildIndex.data();
    for (int32_t j = 0; j < buildOutputColsCount; j++) {
        uint32_t outputCol = buildOutputCols[j];
        BaseVector *buildColumn = nullptr;
        switch (buildOutputTypes[j]) {
            case OMNI_LONG:
            case OMNI_DECIMAL64:
                buildColumn = ConstructBuildColumn<int64_t>(buildTemp, outputCol, probeRowCount);
                break;
            case OMNI_INT:
            case OMNI_DATE32:
                buildColumn = ConstructBuildColumn<int32_t>(buildTemp, outputCol, probeRowCount);
                break;
            case OMNI_VARCHAR:
            case OMNI_CHAR:
                buildColumn = ConstructBuildVarcharColumn(buildTemp, outputCol, probeRowCount);
                break;
            case OMNI_DECIMAL128:
                buildColumn = ConstructBuildColumn<Decimal128>(buildTemp, outputCol, probeRowCount);
                break;
            case OMNI_SHORT:
                buildColumn = ConstructBuildColumn<int16_t>(buildTemp, outputCol, probeRowCount);
                break;
            case OMNI_DOUBLE:
                buildColumn = ConstructBuildColumn<double>(buildTemp, outputCol, probeRowCount);
                break;
            case OMNI_BOOLEAN:
                buildColumn = ConstructBuildColumn<bool>(buildTemp, outputCol, probeRowCount);
                break;
            default:
                break;
        }
        vectorBatch->Append(buildColumn);
    }
}

void LookupJoinOutputBuilder::BuildOutput(BaseVector **probeOutputColumns, VectorBatch **outputVecBatch)
{
    if (probeRowCount > 0) {
        auto output = new VectorBatch(probeRowCount);
        if (probeOutputColsCount > 0) {
            ConstructProbeColumns(output, probeOutputColumns);
        }
        if (buildOutputColsCount > 0) {
            ConstructBuildColumns(output);
        }

        *outputVecBatch = output;
        probeIndex.clear();
        buildIndex.clear();
        probeRowCount = 0;
    }
}
} // end of op
} // end of omniruntime
