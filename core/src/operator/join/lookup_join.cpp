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
LookupJoinOperatorFactory::LookupJoinOperatorFactory(const type::DataTypes &probeTypes, int32_t *probeOutputCols,
    int32_t probeOutputColsCount, int32_t *probeHashCols, int32_t probeHashColsCount, int32_t *buildOutputCols,
    int32_t buildOutputColsCount, const type::DataTypes &buildOutputTypes, JoinType joinType,
    JoinHashTables *hashTables, OverflowConfig *overflowConfig)
    : probeTypes(probeTypes), buildOutputTypes(buildOutputTypes), joinType(joinType), hashTables(hashTables)
{
    int32_t tempProbeHashColTypes[probeHashColsCount];
    for (int32_t i = 0; i < probeHashColsCount; i++) {
        tempProbeHashColTypes[i] = probeTypes.GetIds()[probeHashCols[i]];
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
    this->hashTables->SetOriginalProbeColsCount(probeTypes.GetSize());
    this->hashTables->SetProbeTypes(&(this->probeTypes));
    this->hashTables->JoinFilterCodeGen(overflowConfig);
}

LookupJoinOperatorFactory::LookupJoinOperatorFactory(const type::DataTypes &probeTypes, int32_t *probeOutputCols,
    int32_t probeOutputColsCount, int32_t *probeHashCols, int32_t probeHashColsCount, int32_t *buildOutputCols,
    int32_t buildOutputColsCount, const type::DataTypes &buildOutputTypes, JoinType joinType,
    JoinHashTables *hashTables, int32_t originalProbeColsCount, OverflowConfig *overflowConfig)
    : LookupJoinOperatorFactory(probeTypes, probeOutputCols, probeOutputColsCount, probeHashCols, probeHashColsCount,
    buildOutputCols, buildOutputColsCount, buildOutputTypes, joinType, hashTables, overflowConfig)
{
    this->hashTables->SetOriginalProbeColsCount(originalProbeColsCount);
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
    : probeTypes(probeTypes),
      probeOutputCols(probeOutputCols),
      probeHashCols(probeHashCols),
      probeHashColTypes(probeHashColTypes),
      buildOutputCols(buildOutputCols),
      buildOutputTypes(buildOutputTypes),
      probeOnOuterSide(joinType == JoinType::OMNI_JOIN_TYPE_LEFT || joinType == JoinType::OMNI_JOIN_TYPE_FULL ||
    joinType == JoinType::OMNI_JOIN_TYPE_LEFT_ANTI),
      needTrackPosition(joinType == JoinType::OMNI_JOIN_TYPE_FULL || joinType == JoinType::OMNI_JOIN_TYPE_RIGHT),
      onlyBuildSideFirstMatch(joinType == JoinType::OMNI_JOIN_TYPE_LEFT_SEMI ||
    joinType == JoinType::OMNI_JOIN_TYPE_LEFT_ANTI),
      currentProbePositionProducedRow(false),
      hashTables(hashTables),
      joinProbe(nullptr),
      partitionedJoinPosition(INVALID_PARTITION_POSITION),
      onlyProbeOnBuildSideInvalid(joinType == JoinType::OMNI_JOIN_TYPE_LEFT_ANTI)
{
    this->outputBuilder = std::make_unique<LookupJoinOutputBuilder>(probeOutputCols.data(), probeOutputCols.size(),
        buildOutputCols.data(), buildOutputCols.size(), buildOutputTypes, outputRowSize, probeTypes);
    this->executionContext = new ExecutionContext();
    // probe output types
    for (int probeOutputCol : probeOutputCols) {
        this->outputTypes.emplace_back(probeTypes.Get()[probeOutputCol]);
    }
    // build output types
    this->outputTypes.insert(this->outputTypes.end(), this->buildOutputTypes.Get().begin(),
        this->buildOutputTypes.Get().end());
}

LookupJoinOperator::~LookupJoinOperator()
{
    delete executionContext;
    executionContext = nullptr;
}

int32_t LookupJoinOperator::AddInput(VectorBatch *vecBatch)
{
    this->input = vecBatch;
    this->joinProbe = new JoinProbe(vecBatch, probeTypes.GetSize(), probeHashCols.data(), probeHashColTypes.data(),
        probeHashCols.size());
    this->partitionedJoinPosition = INVALID_PARTITION_POSITION;

    // start probe
    ProcessProbe();
    // maybe the data has been pulled after the previous probe, and the operator status will be set to finished
    // and needs to be reset to normal.
    SetStatus(OMNI_STATUS_NORMAL);
    return 0;
}

int32_t LookupJoinOperator::GetOutput(VectorBatch **outputVecBatch)
{
    if (!outputBuilder->HasNext()) {
        if (joinProbe != nullptr) {
            delete joinProbe;
            joinProbe = nullptr;
        }
        if (input != nullptr) {
            VectorHelper::FreeVecBatch(input);
            input = nullptr;
        }
        outputBuilder->Clear();
        SetStatus(OMNI_STATUS_FINISHED);
        return 0;
    }
    // build output data
    outputBuilder->BuildOutput(joinProbe, hashTables, outputVecBatch);
    if (!outputBuilder->HasNext()) {
        outputBuilder->Clear();
        SetStatus(OMNI_STATUS_FINISHED);
        delete joinProbe;
        joinProbe = nullptr;
        VectorHelper::FreeVecBatch(input);
        input = nullptr;
    }
    return 0;
}

void LookupJoinOperator::ProcessProbe()
{
    if (!AdvanceProbePosition()) {
        return;
    }

    if (hashTables->GetSimpleFilter()) {
        // the join has filter expression
        while (joinProbe->GetPosition() >= 0) {
            JoinCurrentPositionWithFilter();
            currentProbePositionProducedRow = false;
            // advance to next probe postition
            if (!AdvanceProbePosition()) {
                break;
            }
        }
    } else {
        // the join does not have filter expression
        while (joinProbe->GetPosition() >= 0) {
            JoinCurrentPosition();
            currentProbePositionProducedRow = false;
            // advance to next probe postition
            if (!AdvanceProbePosition()) {
                break;
            }
        }
    }
}

void LookupJoinOperator::JoinCurrentPosition()
{
    // match in hash table
    if (needTrackPosition) {
        while (partitionedJoinPosition != INVALID_PARTITION_POSITION) {
            // handle data of build
            currentProbePositionProducedRow = true;
            outputBuilder->AppendRow(joinProbe->GetPosition(), partitionedJoinPosition);
            hashTables->PositionVisited(partitionedJoinPosition);
            partitionedJoinPosition = GetNextJoinPosition(partitionedJoinPosition);
        }
    } else {
        while (partitionedJoinPosition != INVALID_PARTITION_POSITION) {
            // handle data of build
            currentProbePositionProducedRow = true;
            if (!onlyProbeOnBuildSideInvalid) {
                outputBuilder->AppendRow(joinProbe->GetPosition(), partitionedJoinPosition);
            }
            if (onlyBuildSideFirstMatch) {
                break;
            }
            partitionedJoinPosition = GetNextJoinPosition(partitionedJoinPosition);
        }
    }

    // do not match in hash table
    if (probeOnOuterSide && !currentProbePositionProducedRow && partitionedJoinPosition == INVALID_PARTITION_POSITION) {
        currentProbePositionProducedRow = true;
        outputBuilder->AppendRow(joinProbe->GetPosition(), INVALID_PARTITION_POSITION);
    }
}

void LookupJoinOperator::JoinCurrentPositionWithFilter()
{
    while (partitionedJoinPosition != INVALID_PARTITION_POSITION) {
        // match in hash table
        if (hashTables->IsJoinPositionEligible(partitionedJoinPosition, joinProbe->GetPosition(),
            joinProbe->GetProbeAllColumns(), joinProbe->GetProbeAllColsCount(), executionContext)) {
            // handle data of build
            currentProbePositionProducedRow = true;
            if (!onlyProbeOnBuildSideInvalid) {
                outputBuilder->AppendRow(joinProbe->GetPosition(), partitionedJoinPosition);
            }
            if (needTrackPosition) {
                hashTables->PositionVisited(partitionedJoinPosition);
            }
            if (onlyBuildSideFirstMatch) {
                break;
            }
        }
        partitionedJoinPosition = GetNextJoinPosition(partitionedJoinPosition);
    }

    // do not match in hash table
    if (probeOnOuterSide && !currentProbePositionProducedRow && partitionedJoinPosition == INVALID_PARTITION_POSITION) {
        currentProbePositionProducedRow = true;
        outputBuilder->AppendRow(joinProbe->GetPosition(), INVALID_PARTITION_POSITION);
    }
}

bool LookupJoinOperator::AdvanceProbePosition()
{
    if (!joinProbe->AdvanceNextPosition()) {
        // finish probe
        return false;
    }

    partitionedJoinPosition = joinProbe->GetCurrentJoinPosition(hashTables);
    return true;
}

uint64_t LookupJoinOperator::GetNextJoinPosition(uint64_t currentJoinPosition) const
{
    return hashTables->GetNextJoinPosition(currentJoinPosition);
}

#ifdef ENABLE_HMPP
template <type::DataTypeId dataTypeId>
void CalculateColHashesHMPP(BaseVector *vector, uint32_t rowCount, int64_t *combinedHash, bool *nulls)
{
    using T = typename NativeType<dataTypeId>::type;
    if (vector->GetEncoding() != OMNI_DICTIONARY) {
        LogDebug("HMPP-Join-hash");
        if (vector->HasNull()) {
            for (uint32_t i = 0; i < rowCount; ++i) {
                if (vector->IsNull(static_cast<int32_t>(i))) {
                    nulls[i] = true;
                    continue;
                }
            }
        }
        HmppResult result = HmppHashUtil::ComputeHash<dataTypeId>(vector, combinedHash, 0, rowCount);
        if (result != HMPP_STS_NO_ERR) {
            throw OmniException("HMPP ERROR", "Join HMPPS_ComputeHash failed for hmpp error");
        }
        return;
    } else {
        int64_t hash;
        for (uint32_t i = 0; i < rowCount; ++i) {
            if (vector->IsNull(static_cast<int32_t>(i))) {
                nulls[i] = true;
                continue;
            }
            hash = HashUtil::HashValue(
                static_cast<Vector<DictionaryContainer<T>> *>(vector)->GetValue(static_cast<int32_t>(i)));
            combinedHash[i] = HashUtil::CombineHash(combinedHash[i], hash);
        }
    }
}

void CalculateColDec64HashesHMPP(BaseVector *vector, uint32_t rowCount, int64_t *combinedHash, bool *nulls)
{
    if (vector->GetEncoding() != OMNI_DICTIONARY) {
        LogDebug("HMPP-Join-hashDec64");
        const auto *decimalAddr =
            reinterpret_cast<const int64_t *>(VectorHelper::UnsafeGetValues(vector, OMNI_DECIMAL64));
        auto *resultHash = new int64_t[rowCount]();
        int8_t *nullAddr = nullptr;
        if (vector->HasNull()) {
            nullAddr = reinterpret_cast<int8_t *>(unsafe::UnsafeBaseVector::GetNulls(vector));
            for (uint32_t i = 0; i < rowCount; ++i) {
                if (vector->IsNull(static_cast<int32_t>(i))) {
                    nulls[i] = true;
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
        return;
    } else {
        int64_t hash;
        for (uint32_t i = 0; i < rowCount; ++i) {
            if (vector->IsNull(static_cast<int32_t>(i))) {
                nulls[i] = true;
                continue;
            }
            hash = HashUtil::HashDecimal64Value(
                reinterpret_cast<Vector<DictionaryContainer<int64_t>> *>(vector)->GetValue(static_cast<int32_t>(i)));
            combinedHash[i] = HashUtil::CombineHash(combinedHash[i], hash);
        }
    }
}

void CalculateColDec128HashesHMPP(BaseVector *vector, uint32_t rowCount, int64_t *combinedHash, bool *nulls)
{
    if (vector->GetEncoding() != OMNI_DICTIONARY) {
        LogDebug("HMPP-Join-hashDec128");
        auto *resultHash = new int64_t[rowCount]();
        int8_t *nullAddr = nullptr;
        auto *decimalAddr = reinterpret_cast<HmppDecimal128 *>(VectorHelper::UnsafeGetValues(vector, OMNI_DECIMAL128));
        if (vector->HasNull()) {
            nullAddr = reinterpret_cast<int8_t *>(unsafe::UnsafeBaseVector::GetNulls(vector));
            for (uint32_t i = 0; i < rowCount; ++i) {
                if (vector->IsNull(static_cast<int32_t>(i))) {
                    nulls[i] = true;
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
        return;
    } else {
        int64_t hash;
        for (uint32_t i = 0; i < rowCount; ++i) {
            if (vector->IsNull(static_cast<int32_t>(i))) {
                nulls[i] = true;
                continue;
            }
            Decimal128 decimal128Value =
                reinterpret_cast<Vector<DictionaryContainer<Decimal128>> *>(vector)->GetValue(static_cast<int32_t>(i));
            hash = HashUtil::HashValue(static_cast<int64_t>(decimal128Value.LowBits()), decimal128Value.HighBits());
            combinedHash[i] = HashUtil::CombineHash(combinedHash[i], hash);
        }
    }
}

void CalculateColVarcharHashesHMPP(BaseVector *vector, uint32_t rowCount, int64_t *combinedHash, bool *nulls)
{
    if (vector->GetEncoding() != OMNI_DICTIONARY) {
        LogDebug("HMPP-Join-hashVarchar");
        int8_t *nullAddr = nullptr;
        auto *resultHash = new int64_t[rowCount]();
        auto *varcharVectorAddr = reinterpret_cast<uint8_t *>(VectorHelper::UnsafeGetValues(vector, OMNI_VARCHAR));
        auto *offset = static_cast<int32_t *>(VectorHelper::UnsafeGetOffsetsAddr(vector, OMNI_VARCHAR));
        if (vector->HasNull()) {
            nullAddr = reinterpret_cast<int8_t *>(unsafe::UnsafeBaseVector::GetNulls(vector));
            for (uint32_t i = 0; i < rowCount; ++i) {
                if (vector->IsNull(static_cast<int32_t>(i))) {
                    nulls[i] = true;
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
        return;
    } else {
        int64_t hash;
        for (uint32_t i = 0; i < rowCount; ++i) {
            if (vector->IsNull(static_cast<int32_t>(i))) {
                nulls[i] = true;
                continue;
            }
            std::string_view varcharValue =
                reinterpret_cast<Vector<DictionaryContainer<std::string_view, LargeStringContainer>> *>(vector)
                    ->GetValue(static_cast<int32_t>(i));
            hash = HashUtil::HashValue(reinterpret_cast<int8_t *>(const_cast<char *>(varcharValue.data())),
                varcharValue.length());
            combinedHash[i] = HashUtil::CombineHash(combinedHash[i], hash);
        }
    }
}
#endif

template <typename T> void CalculateColHashes(BaseVector *vec, uint32_t rowCount, int64_t *hashes, bool *nulls)
{
    int64_t hash;
    if (vec->GetEncoding() != OMNI_DICTIONARY) {
        for (uint32_t i = 0; i < rowCount; ++i) {
            if (vec->IsNull(static_cast<int32_t>(i))) {
                nulls[i] = true;
                continue;
            }
            hash = HashUtil::HashValue(static_cast<Vector<T> *>(vec)->GetValue(static_cast<int32_t>(i)));
            hashes[i] = HashUtil::CombineHash(hashes[i], hash);
        }
    } else {
        for (uint32_t i = 0; i < rowCount; ++i) {
            if (vec->IsNull(static_cast<int32_t>(i))) {
                nulls[i] = true;
                continue;
            }
            hash = HashUtil::HashValue(
                static_cast<Vector<DictionaryContainer<T>> *>(vec)->GetValue(static_cast<int32_t>(i)));
            hashes[i] = HashUtil::CombineHash(hashes[i], hash);
        }
    }
}

void CalculateColDec64Hashes(BaseVector *vec, uint32_t rowCount, int64_t *hashes, bool *nulls)
{
    int64_t hash;
    if (vec->GetEncoding() != OMNI_DICTIONARY) {
        for (uint32_t i = 0; i < rowCount; ++i) {
            if (vec->IsNull(static_cast<int32_t>(i))) {
                nulls[i] = true;
                continue;
            }
            hash = HashUtil::HashDecimal64Value(static_cast<Vector<int64_t> *>(vec)->GetValue(static_cast<int32_t>(i)));
            hashes[i] = HashUtil::CombineHash(hashes[i], hash);
        }
    } else {
        for (uint32_t i = 0; i < rowCount; ++i) {
            if (vec->IsNull(static_cast<int32_t>(i))) {
                nulls[i] = true;
                continue;
            }
            hash = HashUtil::HashDecimal64Value(
                static_cast<Vector<DictionaryContainer<int64_t>> *>(vec)->GetValue(static_cast<int32_t>(i)));
            hashes[i] = HashUtil::CombineHash(hashes[i], hash);
        }
    }
}

void CalculateColDec128Hashes(BaseVector *vec, uint32_t rowCount, int64_t *hashes, bool *nulls)
{
    int64_t hash;
    Decimal128 decimal128Value;
    if (vec->GetEncoding() != OMNI_DICTIONARY) {
        for (uint32_t i = 0; i < rowCount; ++i) {
            if (vec->IsNull(static_cast<int32_t>(i))) {
                nulls[i] = true;
                continue;
            }
            decimal128Value = static_cast<Vector<Decimal128> *>(vec)->GetValue(static_cast<int32_t>(i));
            hash = HashUtil::HashValue(static_cast<int64_t>(decimal128Value.LowBits()), decimal128Value.HighBits());
            hashes[i] = HashUtil::CombineHash(hashes[i], hash);
        }
    } else {
        for (uint32_t i = 0; i < rowCount; ++i) {
            if (vec->IsNull(static_cast<int32_t>(i))) {
                nulls[i] = true;
                continue;
            }
            decimal128Value =
                static_cast<Vector<DictionaryContainer<Decimal128>> *>(vec)->GetValue(static_cast<int32_t>(i));
            hash = HashUtil::HashValue(static_cast<int64_t>(decimal128Value.LowBits()), decimal128Value.HighBits());
            hashes[i] = HashUtil::CombineHash(hashes[i], hash);
        }
    }
}

void CalculateColVarcharHashes(BaseVector *vec, uint32_t rowCount, int64_t *hashes, bool *nulls)
{
    int64_t hash;
    if (vec->GetEncoding() != OMNI_DICTIONARY) {
        for (uint32_t i = 0; i < rowCount; ++i) {
            if (vec->IsNull(static_cast<int32_t>(i))) {
                nulls[i] = true;
                continue;
            }

            std::string_view varcharValue =
                static_cast<Vector<LargeStringContainer<std::string_view>> *>(vec)->GetValue(static_cast<int32_t>(i));
            hash = HashUtil::HashValue(reinterpret_cast<int8_t *>(const_cast<char *>(varcharValue.data())),
                varcharValue.length());
            hashes[i] = HashUtil::CombineHash(hashes[i], hash);
        }
    } else {
        for (uint32_t i = 0; i < rowCount; ++i) {
            if (vec->IsNull(static_cast<int32_t>(i))) {
                nulls[i] = true;
                continue;
            }
            std::string_view varcharValue =
                static_cast<Vector<DictionaryContainer<std::string_view, LargeStringContainer>> *>(vec)->GetValue(
                    static_cast<int32_t>(i));
            hash = HashUtil::HashValue(reinterpret_cast<int8_t *>(const_cast<char *>(varcharValue.data())),
                varcharValue.length());
            hashes[i] = HashUtil::CombineHash(hashes[i], hash);
        }
    }
}

template <type::DataTypeId dataTypeId>
void CalculateColHashesProxy(BaseVector *vec, uint32_t rowCount, int64_t *hashes, bool *nulls)
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

void CalculateColDec64HashesProxy(BaseVector *vec, uint32_t rowCount, int64_t *hashes, bool *nulls)
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

void CalculateColDec128HashesProxy(BaseVector *vec, uint32_t rowCount, int64_t *hashes, bool *nulls)
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

void CalculateColVarcharHashesProxy(BaseVector *vec, uint32_t rowCount, int64_t *hashes, bool *nulls)
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

template <type::DataTypeId dataTypeId>
void CalculateOneColumnHash(BaseVector *vec, uint32_t rowCount, int64_t *hashes, bool *nulls)
{
    using T = typename NativeType<dataTypeId>::type;
    if constexpr (std::is_same_v<T, std::string_view> || std::is_same_v<T, uint8_t>) {
        CalculateColVarcharHashesProxy(vec, rowCount, hashes, nulls);
    } else if constexpr (std::is_same_v<T, Decimal128>) {
        CalculateColDec128HashesProxy(vec, rowCount, hashes, nulls);
    } else if constexpr (dataTypeId == OMNI_DECIMAL64) {
        CalculateColDec64HashesProxy(vec, rowCount, hashes, nulls);
    } else {
        CalculateColHashesProxy<dataTypeId>(vec, rowCount, hashes, nulls);
    }
}

void PopulateHashes(BaseVector **hashCols, uint32_t rowCount, int32_t *hashColTypes, uint32_t hashColsCount,
    int64_t *hashes, bool *nulls)
{
    for (uint32_t i = 0; i < hashColsCount; ++i) {
        DYNAMIC_TYPE_DISPATCH(CalculateOneColumnHash, hashColTypes[i], hashCols[i], rowCount, hashes, nulls);
    }
}

JoinProbe::JoinProbe(VectorBatch *input, uint32_t allColsCount, int32_t *hashCols, int32_t *hashColTypes,
    uint32_t hashColsCount)
    : probeAllColsCount(allColsCount),
      positionCount(input->GetRowCount()),
      probeHashColTypes(hashColTypes),
      probeHashColsCount(hashColsCount),
      position(-1)
{
    this->probeAllColumns = new BaseVector *[allColsCount];
    for (uint32_t columnIdx = 0; columnIdx < allColsCount; columnIdx++) {
        probeAllColumns[columnIdx] = input->Get(static_cast<int32_t>(columnIdx));
    }

    this->probeHashColumns = new BaseVector *[hashColsCount];
    for (uint32_t columnIdx = 0; columnIdx < hashColsCount; columnIdx++) {
        auto hashColumn = hashCols[columnIdx];
        probeHashColumns[columnIdx] = probeAllColumns[hashColumn];
    }

    this->hashes = new int64_t[this->positionCount]();
    this->nulls = new bool[this->positionCount]();
    PopulateHashes(probeHashColumns, this->positionCount, hashColTypes, hashColsCount, hashes, nulls);
}

JoinProbe::~JoinProbe()
{
    delete[] probeAllColumns;
    delete[] probeHashColumns;
    delete[] hashes;
    delete[] nulls;
}

bool JoinProbe::AdvanceNextPosition()
{
    position++;
    return position < static_cast<int32_t>(positionCount);
}

uint64_t JoinProbe::GetCurrentJoinPosition(const JoinHashTables *hashTables) const
{
    if (nulls[position]) {
        return INVALID_PARTITION_POSITION;
    }

    auto currentJoinPosition = hashTables->GetJoinPosition(position, probeHashColumns, hashes[position]);
    return currentJoinPosition;
}

LookupJoinOutputBuilder::LookupJoinOutputBuilder(int32_t *probeOutputCols, int32_t probeOutputColsCount,
    int32_t *buildOutputCols, int32_t buildOutputColsCount, const type::DataTypes &buildOutputTypes,
    int32_t outputRowSize, const type::DataTypes &probeTypes)
    : probeOutputCols(probeOutputCols),
      probeOutputColsCount(probeOutputColsCount),
      buildOutputCols(buildOutputCols),
      buildOutputColsCount(buildOutputColsCount),
      buildOutputTypes(buildOutputTypes),
      outputRowSize(outputRowSize),
      isSequentialProbeIndices(true),
      probeTypes(probeTypes)
{
    // if the probe and build do not have output columns, the row size is setted to DEFAULT_ROW_SIZE
    this->maxRowCount = OperatorUtil::GetMaxRowCount((outputRowSize != 0) ? outputRowSize : DEFAULT_ROW_SIZE);
}

void LookupJoinOutputBuilder::AppendRow(int32_t probePosition, uint64_t partitionedJoinPosition)
{
    int32_t previousPosition = probeIndex.empty() ? -1 : probeIndex[probeIndex.size() - 1];
    isSequentialProbeIndices =
        isSequentialProbeIndices && ((probePosition == previousPosition + 1) || (previousPosition == -1));
    probeIndex.push_back(probePosition);
    buildIndex.push_back(partitionedJoinPosition);
}

BaseVector *GetBuildColumnAndRowIndex(const JoinHashTables *hashTables, uint64_t partitionedJoinPosition,
    int32_t outputCol, int32_t &originalRowIndex)
{
    auto partition = hashTables->DecodePartition(partitionedJoinPosition);
    auto joinPosition = hashTables->DecodeJoinPosition(partitionedJoinPosition);
    auto hashTable = hashTables->GetHashTable(partition);
    auto pagesHash = hashTable->GetPagesHash();
    auto address = pagesHash->GetAddresses()[joinPosition];
    auto vecBatchIndex = DecodeSliceIndex(address);
    auto rowIndex = DecodePosition(address);
    auto vector = pagesHash->GetPagesHashStrategy()->GetBuildColumns()[outputCol][vecBatchIndex];
    originalRowIndex = static_cast<int32_t>(rowIndex);
    return vector;
}

template <DataTypeId typeId>
std::unique_ptr<BaseVector> ConstructBuildColumn(const JoinHashTables *hashTables, int32_t outputCol,
    uint64_t *buildIndex, int32_t offset, int32_t length)
{
    using T = typename NativeType<typeId>::type;
    using FlatVector = Vector<T>;
    using DictionaryVector = Vector<DictionaryContainer<T>>;
    auto output = std::make_unique<FlatVector>(length);
    int32_t start = offset;
    int32_t end = offset + length;
    int32_t index = 0;
    for (int32_t rowIdx = start; rowIdx < end; rowIdx++) {
        auto partitionedJoinPosition = buildIndex[rowIdx];
        if (partitionedJoinPosition != INVALID_PARTITION_POSITION) {
            int32_t originalRowIndex;
            BaseVector *buildVector =
                GetBuildColumnAndRowIndex(hashTables, partitionedJoinPosition, outputCol, originalRowIndex);
            if (buildVector->IsNull(originalRowIndex)) {
                output->SetNull(index++);
                continue;
            }
            // todo::can the processing of dictionary be placed earlier?
            if (buildVector->GetEncoding() != OMNI_DICTIONARY) {
                output->SetValue(index++, static_cast<FlatVector *>(buildVector)->GetValue(originalRowIndex));
            } else {
                output->SetValue(index++, static_cast<DictionaryVector *>(buildVector)->GetValue(originalRowIndex));
            }
        } else {
            output->SetNull(index++);
        }
    }
    return output;
}

std::unique_ptr<BaseVector> ConstructBuildVarcharColumn(const JoinHashTables *hashTables, int32_t outputCol,
    uint64_t *buildIndex, int32_t offset, int32_t length)
{
    using VarcharVector = Vector<LargeStringContainer<std::string_view>>;
    using DictionaryVector = Vector<DictionaryContainer<std::string_view, LargeStringContainer>>;
    auto output = std::make_unique<Vector<LargeStringContainer<std::string_view>>>(length);
    int32_t start = offset;
    int32_t end = offset + length;
    int32_t index = 0;
    for (int32_t rowIdx = start; rowIdx < end; rowIdx++) {
        auto partitionedJoinPosition = buildIndex[rowIdx];
        if (partitionedJoinPosition != INVALID_PARTITION_POSITION) {
            int32_t originalRowIndex;
            auto buildVector =
                GetBuildColumnAndRowIndex(hashTables, partitionedJoinPosition, outputCol, originalRowIndex);
            if (buildVector->IsNull(originalRowIndex)) {
                output->SetNull(index++);
                continue;
            }

            if (buildVector->GetEncoding() != OMNI_DICTIONARY) {
                auto value = static_cast<VarcharVector *>(buildVector)->GetValue(originalRowIndex);
                output->SetValue(index++, value);
            } else {
                auto value = static_cast<DictionaryVector *>(buildVector)->GetValue(originalRowIndex);
                output->SetValue(index++, value);
            }
        } else {
            output->SetNull(index++);
        }
    }
    return output;
}

template <DataTypeId typeId>
std::unique_ptr<BaseVector> ConstructColumnsFromSlice(BaseVector *column, int32_t offset, int32_t length)
{
    using T = typename NativeType<typeId>::type;
    // fixme::remove std::is_same_v<T, uint8_t> if NativeType<T>:type is std::string_view for varchar
    if constexpr (std::is_same_v<T, std::string_view> || std::is_same_v<T, uint8_t>) {
        return reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(column)->Slice(offset, length);
    }
    return reinterpret_cast<Vector<T> *>(column)->Slice(offset, length);
}

template <DataTypeId typeId>
std::unique_ptr<BaseVector> ConstructColumnsFromSliceForDictionary(BaseVector *column, int32_t offset, int32_t length)
{
    using T = typename NativeType<typeId>::type;
    // fixme::remove std::is_same_v<T, uint8_t> if NativeType<T>:type is std::string_view for varchar
    if constexpr (std::is_same_v<T, std::string_view> || std::is_same_v<T, uint8_t>) {
        return reinterpret_cast<Vector<DictionaryContainer<std::string_view, LargeStringContainer>> *>(column)->Slice(
            offset, length);
    }
    return reinterpret_cast<Vector<DictionaryContainer<T>> *>(column)->Slice(offset, length);
}

void ConstructProbeColumnsFromSlice(VectorBatch *vectorBatch, BaseVector **probeAllColumns,
    const int32_t *probeOutputCols, int32_t probeOutputColsCount, std::vector<int32_t> &probeIndex, int32_t position,
    int32_t rowCount, const std::vector<DataTypePtr> &probeTypes)
{
    BaseVector *column = nullptr;
    std::unique_ptr<BaseVector> probeColumn;
    for (int32_t columnIdx = 0; columnIdx < probeOutputColsCount; columnIdx++) {
        int32_t probeOutputCol = probeOutputCols[columnIdx];
        column = probeAllColumns[probeOutputCols[columnIdx]];
        // todo::can the processing of dictionary be placed earlier?
        if (column->GetEncoding() != OMNI_DICTIONARY) {
            probeColumn = DYNAMIC_TYPE_DISPATCH(ConstructColumnsFromSlice, probeTypes[probeOutputCol]->GetId(), column,
                probeIndex[position], rowCount);
        } else {
            probeColumn = DYNAMIC_TYPE_DISPATCH(ConstructColumnsFromSliceForDictionary,
                probeTypes[probeOutputCol]->GetId(), column, probeIndex[position], rowCount);
        }
        vectorBatch->Append(probeColumn.release());
    }
}

void ConstructProbeColumnsFromReuse(VectorBatch *vectorBatch, BaseVector **probeAllColumns,
    const int32_t *probeOutputCols, int32_t probeOutputColsCount, const std::vector<DataTypePtr> &probeTypes)
{
    BaseVector *column = nullptr;
    std::unique_ptr<BaseVector> probeColumn;
    for (int32_t columnIdx = 0; columnIdx < probeOutputColsCount; columnIdx++) {
        int32_t probeOutputCol = probeOutputCols[columnIdx];
        column = probeAllColumns[probeOutputCols[columnIdx]];
        int32_t rowCnt = column->GetSize();
        // todo::can the processing of dictionary be placed earlier?
        if (column->GetEncoding() != OMNI_DICTIONARY) {
            probeColumn = DYNAMIC_TYPE_DISPATCH(ConstructColumnsFromSlice, probeTypes[probeOutputCol]->GetId(), column,
                0, rowCnt);
        } else {
            probeColumn = DYNAMIC_TYPE_DISPATCH(ConstructColumnsFromSliceForDictionary,
                probeTypes[probeOutputCol]->GetId(), column, 0, rowCnt);
        }
        vectorBatch->Append(probeColumn.release());
    }
}

template <DataTypeId typeId>
std::unique_ptr<BaseVector> ConstructColumnFromPositions(BaseVector *column, int32_t *positions, int32_t rowCnt)
{
    using T = typename NativeType<typeId>::type;
    if constexpr (std::is_same_v<T, std::string_view> || std::is_same_v<T, uint8_t>) {
        return VectorHelper::CreateStringDictionary(positions, rowCnt,
            reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(column));
    } else {
        return VectorHelper::CreateDictionary(positions, rowCnt, reinterpret_cast<Vector<T> *>(column));
    }
}

template <DataTypeId typeId>
std::unique_ptr<BaseVector> ConstructColumnFromPositionsForDictionary(BaseVector *column, int32_t *positions,
    int32_t rowCnt)
{
    using T = typename NativeType<typeId>::type;
    if constexpr (std::is_same_v<T, std::string_view> || std::is_same_v<T, uint8_t>) {
        return reinterpret_cast<Vector<DictionaryContainer<T, LargeStringContainer>> *>(column)->CopyPositions(
            positions, 0, rowCnt);
    } else {
        return reinterpret_cast<Vector<DictionaryContainer<T>> *>(column)->CopyPositions(positions, 0, rowCnt);
    }
}


void ConstructProbeColumnsFromPositions(VectorBatch *vectorBatch, BaseVector **probeAllColumns,
    const int32_t *probeOutputCols, int32_t probeOutputColsCount, std::vector<int32_t> &probeIndex, int32_t position,
    int32_t rowCount, const std::vector<DataTypePtr> &probeTypes)
{
    BaseVector *column = nullptr;
    std::unique_ptr<BaseVector> probeColumn;

    for (int32_t columnIdx = 0; columnIdx < probeOutputColsCount; columnIdx++) {
        int32_t probeOutputCol = probeOutputCols[columnIdx];
        column = probeAllColumns[probeOutputCols[columnIdx]];
        // we want to keep only one level dictionary vector here
        // if the data is non-dictionary, we build dictionary to avoid data copy
        if (column->GetEncoding() == OMNI_DICTIONARY) {
            probeColumn = DYNAMIC_TYPE_DISPATCH(ConstructColumnFromPositionsForDictionary,
                probeTypes[probeOutputCol]->GetId(), column, &probeIndex[position], rowCount);
        } else {
            probeColumn = DYNAMIC_TYPE_DISPATCH(ConstructColumnFromPositions, probeTypes[probeOutputCol]->GetId(),
                column, &probeIndex[position], rowCount);
        }
        vectorBatch->Append(probeColumn.release());
    }
}

void ConstructProbeColumns(VectorBatch *vectorBatch, BaseVector **probeAllColumns, const int32_t *probeOutputCols,
    int32_t probeOutputColsCount, bool isSequentialProbeIndices, std::vector<int32_t> &probeIndex, int32_t position,
    int32_t rowCount, const std::vector<DataTypePtr> &probeTypes)
{
    if (probeOutputCols == nullptr) {
        return;
    }
    auto probeLength = static_cast<int32_t>(probeIndex.size());
    if (!isSequentialProbeIndices || probeLength == 0) {
        // probeIndices are discrete
        ConstructProbeColumnsFromPositions(vectorBatch, probeAllColumns, probeOutputCols, probeOutputColsCount,
            probeIndex, position, rowCount, probeTypes);
    } else if ((probeLength == probeAllColumns[probeOutputCols[0]]->GetSize()) && (probeLength == rowCount)) {
        // probeIndices are a simple covering of the vector
        ConstructProbeColumnsFromReuse(vectorBatch, probeAllColumns, probeOutputCols, probeOutputColsCount, probeTypes);
    } else {
        // probeIndices are sequential without holes
        ConstructProbeColumnsFromSlice(vectorBatch, probeAllColumns, probeOutputCols, probeOutputColsCount, probeIndex,
            position, rowCount, probeTypes);
    }
}

void ConstructBuildColumns(VectorBatch *vectorBatch, const JoinHashTables *hashTables,
    const std::vector<DataTypePtr> &buildOutputTypes, int32_t *buildOutputCols, int32_t buildOutputColsCount,
    std::vector<uint64_t> &buildIndex, int32_t position, int32_t rowCount)
{
    std::unique_ptr<BaseVector> buildColumn;
    int32_t buildOutputCol = 0;
    for (int32_t columnIdx = 0; columnIdx < buildOutputColsCount; columnIdx++) {
        buildOutputCol = buildOutputCols[columnIdx];
        const DataTypePtr &dataType = buildOutputTypes[columnIdx];
        if (dataType->GetId() == OMNI_VARCHAR || dataType->GetId() == OMNI_CHAR) {
            buildColumn =
                ConstructBuildVarcharColumn(hashTables, buildOutputCol, buildIndex.data(), position, rowCount);
        } else {
            buildColumn = DYNAMIC_TYPE_DISPATCH(ConstructBuildColumn, dataType->GetId(), hashTables, buildOutputCol,
                buildIndex.data(), position, rowCount);
        }
        vectorBatch->Append(buildColumn.release());
    }
}

void LookupJoinOutputBuilder::BuildOutput(const JoinProbe *joinProbe,
    const JoinHashTables *hashTables, VectorBatch **outputVecBatch)
{
    BaseVector **probeAllColumns = joinProbe->GetProbeAllColumns();

    int32_t currentRowToReturn = std::min(maxRowCount, static_cast<int32_t>(probeIndex.size()) - positionReturned);
    auto output = new VectorBatch(currentRowToReturn);
    ConstructProbeColumns(output, probeAllColumns, probeOutputCols, probeOutputColsCount,
        isSequentialProbeIndices, probeIndex, positionReturned, currentRowToReturn, probeTypes.Get());
    ConstructBuildColumns(output, hashTables, buildOutputTypes.Get(),
        buildOutputCols, buildOutputColsCount, buildIndex, positionReturned, currentRowToReturn);

    *outputVecBatch = output;
    positionReturned += currentRowToReturn;
}
} // end of op
} // end of omniruntime
