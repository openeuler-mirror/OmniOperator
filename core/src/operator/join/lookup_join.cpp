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

using namespace omniruntime::vec;
namespace omniruntime {
namespace op {

LookupJoinOperatorFactory::LookupJoinOperatorFactory(const type::DataTypes &probeTypes, int32_t *probeOutputCols,
    int32_t probeOutputColsCount, int32_t *probeHashCols, int32_t probeHashColsCount, int32_t *buildOutputCols,
    const type::DataTypes &buildOutputTypes, JoinType joinType, JoinHashTables *hashTables)
    : probeTypes(probeTypes), buildOutputTypes(buildOutputTypes), joinType(joinType), hashTables(hashTables)
{
    int32_t probeHashColTypes[probeHashColsCount];
    for (int32_t i = 0; i < probeHashColsCount; i++) {
        probeHashColTypes[i] = probeTypes.GetIds()[probeHashCols[i]];
    }
    this->probeOutputCols.insert(this->probeOutputCols.end(), probeOutputCols, probeOutputCols + probeOutputColsCount);
    this->probeHashCols.insert(this->probeHashCols.end(), probeHashCols, probeHashCols + probeHashColsCount);
    this->probeHashColTypes.insert(this->probeHashColTypes.end(), probeHashColTypes,
        probeHashColTypes + probeHashColsCount);
    this->buildOutputCols.insert(this->buildOutputCols.end(), buildOutputCols,
        buildOutputCols + buildOutputTypes.GetSize());
    this->rowSize = OperatorUtil::GetOutputRowSize(probeTypes.Get(), probeOutputCols, probeOutputColsCount);
    this->rowSize += OperatorUtil::GetRowSize(buildOutputTypes.Get());
    this->hashTables->SetProbeTypes(&(this->probeTypes));
    this->hashTables->JoinFilterCodeGen();
}

LookupJoinOperatorFactory::~LookupJoinOperatorFactory() = default;

LookupJoinOperatorFactory *LookupJoinOperatorFactory::CreateLookupJoinOperatorFactory(const DataTypes &probeTypes,
    int32_t *probeOutputCols, int32_t probeOutputColsCount, int32_t *probeHashCols, int32_t probeHashColsCount,
    int32_t *buildOutputCols, const DataTypes &buildOutputTypes, JoinType inputJoinType, int64_t hashBuilderFactoryAddr)
{
    auto hashBuilderFactory = reinterpret_cast<HashBuilderOperatorFactory *>(hashBuilderFactoryAddr);
    auto pOperatorFactory =
        new LookupJoinOperatorFactory(probeTypes, probeOutputCols, probeOutputColsCount, probeHashCols,
        probeHashColsCount, buildOutputCols, buildOutputTypes, inputJoinType, hashBuilderFactory->GetHashTables());
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
      probeOnOuterSide(joinType == JoinType::OMNI_JOIN_TYPE_LEFT || joinType == JoinType::OMNI_JOIN_TYPE_FULL),
      needTrackPosition(joinType == JoinType::OMNI_JOIN_TYPE_FULL || joinType == JoinType::OMNI_JOIN_TYPE_RIGHT),
      currentProbePositionProducedRow(false),
      hashTables(hashTables),
      joinProbe(nullptr),
      partitionedJoinPosition(INVALID_PARTITION_POSITION)
{
    this->outputBuilder = std::make_unique<LookupJoinOutputBuilder>(probeOutputCols.data(), probeOutputCols.size(),
        buildOutputCols.data(), buildOutputTypes, outputRowSize);
    this->executionContext = new ExecutionContext();
    this->executionContext->GetArena()->SetAllocator(vecAllocator);
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
    return 0;
}

int32_t LookupJoinOperator::GetOutput(std::vector<VectorBatch *> &outputPages)
{
    // build output data
    outputBuilder->BuildOutput(vecAllocator, joinProbe, hashTables, outputPages);
    SetStatus(OMNI_STATUS_FINISHED);
    delete joinProbe;
    joinProbe = nullptr;
    VectorHelper::FreeVecBatch(input);
    input = nullptr;
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
            outputBuilder->AppendRow(joinProbe->GetPosition(), partitionedJoinPosition);
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
            outputBuilder->AppendRow(joinProbe->GetPosition(), partitionedJoinPosition);
            if (needTrackPosition) {
                hashTables->PositionVisited(partitionedJoinPosition);
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

template <typename T>
void CalculateColHashes(omniruntime::vec::Vector *vec, uint32_t rowCount, int64_t *hashes, bool *nulls)
{
    int64_t hash;
    omniruntime::vec::Vector *result = nullptr;
    int32_t idIndex;
    if (vec->GetEncoding() != omniruntime::vec::OMNI_VEC_ENCODING_DICTIONARY) {
        for (uint32_t i = 0; i < rowCount; ++i) {
            if (vec->IsValueNull(static_cast<int32_t>(i))) {
                nulls[i] = true;
                continue;
            }
            hash = HashUtil::HashValue(static_cast<T *>(vec)->GetValue(static_cast<int32_t>(i)));
            hashes[i] = HashUtil::CombineHash(hashes[i], hash);
        }
    } else {
        for (uint32_t i = 0; i < rowCount; ++i) {
            result = static_cast<DictionaryVector *>(vec)->ExtractDictionaryAndId(static_cast<int32_t>(i), idIndex);
            if (result->IsValueNull(idIndex)) {
                nulls[i] = true;
                continue;
            }
            hash = HashUtil::HashValue(static_cast<T *>(result)->GetValue(idIndex));
            hashes[i] = HashUtil::CombineHash(hashes[i], hash);
        }
    }
}

void CalculateColDec64Hashes(omniruntime::vec::Vector *vec, uint32_t rowCount, int64_t *hashes, bool *nulls)
{
    int64_t hash;
    omniruntime::vec::Vector *result = nullptr;
    int32_t idIndex;
    if (vec->GetEncoding() != omniruntime::vec::OMNI_VEC_ENCODING_DICTIONARY) {
        for (uint32_t i = 0; i < rowCount; ++i) {
            if (vec->IsValueNull(static_cast<int32_t>(i))) {
                nulls[i] = true;
                continue;
            }
            hash = HashUtil::HashDecimal64Value(
                static_cast<omniruntime::vec::LongVector *>(vec)->GetValue(static_cast<int32_t>(i)));
            hashes[i] = HashUtil::CombineHash(hashes[i], hash);
        }
    } else {
        for (uint32_t i = 0; i < rowCount; ++i) {
            result = static_cast<DictionaryVector *>(vec)->ExtractDictionaryAndId(static_cast<int32_t>(i), idIndex);
            if (result->IsValueNull(idIndex)) {
                nulls[i] = true;
                continue;
            }
            hash = HashUtil::HashDecimal64Value(static_cast<omniruntime::vec::LongVector *>(result)->GetValue(idIndex));
            hashes[i] = HashUtil::CombineHash(hashes[i], hash);
        }
    }
}

void CalculateColDec128Hashes(omniruntime::vec::Vector *vec, uint32_t rowCount, int64_t *hashes, bool *nulls)
{
    int64_t hash;
    omniruntime::vec::Vector *result = nullptr;
    int32_t idIndex;
    Decimal128 decimal128Value;
    if (vec->GetEncoding() != omniruntime::vec::OMNI_VEC_ENCODING_DICTIONARY) {
        for (uint32_t i = 0; i < rowCount; ++i) {
            if (vec->IsValueNull(static_cast<int32_t>(i))) {
                nulls[i] = true;
                continue;
            }
            decimal128Value = static_cast<omniruntime::vec::Decimal128Vector *>(vec)->GetValue(static_cast<int32_t>(i));
            hash = HashUtil::HashValue(static_cast<int64_t>(decimal128Value.LowBits()), decimal128Value.HighBits());
            hashes[i] = HashUtil::CombineHash(hashes[i], hash);
        }
    } else {
        for (uint32_t i = 0; i < rowCount; ++i) {
            result = static_cast<DictionaryVector *>(vec)->ExtractDictionaryAndId(static_cast<int32_t>(i), idIndex);
            if (result->IsValueNull(idIndex)) {
                nulls[i] = true;
                continue;
            }
            decimal128Value = static_cast<omniruntime::vec::Decimal128Vector *>(result)->GetValue(idIndex);
            hash = HashUtil::HashValue(static_cast<int64_t>(decimal128Value.LowBits()), decimal128Value.HighBits());
            hashes[i] = HashUtil::CombineHash(hashes[i], hash);
        }
    }
}

void CalculateColVarcharHashes(omniruntime::vec::Vector *vec, uint32_t rowCount, int64_t *hashes, bool *nulls)
{
    int64_t hash;
    uint8_t *varcharValue = nullptr;
    int32_t valueLength;
    omniruntime::vec::Vector *result = nullptr;
    int32_t idIndex;
    if (vec->GetEncoding() != omniruntime::vec::OMNI_VEC_ENCODING_DICTIONARY) {
        for (uint32_t i = 0; i < rowCount; ++i) {
            if (vec->IsValueNull(static_cast<int32_t>(i))) {
                nulls[i] = true;
                continue;
            }
            varcharValue = nullptr;
            valueLength =
                static_cast<omniruntime::vec::VarcharVector *>(vec)->GetValue(static_cast<int32_t>(i), &varcharValue);
            hash = HashUtil::HashValue(reinterpret_cast<int8_t *>(varcharValue), valueLength);
            hashes[i] = HashUtil::CombineHash(hashes[i], hash);
        }
    } else {
        for (uint32_t i = 0; i < rowCount; ++i) {
            result = static_cast<DictionaryVector *>(vec)->ExtractDictionaryAndId(static_cast<int32_t>(i), idIndex);
            if (result->IsValueNull(idIndex)) {
                nulls[i] = true;
                continue;
            }
            varcharValue = nullptr;
            valueLength = static_cast<omniruntime::vec::VarcharVector *>(result)->GetValue(idIndex, &varcharValue);
            hash = HashUtil::HashValue(reinterpret_cast<int8_t *>(varcharValue), valueLength);
            hashes[i] = HashUtil::CombineHash(hashes[i], hash);
        }
    }
}

void PopulateHashes(Vector **hashCols, uint32_t rowCount, int32_t *hashColTypes, uint32_t hashColsCount,
    int64_t *hashes, bool *nulls)
{
    for (uint32_t i = 0; i < hashColsCount; ++i) {
        switch (hashColTypes[i]) {
            case omniruntime::type::OMNI_INT:
            case omniruntime::type::OMNI_DATE32:
                CalculateColHashes<omniruntime::vec::IntVector>(hashCols[i], rowCount, hashes, nulls);
                break;
            case omniruntime::type::OMNI_SHORT:
                CalculateColHashes<omniruntime::vec::ShortVector>(hashCols[i], rowCount, hashes, nulls);
                break;
            case omniruntime::type::OMNI_LONG:
                CalculateColHashes<omniruntime::vec::LongVector>(hashCols[i], rowCount, hashes, nulls);
                break;
            case omniruntime::type::OMNI_DOUBLE:
                CalculateColHashes<omniruntime::vec::DoubleVector>(hashCols[i], rowCount, hashes, nulls);
                break;
            case omniruntime::type::OMNI_BOOLEAN:
                CalculateColHashes<omniruntime::vec::BooleanVector>(hashCols[i], rowCount, hashes, nulls);
                break;
            case omniruntime::type::OMNI_DECIMAL64:
                CalculateColDec64Hashes(hashCols[i], rowCount, hashes, nulls);
                break;
            case omniruntime::type::OMNI_DECIMAL128: {
                CalculateColDec128Hashes(hashCols[i], rowCount, hashes, nulls);
                break;
            }
            case omniruntime::type::OMNI_VARCHAR:
            case omniruntime::type::OMNI_CHAR: {
                CalculateColVarcharHashes(hashCols[i], rowCount, hashes, nulls);
                break;
            }
            default: {
                break;
            }
        }
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
    this->probeAllColumns = new Vector *[allColsCount];
    for (uint32_t columnIdx = 0; columnIdx < allColsCount; columnIdx++) {
        probeAllColumns[columnIdx] = input->GetVector(static_cast<int32_t>(columnIdx));
    }

    this->probeHashColumns = new Vector *[hashColsCount];
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
    int32_t *buildOutputCols, const type::DataTypes &buildOutputTypes, int32_t outputRowSize)
    : probeOutputCols(probeOutputCols),
      probeOutputColsCount(probeOutputColsCount),
      buildOutputCols(buildOutputCols),
      buildOutputTypes(buildOutputTypes),
      outputRowSize(outputRowSize),
      isSequentialProbeIndices(true)
{}

void LookupJoinOutputBuilder::AppendRow(int32_t probePosition, uint64_t partitionedJoinPosition)
{
    int32_t previousPosition = probeIndex.empty() ? -1 : probeIndex[probeIndex.size() - 1];
    isSequentialProbeIndices =
        isSequentialProbeIndices && ((probePosition == previousPosition + 1) || (previousPosition == -1));
    probeIndex.push_back(probePosition);
    buildIndex.push_back(partitionedJoinPosition);
}

Vector *GetBuildColumnAndRowIndex(const JoinHashTables *hashTables, uint64_t partitionedJoinPosition, int32_t outputCol,
    int32_t &originalRowIndex)
{
    auto partition = hashTables->DecodePartition(partitionedJoinPosition);
    auto joinPosition = hashTables->DecodeJoinPosition(partitionedJoinPosition);
    auto hashTable = hashTables->GetHashTable(partition);
    auto pagesHash = hashTable->GetPagesHash();
    auto address = pagesHash->GetAddresses()[joinPosition];
    auto vecBatchIndex = DecodeSliceIndex(address);
    auto rowIndex = DecodePosition(address);
    auto vector = pagesHash->GetPagesHashStrategy()->GetBuildColumns()[outputCol][vecBatchIndex];
    vector = VectorHelper::ExpandVectorAndIndex(vector, static_cast<int32_t>(rowIndex), originalRowIndex);
    return vector;
}

template <typename T, typename V>
T *ConstructBuildColumn(VectorAllocator *vecAllocator, const JoinHashTables *hashTables, int32_t outputCol,
    uint64_t *buildIndex, int32_t offset, int32_t length)
{
    auto vector = new T(vecAllocator, length);
    int32_t start = offset;
    int32_t end = offset + length;
    int32_t index = 0;
    for (int32_t rowIdx = start; rowIdx < end; rowIdx++) {
        auto partitionedJoinPosition = buildIndex[rowIdx];
        if (partitionedJoinPosition != INVALID_PARTITION_POSITION) {
            int32_t originalRowIndex;
            T *buildVector = static_cast<T *>(
                GetBuildColumnAndRowIndex(hashTables, partitionedJoinPosition, outputCol, originalRowIndex));
            if (buildVector->IsValueNull(originalRowIndex)) {
                vector->SetValueNull(index++);
            } else {
                vector->SetValue(index++, buildVector->GetValue(originalRowIndex));
            }
        } else {
            vector->SetValueNull(index++);
        }
    }

    return vector;
}

VarcharVector *ConstructBuildVarcharColumn(VectorAllocator *vecAllocator, const JoinHashTables *hashTables,
    int32_t outputCol, uint64_t *buildIndex, int32_t offset, int32_t length, uint32_t width)
{
    auto *vector = new VarcharVector(vecAllocator, static_cast<int32_t>(length * width), length);
    int32_t start = offset;
    int32_t end = offset + length;
    int32_t index = 0;
    for (int32_t rowIdx = start; rowIdx < end; rowIdx++) {
        auto partitionedJoinPosition = buildIndex[rowIdx];
        if (partitionedJoinPosition != INVALID_PARTITION_POSITION) {
            int32_t originalRowIndex;
            auto buildVector = static_cast<VarcharVector *>(
                GetBuildColumnAndRowIndex(hashTables, partitionedJoinPosition, outputCol, originalRowIndex));
            if (buildVector->IsValueNull(originalRowIndex)) {
                vector->SetValueNull(index++);
            } else {
                uint8_t *value = nullptr;
                int32_t valueLen = buildVector->GetValue(originalRowIndex, &value);
                vector->SetValue(index++, value, valueLen);
            }
        } else {
            vector->SetValueNull(index++);
        }
    }
    return vector;
}

void ConstructProbeColumnsFromSlice(VectorBatch *vectorBatch, Vector **probeAllColumns, const int32_t *probeOutputCols,
    int32_t probeOutputColsCount, std::vector<int32_t> &probeIndex, int32_t position, int32_t rowCount)
{
    int32_t outputColumnIdx = 0;
    Vector *column = nullptr;
    Vector *probeColumn = nullptr;
    for (int32_t columnIdx = 0; columnIdx < probeOutputColsCount; columnIdx++) {
        column = probeAllColumns[probeOutputCols[columnIdx]];
        probeColumn = column->Slice(probeIndex[position], rowCount);
        vectorBatch->SetVector(outputColumnIdx++, probeColumn);
    }
}

void ConstructProbeColumnsFromReuse(VectorBatch *vectorBatch, Vector **probeAllColumns, const int32_t *probeOutputCols,
    int32_t probeOutputColsCount)
{
    int32_t outputColumnIdx = 0;
    Vector *column = nullptr;
    for (int32_t columnIdx = 0; columnIdx < probeOutputColsCount; columnIdx++) {
        column = probeAllColumns[probeOutputCols[columnIdx]];
        column = column->Slice(0, column->GetSize());
        vectorBatch->SetVector(outputColumnIdx++, column);
    }
}

void ConstructProbeColumnsFromPositions(VectorBatch *vectorBatch, Vector **probeAllColumns,
    const int32_t *probeOutputCols, int32_t probeOutputColsCount, std::vector<int32_t> &probeIndex, int32_t position,
    int32_t rowCount)
{
    int32_t outputColumnIdx = 0;
    Vector *column = nullptr;
    Vector *probeColumn = nullptr;

    for (int32_t columnIdx = 0; columnIdx < probeOutputColsCount; columnIdx++) {
        column = probeAllColumns[probeOutputCols[columnIdx]];
        // we want to keep only one level dictionary vector here
        // if the data is non-dictionary, we build dictionary to avoid data copy
        if (column->GetEncoding() == vec::OMNI_VEC_ENCODING_DICTIONARY) {
            probeColumn = column->CopyPositions(&probeIndex[position], 0, rowCount);
        } else {
            probeColumn = new DictionaryVector(column, &probeIndex[position], rowCount);
        }
        vectorBatch->SetVector(outputColumnIdx++, probeColumn);
    }
}

void ConstructProbeColumns(VectorBatch *vectorBatch, Vector **probeAllColumns, const int32_t *probeOutputCols,
    int32_t probeOutputColsCount, bool isSequentialProbeIndices, std::vector<int32_t> &probeIndex, int32_t position,
    int32_t rowCount)
{
    if (probeOutputCols == nullptr) {
        return;
    }
    auto probeLength = static_cast<int32_t>(probeIndex.size());
    if (!isSequentialProbeIndices || probeLength == 0) {
        // probeIndices are discrete
        ConstructProbeColumnsFromPositions(vectorBatch, probeAllColumns, probeOutputCols, probeOutputColsCount,
            probeIndex, position, rowCount);
    } else if ((probeLength == probeAllColumns[probeOutputCols[0]]->GetSize()) && (probeLength == rowCount)) {
        // probeIndices are a simple covering of the vector
        ConstructProbeColumnsFromReuse(vectorBatch, probeAllColumns, probeOutputCols, probeOutputColsCount);
    } else {
        // probeIndices are sequential without holes
        ConstructProbeColumnsFromSlice(vectorBatch, probeAllColumns, probeOutputCols, probeOutputColsCount, probeIndex,
            position, rowCount);
    }
}

void ConstructBuildColumns(VectorBatch *vectorBatch, const JoinHashTables *hashTables,
    const std::vector<DataTypePtr> &buildOutputTypes, const int32_t *buildOutputIds, int32_t *buildOutputCols,
    int32_t buildOutputColsCount, int32_t probeOutputColsCount, std::vector<uint64_t> &buildIndex, int32_t position,
    int32_t rowCount, VectorAllocator *vecAllocator)
{
    Vector *buildColumn = nullptr;
    int32_t buildOutputCol = 0;
    int32_t outputColumnIndex = probeOutputColsCount;
    for (int32_t columnIdx = 0; columnIdx < buildOutputColsCount; columnIdx++) {
        buildOutputCol = buildOutputCols[columnIdx];
        const DataTypePtr &dataType = buildOutputTypes[columnIdx];
        switch (buildOutputIds[columnIdx]) {
            case OMNI_INT:
            case OMNI_DATE32:
                buildColumn = ConstructBuildColumn<IntVector, int32_t>(vecAllocator, hashTables, buildOutputCol,
                    buildIndex.data(), position, rowCount);
                break;
            case OMNI_SHORT:
                buildColumn = ConstructBuildColumn<ShortVector, int16_t>(vecAllocator, hashTables, buildOutputCol,
                    buildIndex.data(), position, rowCount);
                break;
            case OMNI_LONG:
            case OMNI_DECIMAL64:
                buildColumn = ConstructBuildColumn<LongVector, int64_t>(vecAllocator, hashTables, buildOutputCol,
                    buildIndex.data(), position, rowCount);
                break;
            case OMNI_DOUBLE:
                buildColumn = ConstructBuildColumn<DoubleVector, double>(vecAllocator, hashTables, buildOutputCol,
                    buildIndex.data(), position, rowCount);
                break;
            case OMNI_BOOLEAN:
                buildColumn = ConstructBuildColumn<BooleanVector, bool>(vecAllocator, hashTables, buildOutputCol,
                    buildIndex.data(), position, rowCount);
                break;
            case OMNI_VARCHAR:
            case OMNI_CHAR: {
                uint32_t width = static_cast<VarcharDataType *>(dataType.get())->GetWidth();
                buildColumn = ConstructBuildVarcharColumn(vecAllocator, hashTables, buildOutputCol, buildIndex.data(),
                    position, rowCount, width);
                break;
            }
            case OMNI_DECIMAL128:
                buildColumn = ConstructBuildColumn<Decimal128Vector, Decimal128>(vecAllocator, hashTables,
                    buildOutputCol, buildIndex.data(), position, rowCount);
                break;
            default:
                break;
        }
        vectorBatch->SetVector(outputColumnIndex++, buildColumn);
    }
}

void LookupJoinOutputBuilder::BuildOutput(VectorAllocator *vecAllocator, const JoinProbe *joinProbe,
    const JoinHashTables *hashTables, std::vector<VectorBatch *> &outputVecBatches)
{
    auto positionCount = static_cast<int32_t>(probeIndex.size());
    // if the probe and build do not have output columns, the row size is setted to DEFAULT_ROW_SIZE
    int32_t maxRowCount = OperatorUtil::GetMaxRowCount((outputRowSize != 0) ? outputRowSize : DEFAULT_ROW_SIZE);
    int32_t tableCount = OperatorUtil::GetVecBatchCount(positionCount, maxRowCount);

    Vector **probeAllColumns = joinProbe->GetProbeAllColumns();
    int32_t columnCount = probeOutputColsCount + buildOutputTypes.GetSize();

    VectorBatch *vectorBatch = nullptr;
    int32_t position = 0;
    int32_t rowCount = 0;
    for (int32_t tableIdx = 0; tableIdx < tableCount; tableIdx++) {
        rowCount = std::min(maxRowCount, positionCount - position);
        vectorBatch = new VectorBatch(columnCount, rowCount);

        ConstructProbeColumns(vectorBatch, probeAllColumns, probeOutputCols, probeOutputColsCount,
            isSequentialProbeIndices, probeIndex, position, rowCount);
        ConstructBuildColumns(vectorBatch, hashTables, buildOutputTypes.Get(), buildOutputTypes.GetIds(),
            buildOutputCols, buildOutputTypes.GetSize(), probeOutputColsCount, buildIndex, position, rowCount,
            vecAllocator);

        position += rowCount;
        outputVecBatches.push_back(vectorBatch);
    }

    isSequentialProbeIndices = true;
    probeIndex.clear();
    buildIndex.clear();
}
} // end of op
} // end of omniruntime
