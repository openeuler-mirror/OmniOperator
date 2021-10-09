/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: lookup join implementations
 */
#include "lookup_join.h"
#include "hash_builder.h"
#include "../../vector/vector_common.h"
#include "../optimization.h"
#include "../../jit/annotation.h"
#include "../util/operator_util.h"
#include "../../vector/vector_helper.h"
#include "../pages_hash_strategy.h"

#include <vector>
#include <memory>

using namespace omniruntime::vec;
namespace omniruntime {
namespace op {
LookupJoinOperatorFactory::LookupJoinOperatorFactory(const vec::VecTypes &probeTypes, int32_t *probeOutputCols,
    int32_t probeOutputColsCount, int32_t *probeHashCols, int32_t probeHashColsCount, int32_t *buildOutputCols,
    const vec::VecTypes &buildOutputTypes, JoinType joinType, JoinHashTables *hashTables)
    : joinType(joinType), hashTables(hashTables)
{
    int32_t probeHashColTypes[probeHashColsCount];
    for (int32_t i = 0; i < probeHashColsCount; i++) {
        probeHashColTypes[i] = probeTypes.GetIds()[probeHashCols[i]];
    }
    this->probeTypes = std::make_unique<VecTypes>(probeTypes);
    this->probeOutputCols.insert(this->probeOutputCols.end(), probeOutputCols, probeOutputCols + probeOutputColsCount);
    this->probeHashCols.insert(this->probeHashCols.end(), probeHashCols, probeHashCols + probeHashColsCount);
    this->probeHashColTypes.insert(this->probeHashColTypes.end(), probeHashColTypes,
        probeHashColTypes + probeHashColsCount);
    this->buildOutputCols.insert(this->buildOutputCols.end(), buildOutputCols,
        buildOutputCols + buildOutputTypes.GetSize());
    this->buildOutputTypes = std::make_unique<VecTypes>(buildOutputTypes);
    this->joinType = joinType;
    this->hashTables = hashTables;
    this->rowSize = OperatorUtil::GetOutputRowSize(probeTypes.Get(), probeOutputCols, probeOutputColsCount);
    this->rowSize += OperatorUtil::GetRowSize(buildOutputTypes.Get());
}

LookupJoinOperatorFactory::~LookupJoinOperatorFactory() {}

LookupJoinOperatorFactory *LookupJoinOperatorFactory::CreateLookupJoinOperatorFactory(const vec::VecTypes &probeTypes,
    int32_t *probeOutputCols, int32_t probeOutputColsCount, int32_t *probeHashCols, int32_t probeHashColsCount,
    int32_t *buildOutputCols, const vec::VecTypes &buildOutputTypes, JoinType joinType, int64_t hashBuilderFactoryAddr)
{
    HashBuilderOperatorFactory *hashBuilderFactory =
        reinterpret_cast<HashBuilderOperatorFactory *>(hashBuilderFactoryAddr);
    JoinHashTables *hashTables = hashBuilderFactory->GetHashTables();
    auto pOperatorFactory =
        std::make_unique<LookupJoinOperatorFactory>(probeTypes, probeOutputCols, probeOutputColsCount, probeHashCols,
        probeHashColsCount, buildOutputCols, buildOutputTypes, joinType, hashTables);
    return pOperatorFactory.release();
}

Operator *LookupJoinOperatorFactory::CreateOperator()
{
    auto pLookupJoinOperator = std::make_unique<LookupJoinOperator>(*(probeTypes.get()), probeOutputCols, probeHashCols,
        probeHashColTypes, buildOutputCols, *(buildOutputTypes.get()), joinType, hashTables, rowSize);
    return pLookupJoinOperator.release();
}

LookupJoinOperator::LookupJoinOperator(const VecTypes &probeTypes, std::vector<int32_t> &probeOutputCols,
    std::vector<int32_t> &probeHashCols, std::vector<int32_t> &probeHashColTypes, std::vector<int32_t> &buildOutputCols,
    const vec::VecTypes &buildOutputTypes, JoinType joinType, JoinHashTables *hashTables, int32_t outputRowSize)
    : probeTypes(probeTypes), buildOutputTypes(buildOutputTypes)
{
    this->probeOutputCols = probeOutputCols;
    this->probeHashCols = probeHashCols;
    this->probeHashColTypes = probeHashColTypes;
    this->buildOutputCols = buildOutputCols;
    this->probeOnOuterSide = (joinType == OMNI_JOIN_TYPE_LEFT || joinType == OMNI_JOIN_TYPE_FULL);
    this->currentProbePositionProducedRow = false;
    this->hashTables = hashTables;
    this->joinProbe = nullptr;
    this->partitionedJoinPosition = -1;
    this->outputBuilder = std::make_unique<LookupJoinOutputBuilder>(probeTypes.GetIds(), probeOutputCols.data(),
        probeOutputCols.size(), buildOutputCols.data(), buildOutputTypes, outputRowSize);
}

LookupJoinOperator::~LookupJoinOperator() {}

int32_t LookupJoinOperator::AddInput(VectorBatch *vecBatch)
{
    this->joinProbe = std::make_unique<JoinProbe>(vecBatch, probeTypes.GetSize(), probeHashCols.data(),
        probeHashColTypes.data(), probeHashCols.size());
    this->partitionedJoinPosition = -1;

    // start probe
    ProcessProbe();
    return 0;
}

int32_t LookupJoinOperator::GetOutput(std::vector<VectorBatch *> &outputPages)
{
    // build output data
    outputBuilder->BuildOutput(vecAllocator, joinProbe.get(), hashTables, outputPages);
    SetStatus(OMNI_STATUS_FINISHED);
    return 0;
}

const int32_t *LookupJoinOperator::GetSourceTypes()
{
    return probeTypes.GetIds();
}

void LookupJoinOperator::ProcessProbe()
{
    if (joinProbe == nullptr) {
        return;
    }

    while (true) {
        if (joinProbe->GetPosition() >= 0) {
            if (!ProbeOnePosition()) {
                break;
            }
        }
        currentProbePositionProducedRow = false;

        // advance to next probe postition
        if (!AdvanceProbePosition()) {
            break;
        }
    }
}

bool LookupJoinOperator::ProbeOnePosition()
{
    // match in hash table
    if (!JoinCurrentPosition()) {
        return false;
    }

    // do not match in hash table
    if (!currentProbePositionProducedRow) {
        currentProbePositionProducedRow = true;
        if (!OuterJoinCurrentPosition()) {
            return false;
        }
    }

    return true;
}

bool LookupJoinOperator::JoinCurrentPosition()
{
    while (partitionedJoinPosition >= 0) {
        // handle data of build
        currentProbePositionProducedRow = true;
        outputBuilder->AppendRow(joinProbe->GetPosition(), partitionedJoinPosition);
        partitionedJoinPosition = GetNextJoinPosition(partitionedJoinPosition, joinProbe->GetPosition());
    }

    return true;
}

bool LookupJoinOperator::OuterJoinCurrentPosition()
{
    if (probeOnOuterSide && partitionedJoinPosition < 0) {
        outputBuilder->AppendRow(joinProbe->GetPosition(), -1);
    }
    return true;
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

int64_t LookupJoinOperator::GetNextJoinPosition(int64_t currentJoinPosition, int32_t probePosition) const
{
    int64_t result = hashTables->GetNextJoinPosition(currentJoinPosition, probePosition);
    return result;
}

template <typename T>
void CalculateColHashes(omniruntime::vec::Vector *vec, int32_t rowCount, int64_t *hashes, bool *nulls)
{
    int64_t hash;
    omniruntime::vec::Vector *result = nullptr;
    int32_t idIndex;
    if (vec->GetTypeId() != omniruntime::vec::OMNI_VEC_TYPE_DICTIONARY) {
        for (int32_t i = 0; i < rowCount; ++i) {
            if (vec->IsValueNull(i)) {
                nulls[i] = true;
                continue;
            }
            hash = HashUtil::HashValue(static_cast<T *>(vec)->GetValue(i));
            hashes[i] = HashUtil::CombineHash(hashes[i], hash);
        }
    } else {
        for (int32_t i = 0; i < rowCount; ++i) {
            result = static_cast<DictionaryVector *>(vec)->ExtractDictionaryAndId(i, idIndex);
            if (result->IsValueNull(idIndex)) {
                nulls[i] = true;
                continue;
            }
            hash = HashUtil::HashValue(static_cast<T *>(result)->GetValue(idIndex));
            hashes[i] = HashUtil::CombineHash(hashes[i], hash);
        }
    }
}

void CalculateColDec64Hashes(omniruntime::vec::Vector *vec, int32_t rowCount, int64_t *hashes, bool *nulls)
{
    int64_t hash;
    omniruntime::vec::Vector *result = nullptr;
    int32_t idIndex;
    omniruntime::vec::DictionaryVector *dictionaryVector = nullptr;
    if (vec->GetTypeId() != omniruntime::vec::OMNI_VEC_TYPE_DICTIONARY) {
        for (int32_t i = 0; i < rowCount; ++i) {
            if (vec->IsValueNull(i)) {
                nulls[i] = true;
                continue;
            }
            hash = HashUtil::HashDecimal64Value(static_cast<omniruntime::vec::LongVector *>(vec)->GetValue(i));
            hashes[i] = HashUtil::CombineHash(hashes[i], hash);
        }
    } else {
        for (int32_t i = 0; i < rowCount; ++i) {
            result = static_cast<DictionaryVector *>(vec)->ExtractDictionaryAndId(i, idIndex);
            if (result->IsValueNull(idIndex)) {
                nulls[i] = true;
                continue;
            }
            hash = HashUtil::HashDecimal64Value(static_cast<omniruntime::vec::LongVector *>(result)->GetValue(i));
            hashes[i] = HashUtil::CombineHash(hashes[i], hash);
        }
    }
}

void CalculateColDec128Hashes(omniruntime::vec::Vector *vec, int32_t rowCount, int64_t *hashes, bool *nulls)
{
    int64_t hash;
    omniruntime::vec::Vector *result = nullptr;
    int32_t idIndex;
    omniruntime::vec::DictionaryVector *dictionaryVector = nullptr;
    Decimal128 decimal128Value;
    if (vec->GetTypeId() != omniruntime::vec::OMNI_VEC_TYPE_DICTIONARY) {
        for (int32_t i = 0; i < rowCount; ++i) {
            if (vec->IsValueNull(i)) {
                nulls[i] = true;
                continue;
            }
            decimal128Value = static_cast<omniruntime::vec::Decimal128Vector *>(vec)->GetValue(i);
            hash = HashUtil::HashValue(decimal128Value.LowBits(), decimal128Value.HighBits());
            hashes[i] = HashUtil::CombineHash(hashes[i], hash);
        }
    } else {
        for (int32_t i = 0; i < rowCount; ++i) {
            result = static_cast<DictionaryVector *>(vec)->ExtractDictionaryAndId(i, idIndex);
            if (result->IsValueNull(idIndex)) {
                nulls[i] = true;
                continue;
            }
            decimal128Value = static_cast<omniruntime::vec::Decimal128Vector *>(result)->GetValue(i);
            hash = HashUtil::HashValue(decimal128Value.LowBits(), decimal128Value.HighBits());
            hashes[i] = HashUtil::CombineHash(hashes[i], hash);
        }
    }
}

void CalculateColVarCharHashes(omniruntime::vec::Vector *vec, int32_t rowCount, int64_t *hashes, bool *nulls)
{
    int64_t hash;
    Decimal128 decimal128Value;
    uint8_t *varcharValue = nullptr;
    int32_t valueLength;
    omniruntime::vec::Vector *result = nullptr;
    int32_t idIndex;
    omniruntime::vec::DictionaryVector *dictionaryVector = nullptr;
    if (vec->GetTypeId() != omniruntime::vec::OMNI_VEC_TYPE_DICTIONARY) {
        for (int32_t i = 0; i < rowCount; ++i) {
            varcharValue = nullptr;
            valueLength = static_cast<omniruntime::vec::VarcharVector *>(vec)->GetValue(i, &varcharValue);
            hash = HashUtil::HashValue(reinterpret_cast<int8_t *>(varcharValue), valueLength);
            hashes[i] = HashUtil::CombineHash(hashes[i], hash);
        }
    } else {
        for (int32_t i = 0; i < rowCount; ++i) {
            result = static_cast<DictionaryVector *>(vec)->ExtractDictionaryAndId(i, idIndex);
            if (result->IsValueNull(idIndex)) {
                nulls[i] = true;
                continue;
            }
            varcharValue = nullptr;
            valueLength = static_cast<omniruntime::vec::VarcharVector *>(result)->GetValue(i, &varcharValue);
            hash = HashUtil::HashValue(reinterpret_cast<int8_t *>(varcharValue), valueLength);
            hashes[i] = HashUtil::CombineHash(hashes[i], hash);
        }
    }
}

SPECIALIZE(OMNIJIT_HASH_LOOKUP_JOIN_POPULATE_HASHES)
void PopulateHashes(Vector **hashCols, int32_t rowCount, int32_t *hashColTypes, int32_t hashColsCount, int64_t *hashes,
    bool *nulls)
{
    for (int32_t i = 0; i < hashColsCount; ++i) {
        switch (hashColTypes[i]) {
            case omniruntime::vec::OMNI_VEC_TYPE_INT:
            case omniruntime::vec::OMNI_VEC_TYPE_DATE32:
                CalculateColHashes<omniruntime::vec::IntVector>(hashCols[i], rowCount, hashes, nulls);
                break;
            case omniruntime::vec::OMNI_VEC_TYPE_LONG:
                CalculateColHashes<omniruntime::vec::LongVector>(hashCols[i], rowCount, hashes, nulls);
                break;
            case omniruntime::vec::OMNI_VEC_TYPE_DOUBLE:
                CalculateColHashes<omniruntime::vec::DoubleVector>(hashCols[i], rowCount, hashes, nulls);
                break;
            case omniruntime::vec::OMNI_VEC_TYPE_BOOLEAN:
                CalculateColHashes<omniruntime::vec::BooleanVector>(hashCols[i], rowCount, hashes, nulls);
                break;
            case omniruntime::vec::OMNI_VEC_TYPE_DECIMAL64:
                CalculateColDec64Hashes(hashCols[i], rowCount, hashes, nulls);
                break;
            case omniruntime::vec::OMNI_VEC_TYPE_DECIMAL128: {
                CalculateColDec128Hashes(hashCols[i], rowCount, hashes, nulls);
                break;
            }
            case omniruntime::vec::OMNI_VEC_TYPE_VARCHAR: {
                CalculateColVarCharHashes(hashCols[i], rowCount, hashes, nulls);
                break;
            }
            default: {
                break;
            }
        }
    }
}

JoinProbe::JoinProbe(VectorBatch *input, int32_t allColsCount, int32_t *hashCols, int32_t *hashColTypes,
    int32_t hashColsCount)
{
    this->probeAllColumns = std::make_unique<Vector *[]>(allColsCount).release();
    for (int32_t columnIdx = 0; columnIdx < allColsCount; columnIdx++) {
        probeAllColumns[columnIdx] = input->GetVector(columnIdx);
    }
    this->probeAllColsCount = allColsCount;
    this->positionCount = input->GetRowCount();
    this->probeHashColTypes = hashColTypes;
    this->probeHashColsCount = hashColsCount;
    this->probeHashColumns = std::make_unique<Vector *[]>(hashColsCount).release();

    int32_t hashColumn;
    for (int32_t columnIdx = 0; columnIdx < hashColsCount; columnIdx++) {
        hashColumn = hashCols[columnIdx];
        probeHashColumns[columnIdx] = probeAllColumns[hashColumn];
    }
    this->position = -1;
    this->hashes = new int64_t[this->positionCount]();
    this->nulls = new bool[this->positionCount]();
    PopulateHashes(probeHashColumns, this->positionCount, hashColTypes, hashColsCount, hashes, nulls);
}

JoinProbe::~JoinProbe()
{
    delete[] hashes;
    delete[] nulls;
}

bool JoinProbe::AdvanceNextPosition()
{
    position++;
    return position < positionCount;
}

int64_t JoinProbe::GetCurrentJoinPosition(const JoinHashTables *hashTables) const
{
    if (nulls[position]) {
        return -1;
    }

    int64_t currentJoinPosition = hashTables->GetJoinPosition(position, probeHashColumns, probeHashColTypes,
        probeHashColsCount, probeAllColumns, probeAllColsCount, hashes[position]);
    return currentJoinPosition;
}

LookupJoinOutputBuilder::LookupJoinOutputBuilder(const int32_t *probeTypes, int32_t *probeOutputCols,
    int32_t probeOutputColsCount, int32_t *buildOutputCols, const vec::VecTypes &buildOutputTypes,
    int32_t outputRowSize)
    : buildOutputTypes(buildOutputTypes)
{
    this->probeTypes = probeTypes;
    this->probeOutputCols = probeOutputCols;
    this->probeOutputColsCount = probeOutputColsCount;
    this->buildOutputCols = buildOutputCols;
    this->outputRowSize = outputRowSize;
    this->isSequentialProbeIndices = true;
}

void LookupJoinOutputBuilder::AppendRow(int32_t probePosition, int64_t partitionedJoinPosition)
{
    int32_t previousPosition = probeIndex.size() == 0 ? -1 : probeIndex[probeIndex.size() - 1];
    isSequentialProbeIndices &= (probePosition == previousPosition + 1) || (previousPosition == -1);
    probeIndex.push_back(probePosition);
    buildIndex.push_back(partitionedJoinPosition);
}

Vector *GetBuildColumnAndRowIndex(const JoinHashTables *hashTables, int64_t partitionedJoinPosition, int32_t outputCol,
    int32_t &originalRowIndex)
{
    JoinHashTable *hashTable = nullptr;
    int32_t joinPosition = -1;
    if (hashTables->GetHashTableCount() != 1) {
        int32_t partition = hashTables->DecodePartition(partitionedJoinPosition);
        joinPosition = hashTables->DecodeJoinPosition(partitionedJoinPosition);
        hashTable = hashTables->GetHashTable(partition);
    } else {
        joinPosition = static_cast<int32_t>(partitionedJoinPosition);
        hashTable = hashTables->GetHashTable(0);
    }

    PagesHash *pagesHash = hashTable->GetPagesHash();
    int64_t address = pagesHash->GetAddresses()[joinPosition];
    int32_t vecBatchIndex = DecodeSliceIndex(address);
    int32_t rowIndex = DecodePosition(address);
    Vector *vector = pagesHash->GetPagesHashStrategy()->GetBuildColumns()[outputCol][vecBatchIndex];
    vector = VectorHelper::ExpandVectorAndIndex(vector, rowIndex, originalRowIndex);
    return vector;
}

template <typename T, typename V>
T *ConstructBuildColumn(VectorAllocator *vecAllocator, const JoinHashTables *hashTables, int32_t outputCol,
    int64_t *buildIndex, int32_t offset, int32_t length)
{
    int64_t partitionedJoinPosition = -1;
    V value = 0;

    auto vector = std::make_unique<T>(vecAllocator, length).release();
    int32_t start = offset;
    int32_t end = offset + length;
    int32_t index = 0;
    for (int32_t rowIdx = start; rowIdx < end; rowIdx++) {
        partitionedJoinPosition = buildIndex[rowIdx];
        if (partitionedJoinPosition != -1) {
            int32_t originalRowIndex;
            T *buildVector = static_cast<T *>(
                GetBuildColumnAndRowIndex(hashTables, partitionedJoinPosition, outputCol, originalRowIndex));
            if (buildVector->IsValueNull(originalRowIndex)) {
                vector->SetValueNull(index++);
            } else {
                V value = buildVector->GetValue(originalRowIndex);
                vector->SetValue(index++, value);
            }
        } else {
            vector->SetValueNull(index++);
        }
    }

    return vector;
}

VarcharVector *ConstructBuildVarcharColumn(VectorAllocator *vecAllocator, const JoinHashTables *hashTables,
    int32_t outputCol, int64_t *buildIndex, int32_t offset, int32_t length, uint32_t width)
{
    int64_t partitionedJoinPosition = -1;
    auto *vector = std::make_unique<VarcharVector>(vecAllocator, length * width, length).release();
    int32_t start = offset;
    int32_t end = offset + length;
    int32_t index = 0;
    for (int32_t rowIdx = start; rowIdx < end; rowIdx++) {
        partitionedJoinPosition = buildIndex[rowIdx];
        if (partitionedJoinPosition != -1) {
            int32_t originalRowIndex;
            VarcharVector *buildVector = static_cast<VarcharVector *>(
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

void ConstructProbeColumnsFromPositions(VectorBatch *vectorBatch, Vector **probeAllColumns, const int32_t *probeTypes,
    const int32_t *probeOutputCols, int32_t probeOutputColsCount, std::vector<int32_t> &probeIndex, int32_t position,
    int32_t rowCount)
{
    int32_t outputColumnIdx = 0;
    Vector *column = nullptr;
    Vector *probeColumn = nullptr;

    for (int32_t columnIdx = 0; columnIdx < probeOutputColsCount; columnIdx++) {
        column = probeAllColumns[probeOutputCols[columnIdx]];
        probeColumn = std::make_unique<DictionaryVector>(column, &probeIndex[position], rowCount).release();
        vectorBatch->SetVector(outputColumnIdx++, probeColumn);
    }
}

void ConstructProbeColumns(VectorBatch *vectorBatch, Vector **probeAllColumns, const int32_t *probeTypes,
    const int32_t *probeOutputCols, int32_t probeOutputColsCount, bool isSequentialProbeIndices,
    std::vector<int32_t> &probeIndex, int32_t position, int32_t rowCount)
{
    if (probeOutputCols == nullptr) {
        return;
    }
    int32_t probeLength = probeIndex.size();
    if (!isSequentialProbeIndices || probeLength == 0) {
        // probeIndices are discrete
        ConstructProbeColumnsFromPositions(vectorBatch, probeAllColumns, probeTypes, probeOutputCols,
            probeOutputColsCount, probeIndex, position, rowCount);
    } else if ((probeLength == probeAllColumns[probeOutputCols[0]]->GetSize()) && (probeLength == rowCount)) {
        // probeIndices are a simple covering of the vector
        ConstructProbeColumnsFromReuse(vectorBatch, probeAllColumns, probeOutputCols, probeOutputColsCount);
    } else {
        // probeIndices are sequential without holes
        ConstructProbeColumnsFromSlice(vectorBatch, probeAllColumns, probeOutputCols, probeOutputColsCount, probeIndex,
            position, rowCount);
    }
}

SPECIALIZE(OMNIJIT_CONSTRUCT_BUILD_COLUMNS)
void ConstructBuildColumns(VectorBatch *vectorBatch, const JoinHashTables *hashTables,
    const std::vector<VecType> &buildOutputTypes, const int32_t *buildOutputIds, int32_t *buildOutputCols,
    int32_t buildOutputColsCount, int32_t probeOutputColsCount, std::vector<int64_t> &buildIndex, int32_t position,
    int32_t rowCount, VectorAllocator *vecAllocator)
{
    Vector *buildColumn = nullptr;
    int32_t buildOutputCol = 0;
    int32_t outputColumnIndex = probeOutputColsCount;
    for (int32_t columnIdx = 0; columnIdx < buildOutputColsCount; columnIdx++) {
        buildOutputCol = buildOutputCols[columnIdx];
        const VecType &vecType = buildOutputTypes[columnIdx];
        switch (buildOutputIds[columnIdx]) {
            case OMNI_VEC_TYPE_INT:
            case OMNI_VEC_TYPE_DATE32:
                buildColumn = ConstructBuildColumn<IntVector, int32_t>(vecAllocator, hashTables, buildOutputCol,
                    buildIndex.data(), position, rowCount);
                break;
            case OMNI_VEC_TYPE_LONG:
            case OMNI_VEC_TYPE_DECIMAL64:
                buildColumn = ConstructBuildColumn<LongVector, int64_t>(vecAllocator, hashTables, buildOutputCol,
                    buildIndex.data(), position, rowCount);
                break;
            case OMNI_VEC_TYPE_DOUBLE:
                buildColumn = ConstructBuildColumn<DoubleVector, double>(vecAllocator, hashTables, buildOutputCol,
                    buildIndex.data(), position, rowCount);
                break;
            case OMNI_VEC_TYPE_BOOLEAN:
                buildColumn = ConstructBuildColumn<BooleanVector, bool>(vecAllocator, hashTables, buildOutputCol,
                    buildIndex.data(), position, rowCount);
                break;
            case OMNI_VEC_TYPE_VARCHAR: {
                uint32_t width = ((VarcharVecType &)vecType).GetWidth();
                buildColumn = ConstructBuildVarcharColumn(vecAllocator, hashTables, buildOutputCol, buildIndex.data(),
                    position, rowCount, width);
                break;
            }
            case OMNI_VEC_TYPE_DECIMAL128:
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
    const JoinHashTables *hashTables, std::vector<VectorBatch *> &outputTables)
{
    int32_t positionCount = probeIndex.size();
    int32_t maxRowCount = OperatorUtil::GetMaxRowCount(outputRowSize);
    int32_t tableCount = OperatorUtil::GetVecBatchCount(positionCount, maxRowCount);
    outputTables.reserve(tableCount);

    Vector **probeAllColumns = joinProbe->GetProbeAllColumns();
    int32_t columnCount = probeOutputColsCount + buildOutputTypes.GetSize();

    VectorBatch *vectorBatch = nullptr;
    int32_t position = 0;
    int32_t rowCount = 0;
    for (int32_t tableIdx = 0; tableIdx < tableCount; tableIdx++) {
        rowCount = std::min(maxRowCount, positionCount - position);
        vectorBatch = std::make_unique<VectorBatch>(columnCount).release();

        ConstructProbeColumns(vectorBatch, probeAllColumns, probeTypes, probeOutputCols, probeOutputColsCount,
            isSequentialProbeIndices, probeIndex, position, rowCount);
        ConstructBuildColumns(vectorBatch, hashTables, buildOutputTypes.Get(), buildOutputTypes.GetIds(),
            buildOutputCols, buildOutputTypes.GetSize(), probeOutputColsCount, buildIndex, position, rowCount,
            vecAllocator);

        position += rowCount;
        outputTables.push_back(vectorBatch);
    }

    probeIndex.clear();
    buildIndex.clear();
}
} // end of op
} // end of omniruntime
