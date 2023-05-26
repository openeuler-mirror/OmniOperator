/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * @Description: lookup outer join implementations
 */
#include <vector>
#include <memory>
#include "hash_builder.h"
#include "lookup_outer_join.h"

using namespace omniruntime::vec;
namespace omniruntime {
namespace op {
LookupOuterJoinOperatorFactory::LookupOuterJoinOperatorFactory(const type::DataTypes &probeTypes,
    int32_t *probeOutputCols, int32_t probeOutputColsCount, int32_t *buildOutputCols,
    const type::DataTypes &buildOutputTypes, JoinHashTables *hashTables)
    : buildOutputTypes(buildOutputTypes), probeTypes(probeTypes), hashTables(hashTables)
{
    this->probeOutputCols.insert(this->probeOutputCols.end(), probeOutputCols, probeOutputCols + probeOutputColsCount);
    this->buildOutputCols.insert(this->buildOutputCols.end(), buildOutputCols,
        buildOutputCols + buildOutputTypes.GetSize());
}

LookupOuterJoinOperatorFactory::~LookupOuterJoinOperatorFactory() = default;

LookupOuterJoinOperatorFactory *LookupOuterJoinOperatorFactory::CreateLookupOuterJoinOperatorFactory(
    const type::DataTypes &probeTypes, int32_t *probeOutputCols, int32_t probeOutputColsCount, int32_t *buildOutputCols,
    const type::DataTypes &buildOutputTypes, int64_t hashBuilderFactoryAddr)
{
    auto hashBuilderFactory = reinterpret_cast<HashBuilderOperatorFactory *>(hashBuilderFactoryAddr);
    auto pOperatorFactory = new LookupOuterJoinOperatorFactory(probeTypes, probeOutputCols, probeOutputColsCount,
        buildOutputCols, buildOutputTypes, hashBuilderFactory->GetHashTables());
    return pOperatorFactory;
}

Operator *LookupOuterJoinOperatorFactory::CreateOperator()
{
    auto probeOutputType = std::vector<type::DataTypePtr>();
    for (auto col : probeOutputCols) {
        probeOutputType.push_back(probeTypes.Get()[col]);
    }
    auto probeOutputTypes = DataTypes(probeOutputType);
    auto lookupOuterJoinOperator =
        new LookupOuterJoinOperator(probeOutputTypes, probeOutputCols, buildOutputCols, buildOutputTypes, hashTables);
    return lookupOuterJoinOperator;
}

LookupOuterJoinOperator::LookupOuterJoinOperator(DataTypes &probeOutputTypes, std::vector<int32_t> &probeOutputCols,
    std::vector<int32_t> &buildOutputCols, const type::DataTypes &buildOutputTypes, JoinHashTables *hashTables)
    : probeOutputTypes(probeOutputTypes),
      probeOutputCols(probeOutputCols),
      buildOutputCols(buildOutputCols),
      buildOutputTypes(buildOutputTypes),
      hashTables(hashTables),
      iterator(new LookupOuterPositionIterator(hashTables)),
      outputColsCount(static_cast<int32_t>(probeOutputCols.size() + buildOutputCols.size()))
{
    int32_t outputRowSize =
        OperatorUtil::GetRowSize(this->buildOutputTypes.Get()) + OperatorUtil::GetRowSize(this->probeOutputTypes.Get());
    maxRowCount = OperatorUtil::GetMaxRowCount((outputColsCount == 0) ? DEFAULT_ROW_SIZE : outputRowSize);

    for (auto &probeOutputType : probeOutputTypes.Get()) {
        outputTypes.push_back(probeOutputType);
    }
    for (auto &buildOutputType : buildOutputTypes.Get()) {
        outputTypes.push_back(buildOutputType);
    }
}

LookupOuterJoinOperator::~LookupOuterJoinOperator()
{
    delete iterator;
    iterator = nullptr;
}

int32_t LookupOuterJoinOperator::AddInput(VectorBatch *vecBatch)
{
    // do noting, lookup outer join just process matched rows in GetOutput
    return 0;
}

int32_t LookupOuterJoinOperator::GetOutput(VectorBatch **outputVecBatch)
{
    totalRowCount = hashTables->GetTotalVisitedCounts() - hashTables->GetVisitedCounts();
    if (totalRowCount <= 0) {
        SetStatus(OMNI_STATUS_FINISHED);
        iterator->Reset();
        return 0;
    }
    int32_t rowCount = std::min(maxRowCount, static_cast<int32_t>(totalRowCount) - outputtedRowCount);
    outputtedRowCount += rowCount;
    auto result = new VectorBatch(rowCount);
    BuildVecBatch(result);
    *outputVecBatch = result;
    if (!HasNext()) {
        SetStatus(OMNI_STATUS_FINISHED);
        iterator->Reset();
        totalRowCount = 0;
        outputtedRowCount = 0;
    }
    return 0;
}

void LookupOuterJoinOperator::BuildVecBatch(VectorBatch *vectorBatch)
{
    auto rowCount = vectorBatch->GetRowCount();
    for (int32_t col = 0; col < probeOutputTypes.GetSize(); col++) {
        auto typeId = probeOutputTypes.GetType(col)->GetId();
        auto vector = VectorHelper::CreateVector(OMNI_FLAT, typeId, rowCount);
        for (int32_t row = 0; row < rowCount; row++) {
            if (typeId == type::OMNI_VARCHAR || typeId == type::OMNI_CHAR) {
                static_cast<Vector<LargeStringContainer<std::string_view>> *>(vector)->SetNull(row);
            } else {
                vector->SetNull(row);
            }
        }
        vectorBatch->Append(vector);
    }
    for (int32_t buildCol = 0; buildCol < buildOutputTypes.GetSize(); buildCol++) {
        auto vector = VectorHelper::CreateVector(OMNI_FLAT, buildOutputTypes.GetType(buildCol)->GetId(), rowCount);
        vectorBatch->Append(vector);
    }
    int32_t rows = 0;
    auto outputIds = buildOutputTypes.GetIds();
    auto buildOutputSize = buildOutputTypes.GetSize();
    auto probeOutputSize = probeOutputTypes.GetSize();
    while (rows < rowCount) {
        AppendToNext(vectorBatch, outputIds, buildOutputSize, probeOutputSize, rows);
        rows++;
    }
}

template <DataTypeId typeId>
void AppendTo(VectorBatch *vectorBatch, int32_t destCol, int32_t destRowIndex, uint32_t srcRowIndex, BaseVector *src)
{
    using Type = typename omniruntime::op::NativeAndVectorType<typeId>::type;
    using Vector = typename omniruntime::op::NativeAndVectorType<typeId>::vector;
    using DictVector = typename omniruntime::op::NativeAndVectorType<typeId>::dictVector;

    auto destVector = static_cast<Vector *>(vectorBatch->Get(destCol));
    if (src->IsNull(srcRowIndex)) {
        destVector->SetNull(destRowIndex);
        return;
    }

    Type value;
    if (src->GetEncoding() == OMNI_DICTIONARY) {
        value = static_cast<DictVector *>(src)->GetValue(srcRowIndex);
    } else {
        value = static_cast<Vector *>(src)->GetValue(srcRowIndex);
    }
    destVector->SetValue(destRowIndex, value);
}

void LookupOuterJoinOperator::AppendToNext(VectorBatch *vectorBatch, const int32_t *buildOutputIds,
    int32_t buildOutputColsCount, int32_t probeOutputColsCount, int32_t destRowIndex)
{
    uint64_t address;
    uint32_t hashTableIndex;
    iterator->NextUnVisitedAddress(hashTableIndex, address);
    auto vecBatchIndex = DecodeSliceIndex(address);
    auto srcRowIndex = DecodePosition(address);
    auto hashTable = hashTables->GetHashTable(hashTableIndex);
    for (int32_t col = 0; col < buildOutputColsCount; col++) {
        auto buildOutputCol = buildOutputCols[col];
        auto destCol = col + probeOutputColsCount;
        auto src = hashTable->GetPagesHash()->GetPagesHashStrategy()->GetBuildColumns()[buildOutputCol][vecBatchIndex];
        DYNAMIC_TYPE_DISPATCH(AppendTo, buildOutputIds[col], vectorBatch, destCol, destRowIndex, srcRowIndex, src);
    }
}

LookupOuterPositionIterator::LookupOuterPositionIterator(JoinHashTables *joinHashTables)
    : currentHashTable(0), currentPosition(0), joinHashTables(joinHashTables)
{}

void LookupOuterPositionIterator::NextUnVisitedAddress(uint32_t &hashTableIndex, uint64_t &address)
{
    while (currentHashTable < joinHashTables->GetHashTableSize()) {
        auto hashTable = joinHashTables->GetHashTable(currentHashTable);
        while (currentPosition < hashTable->GetVisitedPositionsSize()) {
            if (!hashTable->HasVisited(currentPosition)) {
                address = hashTable->GetPagesHash()->GetAddresses()[currentPosition];
                hashTableIndex = currentHashTable;
                currentPosition++;
                return;
            }
            currentPosition++;
        }
        currentPosition = 0;
        currentHashTable++;
    }
}

void LookupOuterPositionIterator::Reset()
{
    currentPosition = 0;
    currentHashTable = 0;
}
} // end of op
} // end of omniruntime
