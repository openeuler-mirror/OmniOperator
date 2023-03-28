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

int32_t LookupOuterJoinOperator::GetOutput(std::vector<VectorBatch *> &outputPages)
{
    totalRowCount = hashTables->GetTotalVisitedCounts() - hashTables->GetVisitedCounts();
    if (totalRowCount <= 0) {
        SetStatus(OMNI_STATUS_FINISHED);
        iterator->Reset();
        return 0;
    }
    int32_t rowCount = std::min(maxRowCount, static_cast<int32_t>(totalRowCount) - outputtedRowCount);
    outputtedRowCount += rowCount;
    auto result = new VectorBatch(outputColsCount, rowCount);
    BuildVecBatch(result);
    outputPages.push_back(result);
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
    int32_t col;
    for (col = 0; col < probeOutputTypes.GetSize(); col++) {
        auto vector =
            VectorHelper::CreateVector(vecAllocator, OMNI_VEC_ENCODING_FLAT, *probeOutputTypes.GetType(col), rowCount);
        vectorBatch->SetVector(col, vector);
        for (int32_t row = 0; row < rowCount; row++) {
            vectorBatch->GetVector(col)->SetValueNull(row);
        }
    }
    for (int32_t buildCol = 0; buildCol < buildOutputTypes.GetSize(); buildCol++) {
        auto vector = VectorHelper::CreateVector(vecAllocator, OMNI_VEC_ENCODING_FLAT,
            *buildOutputTypes.GetType(buildCol), rowCount);
        vectorBatch->SetVector(col, vector);
        col++;
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

template <typename T>
void AppendTo(VectorBatch *vectorBatch, int32_t destCol, int32_t destRowIndex, uint32_t srcRowIndex, Vector *src)
{
    auto dest = static_cast<T *>(vectorBatch->GetVector(destCol));
    int32_t originalRowIndex;
    T *originalVector =
        static_cast<T *>(VectorHelper::ExpandVectorAndIndex(src, static_cast<int32_t>(srcRowIndex), originalRowIndex));
    if (originalVector->IsValueNull(originalRowIndex)) {
        dest->SetValueNull(destRowIndex);
    } else {
        dest->SetValue(destRowIndex, originalVector->GetValue(originalRowIndex));
    }
}

void AppendToVarchar(VectorBatch *vectorBatch, int32_t destCol, int32_t destRowIndex, uint32_t srcRowIndex, Vector *src)
{
    auto dest = static_cast<VarcharVector *>(vectorBatch->GetVector(destCol));
    int32_t originalRowIndex;
    auto buildVector = static_cast<VarcharVector *>(
        VectorHelper::ExpandVectorAndIndex(src, static_cast<int32_t>(srcRowIndex), originalRowIndex));
    if (buildVector->IsValueNull(originalRowIndex)) {
        dest->SetValueNull(destRowIndex);
    } else {
        uint8_t *value = nullptr;
        int32_t valueLen = buildVector->GetValue(originalRowIndex, &value);
        dest->SetValue(destRowIndex, value, valueLen);
    }
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
        switch (buildOutputIds[col]) {
            case OMNI_SHORT:
                AppendTo<ShortVector>(vectorBatch, destCol, destRowIndex, srcRowIndex, src);
                break;
            case OMNI_INT:
            case OMNI_DATE32:
                AppendTo<IntVector>(vectorBatch, destCol, destRowIndex, srcRowIndex, src);
                break;
            case OMNI_LONG:
            case OMNI_DECIMAL64:
                AppendTo<LongVector>(vectorBatch, destCol, destRowIndex, srcRowIndex, src);
                break;
            case OMNI_DOUBLE:
                AppendTo<DoubleVector>(vectorBatch, destCol, destRowIndex, srcRowIndex, src);
                break;
            case OMNI_BOOLEAN:
                AppendTo<BooleanVector>(vectorBatch, destCol, destRowIndex, srcRowIndex, src);
                break;
            case OMNI_VARCHAR:
            case OMNI_CHAR: {
                AppendToVarchar(vectorBatch, destCol, destRowIndex, srcRowIndex, src);
                break;
            }
            case OMNI_DECIMAL128:
                AppendTo<Decimal128Vector>(vectorBatch, destCol, destRowIndex, srcRowIndex, src);
                break;
            default:
                LogError("No such data type %d", buildOutputIds[col]);
                break;
        }
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
