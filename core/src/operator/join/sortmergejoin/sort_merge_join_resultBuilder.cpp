/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: join result builder implementations
 */
#include "sort_merge_join_resultBuilder.h"
#include <memory>
#include "expression/jsonparser/jsonparser.h"
#include "operator/pages_index.h"

namespace omniruntime {
namespace op {
using namespace omniruntime::vec;
using namespace omniruntime::type;

JoinResultBuilder::JoinResultBuilder(const type::DataTypes &leftTableOutputTypes, int32_t *leftTableOutputCols,
    int32_t leftTableOutputColsCount, DynamicPagesIndex *leftTablePagesIndex,
    const type::DataTypes &rightTableOutputTypes, int32_t *rightTableOutputCols, int32_t rightTableOutputColsCount,
    DynamicPagesIndex *rightTablePagesIndex, std::string &filter, VectorAllocator *vecAllocator,
    OverflowConfig *overflowConfig)
    : leftTableOutputTypes(leftTableOutputTypes),
      leftTableOutputCols(leftTableOutputCols),
      leftTableOutputColsCount(leftTableOutputColsCount),
      leftTablePagesIndex(leftTablePagesIndex),
      rightTableOutputTypes(rightTableOutputTypes),
      rightTableOutputCols(rightTableOutputCols),
      rightTableOutputColsCount(rightTableOutputColsCount),
      rightTablePagesIndex(rightTablePagesIndex),
      filterExpStr(filter),
      vecAllocator(vecAllocator)
{
    int32_t leftRowSize =
        OperatorUtil::GetOutputRowSize(this->leftTableOutputTypes.Get(), leftTableOutputCols, leftTableOutputColsCount);
    int32_t rightRowSize = OperatorUtil::GetOutputRowSize(this->rightTableOutputTypes.Get(), rightTableOutputCols,
        rightTableOutputColsCount);
    int32_t eachRowSize = leftRowSize + rightRowSize;
    this->maxRowCount = OperatorUtil::GetMaxRowCount(eachRowSize);
    this->JoinFilterCodeGen(overflowConfig);
}

void JoinResultBuilder::JoinFilterCodeGen(OverflowConfig *overflowConfig)
{
    Parser parser;
    if (!filterExpStr.empty()) {
        omniruntime::expressions::Expr *filterExpr = JSONParser::ParseJSON(nlohmann::json::parse(filterExpStr));
        executionContext = new ExecutionContext();
        executionContext->GetArena()->SetAllocator(vecAllocator);
        simpleFilter = new SimpleFilter(*filterExpr);
        simpleFilter->Initialize(overflowConfig);
    }
}

VectorBatch *JoinResultBuilder::NewEmptyVectorBatch() const
{
    int32_t outputColCount = leftTableOutputColsCount + rightTableOutputColsCount;
    VectorBatch *vectorBatch = new VectorBatch(outputColCount, maxRowCount);
    std::vector<DataTypePtr> allTypes;
    allTypes.reserve(outputColCount);
    std::vector<DataTypePtr> leftTypes = leftTableOutputTypes.Get();
    for (int idx = 0; idx < leftTableOutputColsCount; idx++) {
        allTypes.push_back(leftTypes.at(leftTableOutputCols[idx]));
    }
    std::vector<DataTypePtr> rightTypes = rightTableOutputTypes.Get();
    for (int idx = 0; idx < rightTableOutputColsCount; idx++) {
        allTypes.push_back(rightTypes.at(rightTableOutputCols[idx]));
    }
    vectorBatch->NewVectors(vecAllocator, allTypes);
    return vectorBatch;
}

template <typename T, typename V>
void AddFixWidthValueToVector(Vector *inputVector, int32_t inputRowId, Vector *outputVector, int32_t outputRowId)
{
    T *fixWidthValueVector = static_cast<T *>(inputVector);
    T *fixWidthBuildVector = static_cast<T *>(outputVector);
    if (fixWidthValueVector->IsValueNull(inputRowId)) {
        fixWidthBuildVector->SetValueNull(outputRowId);
    } else {
        V value = fixWidthValueVector->GetValue(inputRowId);
        fixWidthBuildVector->SetValue(outputRowId, value);
    }
}

void AddVarcharValueToVector(Vector *inputVector, int32_t inputRowId, Vector *outputVector, int32_t outputRowId)
{
    auto *varcharInputVector = static_cast<VarcharVector *>(inputVector);
    auto *varcharOutputVector = static_cast<VarcharVector *>(outputVector);
    if (varcharInputVector->IsValueNull(inputRowId)) {
        varcharOutputVector->SetValueNull(outputRowId);
    } else {
        uint8_t *value = nullptr;
        int32_t valueLen = varcharInputVector->GetValue(inputRowId, &value);
        varcharOutputVector->SetValue(outputRowId, value, valueLen);
    }
}

void AddValueToBuildVector(Vector *inputVector, int32_t inputRowId, Vector *outputVector, int32_t outputRowId)
{
    int32_t originalId;
    Vector *originalVector = VectorHelper::ExpandVectorAndIndex(inputVector, inputRowId, originalId);
    switch (originalVector->GetTypeId()) {
        case OMNI_INT:
        case OMNI_DATE32:
            AddFixWidthValueToVector<IntVector, int32_t>(originalVector, originalId, outputVector, outputRowId);
            break;
        case OMNI_SHORT:
            AddFixWidthValueToVector<ShortVector, int16_t>(originalVector, originalId, outputVector, outputRowId);
            break;
        case OMNI_LONG:
        case OMNI_DECIMAL64:
            AddFixWidthValueToVector<LongVector, int64_t>(originalVector, originalId, outputVector, outputRowId);
            break;
        case OMNI_DOUBLE:
            AddFixWidthValueToVector<DoubleVector, double>(originalVector, originalId, outputVector, outputRowId);
            break;
        case OMNI_BOOLEAN:
            AddFixWidthValueToVector<BooleanVector, bool>(originalVector, originalId, outputVector, outputRowId);
            break;
        case OMNI_CHAR:
        case OMNI_VARCHAR:
            AddVarcharValueToVector(originalVector, originalId, outputVector, outputRowId);
            break;
        case OMNI_DECIMAL128:
            AddFixWidthValueToVector<Decimal128Vector, Decimal128>(originalVector, originalId, outputVector,
                outputRowId);
            break;
        default:
            break;
    }
}

int32_t JoinResultBuilder::AddJoinValueAddresses(std::vector<bool> &isPreKeyMatched,
    std::vector<int64_t> &streamedTableValueAddresses, std::vector<int64_t> &bufferedTableValueAddresses)
{
    bool isFillOneBatch = false;
    int32_t buildRowCount = 0;
    int32_t inputSize = streamedTableValueAddresses.size();
    vec::VectorBatch *buildVectorBatch = nullptr;

    if (buildVectorBatchCount == 0) {
        buildVectorBatch = NewEmptyVectorBatch();
        buildVectorBatchs.push_back(buildVectorBatch);
        buildVectorBatchRowCount.push_back(0);
        buildVectorBatchCount++;
    } else {
        buildVectorBatch = buildVectorBatchs[buildVectorBatchCount - 1];
        buildRowCount = buildVectorBatchRowCount[buildVectorBatchCount - 1];
    }

    for (int32_t addressPosition = 0; addressPosition < inputSize; addressPosition++) {
        int64_t leftAddress = streamedTableValueAddresses[addressPosition];
        int32_t leftBatchId = DecodeSliceIndex(leftAddress);
        int32_t leftRowId = DecodePosition(leftAddress);

        int64_t rightAddress = bufferedTableValueAddresses[addressPosition];
        int32_t rightBatchId = DecodeSliceIndex(rightAddress);
        int32_t rightRowId = DecodePosition(rightAddress);

        FreeVectorBatches(isPreKeyMatched[addressPosition], leftBatchId, rightBatchId);

        if (IsJoinPositionEligible(leftBatchId, leftRowId, rightBatchId, rightRowId)) {
            for (int columnIdx = 0; columnIdx < leftTableOutputColsCount; columnIdx++) {
                AddValueToBuildVector(leftTablePagesIndex->GetColumns(leftBatchId, leftTableOutputCols[columnIdx]),
                    leftRowId, buildVectorBatch->GetVector(columnIdx), buildRowCount);
            }
            for (int columnIdx = 0; columnIdx < rightTableOutputColsCount; columnIdx++) {
                int32_t buildColumnIdx = leftTableOutputColsCount + columnIdx;
                AddValueToBuildVector(rightTablePagesIndex->GetColumns(rightBatchId, rightTableOutputCols[columnIdx]),
                    rightRowId, buildVectorBatch->GetVector(buildColumnIdx), buildRowCount);
            }
            buildRowCount++;
            if (buildRowCount >= maxRowCount) {
                isFillOneBatch = true;
                buildVectorBatch = NewEmptyVectorBatch();
                buildVectorBatchs.push_back(buildVectorBatch);
                buildVectorBatchRowCount.push_back(0);
                buildVectorBatchRowCount[buildVectorBatchCount - 1] = buildRowCount;
                buildRowCount = 0;
                buildVectorBatchCount++;
            }
        }
    }

    buildVectorBatchRowCount[buildVectorBatchCount - 1] = buildRowCount;

    return isFillOneBatch ? 1 : 0;
}

void JoinResultBuilder::FreeVectorBatches(bool isPreMatched, int32_t leftBatchId, int32_t rightBatchId)
{
    if (!isPreMatched && leftBatchId > lastUnMatchedStreamedBatchId) {
        leftTablePagesIndex->FreeBeforeVecBatch(leftBatchId);
        rightTablePagesIndex->FreeBeforeVecBatch(rightBatchId);
        lastUnMatchedStreamedBatchId = leftBatchId;
    }
}

VectorBatch *GetVectorBatchFromSlice(VectorBatch *vectorBatch, int32_t rowCount)
{
    int32_t outputColCount = vectorBatch->GetVectorCount();
    VectorBatch *sliceBatch = new VectorBatch(outputColCount, rowCount);
    Vector **vectors = vectorBatch->GetVectors();
    for (int32_t columnIdx = 0; columnIdx < outputColCount; columnIdx++) {
        sliceBatch->SetVector(columnIdx, vectors[columnIdx]->Slice(0, rowCount));
    }
    return sliceBatch;
}

int32_t JoinResultBuilder::GetOutput(std::vector<omniruntime::vec::VectorBatch *> &outputPages)
{
    for (int32_t batchIdx = 0; batchIdx < buildVectorBatchCount; batchIdx++) {
        if (buildVectorBatchRowCount[batchIdx] > 0) {
            if (buildVectorBatchRowCount[batchIdx] == maxRowCount) {
                outputPages.push_back(buildVectorBatchs[batchIdx]);
            } else {
                outputPages.push_back(
                    GetVectorBatchFromSlice(buildVectorBatchs[batchIdx], buildVectorBatchRowCount[batchIdx]));
                VectorHelper::FreeVecBatch(buildVectorBatchs[batchIdx]);
            }
        } else {
            VectorHelper::FreeVecBatch(buildVectorBatchs[batchIdx]);
        }
    }
    this->buildVectorBatchCount = 0;
    this->buildVectorBatchs.clear();
    this->buildVectorBatchRowCount.clear();

    return 0;
}

bool JoinResultBuilder::IsJoinPositionEligible(int32_t leftBatchId, int32_t leftRowId, int32_t rightBatchId,
    int32_t rightRowId) const
{
    if (!simpleFilter) {
        return true;
    }

    const int32_t allColsCount = leftTableOutputTypes.GetSize() + rightTableOutputTypes.GetSize();

    int64_t values[allColsCount];
    bool nulls[allColsCount];
    int32_t lengths[allColsCount];

    for (int32_t leftColIdx = 0; leftColIdx < leftTableOutputTypes.GetSize(); leftColIdx++) {
        auto leftVector = leftTablePagesIndex->GetColumns(leftBatchId, leftColIdx);
        nulls[leftColIdx] = leftVector->IsValueNull(leftRowId);
        values[leftColIdx] = VectorHelper::GetValuePtrAndLength(leftVector, leftRowId, lengths + leftColIdx);
    }

    for (int32_t rightColIdx = 0; rightColIdx < rightTableOutputTypes.GetSize(); rightColIdx++) {
        int32_t colIdx = leftTableOutputTypes.GetSize() + rightColIdx;
        auto rightVector = rightTablePagesIndex->GetColumns(rightBatchId, rightColIdx);
        nulls[colIdx] = rightVector->IsValueNull(rightRowId);
        values[colIdx] = VectorHelper::GetValuePtrAndLength(rightVector, rightRowId, lengths + colIdx);
    }

    return simpleFilter->Evaluate(values, nulls, lengths, reinterpret_cast<int64_t>(executionContext));
}

void JoinResultBuilder::Finish()
{
    if (!isFinished) {
        leftTablePagesIndex->FreeAllRemainingVecBatch();
        rightTablePagesIndex->FreeAllRemainingVecBatch();
        isFinished = true;
    }
}

JoinResultBuilder::~JoinResultBuilder()
{
    if (simpleFilter != nullptr) {
        delete simpleFilter->GetExpression();
        delete simpleFilter;
        simpleFilter = nullptr;
    }
    if (executionContext != nullptr) {
        delete executionContext;
        executionContext = nullptr;
    }
}
}
}
