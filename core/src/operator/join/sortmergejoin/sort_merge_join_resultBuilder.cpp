/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: join result builder implementations
 */
#include "sort_merge_join_resultBuilder.h"
#include <memory>
#include "expression/jsonparser/jsonparser.h"
#include "operator/pages_index.h"
#include "sort_merge_join_scanner.h"
#include "util/compiler_util.h"

namespace omniruntime {
namespace op {
using namespace omniruntime::vec;
using namespace omniruntime::type;

JoinResultBuilder::JoinResultBuilder(const type::DataTypes &leftTableOutputTypes, int32_t *leftTableOutputCols,
    int32_t leftTableOutputColsCount, DynamicPagesIndex *leftTablePagesIndex,
    const type::DataTypes &rightTableOutputTypes, int32_t *rightTableOutputCols, int32_t rightTableOutputColsCount,
    DynamicPagesIndex *rightTablePagesIndex, std::string &filter, VectorAllocator *vecAllocator,
    OverflowConfig *overflowConfig, JoinType joinType)
    : leftTableOutputTypes(leftTableOutputTypes),
      leftTableOutputCols(leftTableOutputCols),
      leftTableOutputColsCount(leftTableOutputColsCount),
      leftTablePagesIndex(leftTablePagesIndex),
      rightTableOutputTypes(rightTableOutputTypes),
      rightTableOutputCols(rightTableOutputCols),
      rightTableOutputColsCount(rightTableOutputColsCount),
      rightTablePagesIndex(rightTablePagesIndex),
      filterExpStr(filter),
      vecAllocator(vecAllocator),
      joinType(joinType)
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

bool ALWAYS_INLINE IsNullFlagBatchAndRow(int32_t batchId, int32_t rowId)
{
    return batchId == JOIN_NULL_FLAG && rowId == JOIN_NULL_FLAG;
}

void JoinResultBuilder::ParsingAndOrganizationResultsForLeftTable(int32_t leftBatchId, int32_t leftRowId,
    vec::VectorBatch *buildVectorBatch, int32_t &buildRowCount)
{
    if (IsNullFlagBatchAndRow(leftBatchId, leftRowId)) {
        for (int columnIdx = 0; columnIdx < leftTableOutputColsCount; columnIdx++) {
            buildVectorBatch->GetVector(columnIdx)->SetValueNull(buildRowCount);
        }
    } else {
        for (int columnIdx = 0; columnIdx < leftTableOutputColsCount; columnIdx++) {
            AddValueToBuildVector(leftTablePagesIndex->GetColumns(leftBatchId, leftTableOutputCols[columnIdx]),
                leftRowId, buildVectorBatch->GetVector(columnIdx), buildRowCount);
        }
    }
}

void JoinResultBuilder::ParsingAndOrganizationResultsForRightTable(int32_t rightBatchId, int32_t rightRowId,
    vec::VectorBatch *buildVectorBatch, int32_t &buildRowCount)
{
    if (IsNullFlagBatchAndRow(rightBatchId, rightRowId)) {
        for (int columnIdx = 0; columnIdx < rightTableOutputColsCount; columnIdx++) {
            int32_t buildColumnIdx = leftTableOutputColsCount + columnIdx;
            buildVectorBatch->GetVector(buildColumnIdx)->SetValueNull(buildRowCount);
        }
    } else {
        for (int columnIdx = 0; columnIdx < rightTableOutputColsCount; columnIdx++) {
            int32_t buildColumnIdx = leftTableOutputColsCount + columnIdx;
            AddValueToBuildVector(rightTablePagesIndex->GetColumns(rightBatchId, rightTableOutputCols[columnIdx]),
                rightRowId, buildVectorBatch->GetVector(buildColumnIdx), buildRowCount);
        }
    }
}

void JoinResultBuilder::PaddingLeftTableNull(int64_t rightTableRowAddress, vec::VectorBatch *buildVectorBatch,
    int32_t &buildRowCount)
{
    ParsingAndOrganizationResultsForLeftTable(JOIN_NULL_FLAG, JOIN_NULL_FLAG, buildVectorBatch, buildRowCount);
    ParsingAndOrganizationResultsForRightTable(DecodeSliceIndex(rightTableRowAddress),
        DecodePosition(rightTableRowAddress), buildVectorBatch, buildRowCount);
    buildRowCount++;
}

void JoinResultBuilder::PaddingRightTableNull(int64_t leftTableRowAddress, vec::VectorBatch *buildVectorBatch,
    int32_t &buildRowCount)
{
    ParsingAndOrganizationResultsForLeftTable(DecodeSliceIndex(leftTableRowAddress),
        DecodePosition(leftTableRowAddress), buildVectorBatch, buildRowCount);
    ParsingAndOrganizationResultsForRightTable(JOIN_NULL_FLAG, JOIN_NULL_FLAG, buildVectorBatch, buildRowCount);
    buildRowCount++;
}

void JoinResultBuilder::UpdateLeftAntiJoinHandler(LeftAntiJoinHandler *leftAntiJoinHandler, int32_t addressPosition,
    std::vector<bool> &isSameBufferedKeyMatched, int32_t inputSize)
{
    if (!isSameBufferedKeyMatched.empty()) {
        if (addressPosition == inputSize - 1) {
            leftAntiJoinHandler->hasSameBufferedRow = false;
        } else {
            if (!isSameBufferedKeyMatched[addressPosition]) {
                leftAntiJoinHandler->printThisStreamRowOutFlag = true;
            }
            if (!isSameBufferedKeyMatched[addressPosition + 1]) {
                leftAntiJoinHandler->hasSameBufferedRow = false;
            } else {
                leftAntiJoinHandler->hasSameBufferedRow = true;
            }
        }
    } else {
        leftAntiJoinHandler->hasSameBufferedRow = false;
        leftAntiJoinHandler->printThisStreamRowOutFlag = true;
    }
}

int32_t JoinResultBuilder::AddJoinValueAddresses()
{
    // 1 represents the rowCount of buildVectorBatch has reached the maxRowCount
    // 0 represents the rowCount of buildVectorBatch doesn't reached the maxRowCount
    int32_t fillStatus = 0;
    int32_t inputSize = streamedTableValueAddresses.size();
    if (buildVectorBatch == nullptr) {
        buildVectorBatch = NewEmptyVectorBatch();
        buildRowCount = 0;
    }

    leftAntiJoinHandler = new LeftAntiJoinHandler;
    for (int32_t addressPosition = addressOffset; addressPosition < inputSize; addressPosition++) {
        UpdateLeftAntiJoinHandler(leftAntiJoinHandler, addressPosition, isSameBufferedKeyMatched, inputSize);
        int64_t streamedRowAddress = streamedTableValueAddresses[addressPosition];
        int64_t bufferedRowAddress = bufferedTableValueAddresses[addressPosition];
        if (preStreamedRowAddress != INT64_MAX && preStreamedRowAddress != streamedRowAddress &&
            !preLeftTableRowMatchedOut) {
            PaddingRightTableNull(preStreamedRowAddress, buildVectorBatch, buildRowCount);
        }
        if (preStreamedRowAddress == INT64_MAX || preStreamedRowAddress != streamedRowAddress) {
            preLeftTableRowMatchedOut = false;
        }

        int32_t leftBatchId = DecodeSliceIndex(streamedRowAddress);
        int32_t rightBatchId = DecodeSliceIndex(bufferedRowAddress);
        if (!(leftBatchId == JOIN_NULL_FLAG || rightBatchId == JOIN_NULL_FLAG)) {
            FreeVectorBatches(isPreKeyMatched[addressPosition], leftBatchId, rightBatchId);
        }

        PaddingNullAndVerifyingTheOutput(isPreKeyMatched, leftAntiJoinHandler, streamedRowAddress, bufferedRowAddress,
            buildVectorBatch, buildRowCount, isSameBufferedKeyMatched, isPreRowMatched, addressPosition);
        preStreamedRowAddress = streamedRowAddress;
        addressOffset++;
        if (buildRowCount >= maxRowCount) {
            fillStatus = 1;
            buildVectorBatchRowCount = buildRowCount;
            return fillStatus;
        }
    }
    delete leftAntiJoinHandler;
    if (preStreamedRowAddress != INT64_MAX && !preLeftTableRowMatchedOut) {
        PaddingRightTableNull(preStreamedRowAddress, buildVectorBatch,
            buildRowCount); // pad NULL for last mismatch row
    }
    buildVectorBatchRowCount = buildRowCount;
    preStreamedRowAddress = INT64_MAX;
    if (buildRowCount >= maxRowCount) {
        fillStatus = 1;
    }
    return fillStatus;
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
    if (buildVectorBatchRowCount > 0) {
        if (buildVectorBatchRowCount == maxRowCount) {
            outputPages.push_back(buildVectorBatch);
        } else {
            outputPages.push_back(GetVectorBatchFromSlice(buildVectorBatch, buildVectorBatchRowCount));
            VectorHelper::FreeVecBatch(buildVectorBatch);
        }
    } else {
        if (buildVectorBatch != nullptr) {
            VectorHelper::FreeVecBatch(buildVectorBatch);
        }
    }

    buildVectorBatchRowCount = 0;
    buildVectorBatch = nullptr;

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

    leftTablePagesIndex->CacheBatch(leftBatchId);
    rightTablePagesIndex->CacheBatch(rightBatchId);
    for (int32_t leftColIdx = 0; leftColIdx < leftTableOutputTypes.GetSize(); leftColIdx++) {
        auto leftVector = leftTablePagesIndex->GetColumnsFormCache(leftColIdx);
        nulls[leftColIdx] = leftVector->IsValueNull(leftRowId);
        values[leftColIdx] = VectorHelper::GetValuePtrAndLength(leftVector, leftRowId, lengths + leftColIdx);
    }

    for (int32_t rightColIdx = 0; rightColIdx < rightTableOutputTypes.GetSize(); rightColIdx++) {
        int32_t colIdx = leftTableOutputTypes.GetSize() + rightColIdx;
        auto rightVector = rightTablePagesIndex->GetColumnsFormCache(rightColIdx);
        nulls[colIdx] = rightVector->IsValueNull(rightRowId);
        values[colIdx] = VectorHelper::GetValuePtrAndLength(rightVector, rightRowId, lengths + colIdx);
    }

    return simpleFilter->Evaluate(values, nulls, lengths, reinterpret_cast<int64_t>(executionContext));
}

void JoinResultBuilder::PaddingNullAndVerifyingTheOutput(std::vector<bool> &isPreKeyMatched,
    LeftAntiJoinHandler *leftAntiJoinHandler, int64_t leftTableRowAddress, int64_t rightTableRowAddress,
    vec::VectorBatch *buildVectorBatch, int32_t &buildRowCount, std::vector<bool> &isSameBufferedKeyMatched,
    bool &isPreRowMatched, int32_t positionAddr)
{
    int32_t leftBatchId = DecodeSliceIndex(leftTableRowAddress);
    int32_t leftRowId = DecodePosition(leftTableRowAddress);
    int32_t rightBatchId = DecodeSliceIndex(rightTableRowAddress);
    int32_t rightRowId = DecodePosition(rightTableRowAddress);

    switch (joinType) {
        case JoinType::OMNI_JOIN_TYPE_INNER:
            if (IsJoinPositionEligible(leftBatchId, leftRowId, rightBatchId, rightRowId)) {
                ParsingAndOrganizationResultsForLeftTable(leftBatchId, leftRowId, buildVectorBatch, buildRowCount);
                ParsingAndOrganizationResultsForRightTable(rightBatchId, rightRowId, buildVectorBatch, buildRowCount);
                buildRowCount++;
            }
            preLeftTableRowMatchedOut = true;
            break;
        case JoinType::OMNI_JOIN_TYPE_LEFT_SEMI:
            // left semi join only needs to output the data of the left table
            if (isSameBufferedKeyMatched[positionAddr]) {
                // same buffered key match and failed to match the previous
                if (!isPreRowMatched && IsJoinPositionEligible(leftBatchId, leftRowId, rightBatchId, rightRowId)) {
                    ParsingAndOrganizationResultsForLeftTable(leftBatchId, leftRowId, buildVectorBatch, buildRowCount);
                    buildRowCount++;
                    isPreRowMatched = true;
                }
            } else {
                // not the same buffered key
                if (IsJoinPositionEligible(leftBatchId, leftRowId, rightBatchId, rightRowId)) {
                    ParsingAndOrganizationResultsForLeftTable(leftBatchId, leftRowId, buildVectorBatch, buildRowCount);
                    buildRowCount++;
                    isPreRowMatched = true;
                } else {
                    isPreRowMatched = false;
                }
            }
            preLeftTableRowMatchedOut = true;
            break;
        case JoinType::OMNI_JOIN_TYPE_LEFT:
            // JOIN_NULL_FLAG row direct output && filter match direct output, pad no NULL
            if (IsNullFlagBatchAndRow(leftBatchId, leftRowId) || IsNullFlagBatchAndRow(rightBatchId, rightRowId) ||
                IsJoinPositionEligible(leftBatchId, leftRowId, rightBatchId, rightRowId)) {
                ParsingAndOrganizationResultsForLeftTable(leftBatchId, leftRowId, buildVectorBatch, buildRowCount);
                ParsingAndOrganizationResultsForRightTable(rightBatchId, rightRowId, buildVectorBatch, buildRowCount);
                buildRowCount++;
            }
            preLeftTableRowMatchedOut = true;
            break;
        case JoinType::OMNI_JOIN_TYPE_LEFT_ANTI: // left anti join only needs to output the data of the left table
            if (leftAntiJoinHandler->hasSameBufferedRow) {
                if (leftAntiJoinHandler->printThisStreamRowOutFlag &&
                    IsJoinPositionEligible(leftBatchId, leftRowId, rightBatchId, rightRowId)) {
                    leftAntiJoinHandler->printThisStreamRowOutFlag = false;
                }
            } else {
                if (IsNullFlagBatchAndRow(rightBatchId, rightRowId)) {
                    ParsingAndOrganizationResultsForLeftTable(leftBatchId, leftRowId, buildVectorBatch, buildRowCount);
                    buildRowCount++;
                } else if (!IsJoinPositionEligible(leftBatchId, leftRowId, rightBatchId, rightRowId) &&
                    leftAntiJoinHandler->printThisStreamRowOutFlag) {
                    ParsingAndOrganizationResultsForLeftTable(leftBatchId, leftRowId, buildVectorBatch, buildRowCount);
                    buildRowCount++;
                } else {
                    leftAntiJoinHandler->printThisStreamRowOutFlag = false;
                }
            }
            preLeftTableRowMatchedOut = true;
            break;
        case JoinType::OMNI_JOIN_TYPE_FULL:
            // JOIN_NULL_FLAG row direct output && filter match direct output, pad no NULL
            if (IsNullFlagBatchAndRow(leftBatchId, leftRowId) || IsNullFlagBatchAndRow(rightBatchId, rightRowId) ||
                IsJoinPositionEligible(leftBatchId, leftRowId, rightBatchId, rightRowId)) {
                ParsingAndOrganizationResultsForLeftTable(leftBatchId, leftRowId, buildVectorBatch, buildRowCount);
                ParsingAndOrganizationResultsForRightTable(rightBatchId, rightRowId, buildVectorBatch, buildRowCount);
                buildRowCount++;
                preLeftTableRowMatchedOut = true;
            } else { // filter mismatch, pad left table NULL
                PaddingLeftTableNull(rightTableRowAddress, buildVectorBatch, buildRowCount);
            }
            break;
        default:
            throw OmniException("OPERATOR_RUNTIME_ERROR", "SMJ unsupported join type");
    }
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
