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

JoinResultBuilder::JoinResultBuilder(const std::vector<DataTypePtr> &leftTableOutputTypes, int32_t *leftTableOutputCols,
    int32_t leftTableOutputColsCount, int32_t originalLeftTableColsCount, DynamicPagesIndex *leftTablePagesIndex,
    const std::vector<DataTypePtr> &rightTableOutputTypes, int32_t *rightTableOutputCols,
    int32_t rightTableOutputColsCount, int32_t originalRightTableColsCount, DynamicPagesIndex *rightTablePagesIndex,
    std::string &filter, VectorAllocator *vecAllocator, JoinType joinType, OverflowConfig *overflowConfig)
    : leftTableOutputTypes(leftTableOutputTypes),
      leftTableOutputCols(leftTableOutputCols),
      leftTableOutputColsCount(leftTableOutputColsCount),
      originalLeftTableColsCount(originalLeftTableColsCount),
      leftTablePagesIndex(leftTablePagesIndex),
      rightTableOutputTypes(rightTableOutputTypes),
      rightTableOutputCols(rightTableOutputCols),
      rightTableOutputColsCount(rightTableOutputColsCount),
      originalRightTableColsCount(originalRightTableColsCount),
      rightTablePagesIndex(rightTablePagesIndex),
      filterExpStr(filter),
      vecAllocator(vecAllocator),
      joinType(joinType)
{
    int32_t leftRowSize = OperatorUtil::GetRowSize(this->leftTableOutputTypes);
    int32_t rightRowSize = OperatorUtil::GetRowSize(this->rightTableOutputTypes);
    int32_t outputRowSize = leftRowSize + rightRowSize;
    this->maxRowCount = OperatorUtil::GetMaxRowCount(outputRowSize != 0 ? outputRowSize : DEFAULT_ROW_SIZE);
    this->JoinFilterCodeGen(overflowConfig);
}

void JoinResultBuilder::JoinFilterCodeGen(OverflowConfig *overflowConfig)
{
    if (filterExpStr.empty()) {
        return;
    }

    executionContext = new ExecutionContext();
    executionContext->GetArena()->SetAllocator(vecAllocator);
    omniruntime::expressions::Expr *filterExpr = JSONParser::ParseJSON(filterExpStr);
    simpleFilter = new SimpleFilter(*filterExpr);
    auto result = simpleFilter->Initialize(overflowConfig);
    if (!result) {
        delete simpleFilter;
        simpleFilter = nullptr;
        delete filterExpr;
        throw omniruntime::exception::OmniException("EXPRESSION_NOT_SUPPORT", "The expression is not supported yet.");
    }
    delete filterExpr;

    streamedColsInFilter = new int32_t[originalLeftTableColsCount];
    bufferedColsInFilter = new int32_t[originalRightTableColsCount];
    auto colsInFilter = simpleFilter->GetVectorIndexes();
    for (auto col : colsInFilter) {
        if (col < originalLeftTableColsCount) {
            streamedColsInFilter[streamedColsCountInFilter++] = col;
        } else {
            bufferedColsInFilter[bufferedColsCountInFilter++] = col - originalLeftTableColsCount;
        }
    }
    auto originalAllColsCount = originalLeftTableColsCount + originalRightTableColsCount;
    values = new int64_t[originalAllColsCount];
    nulls = new bool[originalAllColsCount];
    lengths = new int32_t[originalAllColsCount];
    memset_s(values, sizeof(int64_t) * originalAllColsCount, 0, sizeof(int64_t) * originalAllColsCount);
    memset_s(nulls, sizeof(bool) * originalAllColsCount, 0, sizeof(bool) * originalAllColsCount);
    memset_s(lengths, sizeof(int32_t) * originalAllColsCount, 0, sizeof(int32_t) * originalAllColsCount);
}

VectorBatch *JoinResultBuilder::NewEmptyVectorBatch() const
{
    int32_t outputColCount = leftTableOutputColsCount + rightTableOutputColsCount;
    VectorBatch *vectorBatch = new VectorBatch(outputColCount, maxRowCount);
    std::vector<DataTypePtr> allTypes;
    allTypes.insert(allTypes.cend(), leftTableOutputTypes.cbegin(), leftTableOutputTypes.cend());
    allTypes.insert(allTypes.cend(), rightTableOutputTypes.cbegin(), rightTableOutputTypes.cend());
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
            auto vector = buildVectorBatch->GetVector(columnIdx);
            auto typeId = vector->GetTypeId();
            if (typeId == OMNI_VARCHAR || typeId == OMNI_CHAR) {
                static_cast<VarcharVector *>(vector)->SetValueNull(buildRowCount);
            } else {
                vector->SetValueNull(buildRowCount);
            }
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
            auto vector = buildVectorBatch->GetVector(buildColumnIdx);
            auto typeId = vector->GetTypeId();
            if (typeId == OMNI_VARCHAR || typeId == OMNI_CHAR) {
                static_cast<VarcharVector *>(vector)->SetValueNull(buildRowCount);
            } else {
                vector->SetValueNull(buildRowCount);
            }
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
            delete leftAntiJoinHandler;
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

int32_t JoinResultBuilder::GetOutput(omniruntime::vec::VectorBatch **outputVecBatch)
{
    if (buildVectorBatchRowCount > 0) {
        if (buildVectorBatchRowCount == maxRowCount) {
            *outputVecBatch = buildVectorBatch;
        } else {
            *outputVecBatch = GetVectorBatchFromSlice(buildVectorBatch, buildVectorBatchRowCount);
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

    leftTablePagesIndex->CacheBatch(leftBatchId);
    for (int32_t i = 0; i < streamedColsCountInFilter; i++) {
        auto col = streamedColsInFilter[i];
        auto leftVector = leftTablePagesIndex->GetColumnsFromCache(col);
        nulls[col] = leftVector->IsValueNull(leftRowId);
        values[col] = VectorHelper::GetValuePtrAndLength(leftVector, leftRowId, lengths + col);
    }
    rightTablePagesIndex->CacheBatch(rightBatchId);
    for (int32_t i = 0; i < bufferedColsCountInFilter; i++) {
        auto col = bufferedColsInFilter[i];
        auto rightVector = rightTablePagesIndex->GetColumnsFromCache(col);
        auto colIdx = col + originalLeftTableColsCount;
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
                preLeftTableRowMatchedOut = true;
            }
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
    delete simpleFilter;
    delete[] streamedColsInFilter;
    delete[] bufferedColsInFilter;
    delete[] values;
    delete[] nulls;
    delete[] lengths;

    delete executionContext;
    executionContext = nullptr;
}
}
}
