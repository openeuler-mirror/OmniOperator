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
            AddValueToBuildVector(leftTablePagesIndex->GetColumn(leftBatchId, leftTableOutputCols[columnIdx]),
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
            AddValueToBuildVector(rightTablePagesIndex->GetColumn(rightBatchId, rightTableOutputCols[columnIdx]),
                rightRowId, buildVectorBatch->GetVector(buildColumnIdx), buildRowCount);
        }
    }
}

void JoinResultBuilder::PaddingLeftTableNull(int32_t rightBatchId, int32_t rightRowId,
    vec::VectorBatch *buildVectorBatch, int32_t &buildRowCount)
{
    ParsingAndOrganizationResultsForLeftTable(JOIN_NULL_FLAG, JOIN_NULL_FLAG, buildVectorBatch, buildRowCount);
    ParsingAndOrganizationResultsForRightTable(rightBatchId, rightRowId, buildVectorBatch, buildRowCount);
    buildRowCount++;
}

void JoinResultBuilder::PaddingRightTableNull(int32_t leftBatchId, int32_t leftRowId,
    vec::VectorBatch *buildVectorBatch, int32_t &buildRowCount)
{
    ParsingAndOrganizationResultsForLeftTable(leftBatchId, leftRowId, buildVectorBatch, buildRowCount);
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

int32_t JoinResultBuilder::ConstructInnerJoinOutput()
{
    auto inputSize = static_cast<int32_t>(streamedTableValueAddresses.size());
    for (int32_t addressPosition = addressOffset; addressPosition < inputSize; addressPosition++) {
        int64_t streamedRowAddress = streamedTableValueAddresses[addressPosition];
        int64_t bufferedRowAddress = bufferedTableValueAddresses[addressPosition];

        if (preStreamedRowAddress == INT64_MAX || preStreamedRowAddress != streamedRowAddress) {
            preLeftTableRowMatchedOut = false;
        }

        int32_t leftBatchId = DecodeSliceIndex(streamedRowAddress);
        int32_t leftRowId = DecodePosition(streamedRowAddress);
        int32_t rightBatchId = DecodeSliceIndex(bufferedRowAddress);
        int32_t rightRowId = DecodePosition(bufferedRowAddress);
        FreeVectorBatches(isPreKeyMatched[addressPosition], leftBatchId, rightBatchId);

        if (IsJoinPositionEligible(leftBatchId, leftRowId, rightBatchId, rightRowId)) {
            for (int32_t columnIdx = 0; columnIdx < leftTableOutputColsCount; columnIdx++) {
                AddValueToBuildVector(leftTablePagesIndex->GetColumn(leftBatchId, leftTableOutputCols[columnIdx]),
                    leftRowId, buildVectorBatch->GetVector(columnIdx), buildRowCount);
            }
            for (int32_t columnIdx = 0; columnIdx < rightTableOutputColsCount; columnIdx++) {
                int32_t buildColumnIdx = leftTableOutputColsCount + columnIdx;
                AddValueToBuildVector(rightTablePagesIndex->GetColumn(rightBatchId, rightTableOutputCols[columnIdx]),
                    rightRowId, buildVectorBatch->GetVector(buildColumnIdx), buildRowCount);
            }
            buildRowCount++;
        }
        preLeftTableRowMatchedOut = true;

        preStreamedRowAddress = streamedRowAddress;
        addressOffset++;
        if (buildRowCount >= maxRowCount) {
            buildVectorBatchRowCount = buildRowCount;
            return 1;
        }
    }

    buildVectorBatchRowCount = buildRowCount;
    preStreamedRowAddress = INT64_MAX;
    if (buildRowCount >= maxRowCount) {
        return 1;
    }
    return 0;
}

int32_t JoinResultBuilder::ConstructLeftJoinOutput()
{
    auto inputSize = static_cast<int32_t>(streamedTableValueAddresses.size());
    for (int32_t addressPosition = addressOffset; addressPosition < inputSize; addressPosition++) {
        int64_t streamedRowAddress = streamedTableValueAddresses[addressPosition];
        int64_t bufferedRowAddress = bufferedTableValueAddresses[addressPosition];
        if (preStreamedRowAddress != INT64_MAX && preStreamedRowAddress != streamedRowAddress &&
            !preLeftTableRowMatchedOut) {
            // set values for left but set nulls for right for left join, full join, left anti
            PaddingRightTableNull(preStreamedBatchId, preStreamedRowId, buildVectorBatch, buildRowCount);
        }
        if (preStreamedRowAddress == INT64_MAX || preStreamedRowAddress != streamedRowAddress) {
            preLeftTableRowMatchedOut = false;
        }

        int32_t leftBatchId = DecodeSliceIndex(streamedRowAddress);
        int32_t leftRowId = DecodePosition(streamedRowAddress);
        int32_t rightBatchId = DecodeSliceIndex(bufferedRowAddress);
        int32_t rightRowId = DecodePosition(bufferedRowAddress);
        if (!(leftBatchId == JOIN_NULL_FLAG || rightBatchId == JOIN_NULL_FLAG)) {
            FreeVectorBatches(isPreKeyMatched[addressPosition], leftBatchId, rightBatchId);
        }

        // JOIN_NULL_FLAG row direct output && filter match direct output, pad no NULL
        if (IsNullFlagBatchAndRow(leftBatchId, leftRowId) || IsNullFlagBatchAndRow(rightBatchId, rightRowId) ||
            IsJoinPositionEligible(leftBatchId, leftRowId, rightBatchId, rightRowId)) {
            ParsingAndOrganizationResultsForLeftTable(leftBatchId, leftRowId, buildVectorBatch, buildRowCount);
            ParsingAndOrganizationResultsForRightTable(rightBatchId, rightRowId, buildVectorBatch, buildRowCount);
            buildRowCount++;
            preLeftTableRowMatchedOut = true;
        }

        preStreamedRowAddress = streamedRowAddress;
        preStreamedBatchId = leftBatchId;
        preStreamedRowId = leftRowId;
        addressOffset++;
        if (buildRowCount >= maxRowCount) {
            buildVectorBatchRowCount = buildRowCount;
            return 1;
        }
    }

    if (preStreamedRowAddress != INT64_MAX && !preLeftTableRowMatchedOut) {
        // pad NULL for last mismatch row
        PaddingRightTableNull(preStreamedBatchId, preStreamedRowId, buildVectorBatch, buildRowCount);
    }
    buildVectorBatchRowCount = buildRowCount;
    preStreamedRowAddress = INT64_MAX;
    if (buildRowCount >= maxRowCount) {
        return 1;
    }
    return 0;
}

int32_t JoinResultBuilder::ConstructFullJoinOutput()
{
    auto inputSize = static_cast<int32_t>(streamedTableValueAddresses.size());
    for (int32_t addressPosition = addressOffset; addressPosition < inputSize; addressPosition++) {
        int64_t streamedRowAddress = streamedTableValueAddresses[addressPosition];
        int64_t bufferedRowAddress = bufferedTableValueAddresses[addressPosition];
        if (preStreamedRowAddress != INT64_MAX && preStreamedRowAddress != streamedRowAddress &&
            !preLeftTableRowMatchedOut) {
            // set values for left but set nulls for right for left join, full join, left anti
            PaddingRightTableNull(preStreamedBatchId, preStreamedRowId, buildVectorBatch, buildRowCount);
        }
        if (preStreamedRowAddress == INT64_MAX || preStreamedRowAddress != streamedRowAddress) {
            preLeftTableRowMatchedOut = false;
        }

        int32_t leftBatchId = DecodeSliceIndex(streamedRowAddress);
        int32_t leftRowId = DecodePosition(streamedRowAddress);
        int32_t rightBatchId = DecodeSliceIndex(bufferedRowAddress);
        int32_t rightRowId = DecodePosition(bufferedRowAddress);
        if (!(leftBatchId == JOIN_NULL_FLAG || rightBatchId == JOIN_NULL_FLAG)) {
            FreeVectorBatches(isPreKeyMatched[addressPosition], leftBatchId, rightBatchId);
        }

        // JOIN_NULL_FLAG row direct output && filter match direct output, pad no NULL
        if (IsNullFlagBatchAndRow(leftBatchId, leftRowId) || IsNullFlagBatchAndRow(rightBatchId, rightRowId) ||
            IsJoinPositionEligible(leftBatchId, leftRowId, rightBatchId, rightRowId)) {
            ParsingAndOrganizationResultsForLeftTable(leftBatchId, leftRowId, buildVectorBatch, buildRowCount);
            ParsingAndOrganizationResultsForRightTable(rightBatchId, rightRowId, buildVectorBatch, buildRowCount);
            buildRowCount++;
            preLeftTableRowMatchedOut = true;
        } else { // filter mismatch, pad left table NULL
            PaddingLeftTableNull(rightBatchId, rightRowId, buildVectorBatch, buildRowCount);
        }

        preStreamedRowAddress = streamedRowAddress;
        preStreamedBatchId = leftBatchId;
        preStreamedRowId = leftRowId;
        addressOffset++;
        if (buildRowCount >= maxRowCount) {
            buildVectorBatchRowCount = buildRowCount;
            return 1;
        }
    }

    if (preStreamedRowAddress != INT64_MAX && !preLeftTableRowMatchedOut) {
        // pad NULL for last mismatch row
        PaddingRightTableNull(preStreamedBatchId, preStreamedRowId, buildVectorBatch, buildRowCount);
    }
    buildVectorBatchRowCount = buildRowCount;
    preStreamedRowAddress = INT64_MAX;
    if (buildRowCount >= maxRowCount) {
        return 1;
    }
    return 0;
}

int32_t JoinResultBuilder::ConstructLeftSemiJoinOutput()
{
    auto inputSize = static_cast<int32_t>(streamedTableValueAddresses.size());
    for (int32_t addressPosition = addressOffset; addressPosition < inputSize; addressPosition++) {
        int64_t streamedRowAddress = streamedTableValueAddresses[addressPosition];
        int64_t bufferedRowAddress = bufferedTableValueAddresses[addressPosition];
        if (preStreamedRowAddress != INT64_MAX && preStreamedRowAddress != streamedRowAddress &&
            !preLeftTableRowMatchedOut) {
            // set values for left but set nulls for right for left join, full join, left anti
            PaddingRightTableNull(preStreamedBatchId, preStreamedRowId, buildVectorBatch, buildRowCount);
        }
        if (preStreamedRowAddress == INT64_MAX || preStreamedRowAddress != streamedRowAddress) {
            preLeftTableRowMatchedOut = false;
        }

        int32_t leftBatchId = DecodeSliceIndex(streamedRowAddress);
        int32_t leftRowId = DecodePosition(streamedRowAddress);
        int32_t rightBatchId = DecodeSliceIndex(bufferedRowAddress);
        int32_t rightRowId = DecodePosition(bufferedRowAddress);
        if (!(leftBatchId == JOIN_NULL_FLAG || rightBatchId == JOIN_NULL_FLAG)) {
            FreeVectorBatches(isPreKeyMatched[addressPosition], leftBatchId, rightBatchId);
        }

        // left semi join only needs to output the data of the left table
        if (isSameBufferedKeyMatched[addressPosition]) {
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

        preStreamedRowAddress = streamedRowAddress;
        preStreamedBatchId = leftBatchId;
        preStreamedRowId = leftRowId;
        addressOffset++;
        if (buildRowCount >= maxRowCount) {
            buildVectorBatchRowCount = buildRowCount;
            return 1;
        }
    }

    if (preStreamedRowAddress != INT64_MAX && !preLeftTableRowMatchedOut) {
        // pad NULL for last mismatch row
        PaddingRightTableNull(preStreamedBatchId, preStreamedRowId, buildVectorBatch, buildRowCount);
    }
    buildVectorBatchRowCount = buildRowCount;
    preStreamedRowAddress = INT64_MAX;
    if (buildRowCount >= maxRowCount) {
        return 1;
    }
    return 0;
}

int32_t JoinResultBuilder::ConstructLeftAntiJoinOutput()
{
    auto inputSize = static_cast<int32_t>(streamedTableValueAddresses.size());
    leftAntiJoinHandler.hasSameBufferedRow = false;
    leftAntiJoinHandler.printThisStreamRowOutFlag = true;
    for (int32_t addressPosition = addressOffset; addressPosition < inputSize; addressPosition++) {
        UpdateLeftAntiJoinHandler(&leftAntiJoinHandler, addressPosition, isSameBufferedKeyMatched,
            inputSize); // only for left anti
        int64_t streamedRowAddress = streamedTableValueAddresses[addressPosition];
        int64_t bufferedRowAddress = bufferedTableValueAddresses[addressPosition];
        if (preStreamedRowAddress != INT64_MAX && preStreamedRowAddress != streamedRowAddress &&
            !preLeftTableRowMatchedOut) {
            // set values for left but set nulls for right for left join, full join, left anti
            PaddingRightTableNull(preStreamedBatchId, preStreamedRowId, buildVectorBatch, buildRowCount);
        }
        if (preStreamedRowAddress == INT64_MAX || preStreamedRowAddress != streamedRowAddress) {
            preLeftTableRowMatchedOut = false;
        }

        int32_t leftBatchId = DecodeSliceIndex(streamedRowAddress);
        int32_t leftRowId = DecodePosition(streamedRowAddress);
        int32_t rightBatchId = DecodeSliceIndex(bufferedRowAddress);
        int32_t rightRowId = DecodePosition(bufferedRowAddress);
        if (!(leftBatchId == JOIN_NULL_FLAG || rightBatchId == JOIN_NULL_FLAG)) {
            FreeVectorBatches(isPreKeyMatched[addressPosition], leftBatchId, rightBatchId);
        }

        if (leftAntiJoinHandler.hasSameBufferedRow) {
            if (leftAntiJoinHandler.printThisStreamRowOutFlag &&
                IsJoinPositionEligible(leftBatchId, leftRowId, rightBatchId, rightRowId)) {
                leftAntiJoinHandler.printThisStreamRowOutFlag = false;
            }
        } else {
            if (IsNullFlagBatchAndRow(rightBatchId, rightRowId)) {
                ParsingAndOrganizationResultsForLeftTable(leftBatchId, leftRowId, buildVectorBatch, buildRowCount);
                buildRowCount++;
            } else if (!IsJoinPositionEligible(leftBatchId, leftRowId, rightBatchId, rightRowId) &&
                leftAntiJoinHandler.printThisStreamRowOutFlag) {
                ParsingAndOrganizationResultsForLeftTable(leftBatchId, leftRowId, buildVectorBatch, buildRowCount);
                buildRowCount++;
            } else {
                leftAntiJoinHandler.printThisStreamRowOutFlag = false;
            }
        }
        preLeftTableRowMatchedOut = true;

        preStreamedRowAddress = streamedRowAddress;
        preStreamedBatchId = leftBatchId;
        preStreamedRowId = leftRowId;
        addressOffset++;
        if (buildRowCount >= maxRowCount) {
            buildVectorBatchRowCount = buildRowCount;
            return 1;
        }
    }

    if (preStreamedRowAddress != INT64_MAX && !preLeftTableRowMatchedOut) {
        // pad NULL for last mismatch row
        PaddingRightTableNull(preStreamedBatchId, preStreamedRowId, buildVectorBatch, buildRowCount);
    }
    buildVectorBatchRowCount = buildRowCount;
    preStreamedRowAddress = INT64_MAX;
    if (buildRowCount >= maxRowCount) {
        return 1;
    }
    return 0;
}

int32_t JoinResultBuilder::AddJoinValueAddresses()
{
    // 1 represents the rowCount of buildVectorBatch has reached the maxRowCount
    // 0 represents the rowCount of buildVectorBatch doesn't reached the maxRowCount
    if (buildVectorBatch == nullptr) {
        buildVectorBatch = NewEmptyVectorBatch();
        buildRowCount = 0;
    }

    switch (joinType) {
        case JoinType::OMNI_JOIN_TYPE_INNER:
            return ConstructInnerJoinOutput();
        case JoinType::OMNI_JOIN_TYPE_LEFT:
            return ConstructLeftJoinOutput();
        case JoinType::OMNI_JOIN_TYPE_FULL:
            return ConstructFullJoinOutput();
        case JoinType::OMNI_JOIN_TYPE_LEFT_SEMI:
            return ConstructLeftSemiJoinOutput();
        case JoinType::OMNI_JOIN_TYPE_LEFT_ANTI:
            return ConstructLeftAntiJoinOutput();
        default:
            throw OmniException("OPERATOR_RUNTIME_ERROR", "SMJ unsupported join type");
    }
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
        auto leftVector = leftTablePagesIndex->GetColumnFromCache(col);
        nulls[col] = leftVector->IsValueNull(leftRowId);
        values[col] = VectorHelper::GetValuePtrAndLength(leftVector, leftRowId, lengths + col);
    }
    rightTablePagesIndex->CacheBatch(rightBatchId);
    for (int32_t i = 0; i < bufferedColsCountInFilter; i++) {
        auto col = bufferedColsInFilter[i];
        auto rightVector = rightTablePagesIndex->GetColumnFromCache(col);
        auto colIdx = col + originalLeftTableColsCount;
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
