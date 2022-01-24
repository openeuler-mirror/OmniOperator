/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: sort merge join scanner implementations
 */

#include "sort_merge_join_scanner.h"
#include "../../pages_index.h"

#include <memory>

using namespace omniruntime::vec;
namespace omniruntime {
namespace op {
SortMergeJoinScanner::SortMergeJoinScanner(const VecTypes &streamedTableKeysTypes, int32_t *streamedTableKeysCols,
    int32_t keyColsCount, DynamicPagesIndex *streamedTablePagesIndex, const VecTypes &bufferedTableKeysTypes,
    int32_t *bufferedTableKeysCols, DynamicPagesIndex *bufferedTablePagesIndex, JoinType joinType, bool firstMatch)
    : joinType(joinType),
      streamedTableKeysCols(streamedTableKeysCols),
      bufferedTableKeysCols(bufferedTableKeysCols),
      keyColsCount(keyColsCount),
      firstMatch(firstMatch),
      streamedPagesIndexPosition(-1),
      bufferedPagesIndexPosition(0)
{
    streamedPagesIndex = streamedTablePagesIndex;
    bufferedPagesIndex = bufferedTablePagesIndex;
    this->streamedTableKeysTypes = std::make_unique<VecTypes>(streamedTableKeysTypes);
    preStreamedValueAddress = -1;
    preStatus = std::make_unique<InitialJoinStatus>();
}

SortMergeJoinScanner::~SortMergeJoinScanner() {}

int64_t SortMergeJoinScanner::FindNextJoinRows()
{
    switch (joinType) {
        case OMNI_JOIN_TYPE_INNER:
            InnerJoin();
            return preStatus->GenerateStatus();
        default:
            LogError("Unsupported join type: %u.", joinType);
            preStatus->Set(SCAN_FINISHED, SCAN_FINISHED, false);
            return preStatus->GenerateStatus();
    }
}

int32_t SortMergeJoinScanner::GetMatchedValueAddresses(std::vector<bool> &isMatched,
    std::vector<int64_t> &streamedTblValueAddresses, std::vector<int64_t> &bufferedTblValueAddresses)
{
    isMatched.insert(isMatched.end(), isPreKeyMatched.begin(), isPreKeyMatched.end());
    streamedTblValueAddresses.insert(streamedTblValueAddresses.end(), streamedValueAddress.begin(),
        streamedValueAddress.end());
    bufferedTblValueAddresses.insert(bufferedTblValueAddresses.end(), bufferedValueAddress.begin(),
        bufferedValueAddress.end());
    isPreKeyMatched.clear();
    streamedValueAddress.clear();
    bufferedValueAddress.clear();
    return 0;
}

void SortMergeJoinScanner::InnerJoin()
{
    if (preStatus->NewStreamedDataAdded()) {
        if (!IsValidAddedStreamedData()) {
            return;
        }
        if (!AdvancedStreamedWithNullFreeJoinKey()) {
            preStatus->TransToNeedStreamedData(HasResult());
            return;
        }
        if (PreKeyMatched()) {
            SavePrevMatchingRows(true);
            return RunInnerJoin();
        }
    } else if (preStatus->NewBufferedDataAdded()) {
        if (!IsValidAddedBufferedData()) {
            return;
        }
        if (!AdvancedBufferedToRowWithNullFreeJoinKey()) {
            preStatus->TransToNeedBufferedData(HasResult());
            return;
        }
    } else {
        if (!AdvancedStreamedWithNullFreeJoinKey()) {
            preStatus->TransToNeedStreamedData(HasResult());
            return;
        }
        if (PreKeyMatched()) {
            SavePrevMatchingRows(true);
            return RunInnerJoin();
        }
    }
    if (!FindMatchingRows()) {
        return;
    }
    return RunInnerJoin();
}

void SortMergeJoinScanner::RunInnerJoin()
{
    if (!AdvancedStreamedWithNullFreeJoinKey()) {
        preStatus->TransToNeedStreamedData(HasResult());
        return;
    }
    if (PreKeyMatched()) {
        SavePrevMatchingRows(true);
        return RunInnerJoin();
    }
    if (!FindMatchingRows()) {
        return;
    }
    return RunInnerJoin();
}

bool SortMergeJoinScanner::FindMatchingRows()
{
    auto comp = FindNextMatchPos();
    if (NeedBufferedData()) {
        preStatus->TransToNeedBufferedData(HasResult());
        bufferedPagesIndexPosition--;
        return false;
    }
    if (comp > 0) {
        preStatus->TransToNeedBufferedData(HasResult());
        return false;
    }
    if (comp < 0) {
        preStatus->TransToNeedStreamedData(HasResult());
        return false;
    }

    BufferMatchingRows();

    return true;
}

bool SortMergeJoinScanner::IsValidAddedStreamedData()
{
    if (streamedPagesIndex->IsDataFinish(streamedPagesIndexPosition) &&
        bufferedPagesIndex->IsDataFinish(bufferedPagesIndexPosition)) {
        preStatus->Set(SCAN_FINISHED, SCAN_FINISHED, false);
        return false;
    }
    if (streamedPagesIndex->IsDataFinish(streamedPagesIndexPosition)) {
        preStatus->Set(SCAN_FINISHED, NEED_DATA, false);
        return false;
    }
    return true;
}

bool SortMergeJoinScanner::IsValidAddedBufferedData()
{
    if (streamedPagesIndex->IsDataFinish(streamedPagesIndexPosition) &&
        bufferedPagesIndex->IsDataFinish(bufferedPagesIndexPosition)) {
        preStatus->Set(SCAN_FINISHED, SCAN_FINISHED, false);
        return false;
    }
    if (bufferedPagesIndex->IsDataFinish(bufferedPagesIndexPosition)) {
        preStatus->Set(NEED_DATA, SCAN_FINISHED, false);
        return false;
    }
    return true;
}

bool SortMergeJoinScanner::AdvancedStreamedWithNullFreeJoinKey()
{
    bool found = false;
    while (!found && HasNext(streamedPagesIndexPosition, streamedPagesIndex)) {
        streamedPagesIndexPosition++;
        found = !CurStreamedHasNull();
    }

    return found;
}

bool SortMergeJoinScanner::AdvancedBufferedToRowWithNullFreeJoinKey()
{
    bool found = false;
    while (!found && HasNext(bufferedPagesIndexPosition, bufferedPagesIndex)) {
        bufferedPagesIndexPosition++;
        found = !CurBufferedHasNull();
    }

    return found;
}

void SortMergeJoinScanner::BufferMatchingRows()
{
    auto streamedValueAddr = streamedPagesIndex->GetValueAddresses(streamedPagesIndexPosition);
    preStreamedValueAddress = streamedValueAddr;
    preBufferedValueAddress.clear();
    int64_t bufferedValueAddr;
    do {
        bufferedValueAddr = bufferedPagesIndex->GetValueAddresses(bufferedPagesIndexPosition);
        preBufferedValueAddress.push_back(bufferedValueAddr);
    } while (AdvancedBufferedToRowWithNullFreeJoinKey() && (CompareCurRowKeys() == 0));
    SavePrevMatchingRows(false);
}

int32_t SortMergeJoinScanner::FindNextMatchPos()
{
    auto compare = CompareCurRowKeys();
    auto cur = streamedPagesIndexPosition;
    while ((compare > 0 && AdvancedBufferedToRowWithNullFreeJoinKey()) ||
        (compare < 0 && AdvancedStreamedWithNullFreeJoinKey())) {
        compare = CompareCurRowKeys();
        // todo no need?
        // cur-stream-key duplicate with pre-key, also save although buffer not match cur-stream-key
        if (compare != 0 && (streamedPagesIndexPosition > cur) && PreKeyMatched()) {
            SavePrevMatchingRows(true);
        }
    }
    return compare;
}

bool SortMergeJoinScanner::PreKeyMatched()
{
    if (preStreamedValueAddress == -1 || preBufferedValueAddress.empty()) {
        return false;
    }
    auto curValueAddr = streamedPagesIndex->GetValueAddresses(streamedPagesIndexPosition);
    bool isMatched = CompareRowKeys(preStreamedValueAddress, streamedPagesIndex, streamedTableKeysCols, curValueAddr,
                                    streamedPagesIndex, streamedTableKeysCols) == 0;
    return isMatched;
}

void SortMergeJoinScanner::SavePrevMatchingRows(bool isMatched)
{
    bufferedValueAddress.insert(bufferedValueAddress.end(), preBufferedValueAddress.begin(),
        preBufferedValueAddress.end());
    auto valueAddr = streamedPagesIndex->GetValueAddresses(streamedPagesIndexPosition);
    streamedValueAddress.insert(streamedValueAddress.end(), preBufferedValueAddress.size(), valueAddr);
    isPreKeyMatched.insert(isPreKeyMatched.end(), preBufferedValueAddress.size(), isMatched);
}

bool SortMergeJoinScanner::HasResult()
{
    return !streamedValueAddress.empty() && !bufferedValueAddress.empty();
}

int32_t SortMergeJoinScanner::CompareCurRowKeys()
{
    auto streamedValueAddr = streamedPagesIndex->GetValueAddresses(streamedPagesIndexPosition);
    auto bufferedValueAddr = bufferedPagesIndex->GetValueAddresses(bufferedPagesIndexPosition);
    return CompareRowKeys(streamedValueAddr, bufferedValueAddr);
}

int32_t SortMergeJoinScanner::CompareRowKeys(int64_t leftRowIndex, DynamicPagesIndex *leftPagesIndex,
    const int32_t *leftKeyCols, int64_t rightRowIndex, DynamicPagesIndex *rightPagesIndex, const int32_t *rightKeyCols)
{
    auto streamedBatchId = DecodeSliceIndex(leftRowIndex);
    auto bufferedBatchId = DecodeSliceIndex(rightRowIndex);
    for (int i = 0; i < keyColsCount; ++i) {
        auto streamedColumn = leftPagesIndex->GetColumns(streamedBatchId, leftKeyCols[i]);
        auto bufferedColumn = rightPagesIndex->GetColumns(bufferedBatchId, rightKeyCols[i]);
        auto com = OperatorUtil::CompareVectorAtPosition(streamedTableKeysTypes->GetIds()[leftKeyCols[i]],
            streamedColumn, DecodePosition(leftRowIndex), bufferedColumn, DecodePosition(rightRowIndex));
        if (com != 0) {
            return com;
        }
    }

    return 0;
}

int32_t SortMergeJoinScanner::CompareRowKeys(int64_t leftRowIndex, int64_t rightRowIndex)
{
    return CompareRowKeys(leftRowIndex, streamedPagesIndex, streamedTableKeysCols, rightRowIndex, bufferedPagesIndex,
        bufferedTableKeysCols);
}

bool SortMergeJoinScanner::CurStreamedHasNull()
{
    auto valueAddress = streamedPagesIndex->GetValueAddresses(streamedPagesIndexPosition);
    auto row = DecodePosition(valueAddress);
    auto curBatchId = DecodeSliceIndex(valueAddress);
    for (int i = 0; i < keyColsCount; ++i) {
        if (streamedPagesIndex->GetColumns(curBatchId, streamedTableKeysCols[i])->IsValueNull(row)) {
            return true;
        }
    }
    return false;
}

bool SortMergeJoinScanner::CurBufferedHasNull()
{
    auto valueAddress = bufferedPagesIndex->GetValueAddresses(bufferedPagesIndexPosition);
    auto row = DecodePosition(valueAddress);
    auto curBatchId = DecodeSliceIndex(valueAddress);
    for (int i = 0; i < keyColsCount; ++i) {
        if (bufferedPagesIndex->GetColumns(curBatchId, bufferedTableKeysCols[i])->IsValueNull(row)) {
            return true;
        }
    }
    return false;
}

bool SortMergeJoinScanner::NeedBufferedData()
{
    if (bufferedPagesIndex->IsDataFinish()) {
        return false;
    }
    auto streamedValueAddr = streamedPagesIndex->GetValueAddresses(streamedPagesIndexPosition);
    auto bufferedValueAddr = bufferedPagesIndex->GetValueAddresses(bufferedPagesIndex->GetPositionCount() - 1);
    if (bufferedValueAddr > -1 && (CompareRowKeys(streamedValueAddr, bufferedValueAddr) == 0)) {
        return true;
    }
    return false;
}

JoinStatus::JoinStatus(JoinTableCode streamedCode, JoinTableCode bufferedCode, JoinResultCode resultCode)
    : streamedCode(streamedCode), bufferedCode(bufferedCode), resultCode(resultCode)
{}

int32_t JoinStatus::GenerateStatus()
{
    return streamedCode << STREAM_SHIFT_24 | bufferedCode << BUFFER_SHIFT_16 | resultCode;
}

JoinStatus::JoinStatus(JoinTableCode streamedCode, JoinTableCode bufferedCode, bool hasResult)
    : streamedCode(streamedCode), bufferedCode(bufferedCode)
{
    this->resultCode = hasResult ? HAS_RESULT : NO_RESULT;
}

void JoinStatus::Set(JoinTableCode streamed, JoinTableCode buffered, bool hasResult)
{
    this->streamedCode = streamed;
    this->bufferedCode = buffered;
    this->resultCode = hasResult ? HAS_RESULT : NO_RESULT;
}

bool JoinStatus::NewStreamedDataAdded()
{
    return streamedCode == NEED_DATA;
}

bool JoinStatus::NewBufferedDataAdded()
{
    return bufferedCode == NEED_DATA;
}

void JoinStatus::Reset()
{
    streamedCode = INVALID;
    bufferedCode = INVALID;
}

void JoinStatus::TransToNeedBufferedData(bool hasResult)
{
    if (bufferedCode == SCAN_FINISHED) {
        Set(SCAN_FINISHED, SCAN_FINISHED, hasResult);
    } else {
        auto streamStatus = (streamedCode == SCAN_FINISHED) ? SCAN_FINISHED : NEED_SCAN;
        Set(streamStatus, NEED_DATA, hasResult);
    }
}

void JoinStatus::TransToNeedStreamedData(bool hasResult)
{
    if (streamedCode == SCAN_FINISHED) {
        Set(SCAN_FINISHED, SCAN_FINISHED, hasResult);
    } else {
        auto bufferedStatus = (bufferedCode == SCAN_FINISHED) ? SCAN_FINISHED : NEED_SCAN;
        Set(NEED_DATA, bufferedStatus, hasResult);
    }
}
}
}