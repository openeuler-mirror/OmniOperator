/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2023. All rights reserved.
 * @Description: sort merge join scanner implementations
 */

#include "sort_merge_join_scanner.h"
#include <memory>
#include "operator/pages_index.h"

using namespace omniruntime::vec;
namespace omniruntime {
namespace op {
SortMergeJoinScanner::SortMergeJoinScanner(const DataTypes &streamedTableKeysTypes, int32_t *streamedTableKeysCols,
    int32_t keyColsCount, DynamicPagesIndex *streamedTablePagesIndex, const DataTypes &bufferedTableKeysTypes,
    int32_t *bufferedTableKeysCols, DynamicPagesIndex *bufferedTablePagesIndex, JoinType joinType,
    bool onlyBufferedFirstMatch)
    : streamedTableKeysTypes(streamedTableKeysTypes),
      joinType(joinType),
      streamedTableKeysCols(streamedTableKeysCols),
      bufferedTableKeysCols(bufferedTableKeysCols),
      keyColsCount(keyColsCount),
      onlyBufferedFirstMatch(onlyBufferedFirstMatch),
      streamedPagesIndexPosition(-1),
      bufferedPagesIndexPosition(0),
      streamedPagesIndex(streamedTablePagesIndex),
      bufferedPagesIndex(bufferedTablePagesIndex),
      preStreamedValueAddress(-1),
      preStatus(std::make_unique<InitialJoinStatus>())
{}

SortMergeJoinScanner::~SortMergeJoinScanner() {}

int64_t SortMergeJoinScanner::FindNextJoinRows()
{
    switch (joinType) {
        case JoinType::OMNI_JOIN_TYPE_INNER:
        case JoinType::OMNI_JOIN_TYPE_LEFT_SEMI:
            InnerJoin();
            break;
        case JoinType::OMNI_JOIN_TYPE_LEFT:
        case JoinType::OMNI_JOIN_TYPE_LEFT_ANTI:
            LeftOuterJoin();
            break;
        case JoinType::OMNI_JOIN_TYPE_FULL:
            FullOuterJoin();
            break;
        default:
            LogError("Unsupported join type: %u.", joinType);
            preStatus->Set(JoinTableCode::SCAN_FINISHED, JoinTableCode::SCAN_FINISHED, false);
            break;
    }
    return preStatus->GenerateStatus();
}

int32_t SortMergeJoinScanner::GetMatchedValueAddresses(std::vector<bool> &isMatched,
    std::vector<int64_t> &streamedTblValueAddresses, std::vector<int64_t> &bufferedTblValueAddresses,
    std::vector<bool> &isBufferedKeyMatched)
{
    isMatched.insert(isMatched.end(), isPreKeyMatched.begin(), isPreKeyMatched.end());
    streamedTblValueAddresses.insert(streamedTblValueAddresses.end(), streamedValueAddress.begin(),
        streamedValueAddress.end());
    bufferedTblValueAddresses.insert(bufferedTblValueAddresses.end(), bufferedValueAddress.begin(),
        bufferedValueAddress.end());
    isBufferedKeyMatched.insert(isBufferedKeyMatched.end(), isSameBufferedKeyMatched.begin(),
        isSameBufferedKeyMatched.end());
    isPreKeyMatched.clear();
    streamedValueAddress.clear();
    bufferedValueAddress.clear();
    isSameBufferedKeyMatched.clear();
    return 0;
}

void SortMergeJoinScanner::InnerJoin()
{
    if (preStatus->NewStreamedDataAdded()) { // streamedCode == NEED_DATA
        // new streamed add data
        if (!IsValidAddedStreamedData()) {
            // streamed data is not valid
            return;
        }
        if (!AdvancedStreamedWithNullFreeJoinKey()) {
            // streamed data hit the end, change join status to
            // streamed NEED_DATA
            preStatus->TransToNeedStreamedData(HasResult());
            return;
        }
        if (PreKeyMatched()) {
            // The new streamed row has the same join key as the previous row, so return the same matches.
            SavePrevMatchingRows(true);
            return RunInnerJoin();
        }
    } else if (preStatus->NewBufferedDataAdded()) { // bufferedCode == NEED_DATA
        // new buffered add data
        if (!IsValidAddedBufferedData()) {
            // buffered data is not valid
            return;
        }
        if (!AdvancedBufferedToRowWithNullFreeJoinKey()) {
            // buffered data hit the end, change join status to
            // buffered NEED_DATA
            preStatus->TransToNeedBufferedData(HasResult());
            return;
        }
    } else { // INVALID 、 NEED_SCAN
        if (!AdvancedStreamedWithNullFreeJoinKey()) {
            // streamed data hit the end, change join status to
            // streamed NEED_DATA
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

void SortMergeJoinScanner::LeftOuterJoin()
{
    if (streamedPagesIndex->IsDataFinish(streamedPagesIndexPosition)) {
        preStatus->Set(JoinTableCode::SCAN_FINISHED, JoinTableCode::SCAN_FINISHED, HasResult());
        return;
    }
    if (preStatus->NewStreamedDataAdded()) {
        // new streamed add data
        if (!AdvancedStreamedJoinKey()) {
            // streamed data hit the end, change join status to
            // streamed NEED_DATA
            preStatus->TransToNeedStreamedData(HasResult());
            return;
        }
        if (PreKeyMatchedWithNullValue()) {
            // The new streamed row has the same join key as the previous row, so return the same matches.
            SavePrevMatchingRows(true);
            return RunLeftOuterJoin();
        }
    } else if (preStatus->NewBufferedDataAdded()) {
        if (!bufferedPagesIndex->IsDataFinish(bufferedPagesIndexPosition)) {
            if (!AdvancedBufferedToRowWithNullFreeJoinKey()) {
                // buffered data hit the end, change join status to
                // buffered NEED_DATA
                preStatus->TransToNeedBufferedData(HasResult());
                return;
            }
            curBufferRowMatchFlag = false;
        }
    } else {
        if (!streamedPagesIndex->IsDataFinish(streamedPagesIndexPosition)) {
            if (!AdvancedStreamedJoinKey()) {
                // streamed data hit the end, change join status to
                // streamed NEED_DATA
                preStatus->TransToNeedStreamedData(HasResult());
                return;
            }
            if (PreKeyMatchedWithNullValue()) {
                SavePrevMatchingRows(true);
                return RunLeftOuterJoin();
            }
        }
    }
    if (!LeftOuterFindJoinRows()) {
        return;
    }
    return RunLeftOuterJoin();
}

void SortMergeJoinScanner::FullOuterJoin()
{
    if (streamedPagesIndex->IsDataFinish(streamedPagesIndexPosition) &&
        bufferedPagesIndex->IsDataFinish(bufferedPagesIndexPosition)) {
        preStatus->Set(JoinTableCode::SCAN_FINISHED, JoinTableCode::SCAN_FINISHED, HasResult());
        return;
    }
    if (preStatus->NewStreamedDataAdded()) {
        // new streamed add data
        if (!streamedPagesIndex->IsDataFinish(streamedPagesIndexPosition)) {
            if (!AdvancedStreamedJoinKey()) {
                // streamed data hit the end, change join status to streamed NEED_DATA
                preStatus->TransToNeedStreamedData(HasResult());
                return;
            } else if (PreKeyMatchedWithNullValue()) {
                // The new streamed row has the same join key as the previous row, so return the same matches.
                SavePrevMatchingRows(true);
                return RunFullOuterJoin();
            }
        }
    } else if (preStatus->NewBufferedDataAdded()) {
        // new buffered add data
        if (!bufferedPagesIndex->IsDataFinish(bufferedPagesIndexPosition)) {
            if (!AdvancedBufferedJoinKey()) {
                // buffered data hit the end, change join status to buffered NEED_DATA
                preStatus->TransToNeedBufferedData(HasResult());
                return;
            }
        }
    } else {
        if (!streamedPagesIndex->IsDataFinish(streamedPagesIndexPosition)) {
            if (!AdvancedStreamedJoinKey()) {
                // streamed data hit the end, change join status to streamed NEED_DATA
                preStatus->TransToNeedStreamedData(HasResult());
                return;
            }
            if (PreKeyMatchedWithNullValue()) {
                SavePrevMatchingRows(true);
                return RunFullOuterJoin();
            }
        }
    }
    if (!FullOuterFindJoinRows()) {
        return;
    }
    return RunFullOuterJoin();
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

void SortMergeJoinScanner::RunLeftOuterJoin()
{
    if (!AdvancedStreamedJoinKey()) {
        preStatus->TransToNeedStreamedData(HasResult());
        return;
    }
    if (PreKeyMatchedWithNullValue()) {
        SavePrevMatchingRows(true);
        return RunLeftOuterJoin();
    }
    if (!LeftOuterFindJoinRows()) {
        return;
    }
    return RunLeftOuterJoin();
}

void SortMergeJoinScanner::RunFullOuterJoin()
{
    if (!AdvancedStreamedJoinKey()) {
        preStatus->TransToNeedStreamedData(HasResult());
        return;
    }
    if (PreKeyMatchedWithNullValue()) {
        SavePrevMatchingRows(true);
        return RunFullOuterJoin();
    }

    if (!FullOuterFindJoinRows()) {
        return;
    }
    return RunFullOuterJoin();
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

bool SortMergeJoinScanner::LeftOuterFindJoinRows()
{
    // handle stream has Null && buffered no data situation
    if (HandleLeftOuterStreamedNullAndBufferedDataFinishedSituation()) {
        return true;
    }

    while (CurBufferedHasNull()) { // skip buffered null value
        if (!AdvancedBufferedJoinKey()) {
            preStatus->TransToNeedBufferedData(HasResult());
            return false;
        }
    }

    if (NeedBufferedData()) { // if this buffered batch last row == stream row, so need add next buffered batch
        preStatus->TransToNeedBufferedData(HasResult());
        if (!curBufferRowMatchFlag) {
            bufferedPagesIndexPosition--;
        }
        return false;
    }
    latestCompareStat = CompareCurRowKeys();
    while (latestCompareStat > 0) { // skip buffered compare >0 value condition
        if (AdvancedBufferedJoinKey()) {
            latestCompareStat = CompareCurRowKeys();
        } else {
            if (bufferedPagesIndex->IsDataFinish(bufferedPagesIndexPosition)) {
                BufferMissingRows();
                return true;
            }
            preStatus->TransToNeedBufferedData(HasResult());
            return false;
        }
    }

    if (latestCompareStat < 0 && !curStreamRowMatchFlag) {
        BufferMissingRows();
        curStreamRowMatchFlag = true;
        if (bufferedPagesIndexPosition > 0) {
            bufferedPagesIndexPosition--;
        }
    } else if (latestCompareStat == 0) {
        BufferMatchingRows();
        curStreamRowMatchFlag = true;
        if (preBufferedPagesIndexPosition == bufferedPagesIndexPosition) {
            curBufferRowMatchFlag = true;
            if (bufferedPagesIndex->IsDataFinish(bufferedPagesIndexPosition)) {
                return true;
            }
            preStatus->TransToNeedBufferedData(HasResult());
            return false;
        } else {
            curBufferRowMatchFlag = false;
        }
    }
    return true;
}

bool SortMergeJoinScanner::FullOuterFindJoinRows()
{
    // handle streamed is finished
    if (!HandleFullOuterStreamedIsFinishedSituation()) {
        return false;
    }

    // handle buffered is finished
    if (HandleFullOuterBufferedIsFinishedSituation()) {
        return true;
    }

    // handle streamed null value
    if (HandleFullOuterStreamedNullValue()) {
        return true;
    }

    // handle buffered null value
    if (!HandleFullOuterBufferedNullValue()) {
        return false;
    }

    // need buffered data?
    if (!HandleFullOuterNeedBufferedData()) {
        return false;
    }

    latestCompareStat = CompareCurRowKeys();
    if (latestCompareStat < 0) {
        if (curStreamRowMatchFlag) {
            return true; // go to the next RunFullOuterJoin circle
        }
        BufferMissingRows();
        curStreamRowMatchFlag = true;
        bufferedPagesIndexPosition--;
    } else if (latestCompareStat > 0 && !curBufferRowMatchFlag) {
        StreamMissingRowsForCompareValue();
        curBufferRowMatchFlag = true;
        streamedPagesIndexPosition--;
        if (preBufferedPagesIndexPosition != bufferedPagesIndexPosition) {
            bufferedPagesIndexPosition--; // not hit buffered end, bufferedPagesIndexPosition need -1
        }
    } else {
        BufferMatchingRowsForFullOuter();
        curStreamRowMatchFlag = true;
        curBufferRowMatchFlag = true;
        if (preBufferedPagesIndexPosition != bufferedPagesIndexPosition) {
            bufferedPagesIndexPosition--; // not hit buffered end, bufferedPagesIndexPosition need -1
        }
    }

    if (!AdvancedBufferedJoinKey()) {
        if (bufferedPagesIndex->IsDataFinish(bufferedPagesIndexPosition)) {
            return true;
        }
        preStatus->TransToNeedBufferedData(HasResult());
        return false;
    }
    return true;
}

bool SortMergeJoinScanner::IsValidAddedStreamedData()
{
    if (streamedPagesIndex->IsDataFinish(streamedPagesIndexPosition) &&
        bufferedPagesIndex->IsDataFinish(bufferedPagesIndexPosition)) {
        preStatus->Set(JoinTableCode::SCAN_FINISHED, JoinTableCode::SCAN_FINISHED, false);
        return false;
    }
    if (streamedPagesIndex->IsDataFinish(streamedPagesIndexPosition)) {
        preStatus->Set(JoinTableCode::SCAN_FINISHED, JoinTableCode::NEED_DATA, false);
        return false;
    }
    return true;
}

bool SortMergeJoinScanner::IsValidAddedBufferedData()
{
    if (streamedPagesIndex->IsDataFinish(streamedPagesIndexPosition) &&
        bufferedPagesIndex->IsDataFinish(bufferedPagesIndexPosition)) {
        preStatus->Set(JoinTableCode::SCAN_FINISHED, JoinTableCode::SCAN_FINISHED, false);
        return false;
    }
    if (bufferedPagesIndex->IsDataFinish(bufferedPagesIndexPosition)) {
        preStatus->Set(JoinTableCode::NEED_DATA, JoinTableCode::SCAN_FINISHED, false);
        return false;
    }
    return true;
}

/**
 * Advance the streamed iterator until we find a row with join key that does not contain nulls.
 * @return true if the streamed iterator returned a row and false otherwise.
 */
bool SortMergeJoinScanner::AdvancedStreamedWithNullFreeJoinKey()
{
    bool found = false;
    while (!found && HasNext(streamedPagesIndexPosition, streamedPagesIndex)) {
        // Advance the streamed side of the join until we find the next row whose join key contains
        // no nulls or we hit the end of the streamed iterator.
        streamedPagesIndexPosition++;
        found = !CurStreamedHasNull();
    }

    return found;
}

bool SortMergeJoinScanner::AdvancedStreamedJoinKey()
{
    if (HasNext(streamedPagesIndexPosition, streamedPagesIndex)) {
        streamedPagesIndexPosition++;
        curStreamRowMatchFlag = false;
        return true;
    }
    return false;
}

/**
 * Advance the buffered iterator until we find a row with join key that does not contain nulls.
 * @return true if the buffered iterator returned a row and false otherwise.
 */
bool SortMergeJoinScanner::AdvancedBufferedToRowWithNullFreeJoinKey()
{
    bool found = false;
    while (!found && HasNext(bufferedPagesIndexPosition, bufferedPagesIndex)) {
        bufferedPagesIndexPosition++;
        curBufferRowMatchFlag = false;
        found = !CurBufferedHasNull();
    }

    return found;
}

bool SortMergeJoinScanner::AdvancedBufferedJoinKey()
{
    if (HasNext(bufferedPagesIndexPosition, bufferedPagesIndex)) {
        bufferedPagesIndexPosition++;
        curBufferRowMatchFlag = false;
        return true;
    }
    return false;
}

void SortMergeJoinScanner::BufferMatchingRows()
{
    auto streamedValueAddr = streamedPagesIndex->GetValueAddresses(streamedPagesIndexPosition);
    preStreamedValueAddress = streamedValueAddr;
    preBufferedValueAddress.clear();
    preBufferedKeyMatched.clear();
    int64_t bufferedValueAddr;
    do {
        bufferedValueAddr = bufferedPagesIndex->GetValueAddresses(bufferedPagesIndexPosition);
        if (!onlyBufferedFirstMatch || preBufferedValueAddress.empty()) {
            preBufferedKeyMatched.emplace_back(!preBufferedValueAddress.empty());
            preBufferedValueAddress.push_back(bufferedValueAddr);
        }
        preBufferedPagesIndexPosition = bufferedPagesIndexPosition;
    } while (AdvancedBufferedToRowWithNullFreeJoinKey() && ((latestCompareStat = CompareCurRowKeys()) == 0));
    SavePrevMatchingRows(false);
}

void SortMergeJoinScanner::BufferMatchingRowsForFullOuter()
{
    auto streamedValueAddr = streamedPagesIndex->GetValueAddresses(streamedPagesIndexPosition);
    preStreamedValueAddress = streamedValueAddr;
    preBufferedValueAddress.clear();
    int64_t bufferedValueAddr;
    do {
        bufferedValueAddr = bufferedPagesIndex->GetValueAddresses(bufferedPagesIndexPosition);
        preBufferedValueAddress.push_back(bufferedValueAddr);
        preBufferedPagesIndexPosition = bufferedPagesIndexPosition;
    } while (AdvancedBufferedJoinKey() && ((latestCompareStat = CompareCurRowKeys()) == 0));
    SavePrevMatchingRows(false);
}

bool SortMergeJoinScanner::HandleLeftOuterStreamedNullAndBufferedDataFinishedSituation()
{
    if (CurStreamedHasNull() || bufferedPagesIndex->IsEmptyBatch() ||
        (bufferedPagesIndex->IsDataFinish(bufferedPagesIndexPosition) && curBufferRowMatchFlag)) {
        if (!curStreamRowMatchFlag) {
            BufferMissingRows();
        }
        return true;
    }
    return false;
}

void SortMergeJoinScanner::BufferMissingRows()
{
    auto streamedValueAddr = streamedPagesIndex->GetValueAddresses(streamedPagesIndexPosition);
    preStreamedValueAddress = streamedValueAddr;
    preBufferedValueAddress.clear();
    preBufferedKeyMatched.clear();
    auto nullValueAddress = EncodeSyntheticAddress(JOIN_NULL_FLAG, JOIN_NULL_FLAG); // null row flag
    preBufferedValueAddress.push_back(nullValueAddress);
    preBufferedKeyMatched.emplace_back(false);
    SavePrevMatchingRows(false);
    preBufferedPagesIndexPosition = bufferedPagesIndexPosition;
}

void SortMergeJoinScanner::StreamMissingRowsForCompareValue()
{
    auto nullValueAddress = EncodeSyntheticAddress(JOIN_NULL_FLAG, JOIN_NULL_FLAG); // null row flag
    preStreamedValueAddress = nullValueAddress;
    preBufferedValueAddress.clear();
    int64_t bufferedValueAddr;
    do {
        bufferedValueAddr = bufferedPagesIndex->GetValueAddresses(bufferedPagesIndexPosition);
        preBufferedValueAddress.push_back(bufferedValueAddr);
        preBufferedPagesIndexPosition = bufferedPagesIndexPosition;
    } while (AdvancedBufferedJoinKey() && ((latestCompareStat = CompareCurRowKeys()) > 0));
    FullOuterJoinSavePrevMatchingRows(false);
}

bool SortMergeJoinScanner::HandleFullOuterStreamedIsFinishedSituation()
{
    if (streamedPagesIndex->IsEmptyBatch() || streamedPagesIndex->IsDataFinish(streamedPagesIndexPosition)) {
        if (!curBufferRowMatchFlag) {
            StreamMissingRowsForStreamIsFinished();
            curBufferRowMatchFlag = true;
        }
        preStatus->TransToNeedBufferedData(HasResult());
        return false;
    }
    return true;
}

bool SortMergeJoinScanner::HandleFullOuterBufferedIsFinishedSituation()
{
    if (bufferedPagesIndex->IsEmptyBatch() ||
        (bufferedPagesIndex->IsDataFinish(bufferedPagesIndexPosition) && curBufferRowMatchFlag)) {
        if (!curStreamRowMatchFlag) {
            BufferMissingRows();
            curStreamRowMatchFlag = true;
        }
        return true;
    }
    return false;
}

bool SortMergeJoinScanner::HandleFullOuterStreamedNullValue()
{
    if (CurStreamedHasNull()) {
        BufferMissingRows();
        curStreamRowMatchFlag = true;
        return true;
    }
    return false;
}

bool SortMergeJoinScanner::HandleFullOuterBufferedNullValue()
{
    if (CurBufferedHasNull()) {
        StreamMissingRowsForNullBuffered();
        if (preBufferedPagesIndexPosition == bufferedPagesIndexPosition) { // hit buffered end
            preStatus->TransToNeedBufferedData(HasResult());
            return false;
        }
    }
    return true;
}

bool SortMergeJoinScanner::HandleFullOuterNeedBufferedData()
{
    if (NeedBufferedData()) { // if this buffered batch last row == stream row, so need add next buffered batch
        preStatus->TransToNeedBufferedData(HasResult());
        bufferedPagesIndexPosition--;
        return false;
    }
    return true;
}

void SortMergeJoinScanner::StreamMissingRowsForStreamIsFinished()
{
    auto nullValueAddress = EncodeSyntheticAddress(JOIN_NULL_FLAG, JOIN_NULL_FLAG); // null row flag
    preStreamedValueAddress = nullValueAddress;
    preBufferedValueAddress.clear();
    int64_t bufferedValueAddr;
    do {
        bufferedValueAddr = bufferedPagesIndex->GetValueAddresses(bufferedPagesIndexPosition);
        preBufferedValueAddress.push_back(bufferedValueAddr);
    } while (AdvancedBufferedJoinKey());
    FullOuterJoinSavePrevMatchingRows(false);
}

void SortMergeJoinScanner::StreamMissingRowsForNullBuffered()
{
    auto nullValueAddress = EncodeSyntheticAddress(JOIN_NULL_FLAG, JOIN_NULL_FLAG); // null row flag
    preStreamedValueAddress = nullValueAddress;
    preBufferedValueAddress.clear();
    int64_t bufferedValueAddr;
    do {
        bufferedValueAddr = bufferedPagesIndex->GetValueAddresses(bufferedPagesIndexPosition);
        preBufferedValueAddress.push_back(bufferedValueAddr);
        preBufferedPagesIndexPosition = bufferedPagesIndexPosition;
        curBufferRowMatchFlag = true;
    } while (AdvancedBufferedJoinKey() && CurBufferedHasNull());
    FullOuterJoinSavePrevMatchingRows(false);
}

int32_t SortMergeJoinScanner::FindNextMatchPos()
{
    auto compare = CompareCurRowKeys();
    auto cur = streamedPagesIndexPosition;
    while ((compare > 0 && AdvancedBufferedToRowWithNullFreeJoinKey()) ||
        (compare < 0 && AdvancedStreamedWithNullFreeJoinKey())) {
        compare = CompareCurRowKeys();
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

bool SortMergeJoinScanner::PreKeyMatchedWithNullValue()
{
    if (preStreamedValueAddress == -1 || preBufferedValueAddress.empty()) {
        return false;
    }
    auto curValueAddr = streamedPagesIndex->GetValueAddresses(streamedPagesIndexPosition);
    if (StreamedRowHasNull(preStreamedValueAddress) || StreamedRowHasNull(curValueAddr)) {
        // if one of them contain NULL return mismatch
        return false;
    }
    bool isMatched = CompareRowKeys(preStreamedValueAddress, streamedPagesIndex, streamedTableKeysCols, curValueAddr,
        streamedPagesIndex, streamedTableKeysCols) == 0;
    return isMatched;
}

void SortMergeJoinScanner::SavePrevMatchingRows(bool isMatched)
{
    bufferedValueAddress.insert(bufferedValueAddress.end(), preBufferedValueAddress.begin(),
        preBufferedValueAddress.end());
    isSameBufferedKeyMatched.insert(isSameBufferedKeyMatched.end(), preBufferedKeyMatched.begin(),
        preBufferedKeyMatched.end());
    auto valueAddr = streamedPagesIndex->GetValueAddresses(streamedPagesIndexPosition);
    streamedValueAddress.insert(streamedValueAddress.end(), preBufferedValueAddress.size(), valueAddr);
    isPreKeyMatched.insert(isPreKeyMatched.end(), preBufferedValueAddress.size(), isMatched);
}

void SortMergeJoinScanner::FullOuterJoinSavePrevMatchingRows(bool isMatched)
{
    bufferedValueAddress.insert(bufferedValueAddress.end(), preBufferedValueAddress.begin(),
        preBufferedValueAddress.end());
    streamedValueAddress.insert(streamedValueAddress.end(), preBufferedValueAddress.size(), preStreamedValueAddress);
    isPreKeyMatched.insert(isPreKeyMatched.end(), preBufferedValueAddress.size(), isMatched);
}

// return true if streamedValueAddress bufferedValueAddress both not empty
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
        auto com = OperatorUtil::CompareVectorAtPosition(streamedTableKeysTypes.GetIds()[leftKeyCols[i]],
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

/* * Returns true if there are any NULL values in this row. */
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

/* * Returns true if there are any NULL values in this row. */
bool SortMergeJoinScanner::StreamedRowHasNull(int64_t valueAddress)
{
    auto row = DecodePosition(valueAddress);
    auto curBatchId = DecodeSliceIndex(valueAddress);
    for (int i = 0; i < keyColsCount; ++i) {
        if (streamedPagesIndex->GetColumns(curBatchId, streamedTableKeysCols[i])->IsValueNull(row)) {
            return true;
        }
    }
    return false;
}

/* * Returns true if there are any NULL values in this row. */
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
    if (bufferedValueAddr > -1 && (CompareRowKeys(streamedValueAddr, bufferedValueAddr) >= 0)) {
        return true;
    }
    return false;
}

JoinStatus::JoinStatus(JoinTableCode streamedCode, JoinTableCode bufferedCode, JoinResultCode resultCode)
    : streamedCode(streamedCode), bufferedCode(bufferedCode), resultCode(resultCode)
{}

uint32_t JoinStatus::GenerateStatus()
{
    return (static_cast<uint32_t>(streamedCode) << STREAM_SHIFT_24) |
        (static_cast<uint32_t>(bufferedCode) << BUFFER_SHIFT_16) | static_cast<uint32_t>(resultCode);
}

JoinStatus::JoinStatus(JoinTableCode streamedCode, JoinTableCode bufferedCode, bool hasResult)
    : streamedCode(streamedCode),
      bufferedCode(bufferedCode),
      resultCode(hasResult ? JoinResultCode::HAS_RESULT : JoinResultCode::NO_RESULT)
{}

void JoinStatus::Set(JoinTableCode inputStreamedCode, JoinTableCode inputBufferedCode, bool hasResult)
{
    this->streamedCode = inputStreamedCode;
    this->bufferedCode = inputBufferedCode;
    this->resultCode = hasResult ? JoinResultCode::HAS_RESULT : JoinResultCode::NO_RESULT;
}

bool JoinStatus::NewStreamedDataAdded()
{
    return streamedCode == JoinTableCode::NEED_DATA;
}

bool JoinStatus::NewBufferedDataAdded()
{
    return bufferedCode == JoinTableCode::NEED_DATA;
}

void JoinStatus::Reset()
{
    streamedCode = JoinTableCode::INVALID;
    bufferedCode = JoinTableCode::INVALID;
}

void JoinStatus::TransToNeedBufferedData(bool hasResult)
{
    if (bufferedCode == JoinTableCode::SCAN_FINISHED) {
        // Inner Join Unique logic, need refactor
        Set(JoinTableCode::SCAN_FINISHED, JoinTableCode::SCAN_FINISHED, hasResult);
    } else {
        auto streamStatus =
            (streamedCode == JoinTableCode::SCAN_FINISHED) ? JoinTableCode::SCAN_FINISHED : JoinTableCode::NEED_SCAN;
        Set(streamStatus, JoinTableCode::NEED_DATA, hasResult);
    }
}

void JoinStatus::TransToNeedStreamedData(bool hasResult)
{
    if (streamedCode == JoinTableCode::SCAN_FINISHED) {
        // Inner Join Unique logic, need refactor
        Set(JoinTableCode::SCAN_FINISHED, JoinTableCode::SCAN_FINISHED, hasResult);
    } else {
        auto bufferedStatus =
            (bufferedCode == JoinTableCode::SCAN_FINISHED) ? JoinTableCode::SCAN_FINISHED : JoinTableCode::NEED_SCAN;
        Set(JoinTableCode::NEED_DATA, bufferedStatus, hasResult);
    }
}
}
}