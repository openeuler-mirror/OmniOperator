/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: sort merge join scanner implementations
 */
#ifndef __SORT_MERGE_JOIN_SCANNER_H__
#define __SORT_MERGE_JOIN_SCANNER_H__

#include "type/data_types.h"
#include "dynamic_pages_index.h"
#include "operator/join/common_join.h"
#include "vector/vector_common.h"
#include "operator/pages_index.h"

namespace omniruntime {
namespace op {
const int JOIN_NULL_FLAG = -1;

enum class JoinTableCode {
    NEED_SCAN = 0,     // Traversing status
    NEED_DATA = 1,     // Adding data status
    SCAN_FINISHED = 2, // Finished status
    INVALID = 3        // Init status
};

enum class JoinResultCode {
    NO_RESULT = 0,
    HAS_RESULT = 1
};

static ALWAYS_INLINE bool HasNext(int32_t pos, DynamicPagesIndex *pagesIndex)
{
    return pos < pagesIndex->GetPositionCount() - 1;
}

constexpr uint32_t STREAM_SHIFT_24 = 24;
constexpr uint32_t BUFFER_SHIFT_16 = 16;
constexpr uint32_t BUFFER_SHIFT_8 = 8;

inline int8_t DecodeStreamedTblResult(uint32_t findNextJoinRowResult)
{
    return static_cast<int8_t>(findNextJoinRowResult >> STREAM_SHIFT_24);
}

inline int8_t DecodeBufferedTblResult(uint32_t findNextJoinRowResult)
{
    return static_cast<int8_t>((findNextJoinRowResult << BUFFER_SHIFT_8) >> STREAM_SHIFT_24);
}

inline int16_t DecodeJoinResult(uint32_t findNextJoinRowResult)
{
    return static_cast<int16_t>((findNextJoinRowResult << BUFFER_SHIFT_16) >> BUFFER_SHIFT_16);
}

class JoinStatus {
public:
    JoinStatus(JoinTableCode streamedCode, JoinTableCode bufferedCode, JoinResultCode resultCode)
        : streamedCode(streamedCode), bufferedCode(bufferedCode), resultCode(resultCode)
    {}

    JoinStatus(JoinTableCode streamedCode, JoinTableCode bufferedCode, bool hasResult)
        : streamedCode(streamedCode),
          bufferedCode(bufferedCode),
          resultCode(hasResult ? JoinResultCode::HAS_RESULT : JoinResultCode::NO_RESULT)
    {}

    virtual ~JoinStatus() {}

    ALWAYS_INLINE uint32_t GenerateStatus()
    {
        return (static_cast<uint32_t>(streamedCode) << STREAM_SHIFT_24) |
            (static_cast<uint32_t>(bufferedCode) << BUFFER_SHIFT_16) | static_cast<uint32_t>(resultCode);
    }

    ALWAYS_INLINE void Set(JoinTableCode inputStreamedCode, JoinTableCode inputBufferedCode, bool hasResult)
    {
        this->streamedCode = inputStreamedCode;
        this->bufferedCode = inputBufferedCode;
        this->resultCode = hasResult ? JoinResultCode::HAS_RESULT : JoinResultCode::NO_RESULT;
    }

    ALWAYS_INLINE void TransToNeedStreamedData(bool hasResult)
    {
        if (streamedCode == JoinTableCode::SCAN_FINISHED) {
            // Inner Join Unique logic, need refactor
            Set(JoinTableCode::SCAN_FINISHED, JoinTableCode::SCAN_FINISHED, hasResult);
        } else {
            auto bufferedStatus = (bufferedCode == JoinTableCode::SCAN_FINISHED) ? JoinTableCode::SCAN_FINISHED :
                                                                                   JoinTableCode::NEED_SCAN;
            Set(JoinTableCode::NEED_DATA, bufferedStatus, hasResult);
        }
    }

    ALWAYS_INLINE void TransToNeedBufferedData(bool hasResult)
    {
        if (bufferedCode == JoinTableCode::SCAN_FINISHED) {
            // Inner Join Unique logic, need refactor
            Set(JoinTableCode::SCAN_FINISHED, JoinTableCode::SCAN_FINISHED, hasResult);
        } else {
            auto streamStatus = (streamedCode == JoinTableCode::SCAN_FINISHED) ? JoinTableCode::SCAN_FINISHED :
                                                                                 JoinTableCode::NEED_SCAN;
            Set(streamStatus, JoinTableCode::NEED_DATA, hasResult);
        }
    }

    ALWAYS_INLINE bool NewStreamedDataAdded()
    {
        return streamedCode == JoinTableCode::NEED_DATA;
    }

    ALWAYS_INLINE bool NewBufferedDataAdded()
    {
        return bufferedCode == JoinTableCode::NEED_DATA;
    }

    ALWAYS_INLINE void Reset()
    {
        streamedCode = JoinTableCode::INVALID;
        bufferedCode = JoinTableCode::INVALID;
    }

private:
    JoinTableCode streamedCode;
    JoinTableCode bufferedCode;
    JoinResultCode resultCode;
};

class InitialJoinStatus : public JoinStatus {
public:
    InitialJoinStatus() : JoinStatus(JoinTableCode::INVALID, JoinTableCode::INVALID, JoinResultCode::NO_RESULT) {};
    ~InitialJoinStatus() override = default;
};

class SortMergeJoinScanner {
public:
    SortMergeJoinScanner(const omniruntime::type::DataTypes &streamedTableKeysTypes, int32_t *streamedTableKeysCols,
        int32_t keyColsCount, DynamicPagesIndex *streamedTablePagesIndex,
        const omniruntime::type::DataTypes &bufferedTableKeysTypes, int32_t *bufferedTableKeysCols,
        DynamicPagesIndex *bufferedTablePagesIndex, JoinType joinType, bool onlyBufferedFirstMatch);

    int64_t FindNextJoinRows();

    int32_t GetMatchedValueAddresses(std::vector<bool> &isPreKeyMatched,
        std::vector<int64_t> &streamedTblValueAddresses, std::vector<int64_t> &bufferedTblValueAddresses,
        std::vector<bool> &isSameBufferedKeyMatched);

    ~SortMergeJoinScanner();

private:
    template <JoinType templateJoinType> void InnerJoin();

    template <JoinType templateJoinType> void LeftOuterJoin();

    void FullOuterJoin();

    bool IsValidAddedStreamedData();

    bool IsValidAddedBufferedData();

    bool NeedBufferedData();

    template <JoinType templateJoinType> int32_t FindNextMatchPos();

    bool AdvancedStreamedWithNullFreeJoinKey();

    bool AdvancedStreamedJoinKey();

    bool AdvancedBufferedToRowWithNullFreeJoinKey();

    bool AdvancedBufferedJoinKey();

    /** Returns true if there are any NULL values in this row. */
    ALWAYS_INLINE bool CurStreamedHasNull()
    {
        return streamedPagesIndex->HaveNull(streamedPagesIndexPosition);
    }

    /** Returns true if there are any NULL values in this row. */
    ALWAYS_INLINE bool CurBufferedHasNull()
    {
        return bufferedPagesIndex->HaveNull(bufferedPagesIndexPosition);
    }

    template <JoinType templateJoinType> void RunInnerJoin();

    template <JoinType templateJoinType> void RunLeftOuterJoin();

    void RunFullOuterJoin();

    template <JoinType templateJoinType> bool FindMatchingRows();

    template <JoinType templateJoinType> bool LeftOuterFindJoinRows();

    bool FullOuterFindJoinRows();

    template <JoinType templateJoinType> void BufferMatchingRows();

    void BufferMatchingRowsForFullOuter();

    template <JoinType templateJoinType> bool HandleLeftOuterStreamedNullAndBufferedDataFinishedSituation();

    template <JoinType templateJoinType> void BufferMissingRows();

    void BufferMissingRowsForFullOuter();

    void StreamMissingRowsForCompareValue();

    bool HandleFullOuterStreamedIsFinishedSituation();

    bool HandleFullOuterBufferedIsFinishedSituation();

    bool HandleFullOuterStreamedNullValue();

    bool HandleFullOuterBufferedNullValue();

    bool HandleFullOuterNeedBufferedData();

    void StreamMissingRowsForStreamIsFinished();

    void StreamMissingRowsForNullBuffered();

    template <JoinType templateJoinType> void SavePrevMatchingRows(bool isMatched);

    void SavePrevMatchingRowsForFullOuter(bool isMatched);

    bool PreKeyMatched();

    bool PreKeyMatchedWithNullValue();

    // return true if streamedValueAddress bufferedValueAddress both not empty
    ALWAYS_INLINE bool HasResult()
    {
        return !streamedValueAddress.empty() && !bufferedValueAddress.empty();
    }

    ALWAYS_INLINE int32_t CompareCurRowKeys()
    {
        auto streamedValueAddr = streamedPagesIndex->GetValueAddresses(streamedPagesIndexPosition);
        auto bufferedValueAddr = bufferedPagesIndex->GetValueAddresses(bufferedPagesIndexPosition);
        return CompareRowKeys(streamedValueAddr, streamedPagesIndex, streamedTableKeysCols, bufferedValueAddr,
            bufferedPagesIndex, bufferedTableKeysCols);
    }

    ALWAYS_INLINE int32_t CompareRowKeys(int64_t leftRowIndex, DynamicPagesIndex *leftPagesIndex,
        const int32_t *leftKeyCols, int64_t rightRowIndex, DynamicPagesIndex *rightPagesIndex,
        const int32_t *rightKeyCols)
    {
        auto streamedBatchId = DecodeSliceIndex(leftRowIndex);
        auto streamedRowId = DecodePosition(leftRowIndex);
        auto bufferedBatchId = DecodeSliceIndex(rightRowIndex);
        auto bufferedRowId = DecodePosition(rightRowIndex);
        for (int i = 0; i < keyColsCount; ++i) {
            auto streamedColumn = leftPagesIndex->GetColumn(streamedBatchId, leftKeyCols[i]);
            auto bufferedColumn = rightPagesIndex->GetColumn(bufferedBatchId, rightKeyCols[i]);
            auto com = OperatorUtil::CompareVectorAtPosition(streamedTableKeysTypes.GetIds()[leftKeyCols[i]],
                streamedColumn, streamedRowId, bufferedColumn, bufferedRowId);
            if (com != 0) {
                return com;
            }
        }

        return 0;
    }

    ALWAYS_INLINE int32_t CompareRowKeys(int64_t leftRowIndex, int64_t rightRowIndex)
    {
        return CompareRowKeys(leftRowIndex, streamedPagesIndex, streamedTableKeysCols, rightRowIndex,
            bufferedPagesIndex, bufferedTableKeysCols);
    }

    omniruntime::type::DataTypes streamedTableKeysTypes;
    JoinType joinType;
    int32_t *streamedTableKeysCols;
    int32_t *bufferedTableKeysCols;
    int32_t keyColsCount;

    // for non-inner-join
    bool onlyBufferedFirstMatch = false;
    bool curStreamRowMatchFlag = false;
    bool curBufferRowMatchFlag = false;

    int32_t latestCompareStat = -1;
    int32_t preBufferedPagesIndexPosition = 0;

    int32_t streamedPagesIndexPosition;
    int32_t bufferedPagesIndexPosition;
    DynamicPagesIndex *streamedPagesIndex;
    DynamicPagesIndex *bufferedPagesIndex;
    int64_t preStreamedValueAddress;
    int32_t preStreamedPagesIndexPosition;
    std::unique_ptr<JoinStatus> preStatus;
    std::vector<bool> isPreKeyMatched;
    std::vector<bool> isSameBufferedKeyMatched;
    std::vector<bool> preBufferedKeyMatched;
    std::vector<int64_t> streamedValueAddress;
    std::vector<int64_t> bufferedValueAddress;
    std::vector<int64_t> preBufferedValueAddress;
};
}
}

#endif // __SORT_MERGE_JOIN_SCANNER_H__
