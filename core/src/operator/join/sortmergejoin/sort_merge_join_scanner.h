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

class JoinStatus {
public:
    JoinStatus(JoinTableCode streamedCode, JoinTableCode bufferedCode, JoinResultCode resultCode);
    JoinStatus(JoinTableCode streamedCode, JoinTableCode bufferedCode, bool hasResult);
    virtual ~JoinStatus() {}

    uint32_t GenerateStatus();
    void Set(JoinTableCode inputStreamedCode, JoinTableCode inputBufferedCode, bool hasResult);
    void TransToNeedStreamedData(bool hasResult);
    void TransToNeedBufferedData(bool hasResult);
    bool NewStreamedDataAdded();
    bool NewBufferedDataAdded();
    void Reset();

private:
    JoinTableCode streamedCode;
    JoinTableCode bufferedCode;
    JoinResultCode resultCode;
};

class InitialJoinStatus : public JoinStatus {
public:
    InitialJoinStatus() : JoinStatus(JoinTableCode::INVALID, JoinTableCode::INVALID, JoinResultCode::NO_RESULT) {};
    ~InitialJoinStatus() override {}
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
    void InnerJoin();

    void LeftOuterJoin();

    void FullOuterJoin();

    bool IsValidAddedStreamedData();

    bool IsValidAddedBufferedData();

    bool NeedBufferedData();

    int32_t FindNextMatchPos();

    bool AdvancedStreamedWithNullFreeJoinKey();

    bool AdvancedStreamedJoinKey();

    bool AdvancedBufferedToRowWithNullFreeJoinKey();

    bool AdvancedBufferedJoinKey();

    bool CurStreamedHasNull();

    bool StreamedRowHasNull(int64_t valueAddress);

    bool CurBufferedHasNull();

    void RunInnerJoin();

    void RunLeftOuterJoin();

    void RunFullOuterJoin();

    bool FindMatchingRows();

    bool LeftOuterFindJoinRows();

    bool FullOuterFindJoinRows();

    void BufferMatchingRows();

    void BufferMatchingRowsForFullOuter();

    bool HandleLeftOuterStreamedNullAndBufferedDataFinishedSituation();

    void BufferMissingRows();

    void StreamMissingRowsForCompareValue();

    bool HandleFullOuterStreamedIsFinishedSituation();

    bool HandleFullOuterBufferedIsFinishedSituation();

    bool HandleFullOuterStreamedNullValue();

    bool HandleFullOuterBufferedNullValue();

    bool HandleFullOuterNeedBufferedData();

    void StreamMissingRowsForStreamIsFinished();

    void StreamMissingRowsForNullBuffered();

    void SavePrevMatchingRows(bool isMatched);

    void FullOuterJoinSavePrevMatchingRows(bool isMatched);

    bool PreKeyMatched();

    bool PreKeyMatchedWithNullValue();

    bool HasResult();

    int32_t CompareCurRowKeys();

    int32_t CompareRowKeys(int64_t leftRowIndex, DynamicPagesIndex *leftPagesIndex, const int32_t *leftKeyCols,
        int64_t rightRowIndex, DynamicPagesIndex *rightPagesIndex, const int32_t *rightKeyCols);

    int32_t CompareRowKeys(int64_t leftRowIndex, int64_t rightRowIndex);

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
    std::unique_ptr<JoinStatus> preStatus;
    std::vector<bool> isPreKeyMatched;
    std::vector<bool> isSameBufferedKeyMatched;
    std::vector<bool> preBufferedKeyMatched;
    std::vector<int64_t> streamedValueAddress;
    std::vector<int64_t> bufferedValueAddress;
    std::vector<int64_t> preBufferedValueAddress;
};

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
}
}

#endif // __SORT_MERGE_JOIN_SCANNER_H__
