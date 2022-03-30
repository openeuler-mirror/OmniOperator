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
enum class JoinTableCode {
    NEED_SCAN = 0,
    NEED_DATA = 1,
    SCAN_FINISHED = 2,
    INVALID = 3
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
        DynamicPagesIndex *bufferedTablePagesIndex, JoinType joinType, bool firstMatch);

    int64_t FindNextJoinRows();

    int32_t GetMatchedValueAddresses(std::vector<bool> &isPreKeyMatched,
        std::vector<int64_t> &streamedTblValueAddresses, std::vector<int64_t> &bufferedTblValueAddresses);

    ~SortMergeJoinScanner();

private:
    void InnerJoin();

    bool IsValidAddedStreamedData();

    bool IsValidAddedBufferedData();

    bool NeedBufferedData();

    int32_t FindNextMatchPos();

    bool AdvancedStreamedWithNullFreeJoinKey();

    bool AdvancedBufferedToRowWithNullFreeJoinKey();

    bool CurStreamedHasNull();

    bool CurBufferedHasNull();

    void RunInnerJoin();

    bool FindMatchingRows();

    void BufferMatchingRows();

    void SavePrevMatchingRows(bool isMatched);

    bool PreKeyMatched();

    bool HasResult();

    int32_t CompareCurRowKeys();

    int32_t CompareRowKeys(int64_t leftRowIndex, DynamicPagesIndex *leftPagesIndex, const int32_t *leftKeyCols,
        int64_t rightRowIndex, DynamicPagesIndex *rightPagesIndex, const int32_t *rightKeyCols);

    int32_t CompareRowKeys(int64_t leftRowIndex, int64_t rightRowIndex);

    JoinType joinType;

    int32_t *streamedTableKeysCols;

    int32_t *bufferedTableKeysCols;

    std::unique_ptr<omniruntime::type::DataTypes> streamedTableKeysTypes;

    int32_t keyColsCount;

    DynamicPagesIndex *streamedPagesIndex;

    DynamicPagesIndex *bufferedPagesIndex;

    // for non-inner-join
    bool firstMatch;

    std::vector<bool> isPreKeyMatched;

    std::vector<int64_t> streamedValueAddress;

    std::vector<int64_t> bufferedValueAddress;

    int64_t preStreamedValueAddress;

    std::vector<int64_t> preBufferedValueAddress;

    std::unique_ptr<JoinStatus> preStatus;

    int32_t streamedPagesIndexPosition;

    int32_t bufferedPagesIndexPosition;
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
