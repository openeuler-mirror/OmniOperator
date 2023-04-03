/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: join result builder implementations
 */

#ifndef __SORT_MERGE_JOIN_RESULT_BUILDER_H__
#define __SORT_MERGE_JOIN_RESULT_BUILDER_H__

#include <string>
#include "dynamic_pages_index.h"
#include "vector/vector.h"
#include "type/data_types.h"
#include "expression/expressions.h"
#include "operator/filter/filter_and_project.h"
#include "operator/join/common_join.h"
#include "vector/vector_allocator.h"

namespace omniruntime {
namespace op {
class JoinResultBuilder {
public:
    JoinResultBuilder(const std::vector<DataTypePtr> &leftTableOutputTypes, int32_t *leftTableOutputCols,
        int32_t leftTableOutputColsCount, int32_t originalLeftTableColsCount, DynamicPagesIndex *leftTablePagesIndex,
        const std::vector<DataTypePtr> &rightTableOutputTypes, int32_t *rightTableOutputCols,
        int32_t rightTableOutputColsCount, int32_t originalRightTableColsCount, DynamicPagesIndex *rightTablePagesIndex,
        std::string &filter, VectorAllocator *vecAllocator, JoinType joinType, OverflowConfig *overflowConfig);

    void ParsingAndOrganizationResultsForLeftTable(int32_t leftBatchId, int32_t leftRowId,
        vec::VectorBatch *buildVectorBatch, int32_t &buildRowCount);

    void ParsingAndOrganizationResultsForRightTable(int32_t rightBatchId, int32_t rightRowId,
        vec::VectorBatch *buildVectorBatch, int32_t &buildRowCount);

    void PaddingLeftTableNull(int32_t rightBatchId, int32_t rightRowId, vec::VectorBatch *buildVectorBatch,
        int32_t &buildRowCount);

    void PaddingRightTableNull(int32_t leftBatchId, int32_t leftRowId, vec::VectorBatch *buildVectorBatch,
        int32_t &buildRowCount);

    int32_t AddJoinValueAddresses();

    int32_t ConstructInnerJoinOutput();

    int32_t ConstructLeftJoinOutput();

    int32_t ConstructFullJoinOutput();

    int32_t ConstructLeftSemiJoinOutput();

    int32_t ConstructLeftAntiJoinOutput();

    int32_t GetOutput(omniruntime::vec::VectorBatch **outputVecBatch);

    void Finish();

    ALWAYS_INLINE std::vector<bool> &GetPreKeyMatched()
    {
        return isPreKeyMatched;
    }

    ALWAYS_INLINE std::vector<int64_t> &GetBufferedTableValueAddresses()
    {
        return bufferedTableValueAddresses;
    }

    ALWAYS_INLINE std::vector<int64_t> &GetStreamedTableValueAddresses()
    {
        return streamedTableValueAddresses;
    }

    ALWAYS_INLINE std::vector<bool> &GetSameBufferedKeyMatched()
    {
        return isSameBufferedKeyMatched;
    }

    ALWAYS_INLINE bool HasNext()
    {
        return buildVectorBatchRowCount != 0;
    }

    ALWAYS_INLINE void Clear()
    {
        VectorHelper::FreeVecBatch(buildVectorBatch);
        buildVectorBatchRowCount = 0;
        addressOffset = 0;
        isPreRowMatched = false;
        isPreKeyMatched.clear();
        bufferedTableValueAddresses.clear();
        streamedTableValueAddresses.clear();
        isSameBufferedKeyMatched.clear();
        buildVectorBatch = nullptr;
    }

    ~JoinResultBuilder();

private:
    struct LeftAntiJoinHandler {
        bool hasSameBufferedRow = false;
        bool printThisStreamRowOutFlag = true; // default this row should be out
    };

    void JoinFilterCodeGen(OverflowConfig *overflowConfig);
    void FreeVectorBatches(bool isPreMatched, int32_t leftBatchId, int32_t rightBatchId);
    bool IsJoinPositionEligible(int32_t leftBatchId, int32_t leftRowId, int32_t rightBatchId, int32_t rightRowId) const;
    VectorBatch *NewEmptyVectorBatch() const;
    void UpdateLeftAntiJoinHandler(LeftAntiJoinHandler *leftAntiJoinHandler, int32_t addressPosition,
        std::vector<bool> &isSameBufferedKeyMatched, int32_t inputSize);

    std::vector<DataTypePtr> leftTableOutputTypes;
    int32_t *leftTableOutputCols;
    int32_t leftTableOutputColsCount;
    int32_t originalLeftTableColsCount;
    DynamicPagesIndex *leftTablePagesIndex;
    std::vector<DataTypePtr> rightTableOutputTypes;
    int32_t *rightTableOutputCols;
    int32_t rightTableOutputColsCount;
    int32_t originalRightTableColsCount;
    DynamicPagesIndex *rightTablePagesIndex;
    std::string filterExpStr;

    int32_t lastUnMatchedStreamedBatchId = -1;

    bool isFinished = false;
    int32_t maxRowCount;

    int64_t preStreamedRowAddress = INT64_MAX;
    int32_t preStreamedBatchId = INT32_MAX;
    int32_t preStreamedRowId = INT32_MAX;
    bool preLeftTableRowMatchedOut = false; // current left row matched out or pad null

    vec::VectorAllocator *vecAllocator;

    int32_t buildVectorBatchRowCount = 0;
    VectorBatch *buildVectorBatch = nullptr;

    ExecutionContext *executionContext = nullptr;
    SimpleFilter *simpleFilter = nullptr;
    int32_t *streamedColsInFilter = nullptr;
    int32_t *bufferedColsInFilter = nullptr;
    int32_t streamedColsCountInFilter = 0;
    int32_t bufferedColsCountInFilter = 0;
    int64_t *values = nullptr;
    bool *nulls = nullptr;
    int32_t *lengths = nullptr;
    JoinType joinType;

    bool isPreRowMatched = false;
    int32_t addressOffset = 0;
    int32_t buildRowCount = 0;
    std::vector<bool> isPreKeyMatched;
    // store the matched valueAddress
    std::vector<int64_t> streamedTableValueAddresses;
    std::vector<int64_t> bufferedTableValueAddresses;
    std::vector<bool> isSameBufferedKeyMatched;
    LeftAntiJoinHandler leftAntiJoinHandler;
};
}
}
#endif // __SORT_MERGE_JOIN_RESULT_BUILDER_H__
