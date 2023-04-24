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

    void ParsingAndOrganizationResultsForLeftTable(int32_t leftBatchId, int32_t leftRowId);

    void ParsingAndOrganizationResultsForRightTable(int32_t rightBatchId, int32_t rightRowId);

    void PaddingLeftTableNull(int32_t rightBatchId, int32_t rightRowId);

    void PaddingRightTableNull(int32_t leftBatchId, int32_t leftRowId);

    int32_t AddJoinValueAddresses();

    int32_t ConstructInnerJoinOutput();

    int32_t ConstructLeftJoinOutput();

    int32_t ConstructFullJoinOutput();

    int32_t ConstructLeftSemiJoinOutput();

    int32_t ConstructLeftAntiJoinOutput();

    int32_t GetOutput(omniruntime::vec::VectorBatch **outputVecBatch);

    void Finish();

    ALWAYS_INLINE std::vector<int8_t> &GetPreKeyMatched()
    {
        return isPreKeyMatched;
    }

    ALWAYS_INLINE std::vector<int32_t> &GetStartBufferedBatchIds()
    {
        return startBufferedBatchIds;
    }

    ALWAYS_INLINE std::vector<int64_t> &GetBufferedTableValueAddresses()
    {
        return bufferedTableValueAddresses;
    }

    ALWAYS_INLINE std::vector<int64_t> &GetStreamedTableValueAddresses()
    {
        return streamedTableValueAddresses;
    }

    ALWAYS_INLINE std::vector<int8_t> &GetSameBufferedKeyMatched()
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

    bool IsJoinPositionEligible(int32_t leftBatchId, int32_t leftRowId, int32_t rightBatchId, int32_t rightRowId) const;

    ALWAYS_INLINE bool NeedDoFilter() const
    {
        return simpleFilter != nullptr;
    }

private:
    struct LeftAntiJoinHandler {
        bool hasSameBufferedRow = false;
        bool printThisStreamRowOutFlag = true; // default this row should be out
    };

    void JoinFilterCodeGen(OverflowConfig *overflowConfig);
    void FreeVectorBatches(bool isPreMatched, int32_t leftBatchId, int32_t rightBatchId);
    VectorBatch *NewEmptyVectorBatch() const;
    void UpdateLeftAntiJoinHandler(int32_t addressPosition, std::vector<int8_t> &isSameBufferedKeyMatched,
        int32_t inputSize);

    ALWAYS_INLINE void PaddingLeftTableNull()
    {
        for (int columnIdx = 0; columnIdx < leftTableOutputColsCount; columnIdx++) {
            auto vector = buildVectorBatch->GetVector(columnIdx);
            auto typeId = vector->GetTypeId();
            if (typeId == OMNI_VARCHAR || typeId == OMNI_CHAR) {
                static_cast<VarcharVector *>(vector)->SetValueNull(buildRowCount);
            } else {
                vector->SetValueNull(buildRowCount);
            }
        }
    }

    ALWAYS_INLINE void PaddingRightTableNull()
    {
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
    }

    std::vector<DataTypePtr> leftTableOutputTypes;
    int32_t *leftTableOutputCols;
    int32_t leftTableOutputColsCount;
    int32_t originalLeftTableColsCount;
    DynamicPagesIndex *leftTablePagesIndex;
    int32_t *rightTableOutputCols;
    int32_t rightTableOutputColsCount;
    int32_t originalRightTableColsCount;
    DynamicPagesIndex *rightTablePagesIndex;
    std::string filterExpStr;

    int32_t lastUnMatchedStreamedBatchId = -1;
    int32_t lastUnMatchedBufferedBatchId = -1;

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
    std::vector<int8_t> isPreKeyMatched;
    std::vector<int32_t> startBufferedBatchIds;
    // store the matched valueAddress
    std::vector<int64_t> streamedTableValueAddresses;
    std::vector<int64_t> bufferedTableValueAddresses;
    std::vector<int8_t> isSameBufferedKeyMatched;
    LeftAntiJoinHandler leftAntiJoinHandler;
    std::vector<DataTypePtr> allTypes;
};
}
}
#endif // __SORT_MERGE_JOIN_RESULT_BUILDER_H__
