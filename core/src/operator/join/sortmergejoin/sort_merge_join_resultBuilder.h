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
    JoinResultBuilder(const type::DataTypes &leftTableOutputTypes, int32_t *leftTableOutputCols,
        int32_t leftTableOutputColsCount, DynamicPagesIndex *leftTablePagesIndex,
        const type::DataTypes &rightTableOutputTypes, int32_t *rightTableOutputCols, int32_t rightTableOutputColsCount,
        DynamicPagesIndex *rightTablePagesIndex, std::string &filter, VectorAllocator *vecAllocator,
        OverflowConfig *overflowConfig, JoinType joinType);

    void ParsingAndOrganizationResultsForLeftTable(int32_t leftBatchId, int32_t leftRowId,
        vec::VectorBatch *buildVectorBatch, int32_t &buildRowCount);

    void ParsingAndOrganizationResultsForRightTable(int32_t rightBatchId, int32_t rightRowId,
        vec::VectorBatch *buildVectorBatch, int32_t &buildRowCount);

    void PaddingLeftTableNull(int64_t leftTableRowAddress, vec::VectorBatch *buildVectorBatch, int32_t &buildRowCount);

    void PaddingRightTableNull(int64_t leftTableRowAddress, vec::VectorBatch *buildVectorBatch, int32_t &buildRowCount);

    int32_t AddJoinValueAddresses();

    int32_t GetOutput(std::vector<omniruntime::vec::VectorBatch *> &outputPages);

    void Finish();

    std::vector<bool> &GetPreKeyMatched()
    {
        return isPreKeyMatched;
    }

    std::vector<int64_t> &GetBufferedTableValueAddresses()
    {
        return bufferedTableValueAddresses;
    }

    std::vector<int64_t> &GetStreamedTableValueAddresses()
    {
        return streamedTableValueAddresses;
    }

    std::vector<bool> &GetSameBufferedKeyMatched()
    {
        return isSameBufferedKeyMatched;
    }

    bool HasNext()
    {
        return buildVectorBatchRowCount != 0;
    }

    void Clear()
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
    void PaddingNullAndVerifyingTheOutput(std::vector<bool> &isPreKeyMatched, LeftAntiJoinHandler *leftAntiJoinHandler,
        int64_t leftTableRowAddress, int64_t rightTableRowAddress, vec::VectorBatch *buildVectorBatch,
        int32_t &buildRowCount, std::vector<bool> &isSameBufferedKeyMatched, bool &isPreRowMatched,
        int32_t positionAddr);
    VectorBatch *NewEmptyVectorBatch() const;

    void UpdateLeftAntiJoinHandler(LeftAntiJoinHandler *leftAntiJoinHandler, int32_t addressPosition,
        std::vector<bool> &isSameBufferedKeyMatched, int32_t inputSize);

    type::DataTypes leftTableOutputTypes;
    int32_t *leftTableOutputCols;
    int32_t leftTableOutputColsCount;
    DynamicPagesIndex *leftTablePagesIndex;
    type::DataTypes rightTableOutputTypes;
    int32_t *rightTableOutputCols;
    int32_t rightTableOutputColsCount;
    DynamicPagesIndex *rightTablePagesIndex;
    std::string filterExpStr;

    int32_t lastUnMatchedStreamedBatchId = -1;

    bool isFinished = false;
    int32_t maxRowCount;

    int64_t preStreamedRowAddress = INT64_MAX;
    bool preLeftTableRowMatchedOut = false; // current left row matched out or pad null

    vec::VectorAllocator *vecAllocator;

    int32_t buildVectorBatchRowCount = 0;
    VectorBatch *buildVectorBatch = nullptr;

    SimpleFilter *simpleFilter = nullptr;
    ExecutionContext *executionContext = nullptr;
    JoinType joinType;

    bool isPreRowMatched = false;
    int32_t addressOffset = 0;
    int32_t buildRowCount = 0;
    std::vector<bool> isPreKeyMatched;
    // store the matched valueAddress
    std::vector<int64_t> streamedTableValueAddresses;
    std::vector<int64_t> bufferedTableValueAddresses;
    std::vector<bool> isSameBufferedKeyMatched;
    LeftAntiJoinHandler *leftAntiJoinHandler;
};
}
}
#endif // __SORT_MERGE_JOIN_RESULT_BUILDER_H__
