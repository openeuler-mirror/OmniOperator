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
#include "vector/vector_allocator.h"

namespace omniruntime {
namespace op {
class JoinResultBuilder {
public:
    JoinResultBuilder(const type::DataTypes &leftTableOutputTypes, int32_t *leftTableOutputCols,
        int32_t leftTableOutputColsCount, DynamicPagesIndex *leftTablePagesIndex,
        const type::DataTypes &rightTableOutputTypes, int32_t *rightTableOutputCols, int32_t rightTableOutputColsCount,
        DynamicPagesIndex *rightTablePagesIndex, std::string &filter, VectorAllocator *vecAllocator);

    int32_t AddJoinValueAddresses(std::vector<bool> &isPreKeyMatched, std::vector<int64_t> &streamedTableValueAddresses,
        std::vector<int64_t> &bufferedTableValueAddresses);

    int32_t GetOutput(std::vector<omniruntime::vec::VectorBatch *> &outputPages);

    void Finish();

    ~JoinResultBuilder();

private:
    void JoinFilterCodeGen();
    void FreeVectorBatches(bool isPreMatched, int32_t leftBatchId, int32_t rightBatchId);
    bool IsJoinPositionEligible(int32_t leftBatchId, int32_t leftRowId, int32_t rightBatchId, int32_t rightRowId) const;
    VectorBatch *NewEmptyVectorBatch() const;

    const type::DataTypes &leftTableOutputTypes;
    int32_t *leftTableOutputCols;
    int32_t leftTableOutputColsCount;
    DynamicPagesIndex *leftTablePagesIndex;
    const type::DataTypes &rightTableOutputTypes;
    int32_t *rightTableOutputCols;
    int32_t rightTableOutputColsCount;
    DynamicPagesIndex *rightTablePagesIndex;
    std::string filterExpStr;

    int32_t lastUnMatchedStreamedBatchId = -1;

    bool isFinished = false;
    int32_t maxRowCount;

    vec::VectorAllocator *vecAllocator;

    int32_t buildVectorBatchCount = 0;
    std::vector<int32_t> buildVectorBatchRowCount;
    std::vector<VectorBatch *> buildVectorBatchs;

    SimpleFilter *simpleFilter = nullptr;
    ExecutionContext *executionContext = nullptr;
};
}
}
#endif // __SORT_MERGE_JOIN_RESULT_BUILDER_H__
