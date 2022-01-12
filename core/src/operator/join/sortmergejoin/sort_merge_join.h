/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: sort merge join core implementations
 */

#ifndef __SORT_MERGE_JOIN_H__
#define __SORT_MERGE_JOIN_H__

#include "../../operator_factory.h"
#include "sort_merge_join_resultBuilder.h"
#include "sort_merge_join_scanner.h"
#include "dynamic_pages_index.h"
#include "../../../vector/vector.h"
#include "../../../vector/vector_types.h"
#include "../common_join.h"

namespace omniruntime {
namespace op {
class SortMergeJoinOperator : public Operator {
public:
    SortMergeJoinOperator(JoinType joinType, std::string &filter);

    ~SortMergeJoinOperator() override;

    void ConfigStreamedTblInfo(const vec::VecTypes &streamedTypes, const std::vector<int32_t> &streamedKeysCols,
        const std::vector<int32_t> &streamedOutputCols);

    void ConfigBufferedTblInfo(const vec::VecTypes &bufferedTypes, std::vector<int32_t> &bufferedKeysCols,
        std::vector<int32_t> &bufferedOutputCols);
    // see SortMergeJoinAddInputCode
    int32_t AddStreamedTableInput(omniruntime::vec::VectorBatch *vecBatch);

    // see SortMergeJoinAddInputCode
    int32_t AddBufferedTableInput(omniruntime::vec::VectorBatch *vecBatch);

    int32_t AddInput(omniruntime::vec::VectorBatch *vecBatch) override;

    int32_t GetOutput(std::vector<omniruntime::vec::VectorBatch *> &outputPages) override;

    OmniStatus Close() override;

    void InitScannerAndResultBuilder();

private:
    int32_t GetJoinResult();

    vec::VecTypes *streamedTypes;
    std::vector<int32_t> streamedKeysCols;
    std::vector<int32_t> streamedOutputCols;
    DynamicPagesIndex *streamedTblPagesIndex;

    vec::VecTypes *bufferedTypes;
    std::vector<int32_t> bufferedKeysCols;
    std::vector<int32_t> bufferedOutputCols;
    DynamicPagesIndex *bufferedTblPagesIndex;

    JoinType joinType;
    std::string filter;
    //
    SortMergeJoinScanner *smjScanner;
    JoinResultBuilder *joinResultBuilder;

    std::vector<VectorBatch *> returnVectorBatchs;
};
}
}


#endif // __SORT_MERGE_JOIN_H__
