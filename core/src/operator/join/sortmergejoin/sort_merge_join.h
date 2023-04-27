/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: sort merge join core implementations
 */

#ifndef __SORT_MERGE_JOIN_H__
#define __SORT_MERGE_JOIN_H__

#include "operator/operator.h"
#include "operator/operator_factory.h"
#include "operator/status.h"
#include "sort_merge_join_resultBuilder.h"
#include "sort_merge_join_scanner.h"
#include "dynamic_pages_index.h"
#include "vector/vector.h"
#include "type/data_types.h"
#include "operator/join/common_join.h"

namespace omniruntime {
namespace op {
constexpr uint32_t SHIFT_SIZE_16 = 16;
class SortMergeJoinOperator : public Operator {
public:
    SortMergeJoinOperator(JoinType joinType, std::string &filter);

    ~SortMergeJoinOperator() override;

    void ConfigStreamedTblInfo(const DataTypes &streamedTypes, const std::vector<int32_t> &streamedKeysCols,
        const std::vector<int32_t> &streamedOutputCols, int32_t originalInputStreamColsCount);

    void ConfigBufferedTblInfo(const DataTypes &bufferedTypes, std::vector<int32_t> &bufferedKeysCols,
        std::vector<int32_t> &bufferedOutputCols, int32_t originalInputBufferedColsCount);

    // see SortMergeJoinAddInputCode
    int32_t AddStreamedTableInput(omniruntime::vec::VectorBatch *vecBatch);

    // see SortMergeJoinAddInputCode
    int32_t AddBufferedTableInput(omniruntime::vec::VectorBatch *vecBatch);

    int32_t AddInput(omniruntime::vec::VectorBatch *vecBatch) override;

    int32_t GetOutput(omniruntime::vec::VectorBatch **outputVecBatch) override;

    OmniStatus Close() override;

    void InitScannerAndResultBuilder(OverflowConfig *overflowConfig);

private:
    int32_t GetJoinResult();

    const type::DataTypes *streamedTypes;
    std::vector<int32_t> streamedKeysCols;
    std::vector<int32_t> streamedOutputCols;
    int32_t originalStreamedColsCount;
    DynamicPagesIndex *streamedTblPagesIndex;

    const type::DataTypes *bufferedTypes;
    std::vector<int32_t> bufferedKeysCols;
    std::vector<int32_t> bufferedOutputCols;
    int32_t originalBufferedColsCount;
    DynamicPagesIndex *bufferedTblPagesIndex;

    JoinType joinType;
    std::string filter;

    SortMergeJoinScanner *smjScanner;
    JoinResultBuilder *joinResultBuilder;

    VectorBatch *returnVectorBatch = nullptr;
};

inline int32_t SetAddFlag(int16_t addFlag, int32_t resultCode)
{
    return (addFlag << SHIFT_SIZE_16) | (resultCode & USHRT_MAX);
}

inline int32_t SetFetchFlag(int16_t fetchFlag, int32_t resultCode)
{
    return ((resultCode >> SHIFT_SIZE_16) << SHIFT_SIZE_16) | fetchFlag;
}
}
}


#endif // __SORT_MERGE_JOIN_H__
