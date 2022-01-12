/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: sort merge join interface layer implementations
 */

#ifndef __SORT_MERGE_JOIN_EXPR_H__
#define __SORT_MERGE_JOIN_EXPR_H__

#include "../../operator_factory.h"
#include "../../projection/projection.h"
#include "../../../vector/vector.h"
#include "../../../vector/vector_types.h"
#include "../common_join.h"
#include "dynamic_pages_index.h"
#include "sort_merge_join.h"

namespace omniruntime {
namespace op {
class StreamedTableWithExprOperatorFactory : public OperatorFactory {
public:
    static StreamedTableWithExprOperatorFactory *CreateStreamedTableWithExprOperatorFactory(
        const vec::VecTypes &streamedTypes,
        const std::vector<omniruntime::expressions::Expr *> &streamedKeyExprCols, int32_t streamedKeyExprColsCnt,
        int32_t *streamedOutputCols, int32_t streamedOutputColsCnt, JoinType joinType, std::string &filter);

    StreamedTableWithExprOperatorFactory(const vec::VecTypes &streamedTypes,
        const std::vector<omniruntime::expressions::Expr *> &streamedKeyExprCols, int32_t streamedKeyExprColsCnt,
        int32_t *streamedOutputCols, int32_t streamedOutputColsCnt, JoinType joinType,
        std::string &filter);

    ~StreamedTableWithExprOperatorFactory() override;

    omniruntime::op::Operator *CreateOperator() override;

    SortMergeJoinOperator *GetSmjOperator();

private:
    std::unique_ptr<vec::VecTypes> streamedTypes;
    std::vector<int32_t> streamedKeyCols;
    std::vector<int32_t> streamedOutputCols;
    JoinType joinType;
    std::string filter;
    SortMergeJoinOperator *smjOperator;
    std::vector<std::unique_ptr<RowProjection>> rowProjections;
    std::vector<RowProjFunc> projectFuncs;
};

class StreamedTableWithExprOperator : public Operator {
public:
    StreamedTableWithExprOperator(const vec::VecTypes &streamedTypes, const std::vector<int32_t> &streamedKeyCols,
        const std::vector<RowProjFunc> &projectFuncs, SortMergeJoinOperator *smjOperator);

    ~StreamedTableWithExprOperator() override;

    // return code see SortMergeJoinAddInputCode
    int32_t AddInput(VectorBatch *vecBatch) override;

    int32_t GetOutput(std::vector<VectorBatch *> &outputPages) override;

    OmniStatus Close() override;

private:
    SortMergeJoinOperator *smjOperator;
    const omniruntime::vec::VecTypes &streamedTypes;
    std::vector<int32_t> streamedKeyCols;
    std::vector<RowProjFunc> projectFuncs;
    std::vector<VectorBatch *> inputVecBatches;
};

class BufferedTableWithExprOperatorFactory : public OperatorFactory {
public:
    static BufferedTableWithExprOperatorFactory *CreateBufferedTableWithExprOperatorFactory(
        const VecTypes &bufferedTypes,
        const std::vector<omniruntime::expressions::Expr *> &bufferedKeyExprCols, int32_t bufferedKeyExprCnt,
        int32_t *bufferedOutputCols, int32_t bufferedOutputColsCnt, int64_t streamedTableFactoryAddr);

    BufferedTableWithExprOperatorFactory(const VecTypes &bufferedTypes,
        const std::vector<omniruntime::expressions::Expr *> &bufferedKeyExprCols, int32_t bufferedKeyExprCnt,
        int32_t *bufferedOutputCols, int32_t bufferedOutputColsCnt,
        int64_t streamedTableFactoryAddr);

    ~BufferedTableWithExprOperatorFactory() override;

    omniruntime::op::Operator *CreateOperator() override;

private:
    std::unique_ptr<VecTypes> bufferedTypes;
    std::vector<int32_t> bufferedKeyCols;
    std::vector<int32_t> bufferedOutputCols;
    std::vector<std::unique_ptr<RowProjection>> rowProjections;
    std::vector<RowProjFunc> projectFuncs;
    StreamedTableWithExprOperatorFactory *streamTblWithExprOperatorFactory;
};

class BufferedTableWithExprOperator : public Operator {
public:
    BufferedTableWithExprOperator(const vec::VecTypes &bufferedTypes, const std::vector<int32_t> &bufferedKeyCols,
        const std::vector<RowProjFunc> &projectFuncs, SortMergeJoinOperator *smjOperator);

    ~BufferedTableWithExprOperator() override;

    // return code see SortMergeJoinAddInputCode
    int32_t AddInput(VectorBatch *vecBatch) override;

    int32_t GetOutput(std::vector<VectorBatch *> &outputPages) override;

    OmniStatus Close() override;

private:
    SortMergeJoinOperator *smjOperator;
    const omniruntime::vec::VecTypes &bufferedTypes;
    std::vector<int32_t> bufferedKeyCols;
    std::vector<RowProjFunc> projectFuncs;
    std::vector<VectorBatch *> inputVecBatches;
};
}
}
#endif // __SORT_MERGE_JOIN_EXPR_H__
