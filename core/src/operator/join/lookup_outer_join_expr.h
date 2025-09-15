/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * @Description: lookup outer join implementations
 */

#ifndef __LOOKUP_OUTER_JOIN_EXPR_H__
#define __LOOKUP_OUTER_JOIN_EXPR_H__

#include "common_join.h"
#include "operator/operator_factory.h"
#include "operator/projection/projection.h"
#include "operator/join/lookup_outer_join.h"
#include "operator/join/hash_builder_expr.h"
#include "type/data_types.h"
#include "operator/status.h"

namespace omniruntime {
namespace op {
class LookupOuterJoinWithExprOperatorFactory : public OperatorFactory {
public:
    static LookupOuterJoinWithExprOperatorFactory *CreateLookupOuterJoinWithExprOperatorFactory(
        const type::DataTypes &probeTypes, int32_t *probeOutputCols, int32_t probeOutputColsCount,
        const std::vector<omniruntime::expressions::Expr *> &probeHashKeys, int32_t probeHashKeysCount,
        int32_t *buildOutputCols, const type::DataTypes &buildOutputTypes, int64_t hashBuilderFactoryAddr);
    static LookupOuterJoinWithExprOperatorFactory *CreateLookupOuterJoinWithExprOperatorFactory(
        std::shared_ptr<const HashJoinNode> planNode,
        HashBuilderWithExprOperatorFactory* hashBuilderOperatorFactory, const config::QueryConfig& queryConfig);
    LookupOuterJoinWithExprOperatorFactory(const type::DataTypes &probeTypes, int32_t *probeOutputCols,
        int32_t probeOutputColsCount, const std::vector<omniruntime::expressions::Expr *> &probeHashKeys,
        int32_t probeHashKeysCount, int32_t *buildOutputCols, const type::DataTypes &buildOutputTypes,
        int64_t hashBuilderFactoryAddr);
    LookupOuterJoinWithExprOperatorFactory(const type::DataTypes &probeTypes, int32_t *probeOutputCols,
        int32_t probeOutputColsCount, const std::vector<omniruntime::expressions::Expr *> &probeHashKeys,
        int32_t probeHashKeysCount, int32_t *buildOutputCols, const type::DataTypes &buildOutputTypes,
        int64_t hashBuilderFactoryAddr, BuildSide buildSide);
    ~LookupOuterJoinWithExprOperatorFactory() override;
    Operator *CreateOperator() override;

private:
    std::unique_ptr<DataTypes> probeTypes; // all types for probe
    std::vector<int32_t> probeHashCols;    // join columns for probe
    std::vector<std::unique_ptr<Projection>> projections;
    LookupOuterJoinOperatorFactory *operatorFactory;
};

class LookupOuterJoinWithExprOperator : public Operator {
public:
    explicit LookupOuterJoinWithExprOperator(LookupOuterJoinOperator *lookupJoinOperator);
    ~LookupOuterJoinWithExprOperator() override;
    int32_t AddInput(omniruntime::vec::VectorBatch *vecBatch) override;
    int32_t GetOutput(omniruntime::vec::VectorBatch **outputVecBatch) override;
    OmniStatus Close() override;

    void noMoreInput() override
    {
        noMoreInput_ = true;
        lookupOuterJoinOperator->noMoreInput();
    }

    void setNoMoreInput(bool noMoreInput) override
    {
        noMoreInput_ = noMoreInput;
        lookupOuterJoinOperator->setNoMoreInput(noMoreInput);
    }

private:
    LookupOuterJoinOperator *lookupOuterJoinOperator;
};
}
}

#endif // __LOOKUP_OUTER_JOIN_EXPR_H__
