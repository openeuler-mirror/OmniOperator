/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: sort implementations
 */

#ifndef __LOOKUP_JOIN_EXPR_H__
#define __LOOKUP_JOIN_EXPR_H__

#include "common_join.h"
#include "operator/operator_factory.h"
#include "operator/projection/projection.h"
#include "operator/join/lookup_join.h"
#include "type/data_types.h"
#include "operator/status.h"

namespace omniruntime {
namespace op {
class LookupJoinWithExprOperatorFactory : public OperatorFactory {
public:
    static LookupJoinWithExprOperatorFactory *CreateLookupJoinWithExprOperatorFactory(const DataTypes &probeTypes,
        int32_t *probeOutputCols, int32_t probeOutputColsCount,
        const std::vector<omniruntime::expressions::Expr *> &probeHashKeys, int32_t probeHashKeysCount,
        int32_t *buildOutputCols, const DataTypes &buildOutputTypes, JoinType joinType, int64_t hashBuilderFactoryAddr);

    LookupJoinWithExprOperatorFactory(const DataTypes &probeTypes, int32_t *probeOutputCols,
        int32_t probeOutputColsCount, const std::vector<omniruntime::expressions::Expr *> &probeHashKeys,
        int32_t probeHashKeysCount, int32_t *buildOutputCols, const DataTypes &buildOutputTypes, JoinType joinType,
        int64_t hashBuilderFactoryAddr);

    ~LookupJoinWithExprOperatorFactory() override;

    omniruntime::op::Operator *CreateOperator() override;

private:
    std::unique_ptr<DataTypes> probeTypes; // all types for probe
    std::vector<int32_t> probeHashCols;    // join columns for probe
    std::vector<std::unique_ptr<RowProjection>> rowProjections;
    std::vector<RowProjFunc> projectFuncs;
    LookupJoinOperatorFactory *operatorFactory;
};

class LookupJoinWithExprOperator : public Operator {
public:
    LookupJoinWithExprOperator(const type::DataTypes &probeTypes, std::vector<int32_t> &probeHashCols,
        std::vector<RowProjFunc> &projectFuncs, LookupJoinOperator *lookupJoinOperator);

    ~LookupJoinWithExprOperator() override;

    int32_t AddInput(omniruntime::vec::VectorBatch *vecBatch) override;

    int32_t GetOutput(std::vector<omniruntime::vec::VectorBatch *> &outputPages) override;

    OmniStatus Close() override;

private:
    DataTypes probeTypes;
    std::vector<int32_t> probeHashCols;
    std::vector<RowProjFunc> projectFuncs;
    LookupJoinOperator *lookupJoinOperator;
};
}
}


#endif // __LOOKUP_JOIN_EXPR_H__
