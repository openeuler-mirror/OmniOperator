/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: sort implementations
 */

#ifndef __LOOKUP_JOIN_EXPR_H__
#define __LOOKUP_JOIN_EXPR_H__

#include "../operator_factory.h"
#include "../projection/projection.h"
#include "../join/lookup_join.h"
#include "../../vector/vector_types.h"

namespace omniruntime {
namespace op {
class LookupJoinWithExprOperatorFactory : public OperatorFactory {
public:
    static LookupJoinWithExprOperatorFactory *CreateLookupJoinWithExprOperatorFactory(const vec::VecTypes &probeTypes,
        int32_t *probeOutputCols, int32_t probeOutputColsCount, std::string *probeHashKeys, int32_t probeHashKeysCount,
        int32_t *buildOutputCols, const vec::VecTypes &buildOutputTypes, JoinType joinType,
        int64_t hashBuilderFactoryAddr);

    LookupJoinWithExprOperatorFactory(const vec::VecTypes &probeTypes, int32_t *probeOutputCols,
        int32_t probeOutputColsCount, std::string *probeHashKeys, int32_t probeHashKeysCount, int32_t *buildOutputCols,
        const vec::VecTypes &buildOutputTypes, JoinType joinType, int64_t hashBuilderFactoryAddr);

    ~LookupJoinWithExprOperatorFactory() override;

    omniruntime::op::Operator *CreateOperator() override;

private:
    std::unique_ptr<vec::VecTypes> probeTypes; // all types for probe
    std::vector<int32_t> probeHashCols;        // join columns for probe
    std::vector<std::unique_ptr<RowProjection>> rowProjections;
    std::vector<RowProjFunc> projectFuncs;
    LookupJoinOperatorFactory *operatorFactory;
};

class LookupJoinWithExprOperator : public Operator {
public:
    LookupJoinWithExprOperator(const vec::VecTypes &probeTypes, std::vector<int32_t> &probeHashCols,
        std::vector<RowProjFunc> &projectFuncs, LookupJoinOperator *lookupJoinOperator);

    ~LookupJoinWithExprOperator() override;

    int32_t AddInput(omniruntime::vec::VectorBatch *vecBatch) override;

    int32_t GetOutput(std::vector<omniruntime::vec::VectorBatch *> &outputPages) override;

    OmniStatus Close() override;

private:
    const omniruntime::vec::VecTypes &probeTypes;
    std::vector<int32_t> probeHashCols;
    std::vector<RowProjFunc> projectFuncs;
    LookupJoinOperator *lookupJoinOperator;
    omniruntime::vec::VectorBatch *newInputVecBatch;
};
}
}


#endif // __LOOKUP_JOIN_EXPR_H__
