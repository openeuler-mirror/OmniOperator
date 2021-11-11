/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: Hash Aggregation WithExpr Header
 */

#ifndef GROUP_AGGREGATION_EXPR_H
#define GROUP_AGGREGATION_EXPR_H

#include "../operator_factory.h"
#include "../projection/projection.h"
#include "../aggregation/group_aggregation.h"
#include "../../vector/vector_types.h"

namespace omniruntime {
namespace op {
class HashAggregationWithExprOperatorFactory : public OperatorFactory {
public:
    HashAggregationWithExprOperatorFactory(std::string *groupByKeys, uint32_t groupByNum, std::string *aggKeys,
        uint32_t aggNum, const VecTypes& sourceVecTypes, const VecTypes& aggOutputTypes, uint32_t *aggFuncTypes,
        bool inputRaw, bool outputPartial);

    ~HashAggregationWithExprOperatorFactory() override;

    Operator *CreateOperator() override;

private:
    std::unique_ptr<vec::VecTypes> sourceTypes;
    std::unique_ptr<vec::VecTypes> groupByTypes;
    std::unique_ptr<vec::VecTypes> aggTypes;
    std::vector<int32_t> projectCols;
    std::vector<std::unique_ptr<RowProjection>> rowProjections;
    std::vector<RowProjFunc> projectFuncs;
    HashAggregationOperatorFactory *hashAggOperatorFactory;
};

class HashAggregationWithExprOperator : public Operator {
public:
    HashAggregationWithExprOperator(const vec::VecTypes &sourceTypes, std::vector<int32_t> &projectCols,
        std::vector<RowProjFunc> &projectFuncs, HashAggregationOperator *hashAggOperator);

    ~HashAggregationWithExprOperator() override;

    int32_t AddInput(omniruntime::vec::VectorBatch *inputVecBatch) override;

    int32_t GetOutput(std::vector<omniruntime::vec::VectorBatch *> &outputVecBatches) override;

    OmniStatus Close() override;

private:
    const omniruntime::vec::VecTypes &sourceTypes;
    std::vector<int32_t> projectCols;
    std::vector<RowProjFunc> projectFuncs;
    HashAggregationOperator *hashAggOperator;
    std::vector<VectorBatch *> inputVecBatches;
};
}
}
#endif // GROUP_AGGREGATION_EXPR_H
