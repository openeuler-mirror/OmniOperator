/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: Hash Aggregation WithExpr Header
 */

#ifndef GROUP_AGGREGATION_EXPR_H
#define GROUP_AGGREGATION_EXPR_H

#include "operator/operator_factory.h"
#include "operator/projection/projection.h"
#include "operator/aggregation/group_aggregation.h"
#include "type/data_types.h"

namespace omniruntime {
namespace op {
class HashAggregationWithExprOperatorFactory : public OperatorFactory {
public:
    HashAggregationWithExprOperatorFactory(const std::vector<omniruntime::expressions::Expr *> &groupByKeys,
        uint32_t groupByNum, const std::vector<omniruntime::expressions::Expr *> &aggKeys, uint32_t aggNum,
        ContainerDataTypePtr &sourceDataTypes, ContainerDataTypePtr &aggOutputTypes, uint32_t *aggFuncTypes,
        uint32_t *maskColumns, bool inputRaw, bool outputPartial);

    ~HashAggregationWithExprOperatorFactory() override;

    Operator *CreateOperator() override;

private:
    ContainerDataTypePtr sourceTypes;
    ContainerDataTypePtr groupByTypes;
    ContainerDataTypePtr aggTypes;
    std::vector<int32_t> projectCols;
    std::vector<std::unique_ptr<RowProjection>> rowProjections;
    std::vector<RowProjFunc> projectFuncs;
    HashAggregationOperatorFactory *hashAggOperatorFactory;
};

class HashAggregationWithExprOperator : public Operator {
public:
    HashAggregationWithExprOperator(ContainerDataTypePtr sourceTypes, std::vector<int32_t> &projectCols,
        std::vector<RowProjFunc> &projectFuncs, HashAggregationOperator *hashAggOperator);

    ~HashAggregationWithExprOperator() override;

    int32_t AddInput(omniruntime::vec::VectorBatch *inputVecBatch) override;

    int32_t GetOutput(std::vector<omniruntime::vec::VectorBatch *> &outputVecBatches) override;

    OmniStatus Close() override;

private:
    ContainerDataTypePtr sourceTypes;
    std::vector<int32_t> projectCols;
    std::vector<RowProjFunc> projectFuncs;
    HashAggregationOperator *hashAggOperator;
};
}
}
#endif // GROUP_AGGREGATION_EXPR_H
