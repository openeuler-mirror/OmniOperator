/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * @Description: sort implementations
 */

#ifndef __SORT_EXPR_H__
#define __SORT_EXPR_H__

#include <memory>
#include "operator/operator_factory.h"
#include "operator/projection/projection.h"
#include "operator/sort/sort.h"
#include "type/data_types.h"

namespace omniruntime {
namespace op {
class SortWithExprOperatorFactory : public OperatorFactory {
public:
    static SortWithExprOperatorFactory *CreateSortWithExprOperatorFactory(const type::DataTypes &sourceTypes,
        int32_t *outputCols, int32_t outputColsCount, const std::vector<omniruntime::expressions::Expr *> &sortKeys,
        int32_t *sortAscendings, int32_t *sortNullFirsts, int32_t sortKeysCount);

    static SortWithExprOperatorFactory *CreateSortWithExprOperatorFactory(const type::DataTypes &sourceTypes,
        int32_t *outputCols, int32_t outputColsCount, const std::vector<omniruntime::expressions::Expr *> &sortKeys,
        int32_t *sortAscendings, int32_t *sortNullFirsts, int32_t sortKeysCount, const OperatorConfig &operatorConfig);

    SortWithExprOperatorFactory(const type::DataTypes &sourceTypes, int32_t *outputCols, int32_t outputColsCount,
        const std::vector<omniruntime::expressions::Expr *> &sortKeys, int32_t *sortAscendings, int32_t *sortNullFirsts,
        int32_t sortKeysCount, const OperatorConfig &operatorConfig);

    ~SortWithExprOperatorFactory() override;

    Operator *CreateOperator() override;

private:
    std::unique_ptr<type::DataTypes> sourceTypes;
    std::vector<int32_t> sortCols;
    std::vector<std::unique_ptr<RowProjection>> rowProjections;
    std::vector<RowProjFunc> projectFuncs;
    SortOperatorFactory *sortOperatorFactory;
};

class SortWithExprOperator : public Operator {
public:
    SortWithExprOperator(const type::DataTypes &sourceTypes, std::vector<int32_t> &sortCols,
        std::vector<RowProjFunc> &projectFuncs, SortOperator *sortOperator);

    ~SortWithExprOperator() override;

    int32_t AddInput(omniruntime::vec::VectorBatch *inputVecBatch) override;

    int32_t GetOutput(std::vector<omniruntime::vec::VectorBatch *> &outputVecBatches) override;

    OmniStatus Close() override;

private:
    const omniruntime::type::DataTypes &sourceTypes;
    std::vector<int32_t> sortCols;
    std::vector<RowProjFunc> projectFuncs;
    SortOperator *sortOperator;
};
}
}
#endif // __SORT_EXPR_H__
