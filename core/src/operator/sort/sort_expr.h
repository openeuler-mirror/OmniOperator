/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: sort implementations
 */

#ifndef __SORT_EXPR_H__
#define __SORT_EXPR_H__

#include <memory>
#include "../operator_factory.h"
#include "../projection/projection.h"
#include "../sort/sort.h"
#include "../../vector/vector_types.h"

namespace omniruntime {
namespace op {
class SortWithExprOperatorFactory : public OperatorFactory {
public:
    static SortWithExprOperatorFactory *CreateSortWithExprOperatorFactory(const vec::VecTypes &sourceTypes,
        int32_t *outputCols, int32_t outputColsCount, std::string *sortKeys, int32_t *sortAscendings,
        int32_t *sortNullFirsts, int32_t sortKeysCount);

    SortWithExprOperatorFactory(const vec::VecTypes &sourceTypes, int32_t *outputCols, int32_t outputColsCount,
        std::string *sortKeys, int32_t *sortAscendings, int32_t *sortNullFirsts, int32_t sortKeysCount);

    ~SortWithExprOperatorFactory() override;

    Operator *CreateOperator() override;

private:
    std::unique_ptr<vec::VecTypes> sourceTypes;
    std::vector<int32_t> sortCols;
    std::vector<std::unique_ptr<RowProjection>> rowProjections;
    std::vector<RowProjFunc> projectFuncs;
    SortOperatorFactory *sortOperatorFactory;
};

class SortWithExprOperator : public Operator {
public:
    SortWithExprOperator(const vec::VecTypes &sourceTypes, std::vector<int32_t> &sortCols,
        std::vector<RowProjFunc> &projectFuncs, SortOperator *sortOperator);

    ~SortWithExprOperator() override;

    int32_t AddInput(omniruntime::vec::VectorBatch *inputVecBatch) override;

    int32_t GetOutput(std::vector<omniruntime::vec::VectorBatch *> &outputVecBatches) override;

    OmniStatus Close() override;

private:
    const omniruntime::vec::VecTypes &sourceTypes;
    std::vector<int32_t> sortCols;
    std::vector<RowProjFunc> projectFuncs;
    SortOperator *sortOperator;
    std::vector<VectorBatch *> inputVecBatches;
};
}
}
#endif // __SORT_EXPR_H__
