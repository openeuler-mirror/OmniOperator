/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: sort implementations
 */

#include "sort_expr.h"
#include "sort.h"
#include "../util/operator_util.h"
#include "../../vector/vector_helper.h"

using namespace std;
namespace omniruntime {
namespace op {
using namespace omniruntime::vec;

SortWithExprOperatorFactory *SortWithExprOperatorFactory::CreateSortWithExprOperatorFactory(
    const vec::VecTypes &sourceTypes, int32_t *outputCols, int32_t outputColsCount,
    const std::vector<omniruntime::expressions::Expr *> &sortKeys, int32_t *sortAscendings, int32_t *sortNullFirsts,
    int32_t sortKeysCount)
{
    auto pOperatorFactory = std::make_unique<SortWithExprOperatorFactory>(sourceTypes, outputCols, outputColsCount,
        sortKeys, sortAscendings, sortNullFirsts, sortKeysCount);
    return pOperatorFactory.release();
}

SortWithExprOperatorFactory::SortWithExprOperatorFactory(const vec::VecTypes &sourceTypes, int32_t *outputCols,
    int32_t outputColsCount, const std::vector<omniruntime::expressions::Expr *> &sortKeys, int32_t *sortAscendings,
    int32_t *sortNullFirsts, int32_t sortKeysCount)
{
    std::vector<VecType> newSourceTypes;
    OperatorUtil::CreateProjectFuncs(sourceTypes, sortKeys, sortKeysCount, newSourceTypes, this->rowProjections,
        this->sortCols, this->projectFuncs);
    this->sourceTypes = std::make_unique<VecTypes>(newSourceTypes);
    this->sortOperatorFactory = SortOperatorFactory::CreateSortOperatorFactory(*(this->sourceTypes.get()), outputCols,
        outputColsCount, sortCols.data(), sortAscendings, sortNullFirsts, sortKeysCount);
}

SortWithExprOperatorFactory::~SortWithExprOperatorFactory()
{
    delete sortOperatorFactory;
}

Operator *SortWithExprOperatorFactory::CreateOperator()
{
    auto sortOperator = static_cast<SortOperator *>(sortOperatorFactory->CreateOperator());
    auto pOperator = std::make_unique<SortWithExprOperator>(*(sourceTypes.get()), sortCols, projectFuncs, sortOperator);
    return pOperator.release();
}

SortWithExprOperator::SortWithExprOperator(const vec::VecTypes &sourceTypes, std::vector<int32_t> &sortCols,
    std::vector<RowProjFunc> &projectFuncs, SortOperator *sortOperator)
    : sourceTypes(sourceTypes), sortCols(sortCols), projectFuncs(projectFuncs), sortOperator(sortOperator)
{}

SortWithExprOperator::~SortWithExprOperator()
{
    delete sortOperator;
}

int32_t SortWithExprOperator::AddInput(VectorBatch *inputVecBatch)
{
    VectorBatch *newInputVecBatch = OperatorUtil::ProjectVectors(inputVecBatch, sourceTypes, projectFuncs, sortCols);
    if (newInputVecBatch != nullptr) {
        sortOperator->AddInput(newInputVecBatch);
        VectorHelper::FreeVecBatch(inputVecBatch);
    } else {
        sortOperator->AddInput(inputVecBatch);
    }
    return 0;
}

int32_t SortWithExprOperator::GetOutput(std::vector<VectorBatch *> &outputVecBatches)
{
    sortOperator->GetOutput(outputVecBatches);
    SetStatus(OMNI_STATUS_FINISHED);
    return 0;
}

OmniStatus SortWithExprOperator::Close()
{
    sortOperator->Close();
    return OMNI_STATUS_NORMAL;
}
}
}