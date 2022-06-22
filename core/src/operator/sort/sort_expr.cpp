/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * @Description: sort implementations
 */

#include "sort_expr.h"
#include "sort.h"
#include "operator/util/operator_util.h"
#include "vector/vector_helper.h"

using namespace std;
namespace omniruntime {
namespace op {
using namespace omniruntime::vec;

SortWithExprOperatorFactory *SortWithExprOperatorFactory::CreateSortWithExprOperatorFactory(
    const type::DataTypes &sourceTypes, int32_t *outputCols, int32_t outputColsCount,
    const std::vector<omniruntime::expressions::Expr *> &sortKeys, int32_t *sortAscendings, int32_t *sortNullFirsts,
    int32_t sortKeysCount)
{
    OperatorConfig defaultConfig;
    auto pOperatorFactory = new SortWithExprOperatorFactory(sourceTypes, outputCols, outputColsCount, sortKeys,
        sortAscendings, sortNullFirsts, sortKeysCount, defaultConfig);
    return pOperatorFactory;
}

SortWithExprOperatorFactory *SortWithExprOperatorFactory::CreateSortWithExprOperatorFactory(
    const type::DataTypes &sourceTypes, int32_t *outputCols, int32_t outputColsCount,
    const std::vector<omniruntime::expressions::Expr *> &sortKeys, int32_t *sortAscendings, int32_t *sortNullFirsts,
    int32_t sortKeysCount, const OperatorConfig &operatorConfig)
{
    auto pOperatorFactory = new SortWithExprOperatorFactory(sourceTypes, outputCols, outputColsCount, sortKeys,
        sortAscendings, sortNullFirsts, sortKeysCount, operatorConfig);
    return pOperatorFactory;
}

SortWithExprOperatorFactory::SortWithExprOperatorFactory(const type::DataTypes &sourceTypes, int32_t *outputCols,
    int32_t outputColsCount, const std::vector<omniruntime::expressions::Expr *> &sortKeys, int32_t *sortAscendings,
    int32_t *sortNullFirsts, int32_t sortKeysCount, const OperatorConfig &operatorConfig)
{
    std::vector<DataTypeRawPtr> newSourceTypes;
    OperatorUtil::CreateProjectFuncs(sourceTypes, sortKeys, sortKeysCount, newSourceTypes, this->rowProjections,
        this->sortCols, this->projectFuncs);
    this->sourceTypes = std::make_unique<DataTypes>(newSourceTypes);
    this->sortOperatorFactory = SortOperatorFactory::CreateSortOperatorFactory(*(this->sourceTypes.get()), outputCols,
        outputColsCount, sortCols.data(), sortAscendings, sortNullFirsts, sortKeysCount, operatorConfig);
}

SortWithExprOperatorFactory::~SortWithExprOperatorFactory()
{
    delete sortOperatorFactory;
}

Operator *SortWithExprOperatorFactory::CreateOperator()
{
    auto sortOperator = static_cast<SortOperator *>(sortOperatorFactory->CreateOperator());
    auto pOperator = new SortWithExprOperator(*(sourceTypes.get()), sortCols, projectFuncs, sortOperator);
    return pOperator;
}

SortWithExprOperator::SortWithExprOperator(const type::DataTypes &sourceTypes, std::vector<int32_t> &sortCols,
    std::vector<RowProjFunc> &projectFuncs, SortOperator *sortOperator)
    : sourceTypes(sourceTypes), sortCols(sortCols), projectFuncs(projectFuncs), sortOperator(sortOperator)
{}

SortWithExprOperator::~SortWithExprOperator()
{
    delete sortOperator;
}

int32_t SortWithExprOperator::AddInput(VectorBatch *inputVecBatch)
{
    VectorBatch *newInputVecBatch =
        OperatorUtil::ProjectVectors(inputVecBatch, sourceTypes, projectFuncs, sortCols, vecAllocator);
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