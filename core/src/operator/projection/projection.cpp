/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Projection operator source file
 */
#include "projection.h"
#include "expression/jsonparser/jsonparser.h"
#include "util/config/QueryConfig.h"
#include "util/config_util.h"
#include "vector/vector_helper.h"

using namespace omniruntime::vec;
using namespace omniruntime::expressions;
using namespace omniruntime::codegen;

namespace omniruntime {
namespace op {
int32_t ProjectionOperator::AddInput(VectorBatch *vecBatch)
{
    if (vecBatch->GetRowCount() > 0) {
        projectedVecs = this->exprEvaluator->Evaluate(vecBatch, executionContext.get());
    }
    VectorHelper::FreeVecBatch(vecBatch);
    ResetInputVecBatch();
    return 0;
}

OperatorFactory *CreateProjectOperatorFactory(
    std::shared_ptr<const ProjectNode> projectNode, const config::QueryConfig &queryConfig)
{
    auto projections = projectNode->GetProjections();
    auto sourceTypes = *(projectNode->Sources()[0]->OutputType());
    auto overflowConfig = queryConfig.IsOverFlowASNull() == true ? new OverflowConfig(OVERFLOW_CONFIG_NULL)
                                                                 : new OverflowConfig(OVERFLOW_CONFIG_EXCEPTION);
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(projections, sourceTypes, overflowConfig);
    return new ProjectionOperatorFactory(move(exprEvaluator));
}

int32_t ProjectionOperator::GetOutput(VectorBatch **outputVecBatch)
{
    if (this->projectedVecs == nullptr) {
        if (noMoreInput_) {
            SetStatus(OMNI_STATUS_FINISHED);
        }
        return -1;
    }
    int rowCount = this->projectedVecs->GetRowCount();
    *outputVecBatch = this->projectedVecs;
    this->projectedVecs = nullptr;
    return rowCount;
}

OmniStatus ProjectionOperator::Close()
{
    if (projectedVecs != nullptr) {
        VectorHelper::FreeVecBatch(projectedVecs);
        projectedVecs = nullptr;
    }
    return OMNI_STATUS_NORMAL;
}

omniruntime::op::Operator *ProjectionOperatorFactory::CreateOperator()
{
    return new ProjectionOperator(this->exprEvaluator);
}
} // namespace op
} // namespace omniruntime
