/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Projection operator source file
 */
#include "projection.h"
#include "vector/vector_helper.h"
#include "expression/jsonparser/jsonparser.h"
#include "util/config_util.h"

using namespace omniruntime::vec;
using namespace omniruntime::expressions;
using namespace omniruntime::codegen;

namespace omniruntime {
namespace op {
int32_t ProjectionOperator::AddInput(VectorBatch *vecBatch)
{
    projectedVecs = this->exprEvaluator->Evaluate(vecBatch, this->context);
    return 0;
}

int32_t ProjectionOperator::GetOutput(VectorBatch **outputVecBatch)
{
    if (this->projectedVecs == nullptr) {
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
    return new ProjectionOperator(new ExecutionContext(), this->exprEvaluator);
}
}
}