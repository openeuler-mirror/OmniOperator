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
RowProjection::RowProjection(const Expr &expression) : codegen(nullptr), expression(&expression) {}

RowProjection::~RowProjection()
{
    this->codegen.reset();
}

// Return nullptr if expression is unsupported
RowProjFunc RowProjection::Create(OverflowConfig *overflowConfig)
{
    if (this->expression == nullptr) {
        return nullptr;
    }
#ifdef DEBUG
    std::cout << "RowProjection: " << std::endl;
    ExprPrinter p;
    this->expression->Accept(p);
    std::cout << std::endl;
#endif
    this->codegen =
        std::make_unique<RowProjectionCodeGen>("single_row_project", *this->expression, false, overflowConfig);
    int64_t fPtr = this->codegen->GetExpressionEvaluator();
    if (fPtr == 0) {
        return nullptr;
    }

    void *refFunc = &fPtr;
    auto castedRef = static_cast<RowProjFunc *>(refFunc);
    return *castedRef;
}

// Return INVALIDDATAD if expression is unsupported
DataTypePtr RowProjection::GetReturnType()
{
    if (this->expression == nullptr) {
        return std::make_shared<InvalidDataType>();
    }
    return this->expression->GetReturnType();
}

bool RowProjection::IsColumnProjection()
{
    return this->expression != nullptr && this->expression->GetType() == ExprType::FIELD_E;
}

int RowProjection::GetIndexIfColumnProjection()
{
    if (!IsColumnProjection()) {
        return -1;
    }
    return static_cast<const FieldExpr *>(this->expression)->colVal;
}

int32_t ProjectionOperator::AddInput(VectorBatch *vecBatch)
{
    projectedVecs = this->exprEvaluator->Evaluate(vecBatch, this->context, this->vecAllocator);
    return 0;
}

int32_t ProjectionOperator::GetOutput(std::vector<VectorBatch *> &data)
{
    if (this->projectedVecs == nullptr) {
        return -1;
    }
    int rowCount = this->projectedVecs->GetRowCount();
    data.push_back(this->projectedVecs);
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