/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#include "ExprEval.h"

using namespace omniruntime::expressions;
using namespace omniruntime::mem;
using namespace omniruntime::vec;

void ExprEval::Visit(const LiteralExpr &e) {}

void ExprEval::Visit(const FieldExpr &e)
{
    inputValues_.push(vecBatch_[e.colVal]);
    vecBatch_[e.colVal]->SetIsField(true);
}

void ExprEval::Visit(const UnaryExpr &e) {}

void ExprEval::Visit(const BinaryExpr &e)
{
    e.left->Accept(*this);
    e.right->Accept(*this);

    auto result = VectorHelper::CreateFlatVector(e.dataType->GetId(), rowSize);
    e.vectorFunction->apply(inputValues_, e.dataType, result, context);
    inputValues_.push(result);
}

void ExprEval::Visit(const InExpr &e) {}

void ExprEval::Visit(const BetweenExpr &e) {}

void ExprEval::Visit(const IfExpr &e) {}

void ExprEval::Visit(const CoalesceExpr &e) {}

void ExprEval::Visit(const IsNullExpr &e) {}

void ExprEval::Visit(const FuncExpr &e)
{
    for (auto arg:e.arguments) {
        arg->Accept(*this);
    }

    auto result = VectorHelper::CreateFlatVector(e.dataType->GetId(), rowSize);
    e.vectorFunction->apply(inputValues_, e.dataType, result, context);
    inputValues_.push(result);
}

void ExprEval::Visit(const SwitchExpr &e) {}
