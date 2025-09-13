/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: visitor accept methods
 */
#include "expr_visitor.h"

using namespace omniruntime::expressions;

void LiteralExpr::Accept(ExprVisitor &visitor) const
{
    return visitor.Visit(*this);
}

void FieldExpr::Accept(ExprVisitor &visitor) const
{
    return visitor.Visit(*this);
}

void BinaryExpr::Accept(ExprVisitor &visitor) const
{
    return visitor.Visit(*this);
}

void InExpr::Accept(ExprVisitor &visitor) const
{
    return visitor.Visit(*this);
}

void BetweenExpr::Accept(ExprVisitor &visitor) const
{
    return visitor.Visit(*this);
}

void SwitchExpr::Accept(ExprVisitor &visitor) const
{
    return visitor.Visit(*this);
}

void IfExpr::Accept(ExprVisitor &visitor) const
{
    return visitor.Visit(*this);
}

void CoalesceExpr::Accept(ExprVisitor &visitor) const
{
    return visitor.Visit(*this);
}

void IsNullExpr::Accept(ExprVisitor &visitor) const
{
    return visitor.Visit(*this);
}

void FuncExpr::Accept(ExprVisitor &visitor) const
{
    return visitor.Visit(*this);
}

void UnaryExpr::Accept(ExprVisitor &visitor) const
{
    return visitor.Visit(*this);
}
