/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: visitor class for expressions
 */
#include "expressions.h"

class ExprVisitor {
public:
    virtual ~ExprVisitor() = default;
    virtual void Visit(omniruntime::expressions::DataExpr &e) = 0;
    virtual void Visit(omniruntime::expressions::UnaryExpr &e) = 0;        
    virtual void Visit(omniruntime::expressions::BinaryExpr &e) = 0;
    virtual void Visit(omniruntime::expressions::InExpr &e) = 0;
    virtual void Visit(omniruntime::expressions::BetweenExpr &e) = 0;
    virtual void Visit(omniruntime::expressions::IfExpr &e) = 0;
    virtual void Visit(omniruntime::expressions::CoalesceExpr &e) = 0;
    virtual void Visit(omniruntime::expressions::IsNullExpr &e) = 0;
    virtual void Visit(omniruntime::expressions::FuncExpr &e) = 0;
};
