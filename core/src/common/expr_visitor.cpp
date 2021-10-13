/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: visitor accept methods
 */
#include "expr_visitor.h"

using namespace omniruntime::expressions;

void DataExpr::Accept(ExprVisitor &visitor) 
{ 
    return visitor.Visit(*this); 
}

void BinaryExpr::Accept(ExprVisitor &visitor) 
{ 
    return visitor.Visit(*this); 
}

void InExpr::Accept(ExprVisitor &visitor) 
{ 
    return visitor.Visit(*this); 
}

void BetweenExpr::Accept(ExprVisitor &visitor) 
{ 
    return visitor.Visit(*this); 
}

void IfExpr::Accept(ExprVisitor &visitor) 
{ 
    return visitor.Visit(*this); 
}

void CoalesceExpr::Accept(ExprVisitor &visitor) 
{ 
    return visitor.Visit(*this); 
}

void IsNullExpr::Accept(ExprVisitor &visitor) 
{ 
    return visitor.Visit(*this); 
}

void FuncExpr::Accept(ExprVisitor &visitor) 
{ 
    return visitor.Visit(*this); 
}

void UnaryExpr::Accept(ExprVisitor &visitor) 
{ 
    return visitor.Visit(*this); 
}
