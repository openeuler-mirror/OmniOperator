
/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: print expression tree visitor for expressions
 */
#include "expr_visitor.h"
#include "expressions.h"

class ExprPrinter : public ExprVisitor {
public:
    void Visit(omniruntime::expressions::DataExpr &e) override;
    void Visit(omniruntime::expressions::UnaryExpr &e) override;        
    void Visit(omniruntime::expressions::BinaryExpr &e) override;
    void Visit(omniruntime::expressions::InExpr &e) override;
    void Visit(omniruntime::expressions::BetweenExpr &e) override;
    void Visit(omniruntime::expressions::IfExpr &e) override;
    void Visit(omniruntime::expressions::CoalesceExpr &e) override;
    void Visit(omniruntime::expressions::IsNullExpr &e) override;
    void Visit(omniruntime::expressions::FuncExpr &e) override;
private:
    std::string BinaryExprPrinterHelper(const omniruntime::expressions::Operator &op) const;
    std::string GenerateIndentation() const;
    int indentationDepth = 0;
};
