/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#pragma once
#include "expression/expr_visitor.h"
#include "memory/allocator.h"
#include "util/config/QueryConfig.h"

namespace omniruntime::vectorization {
class ExprEval : public ExprVisitor {
public:
    ExprEval(vec::VectorBatch *vectorBatch, op::ExecutionContext *context);

    ~ExprEval() override {}

    void Visit(const expressions::LiteralExpr &e) override;

    void Visit(const expressions::FieldExpr &e) override;

    void Visit(const expressions::UnaryExpr &e) override;

    void Visit(const expressions::BinaryExpr &e) override;

    void Visit(const expressions::InExpr &e) override;

    void Visit(const expressions::BetweenExpr &e) override;

    void Visit(const expressions::IfExpr &e) override;

    void Visit(const expressions::CoalesceExpr &e) override;

    void Visit(const expressions::IsNullExpr &e) override;

    void Visit(const expressions::FuncExpr &e) override;

    void Visit(const expressions::SwitchExpr &e) override;

    void VisitExpr(const expressions::Expr &e);

    vec::BaseVector *GetResult();

    int32_t GetRowCount() const;

private:
    std::vector<type::DataTypeId> typeIds;
    op::ExecutionContext *context;
    std::vector<vec::BaseVector *> vecBatch_;
    std::stack<vec::BaseVector *> inputValues_;
    int32_t rowSize;
    mem::Allocator *allocator = mem::Allocator::GetAllocator();
    config::QueryConfig queryConfig;
};
}
