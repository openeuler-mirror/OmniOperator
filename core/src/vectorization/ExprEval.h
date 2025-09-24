/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#pragma once
#include <queue>
#include "expression/expr_visitor.h"
#include "memory/allocator.h"

class ExprEval : public ExprVisitor {
public:
    ExprEval(omniruntime::vec::VectorBatch *vectorBatch, omniruntime::op::ExecutionContext *context): context(context)
    {
        for (int i = 0; i < vectorBatch->GetVectorCount(); i++) {
            vecBatch_.push_back(vectorBatch->Get(i));
        }
        rowSize = vectorBatch->GetRowCount();
    }

    ~ExprEval() override {}

    void Visit(const omniruntime::expressions::LiteralExpr &e) override;

    void Visit(const omniruntime::expressions::FieldExpr &e) override;

    void Visit(const omniruntime::expressions::UnaryExpr &e) override;

    void Visit(const omniruntime::expressions::BinaryExpr &e) override;

    void Visit(const omniruntime::expressions::InExpr &e) override;

    void Visit(const omniruntime::expressions::BetweenExpr &e) override;

    void Visit(const omniruntime::expressions::IfExpr &e) override;

    void Visit(const omniruntime::expressions::CoalesceExpr &e) override;

    void Visit(const omniruntime::expressions::IsNullExpr &e) override;

    void Visit(const omniruntime::expressions::FuncExpr &e) override;

    void Visit(const omniruntime::expressions::SwitchExpr &e) override;

    omniruntime::vec::BaseVector *GetResult()
    {
        return inputValues_.top();
    }

    int32_t GetRowCount() const
    {
        return rowSize;
    }

private:
    omniruntime::op::ExecutionContext *context;
    std::vector<omniruntime::vec::BaseVector *> vecBatch_;
    std::stack<omniruntime::vec::BaseVector *> inputValues_;
    int32_t rowSize;
    omniruntime::mem::Allocator *allocator = omniruntime::mem::Allocator::GetAllocator();
};
