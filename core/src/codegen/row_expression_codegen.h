/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Expression code generator
 */
#ifndef OMNI_RUNTIME_ROW_EXPRESSION_CODEGEN_H
#define OMNI_RUNTIME_ROW_EXPRESSION_CODEGEN_H

#include "expression_codegen.h"

#include <utility>

class RowExpressionCodeGen : public ExpressionCodeGen {
public:
    /**
     * Method to create and initialize a RowExpressionCodeGen instance
     *
     * @param name Name for RowExpressionCodeGen module
     * @param expression the expression
     * @return unique_ptr to the RowExpressionCodeGen instance
     */
    static std::unique_ptr<RowExpressionCodeGen> Create(
        std::string name, const omniruntime::expressions::Expr &expression);

    ~RowExpressionCodeGen() override = default;

    void Visit(const omniruntime::expressions::DataExpr &e) override;
    int64_t GetFunction() override;

private:
    RowExpressionCodeGen(std::string name, const omniruntime::expressions::Expr &expression)
        : ExpressionCodeGen(std::move(name), expression) {}
    llvm::Function* CreateFunction() override;
    bool InitializeCodegenContext(llvm::iterator_range<llvm::Function::arg_iterator> args);
};


#endif //OMNI_RUNTIME_ROW_EXPRESSION_CODEGEN_H
