/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Expression code generator
 */
#ifndef OMNI_RUNTIME_ROW_EXPRESSION_CODEGEN_H
#define OMNI_RUNTIME_ROW_EXPRESSION_CODEGEN_H

#include "expression_codegen.h"

class RowExpressionCodeGen : public ExpressionCodeGen {
public:
    RowExpressionCodeGen(std::string name, omniruntime::expressions::Expr &expression)
    : ExpressionCodeGen(name, expression) {}
    ~RowExpressionCodeGen() {}
    void Visit(omniruntime::expressions::DataExpr &e) override;
    int64_t GetFunction() override;

private:
    llvm::Function* CreateFunction() override;
    bool InitializeCodegenContext(llvm::iterator_range<llvm::Function::arg_iterator> args);
};


#endif //OMNI_RUNTIME_ROW_EXPRESSION_CODEGEN_H
