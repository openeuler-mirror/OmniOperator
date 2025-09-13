/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: simple filter code generator
 */
#ifndef OMNI_RUNTIME_SIMPLE_FILTER_CODEGEN_H
#define OMNI_RUNTIME_SIMPLE_FILTER_CODEGEN_H

#include <utility>

#include "expression_codegen.h"
#include "util/type_util.h"

namespace omniruntime {
namespace codegen {
class SimpleFilterCodeGen : public ExpressionCodeGen {
public:
    /* *
     * Method to initialize a SimpleFilterCodeGen instance
     * @param name Name for SimpleFilterCodeGen module
     * @param expression the expression
     * @param overflowConfig
     */
    SimpleFilterCodeGen(std::string name, const omniruntime::expressions::Expr &expression,
        omniruntime::op::OverflowConfig *overflowConfig)
        : ExpressionCodeGen(std::move(name), expression, overflowConfig)
    {
        this->ExtractVectorIndexes();
    }

    ~SimpleFilterCodeGen() override = default;

    intptr_t GetFunction();

    llvm::Function *CreateFunction();

    void Visit(const omniruntime::expressions::FieldExpr &e) override;

private:
    bool InitCodegenContext(llvm::iterator_range<llvm::Function::arg_iterator> args);
};
}
}

#endif
