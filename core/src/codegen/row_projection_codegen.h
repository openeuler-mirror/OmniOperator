/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: row projection code generator
 */
#ifndef OMNI_RUNTIME_ROW_PROJECTION_CODEGEN_H
#define OMNI_RUNTIME_ROW_PROJECTION_CODEGEN_H

#include <utility>

#include "expression_codegen.h"
#include "util/type_util.h"

namespace omniruntime {
namespace codegen {
class RowProjectionCodeGen : public ExpressionCodeGen {
public:
    /* *
     * Method to initialize a RowProjectionCodeGen instance
     * @param name Name for RowProjectionCodeGen module
     * @param expr the expression
     * @param filter whether to support filter
     * @param overflowConfig
     */
    RowProjectionCodeGen(std::string name, const omniruntime::expressions::Expr &expr, bool filter,
        omniruntime::op::OverflowConfig *overflowConfig)
        : ExpressionCodeGen(std::move(name), expr, overflowConfig), filter(filter)
    {}

    ~RowProjectionCodeGen() override = default;

    intptr_t GetExpressionEvaluator();

    void Visit(const omniruntime::expressions::FieldExpr &e) override;

private:
    llvm::Function *CreateFunction();

    bool filter;
};
}
}


#endif
