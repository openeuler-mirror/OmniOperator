/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: filter code generation methods
 */
#ifndef FILTER_CODEGEN_H
#define FILTER_CODEGEN_H

#include <utility>
#include "expression_codegen.h"

class FilterCodeGen : public ExpressionCodeGen {
public:
    /**
     * Method to create and initialize a FilterCodeGen instance
     *
     * @param name Name for FilterCodeGen module
     * @param expression the filter expression
     * @return unique_ptr to the FilterCodeGen instance
     */
    static std::unique_ptr<FilterCodeGen> Create(std::string name, const omniruntime::expressions::Expr &expression);

    ~FilterCodeGen() override = default;

    int64_t GetFunction() override;

    int64_t GetExpressionEvaluator();

private:
    FilterCodeGen(std::string name, const omniruntime::expressions::Expr &expression)
        : ExpressionCodeGen(std::move(name), expression) {}
    int64_t CreateWrapper(llvm::Function &filter);
};
#endif