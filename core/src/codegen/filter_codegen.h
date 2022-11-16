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
    static std::unique_ptr<FilterCodeGen> Create(std::string name, const omniruntime::expressions::Expr &expression,
        omniruntime::op::OverflowConfig *overflowConfig);

    ~FilterCodeGen() override = default;

    int64_t GetFunction() override;

private:
    FilterCodeGen(std::string name, const omniruntime::expressions::Expr &expression,
        omniruntime::op::OverflowConfig *overflowConfig)
        : ExpressionCodeGen(std::move(name), expression, overflowConfig)
    {}
    int64_t CreateWrapper(llvm::Function &filter);
};
#endif