/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: filter code generation methods
 */
#ifndef FILTER_CODEGEN_H
#define FILTER_CODEGEN_H

#include "expression_codegen.h"

namespace omniruntime {
namespace codegen {
class FilterCodeGen : public ExpressionCodeGen {
public:
    /**
     * Method to initialize a FilterCodeGen instance
     * @param name FilterCodeGen module name
     * @param expression the filter expression to code generation
     * @param overflowConfig config of overflow
     */
    FilterCodeGen(std::string name, const omniruntime::expressions::Expr &expression,
        omniruntime::op::OverflowConfig *overflowConfig)
        : ExpressionCodeGen(std::move(name), expression, overflowConfig)
    {}

    ~FilterCodeGen() override = default;

    /**
     * Method to get function of processing filter expression
     * @param inputDataTypes is used to provide data type when preload data
     * @return the address of function
     */
    intptr_t GetFunction(const DataTypes &inputDataTypes) override;

private:
    /**
     * Method to generate function by using LLVM API which processes filter expression line by line
     * @return the address of function
     */
    intptr_t CreateWrapper();
};
}
}
#endif