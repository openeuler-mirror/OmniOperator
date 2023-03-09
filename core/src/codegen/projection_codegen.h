/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: project  codegen
 */
#ifndef PROJECTION_CODEGEN_H
#define PROJECTION_CODEGEN_H

#include "expression_codegen.h"
#include "util/type_util.h"
#include "vector/vector_batch.h"

namespace omniruntime {
namespace codegen {
class ProjectionCodeGen : public ExpressionCodeGen {
public:
    /**
     * Method to initialize a ProjectionCodeGen instance
     * @param name ProjectionCodeGen module name
     * @param expr the projection expression to code generation
     * @param filter whether to support filter
     * @param overflowConfig config of overflow
     */
    ProjectionCodeGen(std::string name, const omniruntime::expressions::Expr &expr, bool filter,
        omniruntime::op::OverflowConfig *overflowConfig)
        : ExpressionCodeGen(std::move(name), expr, overflowConfig), filter(filter)
    {}

    ~ProjectionCodeGen() override = default;

    /**
     * Method to get function of processing projection expression
     * @param inputDataTypes is used to provide data type when preload data
     * @return the address of function
     */
    intptr_t GetFunction(const DataTypes &inputDataTypes) override;

private:
    /**
     * Method to generate function by using LLVM API which processes projection expression line by line
     * @return the address of function
     */
    intptr_t CreateWrapper();

    bool filter;
};
}
}

#endif